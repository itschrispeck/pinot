/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.io.writer.impl;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MutableCachingOffHeapByteArrayStore provides similar functionality as MutableOffHeapByteArrayStore, but inserted
 * values are cached before being flushed to the underlying store. This can reduce the number of writes to disk,
 * improving performance in disk bound scenarios. This class provides the same guarantees as the original
 * implementation, namely, for a single writer and multiple readers this class is thread-safe.
 * <p>
 * For a detailed explanation of the flushed data layout and expansion mechanism, see the comments in the original
 * implementation {@link MutableOffHeapByteArrayStore}.
 * <p>
 * This class adds a caching layer. The cache is a byte[] array that stores the inserted values, appended from
 * the end. An int[] array tracks the offsets, and is sized based on the average value length to avoid excessive
 * memory usage. When the cache cannot hold any new values, or the offset array cannot hold any new values, then the
 * values are flushed to the underlying store as a batch.
 * <p>
 * Externally, reads have the same guarantees as the original implementation. Internally, they require some locking
 * to ensure for some index the value is read from the right place (cache, or flushed buffer).
 */
public class MutableCachingOffHeapByteArrayStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(MutableCachingOffHeapByteArrayStore.class);

  private static class Buffer implements Closeable {
    private final PinotDataBuffer _pinotDataBuffer;
    private final int _startIndex;
    private final int _size;

    private volatile int _numValues = 0;
    private int _availEndOffset;  // Exclusive

    private Buffer(int size, int startIndex, PinotDataBufferMemoryManager memoryManager, String allocationContext) {
      LOGGER.info("Allocating byte array store buffer of size {} for: {}", size, allocationContext);
      _pinotDataBuffer = memoryManager.allocate(size, allocationContext);
      _startIndex = startIndex;
      _size = size;
      _availEndOffset = size;
    }

    private int add(byte[] value) {
      int startOffset = _availEndOffset - value.length;
      if (startOffset < (_numValues + 1) * Integer.BYTES) {
        // full
        return -1;
      }
      _pinotDataBuffer.readFrom(startOffset, value);
      _pinotDataBuffer.putInt(_numValues * Integer.BYTES, startOffset);
      _availEndOffset = startOffset;
      return _numValues++;
    }

    private boolean equalsValueAt(byte[] value, int index) {
      int startOffset = _pinotDataBuffer.getInt(index * Integer.BYTES);
      int endOffset;
      if (index != 0) {
        endOffset = _pinotDataBuffer.getInt((index - 1) * Integer.BYTES);
      } else {
        endOffset = _size;
      }
      if ((endOffset - startOffset) != value.length) {
        return false;
      }
      for (int i = 0, j = startOffset; i < value.length; i++, j++) {
        if (value[i] != _pinotDataBuffer.getByte(j)) {
          return false;
        }
      }
      return true;
    }

    private byte[] get(int index) {
      int startOffset = _pinotDataBuffer.getInt(index * Integer.BYTES);
      int endOffset;
      if (index != 0) {
        endOffset = _pinotDataBuffer.getInt((index - 1) * Integer.BYTES);
      } else {
        endOffset = _size;
      }
      byte[] value = new byte[endOffset - startOffset];
      _pinotDataBuffer.copyTo(startOffset, value);
      return value;
    }

    private int getSize() {
      return _size;
    }
    private int getAvailEndOffset() {
      return _availEndOffset;
    }

    private int getNumValues() {
      return _numValues;
    }

    private int getStartIndex() {
      return _startIndex;
    }

    @Override
    public void close()
        throws IOException {
      // NOTE: DO NOT close the PinotDataBuffer here because it is tracked in the PinotDataBufferMemoryManager.
    }
  }

  private volatile List<MutableCachingOffHeapByteArrayStore.Buffer> _buffers = new LinkedList<>();
  private volatile int _numElements = 0; // volatile required for CachingMutableCachingOffHeapByteArrayStore
  private volatile MutableCachingOffHeapByteArrayStore.Buffer _currentBuffer;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;
  private long _totalStringSize = 0;
  private final int _startSize;
  // default cache capacity
  private static final int DEFAULT_CACHE_SIZE = 1024 * 1024; // 1MB

  // offset list capacity relative to cache size, e.g. 1MB / 16 = 64KB. This provides an upper bound on the amount of
  // memory required to track offsets of values in the cache. The actual offset array length will be less if the
  // average value length is greater than 16 bytes
  private static final int MIN_OFFSET_LIST_SIZE_FACTOR = 16;

  private final int _cacheSize; // cache capacity in bytes
  private final byte[] _cache; // cache inserted values in a single buffer
  private final int[] _cacheOffsets; // holds the offsets of values stored in _cacheBuffer
  private int _cacheBufferEndOffset; // end offset of the next value to be inserted into the cache (exclusive)
  private final AtomicInteger _cacheSizeBytes = new AtomicInteger();
  private final AtomicInteger _cacheSizeNumValues = new AtomicInteger();

  private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();


  @VisibleForTesting
  public static int getStartSize(int numArrays, int avgArrayLen) {
    // For each array, we store the array and its startoffset (4 bytes)
    long estimatedSize = numArrays * ((long) avgArrayLen + 4);
    if (estimatedSize > 0 && estimatedSize <= Integer.MAX_VALUE) {
      return (int) estimatedSize;
    }
    return Integer.MAX_VALUE;
  }

  public MutableCachingOffHeapByteArrayStore(PinotDataBufferMemoryManager memoryManager, String allocationContext,
      int numArrays, int avgArrayLen) {
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    _startSize = getStartSize(numArrays, avgArrayLen);
    expand(_startSize);

    _cacheSize = DEFAULT_CACHE_SIZE;
    _cache = new byte[_cacheSize];
    int offsetListSizeFactor = Math.min(MIN_OFFSET_LIST_SIZE_FACTOR, avgArrayLen);
    _cacheOffsets = new int[_cacheSize / offsetListSizeFactor];
    _cacheBufferEndOffset = _cacheSize;
  }

  /**
   * Expand the buffer list to add a new buffer, allocating a buffer that can definitely fit
   * the new value.
   *
   * @param size Size of the expanded buffer
   * @return Expanded buffer
   */
  private MutableCachingOffHeapByteArrayStore.Buffer expand(int size) {
    MutableCachingOffHeapByteArrayStore.Buffer
        buffer = new MutableCachingOffHeapByteArrayStore.Buffer(size, _numElements, _memoryManager, _allocationContext);
    List<MutableCachingOffHeapByteArrayStore.Buffer> newList = new LinkedList<>(_buffers);
    newList.add(buffer);
    _buffers = newList;
    _currentBuffer = buffer;
    return buffer;
  }

  public byte[] get(int index) {
    // check the current buffer first, if index is not in the current buffer, check the cache
    MutableCachingOffHeapByteArrayStore.Buffer curr = _currentBuffer;

    // Adding the value updates buffer state, lock to ensure reads don't read an inconsistent state
    _lock.readLock().lock();
    try {
      if (index >= (curr.getStartIndex() + curr.getNumValues())) {
        int cacheIndex = index - _numElements;
        return getCachedElement(cacheIndex);
      }
    } finally {
      _lock.readLock().unlock();
    }

    // otherwise, check buffers as usual
    return nonCachedGet(index);
  }

  public boolean equalsValueAt(byte[] value, int index) {
    // check the current buffer first, if index is not in the current buffer, check the cache
    MutableCachingOffHeapByteArrayStore.Buffer curr = _currentBuffer;
    _lock.readLock().lock();
    try {
      if (index >= (curr.getStartIndex() + curr.getNumValues())) {
        int cacheIndex = index - _numElements;
        if (cacheIndex < 0) {
          return false;
        }
        return Arrays.equals(value, getCachedElement(cacheIndex));
      }
    } finally {
      _lock.readLock().unlock();
    }

    // otherwise, check buffers as usual
    return nonCachedEqualsValueAt(value, index);
  }

  public int add(byte[] value) {
    if (value.length > _cacheSize) {
      flush();                  // flush to ensure the ordering is correct
      return nonCachedAdd(value);  // cache is empty, fallback to direct write
    }

    // flush if (1) the value cannot fit in the cache, or (2) if the cache offset array is full
    if (value.length + _cacheSizeBytes.get() > _cacheSize || _cacheSizeNumValues.get() == _cacheOffsets.length) {
      flush();
    }

    // check if the value and existing cached values can all fit in the current buffer
    MutableCachingOffHeapByteArrayStore.Buffer buffer = _currentBuffer;
    int totalLength = _cacheSizeBytes.get() + value.length;
    int totalValues = _cacheSizeNumValues.get() + 1;
    int startOffset = buffer.getAvailEndOffset() - totalLength;
    if (startOffset < (buffer.getNumValues() + totalValues) * Integer.BYTES) {
      // all values cannot fit, since maximal packing isn't necessary we will simply expand the buffer
      // the additional waste is limited to the size of the cache i.e. _cacheSize
      int currentBufferSize = buffer.getSize();
      if ((currentBufferSize << 1) >= 0) {
        // The expanded buffer size should be enough for the current value
        expand(Math.max(currentBufferSize << 1, totalLength + Integer.BYTES));
      } else {
        // Int overflow
        expand(Integer.MAX_VALUE);
      }
    }

    // value and existing cached values can fit, so simply copy the value into the cache
    System.arraycopy(value, 0, _cache, _cacheBufferEndOffset - value.length, value.length);
    _cacheBufferEndOffset -= value.length;
    _cacheOffsets[_cacheSizeNumValues.get()] = _cacheBufferEndOffset;
    _totalStringSize += value.length;
    _cacheSizeBytes.addAndGet(value.length);
    _cacheSizeNumValues.incrementAndGet();

    // return index of the latest added value
    return _numElements + _cacheSizeNumValues.get() - 1;
  }

  /**
   * Flush the cache to the buffer. This method assumes that the cache can fit in the current buffer.
   * <p>
   * The implementation of add() ensures the cache can fit in the current buffer before adding to the cache. No other
   * method will add to the cache, or fill the buffer, therefore there is no risk of the cache not fitting.
   */
  private void flush() {
    int totalValues = _cacheSizeNumValues.get();
    if (totalValues == 0) {
      return; // nothing to flush
    }
    MutableCachingOffHeapByteArrayStore.Buffer buffer = _currentBuffer;
    int totalLength = _cacheSizeBytes.get();

    // save the values
    int joinedStartOffset = buffer.getAvailEndOffset() - totalLength;
    ByteBuffer cacheBuffer = ByteBuffer.wrap(_cache, _cacheBufferEndOffset, totalLength);
    buffer._pinotDataBuffer.readFrom(joinedStartOffset, cacheBuffer);

    // save the offsets, convert the cache offsets to buffer offsets
    int[] bufferOffsets = new int[_cacheSizeNumValues.get()];
    bufferOffsets[0] = buffer.getAvailEndOffset() - getCachedElementLength(0);
    for (int i = 1; i < _cacheSizeNumValues.get(); i++) {
      bufferOffsets[i] = bufferOffsets[i - 1] - getCachedElementLength(i);
    }
    ByteBuffer buf = ByteBuffer.allocate(_cacheSizeNumValues.get() * 4);
    buf.order(ByteOrder.nativeOrder()).asIntBuffer().put(bufferOffsets);
    buffer._pinotDataBuffer.readFrom((long) buffer.getNumValues() * Integer.BYTES, buf.array());

    // update buffer state
    _lock.writeLock().lock();
    try {
      buffer._availEndOffset = joinedStartOffset;
      buffer._numValues += totalValues;
      _numElements += totalValues;
    } finally {
      _lock.writeLock().unlock();
    }

    // clear the buffer
    _cacheBufferEndOffset = _cacheSize;
    _cacheSizeBytes.set(0);
    _cacheSizeNumValues.set(0);
  }

  private byte[] getCachedElement(int index) {
    int valueLength = getCachedElementLength(index);
    byte[] val = new byte[valueLength];
    System.arraycopy(_cache, _cacheOffsets[index], val, 0, valueLength);

    return val;
  }

  /**
   * Get the length of the cached element at the given index. Unsafe if the index is out of bounds.
   */
  private int getCachedElementLength(int index) {
    if (index == 0) {
      return _cacheSize - _cacheOffsets[0];
    }
    return _cacheOffsets[index - 1] - _cacheOffsets[index];
  }

  // Copied from {@link MutableOffHeapByteArrayStore#get}
  public byte[] nonCachedGet(int index) {
    List<MutableCachingOffHeapByteArrayStore.Buffer> bufList = _buffers;
    for (int x = bufList.size() - 1; x >= 0; x--) {
      MutableCachingOffHeapByteArrayStore.Buffer buffer = bufList.get(x);
      if (index >= buffer.getStartIndex()) {
        return buffer.get(index - buffer.getStartIndex());
      }
    }
    // Assumed that we will never ask for an index that does not exist.
    throw new RuntimeException("dictionary ID '" + index + "' too low");
  }

  /**
   * Copied from {@link MutableOffHeapByteArrayStore#add}
   */
  public int nonCachedAdd(byte[] value) {
    int valueLength = value.length;
    MutableCachingOffHeapByteArrayStore.Buffer buffer = _currentBuffer;

    // Adding the value updates buffer state, lock to ensure reads don't read an inconsistent state
    _lock.writeLock().lock();
    try {
      int index = buffer.add(value);
      if (index < 0) {
        // Need to expand the buffer
        int currentBufferSize = buffer.getSize();
        if ((currentBufferSize << 1) >= 0) {
          // The expanded buffer size should be enough for the current value
          buffer = expand(Math.max(currentBufferSize << 1, valueLength + Integer.BYTES));
        } else {
          // Int overflow
          buffer = expand(Integer.MAX_VALUE);
        }
        buffer.add(value);
      }
      _totalStringSize += valueLength;
      _numElements++;
    } finally {
      _lock.writeLock().unlock();
    }
    return _numElements - 1;
  }

  /**
   * Copied from {@link MutableOffHeapByteArrayStore#equalsValueAt}
   */
  public boolean nonCachedEqualsValueAt(byte[] value, int index) {
    List<MutableCachingOffHeapByteArrayStore.Buffer> bufList = _buffers;
    for (int x = bufList.size() - 1; x >= 0; x--) {
      MutableCachingOffHeapByteArrayStore.Buffer buffer = bufList.get(x);
      if (index >= buffer.getStartIndex()) {
        return buffer.equalsValueAt(value, index - buffer.getStartIndex());
      }
    }
    throw new RuntimeException("dictionary ID '" + index + "' too low");
  }

  public void close()
      throws IOException {
    for (MutableCachingOffHeapByteArrayStore.Buffer buffer : _buffers) {
      buffer.close();
    }
  }

  public long getTotalOffHeapMemUsed() {
    long ret = 0;
    for (MutableCachingOffHeapByteArrayStore.Buffer buffer : _buffers) {
      ret += buffer.getSize();
    }
    return ret;
  }

  public long getAvgValueSize() {
    if (_numElements > 0) {
      return _totalStringSize / _numElements;
    }
    return 0;
  }
}
