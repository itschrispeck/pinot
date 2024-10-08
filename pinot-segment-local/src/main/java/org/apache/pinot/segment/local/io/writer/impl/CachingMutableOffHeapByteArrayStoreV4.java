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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;


/**
 * CachingMutableOffHeapByteArrayStoreV2 is a subclass of MutableOffHeapByteArrayStore that caches inserts up to a
 * configured size. When the limit is reached, the cache is flushed to the underlying store as a batch. This can
 * greatly reduce the number of writes to disk, for example when dictionaries are high cardinality.
 * <p>
 * This class provides the same interface as MutableOffHeapByteArrayStore and the same guarantees - i.e. it is
 * thread-safe for a single writer and multiple readers.
 *
 * V4 uses a byte[] array and synchronized int list
 */
public class CachingMutableOffHeapByteArrayStoreV4 extends MutableOffHeapByteArrayStore {
  private static final int DEFAULT_CACHE_SIZE = 1024 * 1024; // 1MB
  private final int _cacheSize;

  // cache inserted values in a single buffer
  private final byte[] _cache;

  // holds the offsets of values stored in _cacheBuffer
//  private final int[] _cacheOffsets;

  // end offset of the next value to be inserted into the cache (exclusive)
  private int _cacheBufferEndOffset;

  // cache size in bytes
  private final AtomicInteger _cacheSizeBytes = new AtomicInteger();

  // cache size as number of values
  private final AtomicInteger _cacheSizeNumValues = new AtomicInteger();

  private final IntList _cacheValueOffsets = IntLists.synchronize(new IntArrayList());

  /**
   * Constructor follows {@link MutableOffHeapByteArrayStore}
   */
  public CachingMutableOffHeapByteArrayStoreV4(PinotDataBufferMemoryManager memoryManager, String allocationContext,
      int numArrays, int avgArrayLen) {
    super(memoryManager, allocationContext, numArrays, avgArrayLen);
    _cacheSize = DEFAULT_CACHE_SIZE;
    _cache = new byte[_cacheSize];
//    _cacheOffsets = new int[_cacheSize]; // TODO too much memory, revisit this
    _cacheBufferEndOffset = _cacheSize;
  }

  @Override
  public byte[] get(int index) {
    // check the current buffer first, if index is not in the current buffer, check the cache
    Buffer curr = getCurrentBuffer();
    if (index >= (curr.getStartIndex() + curr.getNumValues())) {
      int cacheIndex = index - _numElements;
      return getCachedElement(cacheIndex);
    }

    // otherwise, check buffers as usual
    return super.get(index);
  }

  @Override
  public boolean equalsValueAt(byte[] value, int index) {
    // check the current buffer first, if index is not in the current buffer, check the cache
    Buffer curr = getCurrentBuffer();
    if (index >= (curr.getStartIndex() + curr.getNumValues())) {
      int cacheIndex = index - _numElements;
      return Arrays.equals(value, getCachedElement(cacheIndex));
    }

    // otherwise, check buffers as usual
    return super.equalsValueAt(value, index);
  }

  @Override
  public int add(byte[] value) {
    if (value.length > _cacheSize) {
      flush();                  // flush to ensure the ordering is correct
      return super.add(value);  // cache is empty, fallback to direct write
    }

    if (value.length + _cacheSizeBytes.get() > _cacheSize) {
      flush();
    }

    // check if the value and existing cached values can all fit in the current buffer
    Buffer buffer = getCurrentBuffer();
    int totalLength = _cacheSizeBytes.get() + value.length;
    int totalValues = _cacheSizeNumValues.get() + 1;
    int startOffset = buffer.getAvailEndOffset() - totalLength;
    if (startOffset < (buffer.getNumValues() + totalValues) * Integer.BYTES) {
      // all values cannot fit, since maximal packing isn't necessary we will simply expand the buffer
      // the 'waste' here is limited to the size of the cache i.e. _cacheSize
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
//    _cacheOffsets[_cacheSizeNumValues.get()] = _cacheBufferEndOffset;
    _cacheValueOffsets.add(_cacheBufferEndOffset);
    _cacheSizeBytes.addAndGet(value.length);
    _cacheSizeNumValues.incrementAndGet();
    _totalStringSize += value.length;

    // return index of the latest added value
    return _numElements + _cacheSizeNumValues.get() - 1;
  }

  /**
   * Flush the cache to the buffer. This method assumes that the cache can fit in the current buffer.
   * <p>
   * The implementation of add() ensures the cache can fit in the current buffer before adding to the cache. No other
   * method will add to the cache, or fill the buffer.
   */
  private void flush() {
    int totalValues = _cacheSizeNumValues.get();
    if (totalValues == 0) {
      return; // nothing to flush
    }
    Buffer buffer = getCurrentBuffer();
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
    buffer._availEndOffset = joinedStartOffset;
    buffer._numValues += totalValues;
    _numElements += totalValues;

    // reset available offset to the end of the buffer, effectively clearing the buffer
    _cacheBufferEndOffset = _cacheSize;
    _cacheSizeBytes.set(0);
    _cacheSizeNumValues.set(0);
    _cacheValueOffsets.clear();
  }

  private byte[] getCachedElement(int index) {
    int valueLength = getCachedElementLength(index);
    byte[] val = new byte[valueLength];
    System.arraycopy(_cache, _cacheValueOffsets.getInt(index), val, 0, valueLength);

    return val;
  }

  /**
   * Get the length of the cached element at the given index. Unsafe if the index is out of bounds.
   */
  private int getCachedElementLength(int index) {
    if (index == 0) {
      return _cacheSize - _cacheValueOffsets.getInt(0);
    }
    return _cacheValueOffsets.getInt(index - 1) - _cacheValueOffsets.getInt(index);
  }
}
