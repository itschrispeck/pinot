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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;


/**
 * CachingMutableOffHeapByteArrayStore is a subclass of MutableOffHeapByteArrayStore that caches inserts up to a
 * configured size. When the limit is reached, the cache is flushed to the underlying store as a batch. This can
 * greatly reduce the number of writes to disk, for example when dictionaries are high cardinality.
 * <p>
 * This class provides the same interface as MutableOffHeapByteArrayStore and the same guarantees - i.e. it is
 * thread-safe for a single writer and multiple readers.
 *
 * V3 is List based
 */
public class CachingMutableOffHeapByteArrayStoreV3 extends MutableOffHeapByteArrayStore {
  private static final int DEFAULT_CACHE_SIZE = 1024 * 1024; // 1MB
  //  private static final int _cacheSize = 1024; // 1KB
  private final int _cacheSize;

  // TODO use faster structure
  List<byte[]> _cache = Collections.synchronizedList(new ArrayList<>());
  private final AtomicInteger _cacheSizeBytes = new AtomicInteger();
  private final AtomicInteger _cacheSizeNumValues = new AtomicInteger();

  /**
   * Constructor follows {@link MutableOffHeapByteArrayStore}
   */
  public CachingMutableOffHeapByteArrayStoreV3(PinotDataBufferMemoryManager memoryManager, String allocationContext,
      int numArrays, int avgArrayLen) {
    super(memoryManager, allocationContext, numArrays, avgArrayLen);
    _cacheSize = DEFAULT_CACHE_SIZE;
  }

  public CachingMutableOffHeapByteArrayStoreV3(PinotDataBufferMemoryManager memoryManager, String allocationContext,
      int numArrays, int avgArrayLen, int cacheSize) {
    super(memoryManager, allocationContext, numArrays, avgArrayLen);
    _cacheSize = cacheSize;
  }

  @Override
  public byte[] get(int index) {
    // check the current buffer first, if index is not in the current buffer, check the cache
    Buffer curr = _currentBuffer;
    int bufferLastIndex = curr.getStartIndex() + curr.getNumValues();
    if (index >= (bufferLastIndex)) {
      assert index - bufferLastIndex < _cacheSizeNumValues.get();
      return _cache.get(index - (bufferLastIndex));
    }

    // otherwise, check buffers as usual
    return super.get(index);
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
    Buffer buffer = _currentBuffer;
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

    // value and cached values can fit, so simply add to cache
    _cache.add(value);
    _cacheSizeBytes.addAndGet(value.length);
    _cacheSizeNumValues.incrementAndGet();
    _totalStringSize += value.length;

    // return index of the latest added value
    return _numElements + _cache.size() - 1;
  }

  // flush the cache to the buffer

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
    Buffer buffer = _currentBuffer;
    int totalLength = _cacheSizeBytes.get();

    // TODO this could be optimized by using a single buffer for the cache
    byte[] joinedVals = new byte[totalLength];  // hold the compound value
    int[] offsets = new int[totalValues];       // hold start offset of each value

    int joinedStartOffset = buffer.getAvailEndOffset() - totalLength;
    int offset = 0;
    // iterate from largest docId to smallest docId so the compound value is correctly ordered
    for (int i = _cacheSizeNumValues.get() - 1; i >= 0; i--) {
      byte[] cachedVal = _cache.get(i);
      System.arraycopy(cachedVal, 0, joinedVals, offset, cachedVal.length);
      offsets[i] = joinedStartOffset + offset;
      offset += cachedVal.length;
    }
    assert offset == totalLength;

    buffer._pinotDataBuffer.readFrom(joinedStartOffset, joinedVals);
    ByteBuffer buf = ByteBuffer.allocate(offsets.length * 4);
    buf.order(ByteOrder.nativeOrder()).asIntBuffer().put(offsets);
    buffer._pinotDataBuffer.readFrom((long) buffer.getNumValues() * Integer.BYTES, buf.array());

    buffer._availEndOffset = joinedStartOffset;
    buffer._numValues += totalValues;
    _numElements += totalValues;

    _cache.clear();
    _cacheSizeBytes.set(0);
    _cacheSizeNumValues.set(0);
  }

  @Override
  public boolean equalsValueAt(byte[] value, int index) {
    // check the current buffer first, if index is not in the current buffer, check the cache
    Buffer curr = _currentBuffer;
    int bufferLastIndex = curr.getStartIndex() + curr.getNumValues();
    if (index >= (bufferLastIndex)) {
      return Arrays.equals(value, _cache.get(index - (bufferLastIndex)));
    }
    return super.equalsValueAt(value, index);
  }

  @Override
  public void close()
      throws IOException {
    flush(); // not really needed, this should not be persisted?
    super.close();
  }
}
