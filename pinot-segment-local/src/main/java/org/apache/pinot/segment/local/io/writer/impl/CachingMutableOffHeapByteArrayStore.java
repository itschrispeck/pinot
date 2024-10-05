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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
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
 */
public class CachingMutableOffHeapByteArrayStore extends MutableOffHeapByteArrayStore {
//  private static final int ON_HEAP_CACHE_SIZE = 1024 * 1024; // 1MB
  private static final int ON_HEAP_CACHE_SIZE = 1024; // 1KB

  List<byte[]> _cache = new ArrayList<>();
  private final AtomicInteger _cacheSizeBytes = new AtomicInteger();
  private final AtomicInteger _cacheSizeNumValues = new AtomicInteger();

  /**
   * Constructor follows {@link MutableOffHeapByteArrayStore}
   */
  public CachingMutableOffHeapByteArrayStore(PinotDataBufferMemoryManager memoryManager, String allocationContext,
      int numArrays, int avgArrayLen) {
    super(memoryManager, allocationContext, numArrays, avgArrayLen);
  }

  @Override
  public byte[] get(int index) {
    // check current buffer first
    Buffer curr = _currentBuffer;

    // TODO remove the example
    // index = 13
    // buff contains 3 values, indexes 10, 11, 12
    // if index = 3, return value from cache
    // if index = 2, return value from buffer

    // curr.getStartIndex = 10
    // curr.getNumValues = 3
    // 13 >= (10 + 3)
    if (index >= (curr.getStartIndex() + curr.getNumValues())) {
      return _cache.get(index - (curr.getStartIndex() + curr.getNumValues()));
    }
    return super.get(index);
  }

  @Override
  public int add(byte[] value) {
    if (value.length > ON_HEAP_CACHE_SIZE) {
      flush();                  // flush to ensure the ordering is correct
      return super.add(value);  // cache is empty, fallback to direct write
    }

    if (value.length + _cacheSizeBytes.get() > ON_HEAP_CACHE_SIZE) {
      flush();
    }

    // check if the value and existing cached values can all fit in the current buffer
    Buffer buffer = _currentBuffer;
    int totalLength =  _cacheSizeBytes.get() + 1;
    int totalValues = _cacheSizeNumValues.get() + 1;
    int startOffset = buffer.getAvailEndOffset() - totalLength;
    if (startOffset < (buffer.getNumValues() + totalValues) * Integer.BYTES) {
      // all values cannot fit, since maximal packing isn't necessary we will simply expand the buffer
      // the 'waste' here is limited to the size of the cache i.e. ON_HEAP_CACHE_SIZE
      int currentBufferSize = buffer.getSize();
      if ((currentBufferSize << 1) >= 0) {
        // The expanded buffer size should be enough for the current value
        expand(Math.max(currentBufferSize << 1, totalLength + Integer.BYTES));
      } else {
        // Int overflow
        expand(Integer.MAX_VALUE);
      }
//      // after expanding the buffer, use new startOffset
//      startOffset = buffer.getAvailEndOffset() - totalLength;
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

    // create the compound value
    byte[] joinedVals = new byte[totalLength];
    int[] offsets = new int[totalValues]; // hold start offset of each value

    int joinedStartOffset = buffer.getAvailEndOffset() - totalLength;
    int offset = 0;
    int startOffsetIndex = 0;
    // iterate from largest docId to smallest docId so the compound value is correctly ordered
    for (int i = _cache.size() - 1; i >= 0; i--) {
      byte[] cachedVal = _cache.get(i);
      System.arraycopy(cachedVal, 0, joinedVals, offset, cachedVal.length);
      offsets[i] = joinedStartOffset + offset;
      offset += cachedVal.length;
    }
    assert offset == totalLength;

    System.out.println("totalValues = " + totalValues + ", joinedStartOffset = " + joinedStartOffset + ", totalLength = " + totalLength + ", offsets offset = " + (long) buffer.getNumValues() * Integer.BYTES);

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
  public void close()
      throws IOException {
    flush();
    super.close();
  }
}
