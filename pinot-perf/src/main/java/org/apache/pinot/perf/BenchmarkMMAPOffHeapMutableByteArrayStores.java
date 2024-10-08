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
package org.apache.pinot.perf;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.CachingMutableOffHeapByteArrayStore;
import org.apache.pinot.segment.local.io.writer.impl.CachingMutableOffHeapByteArrayStoreV4;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MutableOffHeapByteArrayStore;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 30)
@Measurement(iterations = 5, time = 30)
@Fork(3)
@State(Scope.Benchmark)
public class BenchmarkMMAPOffHeapMutableByteArrayStores {
  private static final int NUM_VALUES = 1_000_000;

  @Param({"8", "32", "128", "1024"})
  private int _maxValueLength;

  private PinotDataBufferMemoryManager _normalStoreMemoryManager;
  private PinotDataBufferMemoryManager _cachingStoreMemoryManager;
  private PinotDataBufferMemoryManager _cachingV4StoreMemoryManager;
  private byte[][] _values;
  private MutableOffHeapByteArrayStore _mutableOffHeapByteArrayStore;
  private CachingMutableOffHeapByteArrayStore _cachingMutableOffHeapByteArrayStore;
  private CachingMutableOffHeapByteArrayStoreV4 _cachingMutableOffHeapByteArrayStoreV4;

  @Setup
  public void setUp() {
    _normalStoreMemoryManager = new MmapMemoryManager(FileUtils.getTempDirectoryPath(), "normal");
    _cachingStoreMemoryManager = new MmapMemoryManager(FileUtils.getTempDirectoryPath(), "caching");
    _cachingV4StoreMemoryManager = new MmapMemoryManager(FileUtils.getTempDirectoryPath(), "cachingV4");
    System.out.println("files at " + FileUtils.getTempDirectoryPath());

    _values = new byte[NUM_VALUES][];
    Random random = new Random();
    for (int i = 0; i < NUM_VALUES; i++) {
      int valueLength = random.nextInt(_maxValueLength + 1);
      byte[] value = new byte[valueLength];
      random.nextBytes(value);
      _values[i] = value;
    }

    _mutableOffHeapByteArrayStore =
        new MutableOffHeapByteArrayStore(_normalStoreMemoryManager, null, NUM_VALUES, _maxValueLength / 2);
    for (byte[] value : _values) {
      _mutableOffHeapByteArrayStore.add(value);
    }
    System.out.println("\nBytes allocated for MutableOffHeapByteArrayStore: "
        + _mutableOffHeapByteArrayStore.getTotalOffHeapMemUsed());

    _cachingMutableOffHeapByteArrayStore =
        new CachingMutableOffHeapByteArrayStore(_cachingStoreMemoryManager, null, NUM_VALUES, _maxValueLength / 2);
    for (byte[] value : _values) {
      _cachingMutableOffHeapByteArrayStore.add(value);
    }
    System.out.println("\nBytes allocated for CachingMutableOffHeapByteArrayStore: "
        + _cachingMutableOffHeapByteArrayStore.getTotalOffHeapMemUsed());

    _cachingMutableOffHeapByteArrayStoreV4 =
        new CachingMutableOffHeapByteArrayStoreV4(_cachingV4StoreMemoryManager, null, NUM_VALUES, _maxValueLength / 2);
    for (byte[] value : _values) {
      _cachingMutableOffHeapByteArrayStoreV4.add(value);
    }
    System.out.println("\nBytes allocated for CachingMutableOffHeapByteArrayStoreV4: "
        + _cachingMutableOffHeapByteArrayStoreV4.getTotalOffHeapMemUsed());
  }

  @TearDown
  public void tearDown()
      throws IOException {
    _mutableOffHeapByteArrayStore.close();
    _normalStoreMemoryManager.close();

    _cachingMutableOffHeapByteArrayStore.close();
    _cachingStoreMemoryManager.close();

    _cachingMutableOffHeapByteArrayStoreV4.close();
    _cachingV4StoreMemoryManager.close();
  }

  @Benchmark
  public int mutableOffHeapByteArrayStoreRead() {
    int sum = 0;
    for (int i = 0; i < NUM_VALUES; i++) {
      sum += _mutableOffHeapByteArrayStore.get(i).length;
    }
    return sum;
  }

  @Benchmark
  public int mutableCachingOffHeapByteArrayStoreRead() {
    int sum = 0;
    for (int i = 0; i < NUM_VALUES; i++) {
      sum += _cachingMutableOffHeapByteArrayStore.get(i).length;
    }
    return sum;
  }
//
//  @Benchmark
//  public int mutableCachingV4OffHeapByteArrayStoreRead() {
//    int sum = 0;
//    for (int i = 0; i < NUM_VALUES; i++) {
//      sum += _cachingMutableOffHeapByteArrayStoreV4.get(i).length;
//    }
//    return sum;
//  }

  @Benchmark
  public int mutableOffHeapByteArrayStoreWrite()
      throws IOException {
    int sum = 0;
    try (MutableOffHeapByteArrayStore mutableOffHeapByteArrayStore = new MutableOffHeapByteArrayStore(
        _normalStoreMemoryManager, null, NUM_VALUES, _maxValueLength / 2)) {
      for (byte[] value : _values) {
        sum += mutableOffHeapByteArrayStore.add(value);
      }
    }
    return sum;
  }

  @Benchmark
  public int mutableCachingOffHeapByteArrayStoreWrite()
      throws IOException {
    int sum = 0;
    try (
        CachingMutableOffHeapByteArrayStore cachingMutableOffHeapByteArrayStore =
            new CachingMutableOffHeapByteArrayStore(
            _cachingStoreMemoryManager, null, NUM_VALUES, _maxValueLength / 2)) {
      for (byte[] value : _values) {
        sum += cachingMutableOffHeapByteArrayStore.add(value);
      }
    }
    return sum;
  }

//  @Benchmark
//  public int mutableCachingV4OffHeapByteArrayStoreWrite()
//      throws IOException {
//    int sum = 0;
//    try (
//        CachingMutableOffHeapByteArrayStoreV4 cachingMutableOffHeapByteArrayStoreV4 =
//            new CachingMutableOffHeapByteArrayStoreV4(
//            _cachingV4StoreMemoryManager, null, NUM_VALUES, _maxValueLength / 2)) {
//      for (byte[] value : _values) {
//        sum += cachingMutableOffHeapByteArrayStoreV4.add(value);
//      }
//    }
//    return sum;
//  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkMMAPOffHeapMutableByteArrayStores.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
