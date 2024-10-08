package org.apache.pinot.segment.local.io.writer.impl;

import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CachingMutableOffHeapByteArrayStoreV3Test {

  private PinotDataBufferMemoryManager _memoryManager;
  private static final int ONE_GB = 1024 * 1024 * 1024;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(MutableOffHeapByteArrayStoreTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testFlow()
      throws IOException {
    int numArrays = 1024;
    int avgArrayLen = 32;
    int cacheSize = 1024; // 1KB cache
    CachingMutableOffHeapByteArrayStoreV3 store =
        new CachingMutableOffHeapByteArrayStoreV3(_memoryManager, "stringColumn", numArrays, avgArrayLen, cacheSize);
    final int maxSize = MutableOffHeapByteArrayStore.getStartSize(numArrays, avgArrayLen) - 4;

    byte[] b1 = new byte[3];
    for (int i = 0; i < b1.length; i++) {
      b1[i] = (byte) i;
    }

    byte[] b2 = new byte[maxSize];
    for (int i = 0; i < b2.length; i++) {
      b2[i] = (byte) (i % Byte.MAX_VALUE);
    }

    // Add small array
    final int i1 = store.add(b1);
    Assert.assertTrue(Arrays.equals(store.get(i1), b1));

    // And now the larger one, should result in a new buffer
    final int i2 = store.add(b2);
    Assert.assertTrue(Arrays.equals(store.get(i2), b2));

    // And now one more, should result in a new buffer but exact fit.
    final int i3 = store.add(b2);
    Assert.assertTrue(Arrays.equals(store.get(i3), b2));

    // One more buffer when we add the small one again.
    final int i4 = store.add(b1);
    Assert.assertTrue(Arrays.equals(store.get(i4), b1));

    // Test with one more 'get' to ensure that things have not changed.
    Assert.assertTrue(Arrays.equals(store.get(i1), b1));
    Assert.assertTrue(Arrays.equals(store.get(i2), b2));
    Assert.assertTrue(Arrays.equals(store.get(i3), b2));
    Assert.assertTrue(Arrays.equals(store.get(i4), b1));

    byte[] b3 = new byte[5];
    for (int i = 0; i < b3.length; i++) {
      b3[i] = (byte) (i + 1);
    }

    int ix = -1;
    final int iters = 1_000_000;

    // Now add the small one multiple times causing many additions.
    for (int i = 0; i < iters; i++) {
      if (ix == -1) {
        ix = store.add(b3);
      }
      store.add(b3);
    }
    for (int i = 0; i < iters; i++) {
      Assert.assertTrue(Arrays.equals(store.get(ix++), b3));
    }

    // Original values should still be good.
    Assert.assertTrue(Arrays.equals(store.get(i1), b1));
    Assert.assertTrue(Arrays.equals(store.get(i2), b2));
    Assert.assertTrue(Arrays.equals(store.get(i3), b2));
    Assert.assertTrue(Arrays.equals(store.get(i4), b1));
    store.close();
  }
}
