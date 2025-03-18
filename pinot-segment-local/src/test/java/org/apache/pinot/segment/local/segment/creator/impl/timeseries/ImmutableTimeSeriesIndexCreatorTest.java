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
package org.apache.pinot.segment.local.segment.creator.impl.timeseries;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.timeseries.MutableTimeSeriesIndexImpl;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.TimeSeriesIndexCreator;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.TimeSeriesIndexConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ImmutableTimeSeriesIndexCreatorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), ImmutableTimeSeriesIndexCreatorTest.class.getSimpleName());
  private static final String SEGMENT_NAME = "tableName__9__1__20240227T0254Z";
  private static final File CONSUMER_DIR = new File(TEMP_DIR, "consumers");
  private static final File INDEX_DIR =
      new File(new File(TEMP_DIR, "tmp-tableName__9__1__20240227T0254Z-1709002522086"), "tmp-" + UUID.randomUUID());
  private static final String COLUMN_NAME = "col";

  // Tests segment conversion of mutable time series index to immutable time series index (i.e. index file is copied)
  @Test
  public void testSegmentConversion()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
    PinotDataBufferMemoryManager memoryManager = new DirectMemoryManager(SEGMENT_NAME);
    MutableTimeSeriesIndexImpl mutableTimeSeriesIndex =
        new MutableTimeSeriesIndexImpl(CONSUMER_DIR, SEGMENT_NAME, memoryManager, COLUMN_NAME,
            TimeSeriesIndexConfig.ENABLED);
    mutableTimeSeriesIndex.add(
        TimeSeriesUtils.getDataPointAsString(List.of("name=pinotmetric", "tag1=queries"), 1.0, 1000L));
    mutableTimeSeriesIndex.add(
        TimeSeriesUtils.getDataPointAsString(List.of("name=pinotmetric", "tag1=exceptions"), 2.0, 2000L));
    mutableTimeSeriesIndex.commit();

    TimeSeriesIndexCreator timeSeriesIndexCreator =
        new ImmutableTimeSeriesIndexCreator(CONSUMER_DIR, INDEX_DIR, COLUMN_NAME, TimeSeriesIndexConfig.ENABLED);
    timeSeriesIndexCreator.seal();
    timeSeriesIndexCreator.close();

    assertEquals(INDEX_DIR.list().length, 1);
    assertEquals(COLUMN_NAME + V1Constants.Indexes.TIME_SERIES_INDEX_FILE_EXTENSION, INDEX_DIR.list()[0]);
  }
}
