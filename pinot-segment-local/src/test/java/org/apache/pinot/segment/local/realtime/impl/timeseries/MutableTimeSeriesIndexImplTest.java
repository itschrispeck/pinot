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
package org.apache.pinot.segment.local.realtime.impl.timeseries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.TimeSeriesIndexConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class MutableTimeSeriesIndexImplTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), MutableTimeSeriesIndexImplTest.class.getSimpleName());
  private static final String SEGMENT_NAME = "MutableTimeSeriesIndexImplTestSegment";
  private static final File CONSUMER_DIR = new File(TEMP_DIR, "consumers");
  private static final String COLUMN_NAME = "col";

  @BeforeClass
  public void setUp() {
    FileUtils.deleteQuietly(CONSUMER_DIR);
  }

  @Test
  public void test()
      throws IOException {
    PinotDataBufferMemoryManager memoryManager = new DirectMemoryManager(SEGMENT_NAME);
    MutableTimeSeriesIndexImpl mutableTimeSeriesIndex =
        new MutableTimeSeriesIndexImpl(CONSUMER_DIR, SEGMENT_NAME, memoryManager, COLUMN_NAME,
            TimeSeriesIndexConfig.ENABLED);
    ForwardIndexReaderContext context = mutableTimeSeriesIndex.createContext();
    List<String> timeSeriesDataPoints = getTestData();
    for (String timeSeriesDataPoint : timeSeriesDataPoints) {
      mutableTimeSeriesIndex.add(timeSeriesDataPoint);
    }

    // single tag search, single returned series
    List<String> searchTags = List.of("tag1=exceptions");
    int[] matchedSeries = mutableTimeSeriesIndex.getMatchingTimeSeriesIds(searchTags).toArray();
    assertEquals(matchedSeries.length, 1);
    int seriesId = matchedSeries[0];
    assertEquals(mutableTimeSeriesIndex.getTagSet(seriesId), "name=pinotmetric,tag1=exceptions");
    assertEquals(mutableTimeSeriesIndex.getTimestamps(seriesId, context), List.of(2000L));
    assertEquals(mutableTimeSeriesIndex.getValues(seriesId, context), List.of(2.0));

    // multiple tag search, single returned series
    searchTags = new ArrayList<>(List.of("name=pinotmetric", "tag1=queries"));
    matchedSeries = mutableTimeSeriesIndex.getMatchingTimeSeriesIds(searchTags).toArray();
    assertEquals(matchedSeries.length, 1);
    seriesId = matchedSeries[0];
    assertEquals(mutableTimeSeriesIndex.getTagSet(seriesId), "name=pinotmetric,tag1=queries");
    assertEquals(mutableTimeSeriesIndex.getTimestamps(seriesId, context), List.of(1000L, 2000L, 3000L));
    assertEquals(mutableTimeSeriesIndex.getValues(seriesId, context), List.of(1.0, 3.0, 4.0));

    // single tag search, multiple returned series
    searchTags = List.of("name=pinotmetric");
    matchedSeries = mutableTimeSeriesIndex.getMatchingTimeSeriesIds(searchTags).toArray();
    assertEquals(matchedSeries.length, 2);
    seriesId = matchedSeries[0];
    assertEquals(mutableTimeSeriesIndex.getTagSet(seriesId), "name=pinotmetric,tag1=queries");
    assertEquals(mutableTimeSeriesIndex.getTimestamps(seriesId, context), List.of(1000L, 2000L, 3000L));
    assertEquals(mutableTimeSeriesIndex.getValues(seriesId, context), List.of(1.0, 3.0, 4.0));
    seriesId = matchedSeries[1];
    assertEquals(mutableTimeSeriesIndex.getTagSet(seriesId), "name=pinotmetric,tag1=exceptions");
    assertEquals(mutableTimeSeriesIndex.getTimestamps(seriesId, context), List.of(2000L));
    assertEquals(mutableTimeSeriesIndex.getValues(seriesId, context), List.of(2.0));

    // TODO unhappy path, add test where one tag is not found

    // test conversion generates an immutable index file
    mutableTimeSeriesIndex.commit();
    File outputDir = new File(CONSUMER_DIR, SEGMENT_NAME);
    assertEquals(outputDir.list().length, 1);
    assertEquals(COLUMN_NAME + V1Constants.Indexes.TIME_SERIES_INDEX_FILE_EXTENSION, outputDir.list()[0]);
    mutableTimeSeriesIndex.close();
  }

  private List<String> getTestData() {
    List<String> timeSeriesDataPoints = new ArrayList<>();
    timeSeriesDataPoints.add(
        TimeSeriesUtils.getDataPointAsString(List.of("name=pinotmetric", "tag1=queries"), 1.0, 1000L));
    timeSeriesDataPoints.add(
        TimeSeriesUtils.getDataPointAsString(List.of("name=pinotmetric", "tag1=exceptions"), 2.0, 2000L));
    timeSeriesDataPoints.add(
        TimeSeriesUtils.getDataPointAsString(List.of("name=pinotmetric", "tag1=queries"), 3.0, 2000L));
    timeSeriesDataPoints.add(
        TimeSeriesUtils.getDataPointAsString(List.of("name=pinotmetric", "tag1=queries"), 4.0, 3000L));

    return timeSeriesDataPoints;
  }
}
