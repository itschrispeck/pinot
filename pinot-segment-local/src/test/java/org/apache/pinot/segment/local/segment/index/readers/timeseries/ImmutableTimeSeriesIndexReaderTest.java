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
package org.apache.pinot.segment.local.segment.index.readers.timeseries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.timeseries.OnHeapImmutableTimeSeriesIndexCreator;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ImmutableTimeSeriesIndexReaderTest {

  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), ImmutableTimeSeriesIndexReaderTest.class.getSimpleName());
  private static final File INDEX_DIR = new File(TEMP_DIR, "indexDir");
  private static final String COLUMN_NAME = "col";

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void test()
      throws Exception {
    try (OnHeapImmutableTimeSeriesIndexCreator indexCreator = new OnHeapImmutableTimeSeriesIndexCreator(INDEX_DIR,
        COLUMN_NAME)) {
      indexCreator.add(TimeSeriesUtils.getDataPointAsString(List.of("name=pinotmetric", "tag1=queries"), 1.0, 1000L));
      indexCreator.add(
          TimeSeriesUtils.getDataPointAsString(List.of("name=pinotmetric", "tag1=exceptions"), 2.0, 2000L));
      indexCreator.add(TimeSeriesUtils.getDataPointAsString(List.of("name=pinotmetric", "tag1=queries"), 3.0, 2000L));
      indexCreator.add(TimeSeriesUtils.getDataPointAsString(List.of("name=pinotmetric", "tag1=queries"), 4.0, 3000L));
      indexCreator.seal();
    }

    try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.TIME_SERIES_INDEX_FILE_EXTENSION))) {
      ImmutableTimeSeriesIndexReader immutableTimeSeriesIndexReader = new ImmutableTimeSeriesIndexReader(dataBuffer);
      ImmutableTimeSeriesIndexReader.TimeSeriesReaderContext context = immutableTimeSeriesIndexReader.createContext();

      // single tag search, single returned series
      List<String> searchTags = List.of("tag1=exceptions");
      int[] matchingSeriesIds = immutableTimeSeriesIndexReader.getMatchingTimeSeriesIds(searchTags).toArray();
      assertEquals(matchingSeriesIds.length, 1);
      int seriesId = matchingSeriesIds[0];
      assertEquals(immutableTimeSeriesIndexReader.getTagSet(seriesId), "name=pinotmetric,tag1=exceptions");
      assertEquals(immutableTimeSeriesIndexReader.getTimestamps(seriesId, context), List.of(2000L));
      assertEquals(immutableTimeSeriesIndexReader.getValues(seriesId, context), List.of(2.0));

      // multiple tag search, single returned series
      searchTags = new ArrayList<>(List.of("name=pinotmetric", "tag1=queries"));
      matchingSeriesIds = immutableTimeSeriesIndexReader.getMatchingTimeSeriesIds(searchTags).toArray();
      assertEquals(matchingSeriesIds.length, 1);
      seriesId = matchingSeriesIds[0];
      assertEquals(immutableTimeSeriesIndexReader.getTagSet(seriesId), "name=pinotmetric,tag1=queries");
      assertEquals(immutableTimeSeriesIndexReader.getTimestamps(seriesId, context), List.of(1000L, 2000L, 3000L));
      assertEquals(immutableTimeSeriesIndexReader.getValues(seriesId, context), List.of(1.0, 3.0, 4.0));

      // single tag search, multiple returned series
      searchTags = List.of("name=pinotmetric");
      matchingSeriesIds = immutableTimeSeriesIndexReader.getMatchingTimeSeriesIds(searchTags).toArray();
      assertEquals(matchingSeriesIds.length, 2);
      seriesId = matchingSeriesIds[0];
      assertEquals(immutableTimeSeriesIndexReader.getTagSet(seriesId), "name=pinotmetric,tag1=exceptions");
      assertEquals(immutableTimeSeriesIndexReader.getTimestamps(seriesId, context), List.of(2000L));
      assertEquals(immutableTimeSeriesIndexReader.getValues(seriesId, context), List.of(2.0));
      seriesId = matchingSeriesIds[1];
      assertEquals(immutableTimeSeriesIndexReader.getTagSet(seriesId), "name=pinotmetric,tag1=queries");
      assertEquals(immutableTimeSeriesIndexReader.getTimestamps(seriesId, context), List.of(1000L, 2000L, 3000L));
      assertEquals(immutableTimeSeriesIndexReader.getValues(seriesId, context), List.of(1.0, 3.0, 4.0));

      // TODO unhappy path, add test where one tag is not found

      context.close();
    }
  }
}
