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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class OnHeapImmutableTimeSeriesIndexCreatorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), OnHeapImmutableTimeSeriesIndexCreatorTest.class.getSimpleName());
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

    assertEquals(INDEX_DIR.list().length, 1);
    assertEquals(COLUMN_NAME + V1Constants.Indexes.TIME_SERIES_INDEX_FILE_EXTENSION, INDEX_DIR.list()[0]);
  }
}
