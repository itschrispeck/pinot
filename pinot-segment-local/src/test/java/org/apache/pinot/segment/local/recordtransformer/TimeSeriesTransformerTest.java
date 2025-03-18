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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.Map;
import org.apache.pinot.segment.local.segment.index.timeseries.TimeSeriesDataPoint;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TimeSeriesTransformerConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TimeSeriesTransformerTest {

  private static TableConfig _tableConfig;
  @BeforeClass
  public void setUp() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    TimeSeriesTransformerConfig timeSeriesTransformerConfig =
        new TimeSeriesTransformerConfig("tagsJson", "value", "timestamp", "timeSeriesIndex", true);
    ingestionConfig.setTimeSeriesTransformerConfig(timeSeriesTransformerConfig);
    _tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").setIngestionConfig(ingestionConfig)
            .build();
  }

  @Test
  public void testTransform() {
    GenericRow inputRow = new GenericRow();
    inputRow.putValue("tagsJson", "{\"tag1\":\"value1\", \"tag2\":\"value2\"}");
    inputRow.putValue("value", 1.0);
    inputRow.putValue("timestamp", 1234567890L);
    TimeSeriesTransformer timeSeriesTransformer = new TimeSeriesTransformer(_tableConfig);
    GenericRow outputRow = timeSeriesTransformer.transform(inputRow);
    assertEquals(outputRow.getValue("timeSeriesIndex").getClass(), String.class);
    TimeSeriesDataPoint timeSeriesDataPoint =
        TimeSeriesUtils.getDataPointFromString((String) outputRow.getValue("timeSeriesIndex"));
    assertEquals(timeSeriesDataPoint.getTagSet().size(), 2);
    assertEquals(timeSeriesDataPoint.getTagSet().get(0), "tag1=value1");
    assertEquals(timeSeriesDataPoint.getTagSet().get(1), "tag2=value2");
    assertEquals(timeSeriesDataPoint.getValue(), 1.0);
    assertEquals(timeSeriesDataPoint.getTimestamp(), 1234567890L);
  }

  @Test
  public void testTransformMap() {
    GenericRow inputRow = new GenericRow();
    inputRow.putValue("tagsJson", Map.of("tag1", "value1", "tag2", "value2"));
    inputRow.putValue("value", 1.0);
    inputRow.putValue("timestamp", 1234567890L);
    TimeSeriesTransformer timeSeriesTransformer = new TimeSeriesTransformer(_tableConfig);
    GenericRow outputRow = timeSeriesTransformer.transform(inputRow);
    assertEquals(outputRow.getValue("timeSeriesIndex").getClass(), String.class);
    TimeSeriesDataPoint timeSeriesDataPoint =
        TimeSeriesUtils.getDataPointFromString((String) outputRow.getValue("timeSeriesIndex"));
    assertEquals(timeSeriesDataPoint.getTagSet().size(), 2);
    assertEquals(timeSeriesDataPoint.getTagSet().get(0), "tag1=value1");
    assertEquals(timeSeriesDataPoint.getTagSet().get(1), "tag2=value2");
    assertEquals(timeSeriesDataPoint.getValue(), 1.0);
    assertEquals(timeSeriesDataPoint.getTimestamp(), 1234567890L);
  }


  @Test
  public void testTransformerNoOp() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").setIngestionConfig(ingestionConfig)
            .build();
    TimeSeriesTransformer timeSeriesTransformer = new TimeSeriesTransformer(tableConfig);
    assertEquals(timeSeriesTransformer.isNoOp(), true);
  }

  @Test
  public void testValidateSchema() {
    Schema schema =
        new Schema.SchemaBuilder().addSingleValueDimension("timeSeriesIndex", FieldSpec.DataType.STRING).build();
    TimeSeriesTransformer.validateSchema(schema,
        new TimeSeriesTransformerConfig("tagsJson", "value", "timestamp", "timeSeriesIndex", true));
  }
}
