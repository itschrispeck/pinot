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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.TimeSeriesTransformerConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * TimeSeriesTransformer transforms some input JSON into a time series index compatible format.
 * <p>
 * For example, the input json string:
 * {"tags_json": {"name": "1", "city": "mtv", "airport": "sfo"}, "value": 12.0304, "timestamp": 1234567890}
 * <p>
 * Will be transformed into the format compatible with time series index ingestion:
 * "name=1,city=mtv,airport=sfo\u00001234567890\u000012.0304"
 */
public class TimeSeriesTransformer implements RecordTransformer {
  private final TimeSeriesTransformerConfig _transformerConfig;

  public TimeSeriesTransformer(TableConfig tableConfig) {
    if (null == tableConfig.getIngestionConfig() || null == tableConfig.getIngestionConfig()
        .getTimeSeriesTransformerConfig()) {
      _transformerConfig = null;
      return;
    }
    _transformerConfig = tableConfig.getIngestionConfig().getTimeSeriesTransformerConfig();
  }

  @Override
  public boolean isNoOp() {
    return null == _transformerConfig;
  }

  @Nullable
  @Override
  public GenericRow transform(GenericRow record) {
    String tagsJson = (String) record.getValue(_transformerConfig.getTagsJsonField());
    Number value = (Number) record.getValue(_transformerConfig.getValueField());
    Long timestamp = (Long) record.getValue(_transformerConfig.getTimestampField());
    if (tagsJson == null || value == null || timestamp == null) {
      if (_transformerConfig.isSkipMalformedRecords()) {
        return null;
      }
      throw new IllegalStateException("Invalid record: " + record);
    }

    List<String> tagSet = new ArrayList<>();
    try {
      JsonNode tagsJsonRoot = JsonUtils.stringToJsonNode(tagsJson);
      tagsJsonRoot.fields().forEachRemaining(entry -> tagSet.add(entry.getKey() + "=" + entry.getValue().asText()));
    } catch (Exception e) {
      if (_transformerConfig.isSkipMalformedRecords()) {
        return null;
      }
      throw new IllegalStateException("Invalid tagsJson: " + tagsJson, e);
    }

//    GenericRow outputRow = new GenericRow();
    record.putValue(_transformerConfig.getTimeSeriesColumn(),
        TimeSeriesUtils.getDataPointAsString(tagSet, value.doubleValue(), timestamp));
    return record;
  }

  public static void validateSchema(@Nonnull Schema schema, @Nonnull TimeSeriesTransformerConfig config) {
    if (!schema.hasColumn(config.getTimeSeriesColumn())) {
      throw new IllegalStateException("Invalid time series column specified");
    }
  }
}
