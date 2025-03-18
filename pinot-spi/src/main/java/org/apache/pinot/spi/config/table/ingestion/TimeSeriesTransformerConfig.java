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
package org.apache.pinot.spi.config.table.ingestion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class TimeSeriesTransformerConfig extends BaseJsonConfig {
  @JsonPropertyDescription("The field to use as the metric tags. This field should be valid JSON.")
  private String _tagsJsonField = "tags_json";
  @JsonPropertyDescription("The field to use as the metric value. This field should be a DOUBLE.")
  private String _valueField = "value";
  @JsonPropertyDescription("The field to use as the metric timestamp. This field must be a LONG.")
  private String _timestampField = "timestamp";
  @JsonPropertyDescription("The column that will be time series indexed.")
  private String _timeSeriesColumn = "timeSeriesIndex";
  @JsonPropertyDescription("Skip malformed records. default = true")
  private boolean _skipMalformedRecords = true;

  @JsonCreator
  public TimeSeriesTransformerConfig(@JsonProperty("tagsJsonField") @Nullable String tagsJsonField,
      @JsonProperty("valueField") @Nullable String valueField,
      @JsonProperty("timestampField") @Nullable String timestampField,
      @JsonProperty("timeSeriesColumn") @Nullable String timeSeriesColumn,
      @JsonProperty("skipMalformedRecords") @Nullable Boolean skipMalformedRecords) {
    _tagsJsonField = tagsJsonField == null ? _tagsJsonField : tagsJsonField;
    _valueField = valueField == null ? _valueField : valueField;
    _timestampField = timestampField == null ? _timestampField : timestampField;
    _timeSeriesColumn = timeSeriesColumn == null ? _timeSeriesColumn : timeSeriesColumn;
    _skipMalformedRecords = skipMalformedRecords == null ? _skipMalformedRecords : skipMalformedRecords;
  }

  public String getTagsJsonField() {
    return _tagsJsonField;
  }

  public String getTimestampField() {
    return _timestampField;
  }

  public String getValueField() {
    return _valueField;
  }

  public String getTimeSeriesColumn() {
    return _timeSeriesColumn;
  }
  public boolean isSkipMalformedRecords() {
    return _skipMalformedRecords;
  }

  public TimeSeriesTransformerConfig setTagsJsonField(String tagsField) {
    _tagsJsonField = tagsField;
    return this;
  }

  public TimeSeriesTransformerConfig setTimestampField(String timestampField) {
    _timestampField = timestampField;
    return this;
  }

  public TimeSeriesTransformerConfig setValueField(String valueField) {
    _valueField = valueField;
    return this;
  }

  public TimeSeriesTransformerConfig setTimeSeriesColumn(String timeSeriesColumn) {
    _timeSeriesColumn = timeSeriesColumn;
    return this;
  }

  public TimeSeriesTransformerConfig setSkipMalformedRecords(boolean skipMalformedRecords) {
    _skipMalformedRecords = skipMalformedRecords;
    return this;
  }
}
