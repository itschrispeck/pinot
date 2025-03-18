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
package org.apache.pinot.segment.local.segment.index.timeseries;

import java.util.List;


/**
 * TimeSeriesDataPoint contains a time-series data point, which is a tuple of
 *   (1) tagSet, e.g. name=1,city=mtv,airport=sfo
 *   (2) value, e.g. 12.0304
 *   (3) timestamp, e.g. 1234567890
 */
public class TimeSeriesDataPoint {
  private final long _timestamp;
  private final double _value;
  private final List<String> _tagSet;

  public TimeSeriesDataPoint(List<String> tagSet, double value, long timestamp) {
    _tagSet = tagSet;
    _value = value;
    _timestamp = timestamp;
  }

  public long getTimestamp() {
    return _timestamp;
  }

  public double getValue() {
    return _value;
  }

  public List<String> getTagSet() {
    return _tagSet;
  }
}
