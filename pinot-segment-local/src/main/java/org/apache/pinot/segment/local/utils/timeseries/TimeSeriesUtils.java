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
package org.apache.pinot.segment.local.utils.timeseries;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.segment.index.timeseries.TimeSeriesDataPoint;


public class TimeSeriesUtils {

  public static final String TAG_DELIMITER = ",";
  public static final String TAG_VALUE_DELIMITER = "=";
  private static final char TAG_DELIMITER_CHAR = ',';
  private static final String ATTRIBUTE_DELIMITER = "\u0001";
  private static final char ATTRIBUTE_DELIMITER_CHAR = '\u0001';

  private TimeSeriesUtils() {
  }

  /**
   * Generate a JSON string representing a time series data point.
   * @return a JSON string in the format of "tag1,tag2,...\u0000timestamp\u0000value"
   */
  public static String getDataPointAsString(List<String> tags, double value, long timestamp) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tags.size() - 1; i++) {
      sb.append(tags.get(i));
      sb.append(TAG_DELIMITER);
    }
    sb.append(tags.get(tags.size() - 1));
    sb.append(ATTRIBUTE_DELIMITER);
    sb.append(timestamp);
    sb.append(ATTRIBUTE_DELIMITER);
    sb.append(value);
    return sb.toString();
  }

  /**
   * Given a JSON string, returns a time series data point. JSON string is expected to be in the format output by
   * the helper method getDataPointAsJsonString(List<String> tags, double value, long timestamp).
   * @return a TimeSeriesDataPoint object
   */
  public static TimeSeriesDataPoint getDataPointFromStringSlow(String input) {
    try {
      String[] components = input.split(ATTRIBUTE_DELIMITER);
      List<String> tags = List.of(components[0].split(TAG_DELIMITER));
      long timestamp = Long.parseLong(components[1]);
      Double value = Double.parseDouble(components[2]);
      return new TimeSeriesDataPoint(tags, value, timestamp);
    } catch (Exception e) {
      throw new RuntimeException("Error parsing time series data point from string: " + input, e);
    }
  }

  /**
   * Given a JSON string, returns a time series data point. JSON string is expected to be in the format output by
   * the helper method getDataPointAsJsonString(List<String> tags, long timestamp, double value).
   * @return a TimeSeriesDataPoint object
   */
  public static TimeSeriesDataPoint getDataPointFromString(String input) {
    try {
      StringBuilder sb = new StringBuilder();
      List<String> tags = new ArrayList<>();
      boolean inTags = true;
      boolean inTimestamp = false;

      long timestamp = -1;

      for (char c : input.toCharArray()) {
        if (inTags) {
          if (c == TAG_DELIMITER_CHAR) {
            tags.add(sb.toString());
            sb.setLength(0);
          } else if (c == ATTRIBUTE_DELIMITER_CHAR) {
            tags.add(sb.toString());
            sb.setLength(0);
            inTags = false;
            inTimestamp = true;
          } else {
            sb.append(c);
          }
        } else if (inTimestamp) {
          if (c == ATTRIBUTE_DELIMITER_CHAR) {
            timestamp = Long.parseLong(sb.toString());
            sb.setLength(0);
            inTimestamp = false;
          } else {
            sb.append(c);
          }
        } else {
          sb.append(c);
        }
      }
      double value = Double.parseDouble(sb.toString());
      return new TimeSeriesDataPoint(tags, value, timestamp);
    } catch (Exception e) {
      throw new RuntimeException("Error parsing time series data point from string: " + input, e);
    }
  }
}
