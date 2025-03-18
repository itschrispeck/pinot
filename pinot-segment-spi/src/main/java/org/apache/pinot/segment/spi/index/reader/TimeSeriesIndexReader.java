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
package org.apache.pinot.segment.spi.index.reader;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.List;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public interface TimeSeriesIndexReader<T extends ForwardIndexReaderContext> extends IndexReader {
  /**
   * Get the time series value for the given set of tags
   */

  MutableRoaringBitmap getMatchingTimeSeriesIds(List<String> tags);

  String getTagSet(int seriesId);

  DoubleArrayList getValues(int seriesId, T context);

  LongArrayList getTimestamps(int seriesId, T context);

  default T createContext() {
    return null;
  }

  class RawTimeSeries {
    private final String _tagSet;
    private final List<Long> _timestamps;
    private final List<Double> _values;

    public RawTimeSeries(String tagSet, List<Double> values, List<Long> timestamps) {
      _tagSet = tagSet;
      _values = values;
      _timestamps = timestamps;
    }

    public String getTagSet() {
      return _tagSet;
    }

    public List<Long> getTimestamps() {
      return _timestamps;
    }

    public List<Double> getValues() {
      return _values;
    }
  }
}
