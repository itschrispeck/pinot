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

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * TODO: Naive implementation of a key vector store for testing. This class needs to be re-implemented.
 */
public class MutableTimeSeriesKeyVectorStoreOnHeap implements MutableTimeSeriesKeyVectorStore {
  private final Map<Integer, DoubleArrayList> _timeSeriesValues;
  private final Map<Integer, LongArrayList> _timeSeriesTimestamps;

  public MutableTimeSeriesKeyVectorStoreOnHeap() {
    _timeSeriesValues = new HashMap<>();
    _timeSeriesTimestamps = new HashMap<>();
  }

  public void add(int id, Double value, long timestamp) {
    _timeSeriesValues.computeIfAbsent(id, k -> new DoubleArrayList()).add(value);
    _timeSeriesTimestamps.computeIfAbsent(id, k -> new LongArrayList()).add(timestamp);
  }

  public Pair<DoubleArrayList, LongArrayList> getSeries(int id) {
    return Pair.of(_timeSeriesValues.get(id), _timeSeriesTimestamps.get(id));
  }

  public DoubleArrayList getValues(int id) {
    return _timeSeriesValues.get(id);
  }

  public LongArrayList getTimestamps(int id) {
    return _timeSeriesTimestamps.get(id);
  }
}
