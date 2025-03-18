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


/**
 * Key vector store for the mutable time series index. The key is an integer id (time series id). Each id maps to two
 * vectors of equal length, (1) the time series values, and (2) the timestamps.
 *
 * TODO: revisit ordering requirements
 */
public interface MutableTimeSeriesKeyVectorStore {

  /**
   * Append a new time series value and timestamp for the given id
   */
  void add(int id, Double value, long timestamp);

  /**
   * Returns the time series values and timestamps for the given id
   */
  Pair<DoubleArrayList, LongArrayList> getSeries(int id);

  /**
   * Returns the time series values for the given id
   */
  DoubleArrayList getValues(int id);

  /**
   * Returns the time series timestamps for the given id
   */
  LongArrayList getTimestamps(int id);
}
