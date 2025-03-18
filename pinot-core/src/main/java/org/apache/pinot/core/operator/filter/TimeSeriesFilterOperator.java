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
package org.apache.pinot.core.operator.filter;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.TimeSeriesPredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.apache.pinot.segment.spi.index.reader.TimeSeriesIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Filter operator for supporting the execution of time series filter queries. Returned docIds are the time series ids,
 * which do not necessarily correspond to actual docIds in the segment. This filter operator should not be used in
 * combination with non-timeseries functions.
 */
public class TimeSeriesFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_TIME_SERIES";
  private final TimeSeriesIndexReader<?> _timeSeriesIndexReader;
  private final TimeSeriesPredicate _predicate;

  // TODO numDocs isn't really needed
  public TimeSeriesFilterOperator(int numDocs, TimeSeriesIndexReader<?> timeSeriesIndexReader,
      TimeSeriesPredicate predicate) {
    super(numDocs, false);
    _timeSeriesIndexReader = timeSeriesIndexReader;
    _predicate = predicate;
  }

  private List<String> getTagFilters() {
    return List.of(_predicate.getValue().split(TimeSeriesUtils.TAG_DELIMITER));
  }

  @Override
  protected BlockDocIdSet getTrues() {
    return new BitmapDocIdSet(_timeSeriesIndexReader.getMatchingTimeSeriesIds(getTagFilters()), _numDocs);
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    return _timeSeriesIndexReader.getMatchingTimeSeriesIds(getTagFilters()).getCardinality();
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    return new BitmapCollection(0, false, new MutableRoaringBitmap());
  }

  @Override
  public String getExplainName() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Nullable
  @Override
  public String toExplainString() {
    return null;
  }
}
