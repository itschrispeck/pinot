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
package org.apache.pinot.core.operator.transform.function;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.local.segment.index.readers.timeseries.ImmutableTimeSeriesIndexReader;
import org.apache.pinot.segment.spi.index.reader.TimeSeriesIndexReader;
import org.apache.pinot.spi.data.FieldSpec;


public class TimeSeriesIndexTimestampsTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = TransformFunctionType.TIME_SERIES_INDEX_TIMESTAMPS.getName();

  private TimeSeriesIndexReader<ImmutableTimeSeriesIndexReader.TimeSeriesReaderContext> _timeSeriesIndexReader;
  private ImmutableTimeSeriesIndexReader.TimeSeriesReaderContext _timeSeriesIndexReaderContext;

  @Override
  public void init(List<TransformFunction> arguments,
      Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof IdentifierTransformFunction) {
      String columnName = ((IdentifierTransformFunction) firstArgument).getColumnName();
      _timeSeriesIndexReader =
          (TimeSeriesIndexReader<ImmutableTimeSeriesIndexReader.TimeSeriesReaderContext>) columnContextMap.get(
              columnName).getDataSource().getTimeSeriesIndex();
      if (_timeSeriesIndexReader == null) {
        throw new IllegalStateException(
            "timeSeriesIndexTimestamps can only be applied on a column with timeseries index");
      }
      _timeSeriesIndexReaderContext = _timeSeriesIndexReader.createContext();
    } else {
      throw new IllegalStateException("The first argument of timeSeriesIndexTimestamps must be an identifier");
    }
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] docIds = valueBlock.getDocIds(); // these should be time series ids, output by the TimeSeriesFilterOperator
    initLongValuesMV(numDocs);
    for (int i = 0; i < numDocs; i++) {
      _longValuesMV[i] = _timeSeriesIndexReader.getTimestamps(docIds[i], _timeSeriesIndexReaderContext).toLongArray();
    }
    return _longValuesMV;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(FieldSpec.DataType.LONG, false, false);
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }
}
