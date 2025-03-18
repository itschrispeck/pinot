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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.apache.pinot.segment.spi.index.reader.TimeSeriesIndexReader;
import org.apache.pinot.spi.data.FieldSpec;


public class TimeSeriesIndexTagSetTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = TransformFunctionType.TIME_SERIES_INDEX_TAG_SET.getName();

  private TimeSeriesIndexReader<?> _timeSeriesIndexReader;
  private Set<String> _filterTags;

  @Override
  public void init(List<TransformFunction> arguments,
      Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    if (arguments.size() < 1) {
      throw new IllegalStateException("timeSeriesIndexTagSet must have at least one argument");
    }
    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof IdentifierTransformFunction) {
      String columnName = ((IdentifierTransformFunction) firstArgument).getColumnName();
      _timeSeriesIndexReader = columnContextMap.get(columnName).getDataSource().getTimeSeriesIndex();
      if (_timeSeriesIndexReader == null) {
        throw new IllegalStateException("timeSeriesIndexTagSet can only be applied on a column with timeseries index");
      }
    } else {
      throw new IllegalStateException("The first argument of timeSeriesIndexTagSet must be an identifier");
    }

    if (arguments.size() < 2) {
      // return all tags
      _filterTags = null;
      return;
    }

    TransformFunction secondArgument = arguments.get(1);
    if (secondArgument instanceof LiteralTransformFunction) {
      String inputString = ((LiteralTransformFunction) secondArgument).getStringLiteral();
      _filterTags = new HashSet<>();
      _filterTags.addAll(Arrays.asList(inputString.split(TimeSeriesUtils.TAG_DELIMITER)));
    } else {
      throw new IllegalStateException(
          "The second argument of timeSeriesIndexTagSet must be a literal of tags to return");
    }
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[] docIds = valueBlock.getDocIds(); // these should be time series ids, output by the TimeSeriesFilterOperator
    initStringValuesSV(numDocs);

    // return all tags
    if (_filterTags == null) {
      for (int i = 0; i < numDocs; i++) {
        _stringValuesSV[i] = _timeSeriesIndexReader.getTagSet(docIds[i]);
      }
      return _stringValuesSV;
    }

    // else, return subset of tags based on the filter
    List<String> filteredTags = new ArrayList<>(16);
    for (int i = 0; i < numDocs; i++) {
      String[] tags = _timeSeriesIndexReader.getTagSet(docIds[i]).split(TimeSeriesUtils.TAG_DELIMITER);
      for (String tag : tags) {
        int delimiterIndex = tag.indexOf(TimeSeriesUtils.TAG_VALUE_DELIMITER);
        if (delimiterIndex < 0) {
          continue;
        }
        String tagPrefix = tag.substring(0, delimiterIndex);
        if (_filterTags.contains(tagPrefix)) {
          filteredTags.add(tag);
        }
      }
      _stringValuesSV[i] = String.join(TimeSeriesUtils.TAG_DELIMITER, filteredTags);
      filteredTags.clear();
    }
    return _stringValuesSV;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(FieldSpec.DataType.STRING, true, false);
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
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
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
