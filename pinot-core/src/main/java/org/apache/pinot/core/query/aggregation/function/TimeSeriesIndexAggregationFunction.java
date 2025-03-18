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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactoryProvider;


/**
 * TODO: Extract common logic to BaseTimeSeriesAggregationFunction?
 */
public class TimeSeriesIndexAggregationFunction implements AggregationFunction<BaseTimeSeriesBuilder, DoubleArrayList> {
  private final TimeSeriesBuilderFactory _factory;
  private final AggInfo _aggInfo;
  private final ExpressionContext _valueExpression;
  private final ExpressionContext _timeExpression;
  private final TimeBuckets _timeBuckets;
  private final long _timeReferencePoint;
  private final long _timeOffset;
  private final long _timeBucketDivisor;

  /**
   * Arguments are as shown below:
   * <pre>
   *   timeSeriesIndexAggregate("m3ql", "MIN", valueExpr, timeExpr, timeUnit, offsetSeconds, firstBucketValue,
   *       bucketLenSeconds, numBuckets, "aggParam1=value1")
   * </pre>
   */
  public TimeSeriesIndexAggregationFunction(List<ExpressionContext> arguments) {
    // Initialize temporary variables.
    Preconditions.checkArgument(arguments.size() == 10, "Expected 10 arguments for time-series agg");
    String language = arguments.get(0).getLiteral().getStringValue();
    String aggFunctionName = arguments.get(1).getLiteral().getStringValue();
    ExpressionContext valueExpression = arguments.get(2);
    ExpressionContext timeExpression = arguments.get(3);
    TimeUnit timeUnit = TimeUnit.valueOf(arguments.get(4).getLiteral().getStringValue().toUpperCase(Locale.ENGLISH));
    long offsetSeconds = arguments.get(5).getLiteral().getLongValue();
    long firstBucketValue = arguments.get(6).getLiteral().getLongValue();
    long bucketWindowSeconds = arguments.get(7).getLiteral().getLongValue();
    int numBuckets = arguments.get(8).getLiteral().getIntValue();
    Map<String, String> aggParams = AggInfo.deserializeParams(arguments.get(9).getLiteral().getStringValue());
    AggInfo aggInfo = new AggInfo(aggFunctionName, true /* is partial agg */, aggParams);
    // Set all values
    _factory = TimeSeriesBuilderFactoryProvider.getSeriesBuilderFactory(language);
    _valueExpression = valueExpression;
    _timeExpression = timeExpression;
    _timeBuckets = TimeBuckets.ofSeconds(firstBucketValue, Duration.ofSeconds(bucketWindowSeconds), numBuckets);
    _aggInfo = aggInfo;
    _timeReferencePoint = timeUnit.convert(Duration.ofSeconds(firstBucketValue - bucketWindowSeconds));
    _timeOffset = timeUnit.convert(Duration.ofSeconds(offsetSeconds));
    _timeBucketDivisor = timeUnit.convert(_timeBuckets.getBucketSize());
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.TIMESERIESINDEXAGGREGATE;
  }

  @Override
  public String getResultColumnName() {
    return AggregationFunctionType.TIMESERIESINDEXAGGREGATE.getName();
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    return List.of(_valueExpression, _timeExpression);
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final long[][] timestamps = blockValSetMap.get(_timeExpression).getLongValuesMV();
    final double[][] values = blockValSetMap.get(_valueExpression).getDoubleValuesMV();
    aggregateNumericValues(length, values, timestamps, aggregationResultHolder);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final long[][] timestamps = blockValSetMap.get(_timeExpression).getLongValuesMV();
    final double[][] values = blockValSetMap.get(_valueExpression).getDoubleValuesMV();
    aggregateGroupByNumericValues(length, groupKeyArray, values, timestamps, groupByResultHolder);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException("MV not supported yet");
  }

  @Override
  public BaseTimeSeriesBuilder extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public BaseTimeSeriesBuilder extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public BaseTimeSeriesBuilder merge(BaseTimeSeriesBuilder ir1, BaseTimeSeriesBuilder ir2) {
    if (ir1 == null && ir2 == null) {
      return _factory.newTimeSeriesBuilder(_aggInfo, "TO_BE_REMOVED", _timeBuckets,
          BaseTimeSeriesBuilder.UNINITIALISED_TAG_NAMES, BaseTimeSeriesBuilder.UNINITIALISED_TAG_VALUES);
    } else if (ir1 == null) {
      return ir2;
    } else if (ir2 == null) {
      return ir1;
    }
    ir1.mergeAlignedSeriesBuilder(ir2);
    return ir1;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DoubleArrayList extractFinalResult(BaseTimeSeriesBuilder seriesBuilder) {
    Double[] doubleValues = seriesBuilder.build().getDoubleValues();
    return new DoubleArrayList(Arrays.asList(doubleValues));
  }

  @Override
  public String toExplainString() {
    return "TIME_SERIES_INDEX_AGGREGATE";
  }

  private void aggregateNumericValues(int length, double[][] values, long[][] timestamps,
      AggregationResultHolder resultHolder) {
    BaseTimeSeriesBuilder currentSeriesBuilder = resultHolder.getResult();
    if (currentSeriesBuilder == null) {
      currentSeriesBuilder = _factory.newTimeSeriesBuilder(_aggInfo, "TO_BE_REMOVED", _timeBuckets,
          BaseTimeSeriesBuilder.UNINITIALISED_TAG_NAMES, BaseTimeSeriesBuilder.UNINITIALISED_TAG_VALUES);
      resultHolder.setValue(currentSeriesBuilder);
    }

    for (int seriesIndex = 0; seriesIndex < length; seriesIndex++) {
      double[] seriesValues = values[seriesIndex];
      long[] seriesTimestamps = timestamps[seriesIndex];

      for (int dataPointIndex = 0; dataPointIndex < seriesValues.length; dataPointIndex++) {
        long timestamp = seriesTimestamps[dataPointIndex];
        double value = seriesValues[dataPointIndex];

        int timeIndex = (int) (((timestamp + _timeOffset) - _timeReferencePoint - 1) / _timeBucketDivisor);
        currentSeriesBuilder.addValueAtIndex(timeIndex, value, timestamp);
      }
    }
  }

  private void aggregateGroupByNumericValues(int length, int[] groupKeyArray, double[][] values, long[][] timestamps,
      GroupByResultHolder resultHolder) {
    for (int seriesIndex = 0; seriesIndex < length; seriesIndex++) {
      int groupId = groupKeyArray[seriesIndex];
      BaseTimeSeriesBuilder currentSeriesBuilder = resultHolder.getResult(groupId);
      if (currentSeriesBuilder == null) {
        currentSeriesBuilder = _factory.newTimeSeriesBuilder(_aggInfo, "TO_BE_REMOVED", _timeBuckets,
            BaseTimeSeriesBuilder.UNINITIALISED_TAG_NAMES, BaseTimeSeriesBuilder.UNINITIALISED_TAG_VALUES);
        resultHolder.setValueForKey(groupId, currentSeriesBuilder);
      }

      double[] seriesValues = values[seriesIndex];
      long[] seriesTimestamps = timestamps[seriesIndex];

      for (int dataPointIndex = 0; dataPointIndex < seriesValues.length; dataPointIndex++) {
        long timestamp = seriesTimestamps[dataPointIndex];
        double value = seriesValues[dataPointIndex];

        int timeIndex = (int) (((timestamp + _timeOffset) - _timeReferencePoint - 1) / _timeBucketDivisor);
        currentSeriesBuilder.addValueAtIndex(timeIndex, value, timestamp);
      }
    }
  }

  public static ExpressionContext create(String language, String valueExpressionStr, String timeExpressionStr,
    TimeUnit timeUnit, long offsetSeconds, TimeBuckets timeBuckets, AggInfo aggInfo) {
    ExpressionContext valueExpression = RequestContextUtils.getExpression(valueExpressionStr);
    ExpressionContext timeExpression = RequestContextUtils.getExpression(timeExpressionStr);
    List<ExpressionContext> arguments = new ArrayList<>();
    arguments.add(ExpressionContext.forLiteral(Literal.stringValue(language)));
    arguments.add(ExpressionContext.forLiteral(Literal.stringValue(aggInfo.getAggFunction())));
    arguments.add(valueExpression);
    arguments.add(timeExpression);
    arguments.add(ExpressionContext.forLiteral(Literal.stringValue(timeUnit.toString())));
    arguments.add(ExpressionContext.forLiteral(Literal.longValue(offsetSeconds)));
    arguments.add(ExpressionContext.forLiteral(Literal.longValue(timeBuckets.getTimeBuckets()[0])));
    arguments.add(ExpressionContext.forLiteral(Literal.longValue(timeBuckets.getBucketSize().getSeconds())));
    arguments.add(ExpressionContext.forLiteral(Literal.intValue(timeBuckets.getNumBuckets())));
    arguments.add(ExpressionContext.forLiteral(Literal.stringValue(AggInfo.serializeParams(aggInfo.getParams()))));
    FunctionContext functionContext = new FunctionContext(FunctionContext.Type.AGGREGATION,
        AggregationFunctionType.TIMESERIESINDEXAGGREGATE.getName(), arguments);
    return ExpressionContext.forFunction(functionContext);
  }
}
