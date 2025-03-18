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
package org.apache.pinot.queries;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.provider.DefaultTableDataManagerProvider;
import org.apache.pinot.core.data.manager.provider.TableDataManagerProvider;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.timeseries.TimeSeriesOperatorUtils;
import org.apache.pinot.core.query.aggregation.function.TimeSeriesIndexAggregationFunction;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimeSeriesIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactoryProvider;
import org.apache.pinot.tsdb.spi.series.builders.MaxTimeSeriesBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class TimeSeriesIndexQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TimeSeriesIndexQueriesTest");
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String TIME_SERIES_LANGUAGE_NAME = "m3";
  private static final String QUERY_EXECUTOR_CONFIG_PATH = "conf/query-executor.properties";
  private final List<String> _segmentNames = new ArrayList<>();

  private static final int NUM_ROWS = 4;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  private QueryExecutor _queryExecutor;

  @Override
  protected String getFilter() {
    return "WHERE time_series_match(STRING_COLUMN, 'name=pinotmetric,tag1=queries')";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  GenericRow addDataPoint(List<String> tags, double value, long timestamp) {
    GenericRow record = new GenericRow();
    record.putValue(STRING_COLUMN, TimeSeriesUtils.getDataPointAsString(tags, value, timestamp));
    return record;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    ServerMetrics.register(mock(ServerMetrics.class));
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    rows.add(addDataPoint(List.of("name=pinotmetric", "tag1=queries"), 1.0, 10L));
    rows.add(addDataPoint(List.of("name=pinotmetric", "tag1=exceptions", "tag2=dc1"), 2.0, 10L));
    rows.add(addDataPoint(List.of("name=pinotmetric", "tag1=queries"), 3.0, 20L));
    rows.add(addDataPoint(List.of("name=pinotmetric", "tag1=queries"), 4.0, 30L));

    Schema schema = new Schema.SchemaBuilder().addMultiValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING).build();

    TimeSeriesIndexConfig timeSeriesIndexConfig = TimeSeriesIndexConfig.ENABLED;
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("timeseries", timeSeriesIndexConfig.toJsonNode());
    List<FieldConfig> fieldConfigList =
        Collections.singletonList(new FieldConfig.Builder(STRING_COLUMN).withIndexes(indexes).build());
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setFieldConfigList(fieldConfigList).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
    _segmentNames.add(driver.getSegmentName());

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = List.of(immutableSegment);

    // Setup table data manager for query executor tests
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(INDEX_DIR.getAbsolutePath());
    TableDataManagerProvider tableDataManagerProvider = new DefaultTableDataManagerProvider();
    tableDataManagerProvider.init(instanceDataManagerConfig, mock(HelixManager.class), new SegmentLocks(), null);
    TableDataManager tableDataManager = tableDataManagerProvider.getTableDataManager(tableConfig);
    tableDataManager.start();
    for (IndexSegment indexSegment : _indexSegments) {
      tableDataManager.addSegment((ImmutableSegment) indexSegment);
    }
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(TABLE_NAME)).thenReturn(tableDataManager);

    // Set up the query executor
    URL resourceUrl = getClass().getClassLoader().getResource(QUERY_EXECUTOR_CONFIG_PATH);
    Assert.assertNotNull(resourceUrl);
    PropertiesConfiguration queryExecutorConfig = CommonsConfigurationUtils.fromFile(new File(resourceUrl.getFile()));
    _queryExecutor = new ServerQueryExecutorV1Impl();
    _queryExecutor.init(new PinotConfiguration(queryExecutorConfig), instanceDataManager, ServerMetrics.get());

    // Setup time series builder factory
    TimeSeriesBuilderFactoryProvider.registerSeriesBuilderFactory(TIME_SERIES_LANGUAGE_NAME,
        new SimpleTimeSeriesBuilderFactory());
  }

  // TODO add tests for transform values and timestamps functions

  @Test
  public void testTimeSeriesFilterAndTagSet() {
    // Test tagSet extraction for single series
    Operator<SelectionResultsBlock> selectOperator = getOperator(
        "SELECT timeSeriesIndexTagSet(stringColumn) FROM testTable WHERE time_series_match(stringColumn, "
            + "'name=pinotmetric,tag1=queries')");
    SelectionResultsBlock block = selectOperator.nextBlock();
    List<Object[]> rows = block.getRows();
    assertNotNull(rows);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0].toString(), "name=pinotmetric,tag1=queries");

    // Test tagSet extraction for multiple series
    selectOperator = getOperator(
        "SELECT timeSeriesIndexTagSet(stringColumn) FROM testTable WHERE time_series_match(stringColumn, "
            + "'name=pinotmetric')");
    block = selectOperator.nextBlock();
    rows = block.getRows();
    assertNotNull(rows);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[0].toString(), "name=pinotmetric,tag1=exceptions,tag2=dc1");
    assertEquals(rows.get(1)[0].toString(), "name=pinotmetric,tag1=queries");

    // Test tagSet extraction with single tag filter
    selectOperator = getOperator(
        "SELECT timeSeriesIndexTagSet(stringColumn, 'tag1') FROM testTable WHERE time_series_match(stringColumn, "
            + "'name=pinotmetric')");
    block = selectOperator.nextBlock();
    rows = block.getRows();
    assertNotNull(rows);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[0].toString(), "tag1=exceptions");
    assertEquals(rows.get(1)[0].toString(), "tag1=queries");

    // Test tagSet extraction with multiple tag filters
    selectOperator = getOperator(
        "SELECT timeSeriesIndexTagSet(stringColumn, 'tag1,tag2') FROM testTable WHERE time_series_match(stringColumn, "
            + "'name=pinotmetric')");
    block = selectOperator.nextBlock();
    rows = block.getRows();
    assertNotNull(rows);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[0].toString(), "tag1=exceptions,tag2=dc1");
    assertEquals(rows.get(1)[0].toString(), "tag1=queries");
  }

  @Test
  public void testTimeSeriesIndexAggregate() {
    Operator<AggregationResultsBlock> aggregationOperator = getOperator(
        "SELECT timeSeriesIndexAggregate('m3', 'MAX', timeSeriesIndexValues(stringColumn), timeSeriesIndexTimestamps"
            + "(stringColumn), 'SECONDS', '0', '0', '10', '5', '') FROM testTable WHERE time_series_match"
            + "(stringColumn, 'name=pinotmetric')");
    List<Object[]> rows = aggregationOperator.nextBlock().getRows();
    assertNotNull(rows);
    assertEquals(rows.size(), 1);

    assertTrue(rows.get(0)[0] instanceof MaxTimeSeriesBuilder);
    TimeSeries ts1 = ((MaxTimeSeriesBuilder) rows.get(0)[0]).build();
    assertEquals(ts1.getDoubleValues(), new Double[]{null, 2.0, 3.0, 4.0, null});
    assertEquals(ts1.getTimeBuckets().getTimeBuckets(), new Long[]{0L, 10L, 20L, 30L, 40L});
  }

  @Test
  public void testTimeSeriesIndexAggregateQueryExecutorAgg() {
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(0, Duration.ofSeconds(10), 5);
    QueryContext queryContext = getQueryContext(timeBuckets, null);
    queryContext.setEndTimeMs(System.currentTimeMillis() + 10000);

    ServerQueryRequest serverQueryRequest =
        new ServerQueryRequest(queryContext, _segmentNames, new HashMap<>(), ServerMetrics.get());
    InstanceResponseBlock instanceResponse =
        _queryExecutor.execute(serverQueryRequest, Executors.newFixedThreadPool(1));

    assertTrue(instanceResponse.getResultsBlock() instanceof AggregationResultsBlock);
    TimeSeriesBlock timeSeriesBlock = TimeSeriesOperatorUtils.buildTimeSeriesBlock(timeBuckets,
        (AggregationResultsBlock) instanceResponse.getResultsBlock());
    assertEquals(timeSeriesBlock.getSeriesMap().size(), 1);
    assertEquals(timeSeriesBlock.getSeriesMap().values().iterator().next().size(), 1);

    TimeSeries aggregatedTimeSeries = timeSeriesBlock.getSeriesMap().values().iterator().next().get(0);
    assertNull(aggregatedTimeSeries.getDoubleValues()[0]);
    assertEquals(aggregatedTimeSeries.getDoubleValues()[1], 2.0);
    assertEquals(aggregatedTimeSeries.getDoubleValues()[2], 3.0);
    assertEquals(aggregatedTimeSeries.getDoubleValues()[3], 4.0);
    assertNull(aggregatedTimeSeries.getDoubleValues()[4]);
  }

  @Test
  public void testTimeSeriesIndexAggregateQueryExecutorGroupBy() {
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(0, Duration.ofSeconds(10), 5);
    QueryContext queryContext = getQueryContext(timeBuckets, "timeSeriesIndexTagSet(stringColumn)");
    queryContext.setEndTimeMs(System.currentTimeMillis() + 10000);

    ServerQueryRequest serverQueryRequest =
        new ServerQueryRequest(queryContext, _segmentNames, new HashMap<>(), ServerMetrics.get());
    InstanceResponseBlock instanceResponse =
        _queryExecutor.execute(serverQueryRequest, Executors.newFixedThreadPool(1));

    assertTrue(instanceResponse.getResultsBlock() instanceof GroupByResultsBlock);
    TimeSeriesBlock timeSeriesBlock = TimeSeriesOperatorUtils.buildTimeSeriesBlock(timeBuckets,
        (GroupByResultsBlock) instanceResponse.getResultsBlock());
    assertEquals(timeSeriesBlock.getSeriesMap().size(), 2);
    List<List<TimeSeries>> timeSeriesList = new ArrayList<>(timeSeriesBlock.getSeriesMap().values());

    TimeSeries aggregatedTimeSeries = timeSeriesList.get(0).get(0);
    assertEquals(aggregatedTimeSeries.getTagNames(), List.of("name", "tag1", "tag2"));
    assertEquals(List.of(aggregatedTimeSeries.getTagValues()), List.of("pinotmetric", "exceptions", "dc1"));
    assertNull(aggregatedTimeSeries.getDoubleValues()[0]);
    assertEquals(aggregatedTimeSeries.getDoubleValues()[1], 2.0);
    assertNull(aggregatedTimeSeries.getDoubleValues()[2]);
    assertNull(aggregatedTimeSeries.getDoubleValues()[3]);
    assertNull(aggregatedTimeSeries.getDoubleValues()[4]);

    aggregatedTimeSeries = timeSeriesList.get(1).get(0);
    assertEquals(aggregatedTimeSeries.getTagNames(), List.of("name", "tag1"));
    assertEquals(List.of(aggregatedTimeSeries.getTagValues()), List.of("pinotmetric", "queries"));
    assertNull(aggregatedTimeSeries.getDoubleValues()[0]);
    assertEquals(aggregatedTimeSeries.getDoubleValues()[1], 1.0);
    assertEquals(aggregatedTimeSeries.getDoubleValues()[2], 3.0);
    assertEquals(aggregatedTimeSeries.getDoubleValues()[3], 4.0);
    assertNull(aggregatedTimeSeries.getDoubleValues()[4]);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private QueryContext getQueryContext(TimeBuckets timeBuckets, String groupByExpression) {
    List<ExpressionContext> groupByExpList = Collections.emptyList();
    if (groupByExpression != null) {
      groupByExpList = List.of(RequestContextUtils.getExpression(groupByExpression));
    }
    String timestampsExpression = "timeSeriesIndexTimestamps(stringColumn)";
    String valuesExpression = "timeSeriesIndexValues(stringColumn)";
    ExpressionContext aggregateExpr =
        TimeSeriesIndexAggregationFunction.create(TIME_SERIES_LANGUAGE_NAME, valuesExpression, timestampsExpression,
            TimeUnit.SECONDS, 0, timeBuckets, new AggInfo("MAX", false, Collections.emptyMap()));
    FilterContext filterContext = RequestContextUtils.getFilter(
        RequestContextUtils.getExpression("time_series_match(stringColumn, 'name=pinotmetric')"));
    QueryContext.Builder builder = new QueryContext.Builder();
    builder.setTableName(TABLE_NAME);
    builder.setAliasList(Collections.emptyList());
    builder.setSelectExpressions(List.of(aggregateExpr));
    if (!groupByExpList.isEmpty()) {
      builder.setGroupByExpressions(groupByExpList);
    }
    builder.setFilter(filterContext);
    builder.setLimit(Integer.MAX_VALUE);
    return builder.build();
  }
}
