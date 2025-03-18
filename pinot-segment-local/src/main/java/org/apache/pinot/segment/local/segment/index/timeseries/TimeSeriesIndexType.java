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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.timeseries.MutableTimeSeriesIndexImpl;
import org.apache.pinot.segment.local.segment.creator.impl.timeseries.ImmutableTimeSeriesIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.timeseries.OnHeapImmutableTimeSeriesIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.TimeSeriesIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.timeseries.ImmutableTimeSeriesIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.TimeSeriesIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.TimeSeriesIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TimeSeriesIndexConfig;
import org.apache.pinot.spi.data.Schema;


public class TimeSeriesIndexType
    extends AbstractIndexType<TimeSeriesIndexConfig, TimeSeriesIndexReader, TimeSeriesIndexCreator> {
  public static final String INDEX_DISPLAY_NAME = "timeseries";
  private static final List<String> EXTENSIONS =
      Collections.singletonList(V1Constants.Indexes.TIME_SERIES_INDEX_FILE_EXTENSION);


  protected TimeSeriesIndexType() {
    super(StandardIndexes.TIME_SERIES_ID);
  }

  @Override
  public Class<TimeSeriesIndexConfig> getIndexConfigClass() {
    return TimeSeriesIndexConfig.class;
  }

  @Override
  public TimeSeriesIndexConfig getDefaultConfig() {
    return TimeSeriesIndexConfig.DISABLED;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, TimeSeriesIndexConfig indexConfig) {
    if (indexConfig.isDisabled()) {
      return null;
    }
    return new MutableTimeSeriesIndexImpl(context.getConsumerDir(), context.getSegmentName(),
        context.getMemoryManager(), context.getFieldSpec().getName(), indexConfig);
  }

  @Override
  public TimeSeriesIndexCreator createIndexCreator(IndexCreationContext context, TimeSeriesIndexConfig indexConfig)
      throws Exception {
    if (context.isRealtimeConversion()) {
      return new ImmutableTimeSeriesIndexCreator(context.getConsumerDir(), context.getIndexDir(),
          context.getFieldSpec().getName(), indexConfig);
    }
    return new OnHeapImmutableTimeSeriesIndexCreator(context.getIndexDir(), context.getFieldSpec().getName());
//    throw new UnsupportedOperationException(
//        "Only realtime segment conversion supported for building time series index");
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new TimeSeriesIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  @Override
  protected ColumnConfigDeserializer<TimeSeriesIndexConfig> createDeserializer() {
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass());
  }

  @Override
  protected IndexReaderFactory<TimeSeriesIndexReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<TimeSeriesIndexConfig, TimeSeriesIndexReader> {
    public static final TimeSeriesIndexType.ReaderFactory INSTANCE = new TimeSeriesIndexType.ReaderFactory();

    private ReaderFactory() {
    }

    @Override
    protected IndexType<TimeSeriesIndexConfig, TimeSeriesIndexReader, ?> getIndexType() {
      return StandardIndexes.timeSeries();
    }

    @Override
    protected TimeSeriesIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        TimeSeriesIndexConfig indexConfig) {
      return new ImmutableTimeSeriesIndexReader(dataBuffer);
    }
  }
}
