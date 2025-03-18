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
package org.apache.pinot.segment.local.segment.creator.impl.timeseries;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.segment.local.segment.index.timeseries.TimeSeriesDataPoint;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.TimeSeriesIndexCreator;
import org.apache.pinot.spi.config.table.TimeSeriesIndexConfig;


/**
 * TimeSeriesIndexCreatorImpl is an implementation of {@link TimeSeriesIndexCreator} for creating the immutable
 * time series index. As the time series index removes the row concept, and is intended to be used without storing
 * the forward index, the index is usually 'created' by copying the existing index from the consumer directory to the
 * index directory.
 */
public class ImmutableTimeSeriesIndexCreator extends BaseTimeSeriesIndexCreator {
  private final File _consumerDir;
  private final File _indexDir;
  private final String _columnName;
  private final List<TimeSeriesDataPoint> _dataPoints = new ArrayList<>();

  public ImmutableTimeSeriesIndexCreator(File consumerDir, File indexDir,
      String columnName, TimeSeriesIndexConfig indexConfig) {
    super(indexDir, columnName);
    _consumerDir = consumerDir;
    _indexDir = indexDir;
    _columnName = columnName;
  }

  @Override
  public void seal()
      throws IOException {
    String indexFileName = _columnName + V1Constants.Indexes.TIME_SERIES_INDEX_FILE_EXTENSION;
    File mutableIndexDir = getMutableIndexDir(_indexDir, _consumerDir);
    File timeSeriesIndexFile = new File(mutableIndexDir, indexFileName);
    try (RandomAccessFile sourceFile = new RandomAccessFile(timeSeriesIndexFile, "r");
        RandomAccessFile destFile = new RandomAccessFile(new File(_indexDir, indexFileName), "rw");
        FileChannel sourceFileChannel = sourceFile.getChannel(); FileChannel destFileChannel = destFile.getChannel()) {
      destFileChannel.write(new ByteBuffer[0]);
      FileUtils.transferBytes(sourceFileChannel, 0, sourceFileChannel.size(), destFileChannel);
      destFileChannel.force(true);
    }

    // remove source
    org.apache.commons.io.FileUtils.deleteQuietly(timeSeriesIndexFile);
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }

  private File getMutableIndexDir(File indexDir, File consumerDir) {
    String segmentName = getSegmentName(indexDir);
    return new File(consumerDir, segmentName);
  }

  // TODO this is common to LuceneTextIndexCreator, move to a Utils file
  private String getSegmentName(File indexDir) {
    // tmpSegmentName format: tmp-tableName__9__1__20240227T0254Z-1709002522086
    String tmpSegmentName = indexDir.getParentFile().getName();
    return tmpSegmentName.substring(tmpSegmentName.indexOf("tmp-") + 4, tmpSegmentName.lastIndexOf('-'));
  }
}
