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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.creator.TimeSeriesIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BaseTimeSeriesIndexCreator implements TimeSeriesIndexCreator {
  public static final int VERSION_1 = 1;
  public static final int HEADER_SIZE = 56;

  protected static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesIndexCreator.class);
  protected static final String TAG_SET_DICTIONARY_FILE_NAME = "tag.set.dictionary.buf";
  protected static final String TAG_DICTIONARY_FILE_NAME = "tag.dictionary.buf";
  protected static final String TAG_INVERTED_INDEX_FILE_NAME = "tag.inverted.index.buf";
  protected static final String KEY_VECTOR_VALUE_STORE_FILE_NAME = "key.vector.val.store.buf";
  protected static final String KEY_VECTOR_TIMESTAMP_STORE_FILE_NAME = "key.vector.ts.store.buf";
  protected static final int KEY_VECTOR_CHUNK_SIZE = 64 * 1024; // 64KB - tune this later based on typical vector sizes

  // LZ4 currently just adds overhead except for highly repetitive data. Once delta encoding is applied, it should
  // provide significant compression. See: https://altinity.com/blog/2019-7-new-encodings-to-improve-clickhouse
  protected final ChunkCompressionType _keyVectorCompressionType = ChunkCompressionType.LZ4;
  protected final File _tagSetDictionaryFile;
  protected final File _tagDictionaryFile;
  protected final File _tagInvertedIndexFile;
  protected final File _keyVectorValueStoreFile;
  protected final File _keyVectorTimestampStoreFile;
  protected int _tagSetLongestValueLength = 0;
  protected int _tagLongestValueLength = 0;
  protected int _numTimeSeries = 0;
  protected int _numTags = 0;
  private File _segmentIndexDir;
  private File _tmpDir;
  private String _columnName;

  public BaseTimeSeriesIndexCreator(File indexDir, String columnName) {
    _segmentIndexDir = indexDir;
    _columnName = columnName;
    _tmpDir = new File(indexDir, "tmp");
    try {
      FileUtils.forceMkdir(_tmpDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    _tagSetDictionaryFile = new File(_tmpDir, TAG_SET_DICTIONARY_FILE_NAME);
    _tagDictionaryFile = new File(_tmpDir, TAG_DICTIONARY_FILE_NAME);
    _tagInvertedIndexFile = new File(_tmpDir, TAG_INVERTED_INDEX_FILE_NAME);
    _keyVectorValueStoreFile = new File(_tmpDir, KEY_VECTOR_VALUE_STORE_FILE_NAME);
    _keyVectorTimestampStoreFile = new File(_tmpDir, KEY_VECTOR_TIMESTAMP_STORE_FILE_NAME);
  }

  public void convert() {
    throw new UnsupportedOperationException("Conversion only supported by MutableTimeSeriesIndexConverter");
  }

  @Override
  public void close()
      throws IOException {
    FileUtils.deleteQuietly(_tmpDir);
  }

  /**
   * Writes a compound index file from the subcomponents. Assumes that the subcomponents files already exist.
   * <p>
   * File format is: [header][tag set dictionary][tag dictionary][tag inverted index][key vector values store][key
   * vector timestamps store]
   * <p>
   * Header format is: [(int) version][(int) length of longest tag set][(int) length of longest value][(long) tag
   * set dictionary length][(long) tag dictionary length][(long) tag inverted index length][(long) key vector values
   * store length][(long) key vector timestamps store length]
   *
   * @return the length of the index file
   */
  protected long buildIndexFile() {
    LOGGER.info("Writing time series index file");
    File indexFile = new File(_segmentIndexDir,
        _columnName + V1Constants.Indexes.TIME_SERIES_INDEX_FILE_EXTENSION);
    try {
      // Create header
      ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
      headerBuffer.putInt(VERSION_1); // version
      headerBuffer.putInt(_tagSetLongestValueLength);
      headerBuffer.putInt(_tagLongestValueLength);
      headerBuffer.putInt(_numTags);
      headerBuffer.putLong(_tagSetDictionaryFile.length());
      headerBuffer.putLong(_tagDictionaryFile.length());
      headerBuffer.putLong(_tagInvertedIndexFile.length());
      headerBuffer.putLong(_keyVectorValueStoreFile.length());
      headerBuffer.putLong(_keyVectorTimestampStoreFile.length());
      headerBuffer.position(0);

      try (RandomAccessFile indexFileRAF = new RandomAccessFile(indexFile, "rw");
          RandomAccessFile tagSetDictionaryFileRAF = new RandomAccessFile(_tagSetDictionaryFile, "r");
          RandomAccessFile tagDictionaryFileRAF = new RandomAccessFile(_tagDictionaryFile, "r");
          RandomAccessFile tagInvertedIndexFileRAF = new RandomAccessFile(_tagInvertedIndexFile, "r");
          RandomAccessFile keyVectorValueStoreFileRAF = new RandomAccessFile(_keyVectorValueStoreFile, "r");
          RandomAccessFile keyVectorTimestampStoreFileRAF = new RandomAccessFile(_keyVectorTimestampStoreFile, "r");
          FileChannel indexFileChannel = indexFileRAF.getChannel();
          FileChannel tagSetDictionaryFileChannel = tagSetDictionaryFileRAF.getChannel();
          FileChannel tagDictionaryFileChannel = tagDictionaryFileRAF.getChannel();
          FileChannel tagInvertedIndexFileChannel = tagInvertedIndexFileRAF.getChannel();
          FileChannel keyVectorValueStoreFileChannel = keyVectorValueStoreFileRAF.getChannel();
          FileChannel keyVectorTimestampStoreFileChannel = keyVectorTimestampStoreFileRAF.getChannel()) {
        // Write header
        int written = indexFileChannel.write(headerBuffer);
        if (written != headerBuffer.limit()) {
          throw new IOException("Failed to write header to index file");
        }

        // Write subcomponents
        org.apache.pinot.common.utils.FileUtils.transferBytes(tagSetDictionaryFileChannel, 0,
            _tagSetDictionaryFile.length(), indexFileChannel);
        org.apache.pinot.common.utils.FileUtils.transferBytes(tagDictionaryFileChannel, 0,
            _tagDictionaryFile.length(), indexFileChannel);
        org.apache.pinot.common.utils.FileUtils.transferBytes(tagInvertedIndexFileChannel, 0,
            _tagInvertedIndexFile.length(), indexFileChannel);
        org.apache.pinot.common.utils.FileUtils.transferBytes(keyVectorValueStoreFileChannel, 0,
            _keyVectorValueStoreFile.length(), indexFileChannel);
        org.apache.pinot.common.utils.FileUtils.transferBytes(keyVectorTimestampStoreFileChannel, 0,
            _keyVectorTimestampStoreFile.length(), indexFileChannel);

        // fsync
        indexFileChannel.force(true);
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to create time series index file", e);
    } finally {
      try {
        FileUtils.forceDelete(_tmpDir);
      } catch (IOException e) {
        LOGGER.warn("Failed to cleanup temp directory: {}", _tmpDir, e);
      }
    }
    return indexFile.length();
  }
}
