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

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.realtime.impl.timeseries.MutableTimeSeriesKeyVectorStore;
import org.apache.pinot.segment.local.realtime.impl.timeseries.MutableTimeSeriesKeyVectorStoreOnHeap;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.local.segment.index.timeseries.TimeSeriesDataPoint;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.roaringbitmap.RoaringBitmap;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * OnHeapImmutableTimeSeriesIndexCreator creates an immutable time series index from raw data points. This direct
 * creation path temporarily stores all data points on heap, and is primarily intended to be used when testing.
 */
public class OnHeapImmutableTimeSeriesIndexCreator extends BaseTimeSeriesIndexCreator {
  List<TimeSeriesDataPoint> _dataPoints = new ArrayList<>();
  Map<String, RoaringBitmap> _tagIndex = new TreeMap<>();
  MutableTimeSeriesKeyVectorStore _keyVectorStore = new MutableTimeSeriesKeyVectorStoreOnHeap();

  /**
   * OnHeapImmutableTimeSeriesIndexCreator is intended to be used for testing only.
   */
  public OnHeapImmutableTimeSeriesIndexCreator(File indexDir, String columnName) {
    super(indexDir, columnName);
  }

  @Override
  public void add(@Nonnull Object value) {
    if (value instanceof String) {
      _dataPoints.add(TimeSeriesUtils.getDataPointFromString((String) value));
    } else {
      throw new IllegalArgumentException("Value must be a valid TimeSeriesDataPoint");
    }
  }

  @Override
  public void seal()
      throws IOException {
    if (_dataPoints.isEmpty()) {
      return;
    }

    // Sort based on tag set, so that the tag set dictionary is sorted
    _dataPoints.sort((o1, o2) -> {
      String tagSet1 = String.join(",", o1.getTagSet());
      String tagSet2 = String.join(",", o2.getTagSet());
      return tagSet1.compareTo(tagSet2);
    });

    Set<String> tagSetSet = new HashSet<>();
    for (TimeSeriesDataPoint dataPoint : _dataPoints) {
      tagSetSet.add(String.join(",", dataPoint.getTagSet()));
    }
    _numTimeSeries = tagSetSet.size();

    // Write the tag set dictionary and build in-memory tag index and key-vector store
    try (VarLengthValueWriter dictionaryWriter = new VarLengthValueWriter(_tagSetDictionaryFile, _numTimeSeries)) {
      int dictId = -1;
      List<String> currentTagSet = null;
      for (TimeSeriesDataPoint dataPoint : _dataPoints) {
        // Update dict id if needed
        if (!dataPoint.getTagSet().equals(currentTagSet)) {
          // Write the new tag set to the dictionary
          currentTagSet = dataPoint.getTagSet();
          dictId++;
          byte[] valueBytes = String.join(",", currentTagSet).getBytes(UTF_8);
          dictionaryWriter.add(valueBytes);
          LOGGER.info("writing " + String.join(",", currentTagSet) + " to dictionary");
          _tagSetLongestValueLength = Math.max(_tagSetLongestValueLength, valueBytes.length);
        }

        // Add dict id to posting lists for tags
        for (String tag : dataPoint.getTagSet()) {
          _tagIndex.computeIfAbsent(tag, k -> new RoaringBitmap()).add(dictId);
        }

        // Append the value and timestamp
        _keyVectorStore.add(dictId, dataPoint.getValue(), dataPoint.getTimestamp());
      }
    }
    LOGGER.info("Wrote tag set dictionary to disk with size {} and {} tagSets", _tagSetDictionaryFile.length(),
        _numTimeSeries);

    // Write the tag index
    _numTags = _tagIndex.size();
    try (VarLengthValueWriter dictionaryWriter = new VarLengthValueWriter(_tagDictionaryFile, _numTags);
        BitmapInvertedIndexWriter invertedIndexWriter = new BitmapInvertedIndexWriter(_tagInvertedIndexFile,
            _numTags)) {
      for (Map.Entry<String, RoaringBitmap> entry : _tagIndex.entrySet()) {
        byte[] keyBytes = entry.getKey().getBytes(UTF_8);
        dictionaryWriter.add(keyBytes);
        invertedIndexWriter.add(entry.getValue());
        _tagLongestValueLength = Math.max(_tagLongestValueLength, keyBytes.length);
      }
    }
    LOGGER.info("Wrote tag index to disk with dictionary size {} and inverted index size {} and {} tags",
        _tagDictionaryFile.length(), _tagInvertedIndexFile.length(), _numTags);

    // Write the key vector store
    try (VarByteChunkForwardIndexWriterV4 valueWriter = new VarByteChunkForwardIndexWriterV4(_keyVectorValueStoreFile,
        _keyVectorCompressionType, KEY_VECTOR_CHUNK_SIZE);
        VarByteChunkForwardIndexWriterV4 timestampWriter = new VarByteChunkForwardIndexWriterV4(
            _keyVectorTimestampStoreFile, _keyVectorCompressionType, KEY_VECTOR_CHUNK_SIZE)) {
      for (int i = 0; i < _numTimeSeries; i++) {
        Pair<DoubleArrayList, LongArrayList> series = _keyVectorStore.getSeries(i);
        valueWriter.putDoubleMV(series.left().toDoubleArray());
        timestampWriter.putLongMV(series.right().toLongArray());
      }
    }

    LOGGER.info("Wrote key vector store to disk with values size {} and timestamps size {}",
        _keyVectorValueStoreFile.length(), _keyVectorTimestampStoreFile.length());

    long fileSize = buildIndexFile();
    LOGGER.info("Wrote time series index to disk with size {}", fileSize);
  }
}
