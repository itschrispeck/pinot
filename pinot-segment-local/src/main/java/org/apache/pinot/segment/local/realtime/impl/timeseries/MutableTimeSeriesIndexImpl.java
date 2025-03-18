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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOffHeapMutableDictionary;
import org.apache.pinot.segment.local.segment.creator.impl.timeseries.MutableTimeSeriesIndexConverter;
import org.apache.pinot.segment.local.segment.index.timeseries.TimeSeriesDataPoint;
import org.apache.pinot.segment.local.utils.timeseries.TimeSeriesUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexUtil;
import org.apache.pinot.segment.spi.index.mutable.MutableTimeSeriesIndex;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.TimeSeriesIndexConfig;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * MutableTimeSeriesIndexImpl is a mutable time series index implementation. The index relies on having time series
 * data points added to it, which are generated with the TimeSeriesTransformer.
 *
 * At a high level the index contains three components: a forward dictionary encoded index, a inverted index, and a
 * key-vector store.
 * <ul>
 *   <li> Forward Dictionary: the forward dictionary is a mapping from tagSet to an internal time series id. The time
 *   series id is is used as the key in the key-vector store, and in the posting lists of the inverted index.
 *   <li> Inverted Index: the inverted index is a mapping from tag to a bitmap of time series ids. This provides fast
 *   intersection of time series ids for a given set of tags.
 *   <li> Key-Vector Store: the key-vector store is a mapping from time series id to a list of values and timestamps.
 *   This is used to store the actual time series data.
 * </ul>
 * Components are intended to be pluggable. For example, a ngram index can enable wildcard search over tags.
 */
public class MutableTimeSeriesIndexImpl implements MutableTimeSeriesIndex {
  private final static int ESTIMATED_CARDINALITY = 100000; // tune later
  private final static int MAX_OVERFLOW_HASH_SIZE = 10000; // tune later
  private final static int AVERAGE_STRING_LENGTH = 120; // tune later

  /**
   * Dictionary for tag set. Used to check if a time series exists for the given tag set already, or if a new one
   * should be created. The dictionary id is reused as the time series id.
   */
  private final StringOffHeapMutableDictionary _tagSetDictionary;

  /**
   * On-heap tags index, which maps a tag to a bitmap of time series ids. The bitmap is used to quickly find the
   * intersection of time series ids for some given tags.
   */
  private final Map<String, RoaringBitmap> _tagIndex;

  /**
   * Key vector store for the raw time series data.
   */
  private final MutableTimeSeriesKeyVectorStore _keyVectorStore;

  /**
   * Number of time series added so far.
   */
  private int _numTimeSeries = 0;
  private final File _segmentIndexDir;
  private final String _columnName;

  public MutableTimeSeriesIndexImpl(File consumerDir, String segmentName, PinotDataBufferMemoryManager memoryManager,
      String columnName, TimeSeriesIndexConfig indexConfig) {
    _segmentIndexDir = new File(consumerDir, segmentName);
    _columnName = columnName;
    String dictionaryAllocationContext =
        IndexUtil.buildAllocationContext(segmentName, _columnName, V1Constants.Dict.FILE_EXTENSION);
    _tagSetDictionary = new StringOffHeapMutableDictionary(ESTIMATED_CARDINALITY, MAX_OVERFLOW_HASH_SIZE, memoryManager,
        dictionaryAllocationContext, AVERAGE_STRING_LENGTH);
    _tagIndex = new HashMap<>();
    _keyVectorStore = new MutableTimeSeriesKeyVectorStoreOnHeap();
  }

  @Override
  public void add(@Nonnull Object value) {
    if (value instanceof String) {
      add(TimeSeriesUtils.getDataPointFromString((String) value));
    } else {
      throw new IllegalArgumentException("Value must be a valid TimeSeriesDataPoint");
    }
  }

  private void add(TimeSeriesDataPoint dataPoint) {
    String key = String.join(",", dataPoint.getTagSet());
    int timeSeriesId = _tagSetDictionary.index(key);
    if (timeSeriesId == _numTimeSeries) {
      // new time series
      _numTimeSeries++;
      updateTagsIndex(dataPoint.getTagSet(), timeSeriesId);
    }
    storeSeriesData(timeSeriesId, dataPoint.getValue(), dataPoint.getTimestamp());
  }

  /**
   * Updates the tags index for all tags with the time series id.
   */
  private void updateTagsIndex(List<String> tags, int timeSeriesId) {
    for (String tag : tags) {
      _tagIndex.computeIfAbsent(tag, g -> new RoaringBitmap()).add(timeSeriesId);
    }
  }

  /**
   * Append a time series data point for the given time series id.
   */
  private void storeSeriesData(int timeSeriesId, Double value, long timestamp) {
    if (value == null) {
      return; // drop nulls
    }
    _keyVectorStore.add(timeSeriesId, value, timestamp);
  }

  @Override
  public MutableRoaringBitmap getMatchingTimeSeriesIds(List<String> tags) {
    RoaringBitmap intersection = null;
    // intuition: tags often happen to be ordered by cardinality, so start with the most selective tag
    for (int i = tags.size() - 1; i >= 0; i--) {
      String tag = tags.get(i);
      RoaringBitmap matchingSeriesIds = _tagIndex.get(tag);
      if (matchingSeriesIds == null) {
        return new MutableRoaringBitmap();
      }
      if (intersection == null) {
        intersection = matchingSeriesIds.clone();
      } else {
        intersection.and(_tagIndex.get(tag));
      }
      if (intersection.isEmpty()) {
        return new MutableRoaringBitmap(); // no time series for the tag search
      }
    }
    return intersection == null ? new MutableRoaringBitmap() : intersection.toMutableRoaringBitmap();
  }

  @Override
  public String getTagSet(int seriesId) {
    return _tagSetDictionary.get(seriesId);
  }

  @Override
  public DoubleArrayList getValues(int timeSeriesId, ForwardIndexReaderContext context) {
    return _keyVectorStore.getValues(timeSeriesId);
  }

  @Override
  public LongArrayList getTimestamps(int timeSeriesId, ForwardIndexReaderContext context) {
    return _keyVectorStore.getTimestamps(timeSeriesId);
  }

  @Override
  public void close()
      throws IOException {
    _tagSetDictionary.doClose();
  }

  @Override
  public void commit() {
    MutableTimeSeriesIndexConverter converter =
        new MutableTimeSeriesIndexConverter(_segmentIndexDir, _columnName, _numTimeSeries, _tagSetDictionary, _tagIndex,
            _keyVectorStore);
    converter.convert();
  }
}
