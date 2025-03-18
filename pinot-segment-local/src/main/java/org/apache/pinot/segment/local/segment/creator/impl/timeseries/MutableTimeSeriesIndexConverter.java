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

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.File;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.timeseries.MutableTimeSeriesKeyVectorStore;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Helper class to convert the time series index from the mutable format to the immutable format.
 */
public class MutableTimeSeriesIndexConverter extends BaseTimeSeriesIndexCreator {
  private StringOffHeapMutableDictionary _mutableTagSetDictionary;
  private Map<String, RoaringBitmap> _mutableTagIndex;
  private MutableTimeSeriesKeyVectorStore _mutableKeyVectorStore;

  /**
   * Create a converter to convert the mutable time series index to the immutable format. Requires the mutable
   * data structures to convert.
   * @param segmentIndexDir writes the converted immutable index to this directory
   * @param columnName
   */
  public MutableTimeSeriesIndexConverter(File segmentIndexDir, String columnName, int numTimeSeries,
      StringOffHeapMutableDictionary tagSetDictionary, Map<String, RoaringBitmap> tagIndex,
      MutableTimeSeriesKeyVectorStore keyVectorStore) {
    super(segmentIndexDir, columnName);
    _numTimeSeries = numTimeSeries;
    _mutableTagSetDictionary = tagSetDictionary;
    _mutableTagIndex = tagIndex;
    _mutableKeyVectorStore = keyVectorStore;
  }

  @Override
  public void convert() {
    LOGGER.info("Started writing time series index to disk");
    // since dict ids change after sorting, map the ids stored by the tag index and key vector store
    int[] mapping = writeTagSetDictionary();
    int[] oldToNewMap = new int[mapping.length];
    for (int i = 0; i < oldToNewMap.length; i++) {
      oldToNewMap[mapping[i]] = i;
    }
    writeTagInvertedIndex(oldToNewMap);
    writeKeyVectorStore(oldToNewMap);
    long fileSize = buildIndexFile();
    LOGGER.info("Wrote time series index to disk with size {}", fileSize);
  }

  /**
   * Writes the tag set dictionary to disk.
   * @return the mapping from the sorted index to the original index
   */
  private int[] writeTagSetDictionary() {
    String[] dictionaryValues = new String[_numTimeSeries];
    int[] idMap = IntStream.range(0, _numTimeSeries).toArray();
    for (int i = 0; i < _numTimeSeries; i++) {
      dictionaryValues[i] = _mutableTagSetDictionary.get(i);
    }

    // Sort the dictionary values and generate a mapping from the sorted index to the original index
    Arrays.quickSort(0, _numTimeSeries, (i1, i2) -> dictionaryValues[i1].compareTo(dictionaryValues[i2]), (i, j) -> {
      String tempVal = dictionaryValues[i];
      dictionaryValues[i] = dictionaryValues[j];
      dictionaryValues[j] = tempVal;
      int tempId = idMap[i];
      idMap[i] = idMap[j];
      idMap[j] = tempId;
    });

    // Write the dictionary values
    try (VarLengthValueWriter dictionaryWriter = new VarLengthValueWriter(_tagSetDictionaryFile, _numTimeSeries)) {
      for (String value : dictionaryValues) {
        byte[] valueBytes = value.getBytes(UTF_8);
        LOGGER.info("writing " + value + " to dictionary");
        dictionaryWriter.add(valueBytes);
        _tagSetLongestValueLength = Math.max(_tagSetLongestValueLength, valueBytes.length);
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to create tag set dictionary file during time series index conversion", e);
    }
    LOGGER.info("Wrote tag set dictionary to disk with size {} and {} tagSets", _tagSetDictionaryFile.length(),
        _numTimeSeries);
    return idMap;
  }

  /**
   * Writes the tag inverted index to disk.
   */
  private void writeTagInvertedIndex(int[] oldToNewMap) {
    _numTags = _mutableTagIndex.size();
    TreeMap<String, RoaringBitmap> sortedTagIndex = new TreeMap<>(_mutableTagIndex);
    try (VarLengthValueWriter dictionaryWriter = new VarLengthValueWriter(_tagDictionaryFile, _numTags);
        BitmapInvertedIndexWriter invertedIndexWriter = new BitmapInvertedIndexWriter(_tagInvertedIndexFile,
            _numTags)) {
      for (Map.Entry<String, RoaringBitmap> entry : sortedTagIndex.entrySet()) {
        byte[] keyBytes = entry.getKey().getBytes(UTF_8);
        dictionaryWriter.add(keyBytes);
        RoaringBitmap bitmap = entry.getValue();
        RoaringBitmap newBitmap = new RoaringBitmap();
        bitmap.forEach((IntConsumer) i -> newBitmap.add(oldToNewMap[i]));
        invertedIndexWriter.add(newBitmap);
        _tagLongestValueLength = Math.max(_tagLongestValueLength, keyBytes.length);
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to create tag index during time series index conversion", e);
    }
    LOGGER.info("Wrote tag index to disk with dictionary size {} and inverted index size {} and {} tags",
        _tagDictionaryFile.length(), _tagInvertedIndexFile.length(), _numTags);
  }

  /**
   * Writes the key vector store to disk.
   */
  private void writeKeyVectorStore(int[] oldToNewMap) {
    try (VarByteChunkForwardIndexWriterV4 valueWriter = new VarByteChunkForwardIndexWriterV4(_keyVectorValueStoreFile,
        _keyVectorCompressionType, KEY_VECTOR_CHUNK_SIZE);
        VarByteChunkForwardIndexWriterV4 timestampWriter = new VarByteChunkForwardIndexWriterV4(
            _keyVectorTimestampStoreFile, _keyVectorCompressionType, KEY_VECTOR_CHUNK_SIZE)) {
      for (int i = 0; i < _numTimeSeries; i++) {
        Pair<DoubleArrayList, LongArrayList> series = _mutableKeyVectorStore.getSeries(oldToNewMap[i]);
        valueWriter.putDoubleMV(series.left().toDoubleArray());
        timestampWriter.putLongMV(series.right().toLongArray());
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to create key vector store during time series index conversion", e);
    }
    LOGGER.info("Wrote key vector store to disk with values size {} and timestamps size {}",
        _keyVectorValueStoreFile.length(), _keyVectorTimestampStoreFile.length());
  }
}
