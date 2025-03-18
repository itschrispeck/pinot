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
package org.apache.pinot.segment.local.segment.index.readers.timeseries;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.segment.creator.impl.timeseries.BaseTimeSeriesIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.TimeSeriesIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class ImmutableTimeSeriesIndexReader
    implements TimeSeriesIndexReader<ImmutableTimeSeriesIndexReader.TimeSeriesReaderContext> {

  private final int _version;
  private final StringDictionary _tagSetDictionary;
  private final StringDictionary _tagDictionary;
  private final BitmapInvertedIndexReader _tagIndex;
  private final VarByteChunkForwardIndexReaderV4 _keyVectorValuesReader;
  private final VarByteChunkForwardIndexReaderV4 _keyVectorTimestampsReader;

  public ImmutableTimeSeriesIndexReader(PinotDataBuffer dataBuffer) {
    _version = dataBuffer.getInt(0);
    Preconditions.checkState(_version == BaseTimeSeriesIndexCreator.VERSION_1,
        "Unsupported time series index version: %s", _version);

    int tagSetLongestValueLength = dataBuffer.getInt(4);
    int tagLongestValueLength = dataBuffer.getInt(8);
    int numTags = dataBuffer.getInt(12);
    long tagSetDictionarySize = dataBuffer.getLong(16);
    long tagDictionarySize = dataBuffer.getLong(24);
    long tagInvertedIndexSize = dataBuffer.getLong(32);
    long keyVectorValueReaderSize = dataBuffer.getLong(40);
    long keyVectorTimestampReaderSize = dataBuffer.getLong(48);

    long tagSetDictionaryOffset = BaseTimeSeriesIndexCreator.HEADER_SIZE;
    long tagDictionaryOffset = tagSetDictionaryOffset + tagSetDictionarySize;
    long tagInvertedIndexOffset = tagDictionaryOffset + tagDictionarySize;
    long keyVectorValueReaderOffset = tagInvertedIndexOffset + tagInvertedIndexSize;
    long keyVectorTimestampReaderOffset = keyVectorValueReaderOffset + keyVectorValueReaderSize;

    _tagSetDictionary =
        new StringDictionary(dataBuffer.view(tagSetDictionaryOffset, tagDictionaryOffset, ByteOrder.BIG_ENDIAN), -1,
            tagSetLongestValueLength);
    _tagDictionary =
        new StringDictionary(dataBuffer.view(tagDictionaryOffset, tagInvertedIndexOffset, ByteOrder.BIG_ENDIAN), -1,
            tagLongestValueLength);
    _tagIndex = new BitmapInvertedIndexReader(
        dataBuffer.view(tagInvertedIndexOffset, keyVectorValueReaderOffset, ByteOrder.BIG_ENDIAN), numTags);
    _keyVectorValuesReader = new VarByteChunkForwardIndexReaderV4(
        dataBuffer.view(keyVectorValueReaderOffset, keyVectorTimestampReaderOffset, ByteOrder.BIG_ENDIAN),
        FieldSpec.DataType.STRING, false);
    _keyVectorTimestampsReader = new VarByteChunkForwardIndexReaderV4(
        dataBuffer.view(keyVectorTimestampReaderOffset, keyVectorTimestampReaderOffset + keyVectorTimestampReaderSize,
            ByteOrder.BIG_ENDIAN), FieldSpec.DataType.STRING, false);
  }

  @Override
  public void close()
      throws IOException {
    // TODO
  }

  @Override
  public MutableRoaringBitmap getMatchingTimeSeriesIds(List<String> tags) {
    List<Integer> dictIds = new ArrayList<>(tags.size());
    for (String tag : tags) {
      int dictId = _tagDictionary.indexOf(tag);
      if (dictId == -1) {
        return new MutableRoaringBitmap(); // no time series for the tag search
      }
      dictIds.add(dictId);
    }

    // intuition: tags often happen to be ordered by cardinality, so start with the most selective tag.
    ImmutableRoaringBitmap first = _tagIndex.getDocIds(dictIds.get(dictIds.size() - 1));
    MutableRoaringBitmap intersection = first.toMutableRoaringBitmap();
    for (int i = dictIds.size() - 2; i >= 0; i--) {
      ImmutableRoaringBitmap matchingSeriesIds = _tagIndex.getDocIds(dictIds.get(i));
      intersection.and(matchingSeriesIds);
      if (intersection.isEmpty()) {
        return intersection; // no time series for the tag search
      }
    }
    return intersection;
  }

  @Override
  public String getTagSet(int seriesId) {
    return _tagSetDictionary.get(seriesId);
  }

  @Override
  public DoubleArrayList getValues(int seriesId, TimeSeriesReaderContext context) {
    return DoubleArrayList.wrap(
        _keyVectorValuesReader.getDoubleMV(seriesId, context.getKeyVectorValuesReaderContext()));
  }

  @Override
  public LongArrayList getTimestamps(int seriesId, TimeSeriesReaderContext context) {
    return LongArrayList.wrap(
        _keyVectorTimestampsReader.getLongMV(seriesId, context.getKeyVectorTimestampsReaderContext()));
  }

  /**
   * Wrap the reader contexts for the forward index readers
   * @return
   */
  @Override
  public TimeSeriesReaderContext createContext() {
    return new TimeSeriesReaderContext();
  }

  public class TimeSeriesReaderContext implements ForwardIndexReaderContext {

    // TODO think of a cleaner way, so that we may evolve the raw index version without tying it to the ts index version
    private final VarByteChunkForwardIndexReaderV4.ReaderContext _keyVectorValuesReaderContext;
    private final VarByteChunkForwardIndexReaderV4.ReaderContext _keyVectorTimestampsReaderContext;

    public TimeSeriesReaderContext() {
      _keyVectorValuesReaderContext = _keyVectorValuesReader.createContext();
      _keyVectorTimestampsReaderContext = _keyVectorTimestampsReader.createContext();
    }

    public VarByteChunkForwardIndexReaderV4.ReaderContext getKeyVectorValuesReaderContext() {
      return _keyVectorValuesReaderContext;
    }

    public VarByteChunkForwardIndexReaderV4.ReaderContext getKeyVectorTimestampsReaderContext() {
      return _keyVectorTimestampsReaderContext;
    }

    @Override
    public void close()
        throws IOException {
      _keyVectorValuesReaderContext.close();
      _keyVectorTimestampsReaderContext.close();
    }
  }
}
