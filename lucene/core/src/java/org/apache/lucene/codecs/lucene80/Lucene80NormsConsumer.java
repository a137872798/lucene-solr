/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.lucene80;

import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.codecs.lucene80.Lucene80NormsFormat.VERSION_CURRENT;

/**
 * Writer for {@link Lucene80NormsFormat}
 * 有关标准因子的索引写入对象
 */
final class Lucene80NormsConsumer extends NormsConsumer {

  /**
   * 抽取标准因子后 写入到这2个输出流中   这2个输出流 底层对接2个索引文件
   */
  IndexOutput data, meta;
  final int maxDoc;

  /**
   *
   * @param state
   * @param dataCodec    "Lucene80NormsData"
   * @param dataExtension    nvd
   * @param metaCodec      "Lucene80NormsMetadata"
   * @param metaExtension    nvm
   * @throws IOException
   */
  Lucene80NormsConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    boolean success = false;
    try {
      // segmentName_suffix.ext
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      // 写入头部信息
      CodecUtil.writeIndexHeader(data, dataCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      // 看来标准因子不需要创建那2个啥  x  m 文件的
      // 生成元数据索引文件
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(meta, metaCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      maxDoc = state.segmentInfo.maxDoc();
      success = true;
    } finally {
      if (!success) {
        // 组装异常信息 并抛出
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeInt(-1); // write EOF marker
        CodecUtil.writeFooter(meta); // write checksum
      }
      if (data != null) {
        CodecUtil.writeFooter(data); // write checksum
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      meta = data = null;
    }
  }

  /**
   *
   * @param field field information  代表该标准因子是属于哪个 field的
   * @param normsProducer NormsProducer of the numeric norm values  该field绑定的所有包含标准因子的 doc
   * @throws IOException
   */
  @Override
  public void addNormsField(FieldInfo field, NormsProducer normsProducer) throws IOException {
    NumericDocValues values = normsProducer.getNorms(field);
    // 记录总计有多少doc 包含标准因子
    int numDocsWithValue = 0;
    // 这里维护了标准因子的 min max
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithValue++;
      long v = values.longValue();
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    assert numDocsWithValue <= maxDoc;

    // 标明之后的数据是针对 fieldNum 为多少的  field
    meta.writeInt(field.number);

    // 不存在标准因子
    if (numDocsWithValue == 0) {
      // 写入特殊值
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    // -1 代表doc是紧凑存储的 并且没有doc被删除 这样直接写入数据就好
    } else if (numDocsWithValue == maxDoc) {
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else {
      // 需要借助 disi 迭代docId
      // 获取当前的文件偏移量
      long offset = data.getFilePointer();
      // 写入文件偏移量
      meta.writeLong(offset); // docsWithFieldOffset
      // 某个域对应的 标准因子 实际上是关联多个doc的
      values = normsProducer.getNorms(field);
      // TODO 这里写入一个什么值
      final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      // 将文件的偏移量变化写入元数据文件
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    // 基础值写完后 写入一个  doc数量
    meta.writeInt(numDocsWithValue);
    // 获取每个值 是多少byte
    int numBytesPerValue = numBytesPerValue(min, max);

    meta.writeByte((byte) numBytesPerValue);
    if (numBytesPerValue == 0) {
      meta.writeLong(min);
    } else {
      meta.writeLong(data.getFilePointer()); // normsOffset
      values = normsProducer.getNorms(field);
      // 将标准因子的值写入到 data文件中
      writeValues(values, numBytesPerValue, data);
    }
  }

  private int numBytesPerValue(long min, long max) {
    if (min >= max) {
      return 0;
    } else if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE) {
      return 1;
    } else if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE) {
      return 2;
    } else if (min >= Integer.MIN_VALUE && max <= Integer.MAX_VALUE) {
      return 4;
    } else {
      return 8;
    }
  }

  private void writeValues(NumericDocValues values, int numBytesPerValue, IndexOutput out) throws IOException, AssertionError {
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      long value = values.longValue();
      switch (numBytesPerValue) {
        case 1:
          out.writeByte((byte) value);
          break;
        case 2:
          out.writeShort((short) value);
          break;
        case 4:
          out.writeInt((int) value);
          break;
        case 8:
          out.writeLong(value);
          break;
        default:
          throw new AssertionError();
      }
    }
  }
}
