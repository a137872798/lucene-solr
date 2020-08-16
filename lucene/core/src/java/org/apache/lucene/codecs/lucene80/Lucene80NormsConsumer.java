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

  /**
   * 代表本次段文件最大的docId  但是doc数量不一定相同 因为在解析doc生成索引数据的过程中可能有部分数据失败  那么那些数据不会写入到索引文件中
   */
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
      // 生成数据文件名  .nvd
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      // 写入头部信息
      CodecUtil.writeIndexHeader(data, dataCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      // 生成元数据索引文件  .nvm
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
   * 写入某个field的标准因子数据
   * @param field field information  代表该标准因子是属于哪个 field的
   * @param normsProducer NormsProducer of the numeric norm values  该field绑定的所有包含标准因子的 doc
   * @throws IOException
   */
  @Override
  public void addNormsField(FieldInfo field, NormsProducer normsProducer) throws IOException {
    // 获取该field的标准因子
    NumericDocValues values = normsProducer.getNorms(field);
    // 记录总计有多少doc 包含标准因子     已经去除掉 无法正常生成索引的 doc了
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

    // 标明这段标准因子是针对 fieldNum为多少的field
    meta.writeInt(field.number);

    // 不存在标准因子
    if (numDocsWithValue == 0) {
      // 写入特殊值
      meta.writeLong(-2); // docsWithFieldOffset        对应data文件该field标准因子数据的起始偏移量
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    // -1 代表所有doc 都是成功写入的  这样它的值 刚好就与 maxDoc一致
    } else if (numDocsWithValue == maxDoc) {
      meta.writeLong(-1); // docsWithFieldOffset           对应data文件该field标准因子数据的起始偏移量
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else {
      // 当doc不是连续存储时 在doc的量比较大的时候  为了避免浪费太多空间使用了特殊的数据结构存储
      long offset = data.getFilePointer();
      // 写入文件偏移量
      meta.writeLong(offset); // docsWithFieldOffset
      values = normsProducer.getNorms(field);
      // 按照此时有效的doc构建跳跃结构
      final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      // 写入文件的偏移量变化
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      // 写入生成的 jump实体数量
      meta.writeShort(jumpTableEntryCount);
      // 写入 使用的收缩因子
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    // 写入有多少doc下包含有效值
    meta.writeInt(numDocsWithValue);
    // 检测使用哪种单位 可以表示所有的值   如果为0代表所有值都一样 如果是1代表所有值都可以用byte表示  2代表所有值可以用short表示 以此类推
    int numBytesPerValue = numBytesPerValue(min, max);

    meta.writeByte((byte) numBytesPerValue);
    if (numBytesPerValue == 0) {
      // 如果所有值一样 就使用一个long存储
      meta.writeLong(min);
    } else {
      // 先写入偏移量
      meta.writeLong(data.getFilePointer()); // normsOffset
      values = normsProducer.getNorms(field);
      // 将所有标准因子的值 按照指定的类型写入到out中
      writeValues(values, numBytesPerValue, data);
    }
  }

  private int numBytesPerValue(long min, long max) {
    // 代表所有数据都是一样的
    if (min >= max) {
      return 0;
    // 计算出标准因子的数据范围
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
