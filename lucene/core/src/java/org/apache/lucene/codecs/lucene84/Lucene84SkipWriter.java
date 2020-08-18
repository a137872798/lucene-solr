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
package org.apache.lucene.codecs.lucene84;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.MultiLevelSkipListWriter;
import org.apache.lucene.index.Impact;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;

/**
 * Write skip lists with multiple levels, and support skip within block ints.
 *
 * Assume that docFreq = 28, skipInterval = blockSize = 12
 *
 *  |       block#0       | |      block#1        | |vInts|
 *  d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
 *                          ^                       ^       (level 0 skip point)
 *
 * Note that skipWriter will ignore first document in block#0, since 
 * it is useless as a skip point.  Also, we'll never skip into the vInts
 * block, only record skip data at the start its start point(if it exist).
 *
 * For each skip point, we will record: 
 * 1. docID in former position, i.e. for position 12, record docID[11], etc.
 * 2. its related file points(position, payload), 
 * 3. related numbers or uptos(position, payload).
 * 4. start offset.
 * 该对象基于跳跃表存储数据
 */
final class Lucene84SkipWriter extends MultiLevelSkipListWriter {
  // 每个层级对应一个 last数据
  private int[] lastSkipDoc;
  private long[] lastSkipDocPointer;
  private long[] lastSkipPosPointer;
  private long[] lastSkipPayPointer;
  private int[] lastPayloadByteUpto;

  private final IndexOutput docOut;
  private final IndexOutput posOut;
  private final IndexOutput payOut;

  private int curDoc;
  private long curDocPointer;
  private long curPosPointer;
  private long curPayPointer;
  private int curPosBufferUpto;
  private int curPayloadByteUpto;
  /**
   * 每个层级对应一个 CompetitiveImpactAccumulator
   * CompetitiveImpactAccumulator 负责保存 Impact (该对象内部包含2个属性  freq 和 norm)
   */
  private CompetitiveImpactAccumulator[] curCompetitiveFreqNorms;
  private boolean fieldHasPositions;
  private boolean fieldHasOffsets;
  private boolean fieldHasPayloads;

  /**
   * 该输出流 专门用于写 impact
   */
  private final ByteBuffersDataOutput freqNormOut = ByteBuffersDataOutput.newResettableInstance();

  /**
   *
   * @param maxSkipLevels   代表跳跃表最大层级为多少    默认为10
   * @param blockSize    每个block内存储多少数据
   * @param docCount   当前段预备 将多少doc 刷盘
   * @param docOut    对应 .doc 索引文件
   * @param posOut    对应 .pos 索引文件
   * @param payOut    对应 .pay 索引文件
   */
  public Lucene84SkipWriter(int maxSkipLevels, int blockSize, int docCount, IndexOutput docOut, IndexOutput posOut, IndexOutput payOut) {
    super(blockSize, 8, maxSkipLevels, docCount);
    this.docOut = docOut;
    this.posOut = posOut;
    this.payOut = payOut;

    // 代表跳跃表每层包含多少doc
    lastSkipDoc = new int[maxSkipLevels];

    lastSkipDocPointer = new long[maxSkipLevels];
    if (posOut != null) {
      // 跳跃表每层包含多少 position信息
      lastSkipPosPointer = new long[maxSkipLevels];
      if (payOut != null) {
        // 每层包含多少 payload/offset 信息
        lastSkipPayPointer = new long[maxSkipLevels];
      }
      lastPayloadByteUpto = new int[maxSkipLevels];
    }
    // 该对象负责记录 freq 和 norm
    curCompetitiveFreqNorms = new CompetitiveImpactAccumulator[maxSkipLevels];
    for (int i = 0; i < maxSkipLevels; ++i) {
      curCompetitiveFreqNorms[i] = new CompetitiveImpactAccumulator();
    }
  }

  /**
   * 标记当前处理的 field 需要将哪些信息写入到索引文件
   * @param fieldHasPositions
   * @param fieldHasOffsets
   * @param fieldHasPayloads
   */
  public void setField(boolean fieldHasPositions, boolean fieldHasOffsets, boolean fieldHasPayloads) {
    this.fieldHasPositions = fieldHasPositions;
    this.fieldHasOffsets = fieldHasOffsets;
    this.fieldHasPayloads = fieldHasPayloads;
  }
  
  // tricky: we only skip data for blocks (terms with more than 128 docs), but re-init'ing the skipper 
  // is pretty slow for rare terms in large segments as we have to fill O(log #docs in segment) of junk.
  // this is the vast majority of terms (worst case: ID field or similar).  so in resetSkip() we save 
  // away the previous pointers, and lazy-init only if we need to buffer skip data for the term.
  private boolean initialized;

  long lastDocFP;
  long lastPosFP;
  long lastPayFP;

  /**
   * 重置跳跃表结构
   */
  @Override
  public void resetSkip() {
    // 在开始处理一个新的 term前  记录之前的文件偏移量
    lastDocFP = docOut.getFilePointer();
    if (fieldHasPositions) {
      lastPosFP = posOut.getFilePointer();
      if (fieldHasOffsets || fieldHasPayloads) {
        lastPayFP = payOut.getFilePointer();
      }
    }

    // 清空之前记录的 norm/impact 信息
    if (initialized) {
      for (CompetitiveImpactAccumulator acc : curCompetitiveFreqNorms) {
        acc.clear();
      }
    }
    // 标记成需要重新创建跳跃表结构  惰性初始化
    initialized = false;
  }

  /**
   * 初始化跳跃表
   */
  private void initSkip() {
    // 需要重新构建 跳跃表结构
    if (!initialized) {
      // 分配跳跃表数组
      super.resetSkip();
      // 使用上次重置前最后的 文件偏移量初始化相关属性
      Arrays.fill(lastSkipDoc, 0);
      Arrays.fill(lastSkipDocPointer, lastDocFP);
      if (fieldHasPositions) {
        Arrays.fill(lastSkipPosPointer, lastPosFP);
        if (fieldHasPayloads) {
          Arrays.fill(lastPayloadByteUpto, 0);
        }
        if (fieldHasOffsets || fieldHasPayloads) {
          Arrays.fill(lastSkipPayPointer, lastPayFP);
        }
      }
      // sets of competitive freq,norm pairs should be empty at this point
      assert Arrays.stream(curCompetitiveFreqNorms)
          .map(CompetitiveImpactAccumulator::getCompetitiveFreqNormPairs)
          .mapToInt(Collection::size)
          .sum() == 0;
      initialized = true;
    }
  }

  /**
   * Sets the values for the current skip data.
   * @param doc 此时对应的docId
   * @param competitiveFreqNorms  当前累加器内已经存储的 term 的频率和 norm 信息
   * @param numDocs  代表处理到该term关联的 第几个 doc
   * @param posFP  当前 .pos文件的起始偏移量
   * @param payFP  当前 .pay文件的起始偏移量
   * @param posBufferUpto 当前 pos相关数组的下标
   * @param payloadByteUpto  对应存储 payload 数组的下标
   */
  public void bufferSkip(int doc, CompetitiveImpactAccumulator competitiveFreqNorms,
      int numDocs, long posFP, long payFP, int posBufferUpto, int payloadByteUpto) throws IOException {
    // 重建跳跃表结构 以及更新 lastXXX属性
    initSkip();
    // 更新当前的最新信息
    this.curDoc = doc;
    this.curDocPointer = docOut.getFilePointer();
    this.curPosPointer = posFP;
    this.curPayPointer = payFP;
    this.curPosBufferUpto = posBufferUpto;
    this.curPayloadByteUpto = payloadByteUpto;
    // 将 freq norm 累加到第一层  curCompetitiveFreqNorms 的长度代表层级
    this.curCompetitiveFreqNorms[0].addAll(competitiveFreqNorms);
    // 将数据写入到 跳跃表结构
    bufferSkip(numDocs);
  }


  /**
   * 因为一开始 跳跃表的 间隔 层级 等都是确定的 所以能够确定要将数据写入到哪里       能够确保每次写入都是往后叠加的一个原因是  每个跳跃表是针对 term在所有doc下的数据 它的相关数据都是单调递增的
   * @param level      the level skip data shall be writing for  标记此时往跳跃表的第几层写入数据
   * @param skipBuffer the skip buffer to write to   对应存储数据的容器
   * @throws IOException
   */
  @Override
  protected void writeSkipData(int level, DataOutput skipBuffer) throws IOException {

    // 计算与上一次写入该层数据的差值
    int delta = curDoc - lastSkipDoc[level];

    // 这里写入的是差值   也就是即使在跳跃表结构中 同一层的数据 每项还是依赖前一项(差值存储)
    skipBuffer.writeVInt(delta);
    // 更新该level 上次写入的doc数量
    lastSkipDoc[level] = curDoc;

    // 写入  .doc文件的 filePoint增量信息
    skipBuffer.writeVLong(curDocPointer - lastSkipDocPointer[level]);
    lastSkipDocPointer[level] = curDocPointer;

    if (fieldHasPositions) {

      skipBuffer.writeVLong(curPosPointer - lastSkipPosPointer[level]);
      lastSkipPosPointer[level] = curPosPointer;

      // 2个下标信息 不使用差值存储
      skipBuffer.writeVInt(curPosBufferUpto);

      if (fieldHasPayloads) {
        skipBuffer.writeVInt(curPayloadByteUpto);
      }

      if (fieldHasOffsets || fieldHasPayloads) {
        skipBuffer.writeVLong(curPayPointer - lastSkipPayPointer[level]);
        lastSkipPayPointer[level] = curPayPointer;
      }
    }

    // 上面是处理 filePoint  docId  2个数组下标 等信息


    // 找到当前level的impact累加器
    CompetitiveImpactAccumulator competitiveFreqNorms = curCompetitiveFreqNorms[level];
    assert competitiveFreqNorms.getCompetitiveFreqNormPairs().size() > 0;
    if (level + 1 < numberOfSkipLevels) {
      curCompetitiveFreqNorms[level + 1].addAll(competitiveFreqNorms);
    }
    // 有关impact的增量数据 写入到临时容器中
    writeImpacts(competitiveFreqNorms, freqNormOut);

    // 将数据转移到 skipBuffer中
    skipBuffer.writeVInt(Math.toIntExact(freqNormOut.size()));
    freqNormOut.copyTo(skipBuffer);
    freqNormOut.reset();
    competitiveFreqNorms.clear();
  }

  /**
   *
   * @param acc
   * @param out
   * @throws IOException
   */
  static void writeImpacts(CompetitiveImpactAccumulator acc, DataOutput out) throws IOException {
    Collection<Impact> impacts = acc.getCompetitiveFreqNormPairs();
    Impact previous = new Impact(0, 0);
    for (Impact impact : impacts) {
      assert impact.freq > previous.freq;
      assert Long.compareUnsigned(impact.norm, previous.norm) > 0;
      // 这里也只是存储增量数据
      int freqDelta = impact.freq - previous.freq - 1;
      long normDelta = impact.norm - previous.norm - 1;
      if (normDelta == 0) {
        // most of time, norm only increases by 1, so we can fold everything in a single byte
        out.writeVInt(freqDelta << 1);
      } else {
        out.writeVInt((freqDelta << 1) | 1);
        out.writeZLong(normDelta);
      }
      previous = impact;
    }
  }
}
