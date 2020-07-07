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
   * @param maxSkipLevels   代表跳跃表最大层级为多少
   * @param blockSize
   * @param docCount   文档数
   * @param docOut    3种用于写入数据的输出流
   * @param posOut
   * @param payOut
   */
  public Lucene84SkipWriter(int maxSkipLevels, int blockSize, int docCount, IndexOutput docOut, IndexOutput posOut, IndexOutput payOut) {
    super(blockSize, 8, maxSkipLevels, docCount);
    this.docOut = docOut;
    this.posOut = posOut;
    this.payOut = payOut;

    // 创建与层级相关的 数组
    lastSkipDoc = new int[maxSkipLevels];
    lastSkipDocPointer = new long[maxSkipLevels];
    if (posOut != null) {
      lastSkipPosPointer = new long[maxSkipLevels];
      if (payOut != null) {
        lastSkipPayPointer = new long[maxSkipLevels];
      }
      lastPayloadByteUpto = new int[maxSkipLevels];
    }
    curCompetitiveFreqNorms = new CompetitiveImpactAccumulator[maxSkipLevels];
    for (int i = 0; i < maxSkipLevels; ++i) {
      curCompetitiveFreqNorms[i] = new CompetitiveImpactAccumulator();
    }
  }

  /**
   * 标明当前存储的字段 是否需要存储 position/offset/payload 信息
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

  // 分别记录3个输出流对应文件的 filePointer
  long lastDocFP;
  long lastPosFP;
  long lastPayFP;

  /**
   * 重置跳跃表结构
   */
  @Override
  public void resetSkip() {
    lastDocFP = docOut.getFilePointer();
    if (fieldHasPositions) {
      lastPosFP = posOut.getFilePointer();
      if (fieldHasOffsets || fieldHasPayloads) {
        lastPayFP = payOut.getFilePointer();
      }
    }
    if (initialized) {
      for (CompetitiveImpactAccumulator acc : curCompetitiveFreqNorms) {
        acc.clear();
      }
    }
    initialized = false;
  }

  /**
   * 初始化跳跃表
   */
  private void initSkip() {
    if (!initialized) {
      // 回收之前 buffer中写入的数据
      super.resetSkip();
      // 重置相关数组
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
   * 将这些数据 写入到跳跃表结构
   */
  public void bufferSkip(int doc, CompetitiveImpactAccumulator competitiveFreqNorms,
      int numDocs, long posFP, long payFP, int posBufferUpto, int payloadByteUpto) throws IOException {
    initSkip();
    // 更新当前的最新信息
    this.curDoc = doc;
    this.curDocPointer = docOut.getFilePointer();
    this.curPosPointer = posFP;
    this.curPayPointer = payFP;
    this.curPosBufferUpto = posBufferUpto;
    this.curPayloadByteUpto = payloadByteUpto;
    // 将 freq norm 累加到第一层
    this.curCompetitiveFreqNorms[0].addAll(competitiveFreqNorms);
    // 这个方法会将数据 写入到多层  转发到 writeSkipData
    bufferSkip(numDocs);
  }


  /**
   * 父类这个跳跃表结构很奇怪 长度 层级 一开始就确定了   每当写入一个新数据时 在调用子类的writeSkipData 后 会在当前level对应的output中写入 下一层的output长度
   * @param level      the level skip data shall be writing for  代表此时要写入的 output在第几个level
   * @param skipBuffer the skip buffer to write to
   * @throws IOException
   */
  @Override
  protected void writeSkipData(int level, DataOutput skipBuffer) throws IOException {

    // 计算与上一次写入该层数据的差值
    int delta = curDoc - lastSkipDoc[level];

    // 这里写入的是差值
    skipBuffer.writeVInt(delta);
    // 更新该level 上次写入的doc数量
    lastSkipDoc[level] = curDoc;

    // 这里也是写入增量数据
    skipBuffer.writeVLong(curDocPointer - lastSkipDocPointer[level]);
    lastSkipDocPointer[level] = curDocPointer;

    if (fieldHasPositions) {

      skipBuffer.writeVLong(curPosPointer - lastSkipPosPointer[level]);
      lastSkipPosPointer[level] = curPosPointer;
      skipBuffer.writeVInt(curPosBufferUpto);

      if (fieldHasPayloads) {
        skipBuffer.writeVInt(curPayloadByteUpto);
      }

      if (fieldHasOffsets || fieldHasPayloads) {
        skipBuffer.writeVLong(curPayPointer - lastSkipPayPointer[level]);
        lastSkipPayPointer[level] = curPayPointer;
      }
    }

    // 以上都是只存储差值

    // 找到当前level的impact累加器
    CompetitiveImpactAccumulator competitiveFreqNorms = curCompetitiveFreqNorms[level];
    assert competitiveFreqNorms.getCompetitiveFreqNormPairs().size() > 0;
    if (level + 1 < numberOfSkipLevels) {
      // impact 是跨级累加的  为啥啊 你大爷
      curCompetitiveFreqNorms[level + 1].addAll(competitiveFreqNorms);
    }
    // 有关impact的增量数据 单独写入到freqNormOut
    writeImpacts(competitiveFreqNorms, freqNormOut);

    // 将 freqNormOut的 长度 以及数据写入到 skipBuffer 中
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
