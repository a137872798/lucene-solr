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
package org.apache.lucene.codecs;


import java.io.IOException;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Extension of {@link PostingsWriterBase}, adding a push
 * API for writing each element of the postings.  This API
 * is somewhat analogous to an XML SAX API, while {@link
 * PostingsWriterBase} is more like an XML DOM API.
 * 
 * @see PostingsReaderBase
 * @lucene.experimental
 */
// TODO: find a better name; this defines the API that the
// terms dict impls use to talk to a postings impl.
// TermsDict + PostingsReader/WriterBase == PostingsConsumer/Producer
public abstract class PushPostingsWriterBase extends PostingsWriterBase {

  // Reused in writeTerm
  private PostingsEnum postingsEnum;
  private int enumFlags;

  /** {@link FieldInfo} of current field being written. */
  protected FieldInfo fieldInfo;

  /** {@link IndexOptions} of current field being
      written */
  protected IndexOptions indexOptions;

  /** True if the current field writes freqs. */
  protected boolean writeFreqs;

  /** True if the current field writes positions. */
  protected boolean writePositions;

  /** True if the current field writes payloads. */
  protected boolean writePayloads;

  /** True if the current field writes offsets. */
  protected boolean writeOffsets;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected PushPostingsWriterBase() {
  }

  /** Return a newly created empty TermState */
  public abstract BlockTermState newTermState() throws IOException;

  /** Start a new term.  Note that a matching call to {@link
   *  #finishTerm(BlockTermState)} is done, only if the term has at least one
   *  document. */
  public abstract void startTerm(NumericDocValues norms) throws IOException;

  /** Finishes the current term.  The provided {@link
   *  BlockTermState} contains the term's summary statistics, 
   *  and will holds metadata from PBF when returned */
  public abstract void finishTerm(BlockTermState state) throws IOException;

  /** 
   * Sets the current field for writing, and returns the
   * fixed length of long[] metadata (which is fixed per
   * field), called when the writing switches to another field. */
  // 代表此时开始处理某个 field
  @Override
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    indexOptions = fieldInfo.getIndexOptions();

    // 根据当前field下 term的哪些信息需要写入到索引文件中 生成flag
    writeFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    writePositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    writeOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;        
    writePayloads = fieldInfo.hasPayloads();

    if (writeFreqs == false) {
      enumFlags = 0;
    } else if (writePositions == false) {
      enumFlags = PostingsEnum.FREQS;
    } else if (writeOffsets == false) {
      if (writePayloads) {
        enumFlags = PostingsEnum.PAYLOADS;
      } else {
        enumFlags = PostingsEnum.POSITIONS;
      }
    } else {
      if (writePayloads) {
        enumFlags = PostingsEnum.PAYLOADS | PostingsEnum.OFFSETS;
      } else {
        enumFlags = PostingsEnum.OFFSETS;
      }
    }
  }

  /**
   * 将 term的position等相关信息写入到该对象
   * @param term  本次要写入的term
   * @param termsEnum   term的迭代器
   * @param docsSeen   docId位图
   * @param norms  该对象可以通过指定field 获取相关的标准因子
   * @return
   * @throws IOException
   */
  @Override
  public final BlockTermState writeTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen, NormsProducer norms) throws IOException {

    // 定义一种可迭代的模板  每次迭代对应到不同的doc 并且 可以获取此时的value
    NumericDocValues normValues;
    // 如果此时正在处理的 field 不包含标准因子信息 忽略
    if (fieldInfo.hasNorms() == false) {
      normValues = null;
    } else {
      // 从索引文件中读取之前写入的 标准因子
      normValues = norms.getNorms(fieldInfo);
    }
    // 代表开始处理该term 会重置一些相关数据
    startTerm(normValues);
    // 生成遍历 所有term位置信息的迭代器   enumFlags 代表需要存储的类型   传入postingEnum是尝试复用该对象 如果该对象还未创建 那么会在该方法内新建
    // 从 doc的解析来看 每个term在termHash中 会维护自己的一个数据分片 用于记录该term出现在哪些doc下 以及 pos/offset/freq 等信息
    postingsEnum = termsEnum.postings(postingsEnum, enumFlags);
    assert postingsEnum != null;

    int docFreq = 0;
    long totalTermFreq = 0;
    while (true) {
      // 遍历该 term所在的 docId  便于构成倒排索引
      int docID = postingsEnum.nextDoc();
      // 代表已经遍历完 该term相关的所有 doc了
      if (docID == PostingsEnum.NO_MORE_DOCS) {
        break;
      }
      docFreq++;
      // 记录当前term出现在哪些doc中
      docsSeen.set(docID);
      int freq;
      // 如果在解析doc时 标记了需要存储 term在某个doc下出现的频率 那么这时可以读取之前写入的数据
      if (writeFreqs) {
        freq = postingsEnum.freq();
        totalTermFreq += freq;
      } else {
        // 未设置频率的情况下 使用-1
        freq = -1;
      }

      // 存储 freq docId 信息 以及将freq和 norm 记录到一个 accumulator对象中
      startDoc(docID, freq);

      // 如果当前处理的 field 携带 position信息
      if (writePositions) {
        // 有多少频率信息 就有多少位置信息
        for(int i=0;i<freq;i++) {
          // 读取当前doc所在位置信息
          int pos = postingsEnum.nextPosition();
          // 如果存在 payload 信息 那么也加载出来
          BytesRef payload = writePayloads ? postingsEnum.getPayload() : null;
          int startOffset;
          int endOffset;
          // 有偏移量信息的话 也读取
          if (writeOffsets) {
            startOffset = postingsEnum.startOffset();
            endOffset = postingsEnum.endOffset();
          } else {
            startOffset = -1;
            endOffset = -1;
          }
          // 将描述位置的4个字段 写入文件中
          addPosition(pos, payload, startOffset, endOffset);
        }
      }

      // 代表某个doc 处理完毕了
      finishDoc();
    }

    // 代表本轮没有处理任何doc 返回null
    if (docFreq == 0) {
      return null;
    } else {
      // 创建一个描述本次写入结果的 实体对象 (IntBlockTermState)
      BlockTermState state = newTermState();
      state.docFreq = docFreq;
      state.totalTermFreq = writeFreqs ? totalTermFreq : -1;
      finishTerm(state);
      return state;
    }
  }

  /** Adds a new doc in this term. 
   * <code>freq</code> will be -1 when term frequencies are omitted
   * for the field. */
  public abstract void startDoc(int docID, int freq) throws IOException;

  /** Add a new position and payload, and start/end offset.  A
   *  null payload means no payload; a non-null payload with
   *  zero length also means no payload.  Caller may reuse
   *  the {@link BytesRef} for the payload between calls
   *  (method must fully consume the payload). <code>startOffset</code>
   *  and <code>endOffset</code> will be -1 when offsets are not indexed. */
  public abstract void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException;

  /** Called when we are done adding positions and payloads
   *  for each doc. */
  public abstract void finishDoc() throws IOException;
}
