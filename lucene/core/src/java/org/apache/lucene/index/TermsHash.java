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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/** This class is passed each token produced by the analyzer
 *  on each field during indexing, and it stores these
 *  tokens in a hash table, and allocates separate byte
 *  streams per token.  Consumers of this class, eg {@link
 *  FreqProxTermsWriter} and {@link TermVectorsConsumer},
 *  write their own byte streams under each term. */
// 该对象会存储在分词过程中产生的 term
abstract class TermsHash {

  /**
   * 本身还是一个链表结构
   */
  final TermsHash nextTermsHash;

  // 因为无法预先得知要申请的大块内存 这里通过一个pool 对象 按需创建内存 并在内部通过一个二维数组进行连接
  final IntBlockPool intPool;
  final ByteBlockPool bytePool;
  /**
   * 这个对象是存储 term数据的
   */
  ByteBlockPool termBytePool;
  final Counter bytesUsed;

  /**
   * 用于描述当前正在解析的 doc
   */
  final DocumentsWriterPerThread.DocState docState;

  /**
   * 设置为  true  那么该对象会与外面的对象 共用计数器
   */
  final boolean trackAllocations;

  /**
   *
   * @param docWriter
   * @param trackAllocations  默认情况为 true   代表是否共用同一个计数器
   * @param nextTermsHash
   */
  TermsHash(final DocumentsWriterPerThread docWriter, boolean trackAllocations, TermsHash nextTermsHash) {
    this.docState = docWriter.docState;
    this.trackAllocations = trackAllocations; 
    this.nextTermsHash = nextTermsHash;
    this.bytesUsed = trackAllocations ? docWriter.bytesUsed : Counter.newCounter();
    intPool = new IntBlockPool(docWriter.intBlockAllocator);
    bytePool = new ByteBlockPool(docWriter.byteBlockAllocator);

    // 如果设置了 next对象  将2个对象的  termBytePool 都指向当前对象的 bytePool  应该是打算共用
    if (nextTermsHash != null) {
      // We are primary
      termBytePool = bytePool;
      nextTermsHash.termBytePool = bytePool;
    }
  }

  /**
   * 终止该对象
   */
  public void abort() {
    try {
      reset();
    } finally {
      // 以链式调用的形式 终止下层的 存储对象
      if (nextTermsHash != null) {
        nextTermsHash.abort();
      }
    }
  }

  // Clear all state
  // 释放内存
  void reset() {
    // we don't reuse so we drop everything and don't fill with 0
    intPool.reset(false, false); 
    bytePool.reset(false, false);
  }

  /**
   * @param fieldsToFlush  每个value 都代表某个field下所有的 term信息
   * @param state  这个对象描述了 将数据写入到段索引中相关的参数
   * @param sortMap
   * @param norms
   * @throws IOException
   */
  void flush(Map<String,TermsHashPerField> fieldsToFlush, final SegmentWriteState state,
      Sorter.DocMap sortMap, NormsProducer norms) throws IOException {
    // 默认实现都是委托 给下层
    if (nextTermsHash != null) {
      Map<String,TermsHashPerField> nextChildFields = new HashMap<>();
      for (final Map.Entry<String,TermsHashPerField> entry : fieldsToFlush.entrySet()) {
        // 注意这里传递到下游的是 nextPerField
        nextChildFields.put(entry.getKey(), entry.getValue().nextPerField);
      }
      nextTermsHash.flush(nextChildFields, state, sortMap, norms);
    }
  }

  /**
   * 传入一个域对象 生成一个从 field中抽取属性的对象
   * @param fieldInvertState
   * @param fieldInfo
   * @return
   */
  abstract TermsHashPerField addField(FieldInvertState fieldInvertState, FieldInfo fieldInfo);


  void finishDocument() throws IOException {
    if (nextTermsHash != null) {
      nextTermsHash.finishDocument();
    }
  }

  void startDocument() throws IOException {
    if (nextTermsHash != null) {
      nextTermsHash.startDocument();
    }
  }
}
