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
import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * 词向量信息consumer 实际上以hash结构存储数据
 */
class TermVectorsConsumer extends TermsHash {
  /**
   * 该对象负责将词向量写入索引文件
   */
  TermVectorsWriter writer;

  /** Scratch term used by TermVectorsConsumerPerField.finishDocument. */
  // 当调用 finishDocument时 每个 perField对象会处理内部的term   每当处理到某个term时 会将对象内部的指针指向 term所在的内存块
  final BytesRef flushTerm = new BytesRef();

  /**
   * 该对象是由哪个线程写入的
   */
  final DocumentsWriterPerThread docWriter;

  /** Used by TermVectorsConsumerPerField when serializing
   *  the term vectors. */
  final ByteSliceReader vectorSliceReaderPos = new ByteSliceReader();
  final ByteSliceReader vectorSliceReaderOff = new ByteSliceReader();

  /**
   * 标记是否存储了向量信息  只要处理的perField中 有至少一个fieldType 设置了storeTermVectors=true就会将该值设置成true
   */
  boolean hasVectors;
  /**
   * 记录有多少个 需要存储向量的field
   */
  int numVectorFields;
  int lastDocID;
  /**
   * 内部对象以 field为单位写入数据
   */
  private TermVectorsConsumerPerField[] perFields = new TermVectorsConsumerPerField[1];

  /**
   * @param docWriter
   */
  public TermVectorsConsumer(DocumentsWriterPerThread docWriter) {
    super(docWriter, false, null);
    this.docWriter = docWriter;
  }

  /**
   * 将数据写入到索引文件中
   * @param fieldsToFlush
   * @param state
   * @param sortMap
   * @param norms
   * @throws IOException
   */
  @Override
  void flush(Map<String, TermsHashPerField> fieldsToFlush, final SegmentWriteState state, Sorter.DocMap sortMap, NormsProducer norms) throws IOException {
    if (writer != null) {
      int numDocs = state.segmentInfo.maxDoc();
      assert numDocs > 0;
      // At least one doc in this run had term vectors enabled
      try {
        // 确保之前的 docData  fieldData 都已经创建
        fill(numDocs);
        assert state.segmentInfo != null;
        writer.finish(state.fieldInfos, numDocs);
      } finally {
        IOUtils.close(writer);
        writer = null;
        lastDocID = 0;
        hasVectors = false;
      }
    }
  }

  /** Fills in no-term-vectors for all docs we haven't seen
   *  since the last doc that had term vectors. */
  // 从0 开始直到填充到 docId 不断地调用 startDocument 和 finishDocument
  void fill(int docID) throws IOException {
    while(lastDocID < docID) {
      // 就是在 writer 不断的填充空的 DocData
      writer.startDocument(0);
      writer.finishDocument();
      lastDocID++;
    }
  }

  /**
   * 确保用于写入 term向量数据的writer对象已经被初始化
   * @throws IOException
   */
  void initTermVectorsWriter() throws IOException {
    if (writer == null) {
      // 获取此时内存中的doc数量
      IOContext context = new IOContext(new FlushInfo(docWriter.getNumDocsInRAM(), docWriter.bytesUsed()));
      writer = docWriter.codec.termVectorsFormat().vectorsWriter(docWriter.directory, docWriter.getSegmentInfo(), context);
      lastDocID = 0;
    }
  }

  /**
   * 此时处理完某个doc下的所有field  准备将数据写入到 索引文件
   * @throws IOException
   */
  @Override
  void finishDocument() throws IOException {

    // 代表不需要存储 field下任何有关term的向量信息 (就是offset/payload/freq/position等信息)
    if (!hasVectors) {
      return;
    }

    // Fields in term vectors are UTF16 sorted:
    // 按照fieldName 进行排序
    ArrayUtil.introSort(perFields, 0, numVectorFields);

    // 在写入前 先初始化 writer对象
    initTermVectorsWriter();

    // 填充空数据
    fill(docState.docID);

    // Append term vectors to the real outputs:
    // 该方法会在 writer上添加一个 DocData 对象
    writer.startDocument(numVectorFields);
    for (int i = 0; i < numVectorFields; i++) {
      perFields[i].finishDocument();
    }
    // 当每个field下的term都写入到writer后执行  也是非立即刷盘 除非满足某些条件
    writer.finishDocument();

    assert lastDocID == docState.docID: "lastDocID=" + lastDocID + " docState.docID=" + docState.docID;

    lastDocID++;

    // 回收存储词向量分片的空间
    super.reset();
    resetFields();
  }

  @Override
  public void abort() {
    hasVectors = false;
    try {
      super.abort();
    } finally {
      IOUtils.closeWhileHandlingException(writer);
      writer = null;
      lastDocID = 0;
      reset();
    }
  }

  /**
   * 每当开始读取一个新的doc时   重置有关上一个doc 生成的 perFields
   */
  void resetFields() {
    Arrays.fill(perFields, null); // don't hang onto stuff from previous doc
    numVectorFields = 0;
  }

  /**
   * 解析field 信息 读取termVectors
   * @param invertState
   * @param fieldInfo
   * @return
   */
  @Override
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
    return new TermVectorsConsumerPerField(invertState, this, fieldInfo);
  }

  /**
   * 代表此时某个field的数据处理完成 这里写入数据并刷盘
   * @param fieldToFlush
   */
  void addFieldToFlush(TermVectorsConsumerPerField fieldToFlush) {
    // 扩容
    if (numVectorFields == perFields.length) {
      int newSize = ArrayUtil.oversize(numVectorFields + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      TermVectorsConsumerPerField[] newArray = new TermVectorsConsumerPerField[newSize];
      System.arraycopy(perFields, 0, newArray, 0, numVectorFields);
      perFields = newArray;
    }

    // 将field对象设置到数组中
    perFields[numVectorFields++] = fieldToFlush;
  }

  /**
   * 当 chain 处理新的doc时 触发该方法
   */
  @Override
  void startDocument() {
    // 清理处理上一个doc 时 遗留的 field信息
    resetFields();
    numVectorFields = 0;
  }
}
