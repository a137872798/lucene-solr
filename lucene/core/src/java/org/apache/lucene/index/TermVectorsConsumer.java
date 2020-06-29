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
 * 该对象和SortingTermVectorsConsumer 作为 FreqProxTermsWriter 的下游 会接受它发来的数据
 */
class TermVectorsConsumer extends TermsHash {
  /**
   * 该对象负责将词向量写入索引文件
   */
  TermVectorsWriter writer;

  /** Scratch term used by TermVectorsConsumerPerField.finishDocument. */
  final BytesRef flushTerm = new BytesRef();

  /**
   * 该对象是由哪个线程写入的
   */
  final DocumentsWriterPerThread docWriter;

  /** Used by TermVectorsConsumerPerField when serializing
   *  the term vectors. */
  final ByteSliceReader vectorSliceReaderPos = new ByteSliceReader();
  final ByteSliceReader vectorSliceReaderOff = new ByteSliceReader();

  boolean hasVectors;
  /**
   * 每当chain 处理一个新的 doc时 会重置该值
   */
  int numVectorFields;
  int lastDocID;
  /**
   * 每次 reset时 会置空这个数组
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
   * 这里将 相关信息写入到索引中
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
      writer.startDocument(0);
      writer.finishDocument();
      lastDocID++;
    }
  }

  /**
   * 初始化 writer对象
   * @throws IOException
   */
  void initTermVectorsWriter() throws IOException {
    if (writer == null) {
      IOContext context = new IOContext(new FlushInfo(docWriter.getNumDocsInRAM(), docWriter.bytesUsed()));
      writer = docWriter.codec.termVectorsFormat().vectorsWriter(docWriter.directory, docWriter.getSegmentInfo(), context);
      lastDocID = 0;
    }
  }

  @Override
  void finishDocument() throws IOException {

    if (!hasVectors) {
      return;
    }

    // Fields in term vectors are UTF16 sorted:
    ArrayUtil.introSort(perFields, 0, numVectorFields);

    initTermVectorsWriter();

    fill(docState.docID);

    // Append term vectors to the real outputs:
    writer.startDocument(numVectorFields);
    for (int i = 0; i < numVectorFields; i++) {
      perFields[i].finishDocument();
    }
    writer.finishDocument();

    assert lastDocID == docState.docID: "lastDocID=" + lastDocID + " docState.docID=" + docState.docID;

    lastDocID++;

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
   * 重置内部的域信息
   */
  void resetFields() {
    Arrays.fill(perFields, null); // don't hang onto stuff from previous doc
    numVectorFields = 0;
  }

  /**
   * 词向量对象 在插入一个域时 会生成 一个  TermVectorsConsumerPerField
   * @param invertState
   * @param fieldInfo
   * @return
   */
  @Override
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
    return new TermVectorsConsumerPerField(invertState, this, fieldInfo);
  }

  void addFieldToFlush(TermVectorsConsumerPerField fieldToFlush) {
    if (numVectorFields == perFields.length) {
      int newSize = ArrayUtil.oversize(numVectorFields + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      TermVectorsConsumerPerField[] newArray = new TermVectorsConsumerPerField[newSize];
      System.arraycopy(perFields, 0, newArray, 0, numVectorFields);
      perFields = newArray;
    }

    perFields[numVectorFields++] = fieldToFlush;
  }

  /**
   * 当 chain 处理新的doc时 触发该方法
   */
  @Override
  void startDocument() {
    resetFields();
    numVectorFields = 0;
  }
}
