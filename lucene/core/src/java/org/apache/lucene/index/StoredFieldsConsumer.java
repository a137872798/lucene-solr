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

import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

/**
 * 该对象专门负责存储 field的相关信息
 */
class StoredFieldsConsumer {
  /**
   * 将doc转换成索引的线程
   */
  final DocumentsWriterPerThread docWriter;
  StoredFieldsWriter writer;
  /**
   * 记录上一个处理的doc
   */
  int lastDoc;

  StoredFieldsConsumer(DocumentsWriterPerThread docWriter) {
    this.docWriter = docWriter;
    this.lastDoc = -1;
  }

  /**
   * 创建将field信息写入到索引文件中的 writer
   * @throws IOException
   */
  protected void initStoredFieldsWriter() throws IOException {
    if (writer == null) {
      // 确保此时用于写入field信息的对象已经被初始化
      this.writer =
          docWriter.codec.storedFieldsFormat().fieldsWriter(docWriter.directory, docWriter.getSegmentInfo(),
              IOContext.DEFAULT);
    }
  }

  /**
   * 代表此时准备处理一个新的doc
   * @param docID  该doc对应的id
   * @throws IOException
   */
  void startDocument(int docID) throws IOException {
    assert lastDoc < docID;
    initStoredFieldsWriter();
    // 当docId 不连续时 会写入空的doc
    while (++lastDoc < docID) {
      writer.startDocument();
      writer.finishDocument();
    }
    writer.startDocument();
  }

  /**
   * 存储域值信息
   * @param info
   * @param field
   * @throws IOException
   */
  void writeField(FieldInfo info, IndexableField field) throws IOException {
    writer.writeField(info, field);
  }

  /**
   * 代表有关某个doc下所有的 field 都已经写入完毕了
   * @throws IOException
   */
  void finishDocument() throws IOException {
    writer.finishDocument();
  }

  void finish(int maxDoc) throws IOException {
    while (lastDoc < maxDoc-1) {
      startDocument(lastDoc);
      finishDocument();
      ++lastDoc;
    }
  }

  void flush(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    try {
      writer.finish(state.fieldInfos, state.segmentInfo.maxDoc());
    } finally {
      IOUtils.close(writer);
      writer = null;
    }
  }

  void abort() {
    if (writer != null) {
      IOUtils.closeWhileHandlingException(writer);
      writer = null;
    }
  }
}
