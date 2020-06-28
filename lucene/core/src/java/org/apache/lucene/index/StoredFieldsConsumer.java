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
 * 该对象用于处理存储的 field
 */
class StoredFieldsConsumer {
  /**
   * 将doc转换成索引的线程
   */
  final DocumentsWriterPerThread docWriter;
  StoredFieldsWriter writer;
  int lastDoc;

  StoredFieldsConsumer(DocumentsWriterPerThread docWriter) {
    this.docWriter = docWriter;
    this.lastDoc = -1;
  }

  /**
   * 惰性初始化
   * @throws IOException
   */
  protected void initStoredFieldsWriter() throws IOException {
    if (writer == null) {
      this.writer =
          docWriter.codec.storedFieldsFormat().fieldsWriter(docWriter.directory, docWriter.getSegmentInfo(),
              IOContext.DEFAULT);
    }
  }

  /**
   * 代表此时正在处理一个新的 doc
   * @param docID  该doc对应的id
   * @throws IOException
   */
  void startDocument(int docID) throws IOException {
    assert lastDoc < docID;
    initStoredFieldsWriter();
    while (++lastDoc < docID) {
      writer.startDocument();
      writer.finishDocument();
    }
    writer.startDocument();
  }

  void writeField(FieldInfo info, IndexableField field) throws IOException {
    writer.writeField(info, field);
  }

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
