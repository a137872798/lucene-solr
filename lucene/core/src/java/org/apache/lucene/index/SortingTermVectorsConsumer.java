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
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * 增加了排序功能
 */
final class SortingTermVectorsConsumer extends TermVectorsConsumer {
  TrackingTmpOutputDirectoryWrapper tmpDirectory;

  public SortingTermVectorsConsumer(DocumentsWriterPerThread docWriter) {
    super(docWriter);
  }

  @Override
  void flush(Map<String, TermsHashPerField> fieldsToFlush, final SegmentWriteState state, Sorter.DocMap sortMap, NormsProducer norms) throws IOException {
    // 首先确保父类的数据已经持久化 之后基于临时文件的数据 配合 sortMap实现重排序
    super.flush(fieldsToFlush, state, sortMap, norms);
    if (tmpDirectory != null) {
      // 此时不需要进行重排序 只要将父类写入的临时文件修改成 普通的文件名就可以
      if (sortMap == null) {
        // we're lucky the index is already sorted, just rename the temporary file and return
        for (Map.Entry<String, String> entry : tmpDirectory.getTemporaryFiles().entrySet()) {
          tmpDirectory.rename(entry.getValue(), entry.getKey());
        }
        return;
      }
      // 读取之前写入的数据
      TermVectorsReader reader = docWriter.codec.termVectorsFormat()
          .vectorsReader(tmpDirectory, state.segmentInfo, state.fieldInfos, IOContext.DEFAULT);
      TermVectorsReader mergeReader = reader.getMergeInstance();
      // 准备好新的写入对象
      TermVectorsWriter writer = docWriter.codec.termVectorsFormat()
          .vectorsWriter(state.directory, state.segmentInfo, IOContext.DEFAULT);
      try {
        reader.checkIntegrity();
        for (int docID = 0; docID < state.segmentInfo.maxDoc(); docID++) {
          // 获取到原来doc对应的数据 之后重新写入
          Fields vectors = mergeReader.get(sortMap.newToOld(docID));
          writeTermVectors(writer, vectors, state.fieldInfos);
        }
        writer.finish(state.fieldInfos, state.segmentInfo.maxDoc());
      } finally {
        IOUtils.close(reader, writer);
        IOUtils.deleteFiles(tmpDirectory,
            tmpDirectory.getTemporaryFiles().values());
      }
    }
  }

  /**
   * 跟 SortingStoredFieldsConsumer 相同的套路 确保父类一开始只是将数据写入到临时索引文件
   * @throws IOException
   */
  @Override
  void initTermVectorsWriter() throws IOException {
    if (writer == null) {
      IOContext context = new IOContext(new FlushInfo(docWriter.getNumDocsInRAM(), docWriter.bytesUsed()));
      tmpDirectory = new TrackingTmpOutputDirectoryWrapper(docWriter.directory);
      writer = docWriter.codec.termVectorsFormat().vectorsWriter(tmpDirectory, docWriter.getSegmentInfo(), context);
      lastDocID = 0;
    }
  }

  @Override
  public void abort() {
    try {
      super.abort();
    } finally {
      if (tmpDirectory != null) {
        IOUtils.deleteFilesIgnoringExceptions(tmpDirectory,
            tmpDirectory.getTemporaryFiles().values());
      }
    }
  }

  /**
   * Safe (but, slowish) default method to copy every vector field in the provided {@link TermVectorsWriter}.
   * 总的来说就是将之前已经写入的数据 按照 sortMap 重排序后重新写入
   * */
  private static void writeTermVectors(TermVectorsWriter writer, Fields vectors, FieldInfos fieldInfos) throws IOException {
    if (vectors == null) {
      writer.startDocument(0);
      writer.finishDocument();
      return;
    }

    int numFields = vectors.size();
    if (numFields == -1) {
      // count manually! TODO: Maybe enforce that Fields.size() returns something valid?
      numFields = 0;
      for (final Iterator<String> it = vectors.iterator(); it.hasNext(); ) {
        it.next();
        numFields++;
      }
    }
    writer.startDocument(numFields);

    String lastFieldName = null;

    TermsEnum termsEnum = null;
    PostingsEnum docsAndPositionsEnum = null;

    int fieldCount = 0;
    for(String fieldName : vectors) {
      fieldCount++;
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);

      assert lastFieldName == null || fieldName.compareTo(lastFieldName) > 0: "lastFieldName=" + lastFieldName + " fieldName=" + fieldName;
      lastFieldName = fieldName;

      final Terms terms = vectors.terms(fieldName);
      if (terms == null) {
        // FieldsEnum shouldn't lie...
        continue;
      }

      final boolean hasPositions = terms.hasPositions();
      final boolean hasOffsets = terms.hasOffsets();
      final boolean hasPayloads = terms.hasPayloads();
      assert !hasPayloads || hasPositions;

      int numTerms = (int) terms.size();
      if (numTerms == -1) {
        // count manually. It is stupid, but needed, as Terms.size() is not a mandatory statistics function
        numTerms = 0;
        termsEnum = terms.iterator();
        while(termsEnum.next() != null) {
          numTerms++;
        }
      }

      writer.startField(fieldInfo, numTerms, hasPositions, hasOffsets, hasPayloads);
      termsEnum = terms.iterator();

      int termCount = 0;
      while(termsEnum.next() != null) {
        termCount++;

        // 写入term的相关信息
        final int freq = (int) termsEnum.totalTermFreq();

        writer.startTerm(termsEnum.term(), freq);

        if (hasPositions || hasOffsets) {
          docsAndPositionsEnum = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.OFFSETS | PostingsEnum.PAYLOADS);
          assert docsAndPositionsEnum != null;

          final int docID = docsAndPositionsEnum.nextDoc();
          assert docID != DocIdSetIterator.NO_MORE_DOCS;
          assert docsAndPositionsEnum.freq() == freq;

          for(int posUpto=0; posUpto<freq; posUpto++) {
            final int pos = docsAndPositionsEnum.nextPosition();
            final int startOffset = docsAndPositionsEnum.startOffset();
            final int endOffset = docsAndPositionsEnum.endOffset();

            final BytesRef payload = docsAndPositionsEnum.getPayload();

            assert !hasPositions || pos >= 0 ;
            writer.addPosition(pos, startOffset, endOffset, payload);
          }
        }
        writer.finishTerm();
      }
      assert termCount == numTerms;
      writer.finishField();
    }
    assert fieldCount == numFields;
    writer.finishDocument();
  }
}
