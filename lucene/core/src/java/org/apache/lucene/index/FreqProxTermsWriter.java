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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;

/**
 * 该对象负责写入词的频率
 */
final class FreqProxTermsWriter extends TermsHash {

  /**
   * 在DefaultIndexingChain 中 该对象初始化时  还传入了一个 词向量对象 作为 下游对象
   * 因为 词向量信息和 词频率信息 都是处理同一份 term 所以可以形成链表结构
   * @param docWriter
   * @param termVectors  实际上会存储一个 TermVectorsConsumer
   */
  public FreqProxTermsWriter(DocumentsWriterPerThread docWriter, TermsHash termVectors) {
    super(docWriter, true, termVectors);
  }

  /**
   * 删除部分数据
   * @param state
   * @param fields
   * @throws IOException
   */
  private void applyDeletes(SegmentWriteState state, Fields fields) throws IOException {
    // Process any pending Term deletes for this newly
    // flushed segment:
    // 代表有数据发生了变化 并且某些数据需要被删除
    if (state.segUpdates != null && state.segUpdates.deleteTerms.size() > 0) {
      Map<Term,Integer> segDeletes = state.segUpdates.deleteTerms;
      List<Term> deleteTerms = new ArrayList<>(segDeletes.keySet());
      Collections.sort(deleteTerms);
      FrozenBufferedUpdates.TermDocsIterator iterator = new FrozenBufferedUpdates.TermDocsIterator(fields, true);
      for(Term deleteTerm : deleteTerms) {
        // 通过精确匹配term的值 返回该term所在的所有docId
        DocIdSetIterator postings = iterator.nextTerm(deleteTerm.field(), deleteTerm.bytes());
        if (postings != null ) {
          int delDocLimit = segDeletes.get(deleteTerm);
          assert delDocLimit < PostingsEnum.NO_MORE_DOCS;
          int doc;
          // 在docId 小于 limit前的 doc都会被删除
          while ((doc = postings.nextDoc()) < delDocLimit) {
            if (state.liveDocs == null) {
              // TODO 如果 liveDocs 还没有被创建 这里默认 doc是连续的
              state.liveDocs = new FixedBitSet(state.segmentInfo.maxDoc());
              state.liveDocs.set(0, state.segmentInfo.maxDoc());
            }
            // 清除对应位
            if (state.liveDocs.get(doc)) {
              state.delCountOnFlush++;
              state.liveDocs.clear(doc);
            }
          }
        }
      }
    }
  }

  /**
   * 当 DefaultIndexingChain 处理完所有的doc 后  会调用该方法
   * @param fieldsToFlush  每个value 都代表某个field下所有的 term信息
   * @param state  这个对象描述了 将数据写入到段索引中相关的参数
   * @param sortMap
   * @param norms
   * @throws IOException
   */
  @Override
  public void flush(Map<String,TermsHashPerField> fieldsToFlush, final SegmentWriteState state,
      Sorter.DocMap sortMap, NormsProducer norms) throws IOException {
    super.flush(fieldsToFlush, state, sortMap, norms);

    // Gather all fields that saw any postings:
    // 这里存储 做好 term排序的 perField 对象
    List<FreqProxTermsWriterPerField> allFields = new ArrayList<>();

    for (TermsHashPerField f : fieldsToFlush.values()) {
      final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) f;
      if (perField.bytesHash.size() > 0) {
        // 这里为 term 排序
        perField.sortPostings();
        assert perField.fieldInfo.getIndexOptions() != IndexOptions.NONE;
        allFields.add(perField);
      }
    }

    // Sort by field name
    // 按照 fieldName 再进行一次排序
    CollectionUtil.introSort(allFields);

    // 包装成一组field 对象 该对象可以遍历内部的 field 以及 term (通过指定field 可以获取到 包含该field下所有 term的 FreqProxTerms 对象)
    Fields fields = new FreqProxFields(allFields);
    // 检查 segmentWriterState中是否有标记为需要删除的doc 有的话从 aliveDoc的位图中移除对应标记位
    applyDeletes(state, fields);
    // 该对象会包装返回的 terms (terms 是该对象通过传入field获取到的)
    if (sortMap != null) {
      fields = new SortingLeafReader.SortingFields(fields, state.fieldInfos, sortMap);
    }

    FieldsConsumer consumer = state.segmentInfo.getCodec().postingsFormat().fieldsConsumer(state);
    boolean success = false;
    try {
      // 将结果写入到索引文件
      consumer.write(fields, norms);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);
      }
    }

  }

  /**
   *
   * @param invertState  内部也是存储域信息
   * @param fieldInfo
   * @return
   */
  @Override
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
    // 先调用下游存储词向量的 TermHash.addField   这时会返回一个 perField 对象  将它作为nextPerField 并初始化 以field为单位存储词频率信息的 writer
    return new FreqProxTermsWriterPerField(invertState, this, fieldInfo, nextTermsHash.addField(invertState, fieldInfo));
  }
}
