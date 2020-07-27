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

import static org.apache.lucene.index.FilterLeafReader.FilterFields;
import static org.apache.lucene.index.FilterLeafReader.FilterTerms;
import static org.apache.lucene.index.FilterLeafReader.FilterTermsEnum;

/** A {@link Fields} implementation that merges multiple
 *  Fields into one, and maps around deleted documents.
 *  This is used for merging. 
 *  @lucene.internal
 *  将多个segment映射到 merge后的新segment上
 */
public class MappedMultiFields extends FilterFields {

  final MergeState mergeState;

  /** Create a new MappedMultiFields for merging, based on the supplied
   * mergestate and merged view of terms. */
  public MappedMultiFields(MergeState mergeState, MultiFields multiFields) {
    super(multiFields);
    this.mergeState = mergeState;
  }

  /**
   * 通过field信息 获取term   在上层是读取每个合并前段的term信息 合并后返回
   * @param field
   * @return
   * @throws IOException
   */
  @Override
  public Terms terms(String field) throws IOException {
    MultiTerms terms = (MultiTerms) in.terms(field);
    if (terms == null) {
      return null;
    } else {
      // 这里会包裹已经整合过的 terms
      return new MappedMultiTerms(field, mergeState, terms);
    }
  }

  /**
   * 对已经整合了 各个segment terms的 对象进一步包装
   */
  private static class MappedMultiTerms extends FilterTerms {
    final MergeState mergeState;
    final String field;

    /**
     *
     * @param field
     * @param mergeState
     * @param multiTerms 已经整合过的 terms 对象
     */
    public MappedMultiTerms(String field, MergeState mergeState, MultiTerms multiTerms) {
      super(multiTerms);
      this.field = field;
      this.mergeState = mergeState;
    }

    /**
     * 迭代合并后的 term信息
     * @return
     * @throws IOException
     */
    @Override
    public TermsEnum iterator() throws IOException {
      TermsEnum iterator = in.iterator();
      if (iterator == TermsEnum.EMPTY) {
        // LUCENE-6826
        return TermsEnum.EMPTY;
      } else {
        return new MappedMultiTermsEnum(field, mergeState, (MultiTermsEnum) iterator);
      }
    }

    @Override
    public long size() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * 包装整合过的 term迭代器
   */
  private static class MappedMultiTermsEnum extends FilterTermsEnum {
    final MergeState mergeState;
    final String field;

    public MappedMultiTermsEnum(String field, MergeState mergeState, MultiTermsEnum multiTermsEnum) {
      super(multiTermsEnum);
      this.field = field;
      this.mergeState = mergeState;
    }

    @Override
    public int docFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * 将当前term 有关pos的信息填充到 reuse中
     * @param reuse
     * @param flags
     * @return
     * @throws IOException
     */
    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      MappingMultiPostingsEnum mappingDocsAndPositionsEnum;
      if (reuse instanceof MappingMultiPostingsEnum) {
        MappingMultiPostingsEnum postings = (MappingMultiPostingsEnum) reuse;
        // 当原对象的 field也相同的情况下 使用原对象  否则创建新对象
        if (postings.field.equals(this.field)) {
          mappingDocsAndPositionsEnum = postings;
        } else {
          mappingDocsAndPositionsEnum = new MappingMultiPostingsEnum(field, mergeState);
        }
      } else {
        mappingDocsAndPositionsEnum = new MappingMultiPostingsEnum(field, mergeState);
      }

      // 在这一步中 数据会填充到 docsAndPositionsEnum
      MultiPostingsEnum docsAndPositionsEnum = (MultiPostingsEnum) in.postings(mappingDocsAndPositionsEnum.multiDocsAndPositionsEnum, flags);
      mappingDocsAndPositionsEnum.reset(docsAndPositionsEnum);
      return mappingDocsAndPositionsEnum;
    }
  }
}
