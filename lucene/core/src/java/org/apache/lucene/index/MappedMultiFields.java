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
 *  实现merge term的核心类 隐藏了内部整合 各个参与merge的segment的 对应fields的逻辑
 *  实现都是委托给 MultiFields
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
   * 合并逻辑就是通过 获取某个field下所有的term 进行写入  而在这里应该会想办法从每个参与merge的segment中获取field下所有term
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
   * 对外暴露的是这个对象
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
     * 在merge时  写入term前 还会将所有field下所有term的 posting信息写入到 PostingWriter中   这时会通过 postings() 获取可以遍历位置信息的迭代器
     * @param reuse
     * @param flags  代表存储了哪些位置信息
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

      // 返回的是被包装的对象生成的 postings
      MultiPostingsEnum docsAndPositionsEnum = (MultiPostingsEnum) in.postings(mappingDocsAndPositionsEnum.multiDocsAndPositionsEnum, flags);
      mappingDocsAndPositionsEnum.reset(docsAndPositionsEnum);
      return mappingDocsAndPositionsEnum;
    }
  }
}
