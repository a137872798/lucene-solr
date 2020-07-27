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
import java.util.List;

import org.apache.lucene.util.BytesRef;

/**
 * Exposes flex API, merged from flex API of sub-segments,
 * remapping docIDs (this is used for segment merging).
 *
 * @lucene.experimental
 */

final class MappingMultiPostingsEnum extends PostingsEnum {

  /**
   * 该对象内部已经整合了多个 postingsEnum 对象
   */
  MultiPostingsEnum multiDocsAndPositionsEnum;
  final String field;
  /**
   * 将每个段对应的数据 整合起来
   */
  final DocIDMerger<MappingPostingsSub> docIDMerger;
  private MappingPostingsSub current;
  private final MappingPostingsSub[] allSubs;
  private final List<MappingPostingsSub> subs = new ArrayList<>();


  /**
   * 该对象相当于做了一个转换
   */
  private static class MappingPostingsSub extends DocIDMerger.Sub {
    public PostingsEnum postings;

    public MappingPostingsSub(MergeState.DocMap docMap) {
      super(docMap);
    }

    @Override
    public int nextDoc() {
      try {
        return postings.nextDoc();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  /**
   * Sole constructor.
   * @param field 此时绑定的field信息  (field定位到一组terms,之后再定位到这里)
   */
  public MappingMultiPostingsEnum(String field, MergeState mergeState) throws IOException {
    this.field = field;
    // 生成一组可以获取posting属性的对象
    allSubs = new MappingPostingsSub[mergeState.fieldsProducers.length];
    for(int i=0;i<allSubs.length;i++) {
      // 使用对应的 docMaps映射对象来初始化
      allSubs[i] = new MappingPostingsSub(mergeState.docMaps[i]);
    }
    // 将一组sub对象整合在一起
    this.docIDMerger = DocIDMerger.of(subs, allSubs.length, mergeState.needsIndexSort);
  }

  /**
   * 使用相关属性进行初始化
   * @param postingsEnum
   * @return
   * @throws IOException
   */
  MappingMultiPostingsEnum reset(MultiPostingsEnum postingsEnum) throws IOException {
    this.multiDocsAndPositionsEnum = postingsEnum;
    // EnumWithSlice 内部包含了当前term读取出来的 PostingsEnum
    MultiPostingsEnum.EnumWithSlice[] subsArray = postingsEnum.getSubs();
    int count = postingsEnum.getNumSubs();
    subs.clear();
    for(int i=0;i<count;i++) {
      // 就是赋值操作
      // 这里将数据填充到 sub 之后就开始构建DocIDMerger 对象了
      MappingPostingsSub sub = allSubs[subsArray[i].slice.readerIndex];
      sub.postings = subsArray[i].postingsEnum;
      subs.add(sub);
    }
    docIDMerger.reset();
    return this;
  }

  @Override
  public int freq() throws IOException {
    return current.postings.freq();
  }

  @Override
  public int docID() {
    if (current == null) {
      return -1;
    } else {
      return current.mappedDocID;
    }
  }

  @Override
  public int advance(int target) {
    throw new UnsupportedOperationException();
  }

  /**
   * 遍历到下一个doc
   * @return
   * @throws IOException
   */
  @Override
  public int nextDoc() throws IOException {
    // 获取此时最小doc所绑定的  postingsEnum
    current = docIDMerger.next();
    if (current == null) {
      return NO_MORE_DOCS;
    } else {
      return current.mappedDocID;
    }
  }

  @Override
  public int nextPosition() throws IOException {
    int pos = current.postings.nextPosition();
    if (pos < 0) {
      throw new CorruptIndexException("position=" + pos + " is negative, field=\"" + field + " doc=" + current.mappedDocID,
                                      current.postings.toString());
    } else if (pos > IndexWriter.MAX_POSITION) {
      throw new CorruptIndexException("position=" + pos + " is too large (> IndexWriter.MAX_POSITION=" + IndexWriter.MAX_POSITION + "), field=\"" + field + "\" doc=" + current.mappedDocID,
                                      current.postings.toString());
    }
    return pos;
  }
  
  @Override
  public int startOffset() throws IOException {
    return current.postings.startOffset();
  }
  
  @Override
  public int endOffset() throws IOException {
    return current.postings.endOffset();
  }
  
  @Override
  public BytesRef getPayload() throws IOException {
    return current.postings.getPayload();
  }

  @Override
  public long cost() {
    long cost = 0;
    for (MappingPostingsSub sub : subs) {
      cost += sub.postings.cost();
    }
    return cost;
  }
}

