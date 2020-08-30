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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.util.MergedIterator;

/**
 * Provides a single {@link Fields} term index view over an
 * {@link IndexReader}.
 * This is useful when you're interacting with an {@link
 * IndexReader} implementation that consists of sequential
 * sub-readers (eg {@link DirectoryReader} or {@link
 * MultiReader}) and you must treat it as a {@link LeafReader}.
 *
 * <p><b>NOTE</b>: for composite readers, you'll get better
 * performance by gathering the sub readers using
 * {@link IndexReader#getContext()} to get the
 * atomic leaves and then operate per-LeafReader,
 * instead of using this class.
 *
 * @lucene.internal
 * 将多个fields 合并成一个 fields
 */
public final class MultiFields extends Fields {
  /**
   * Fields 只是一个高层抽象 代表可以遍历field信息 子类实现还可以展示field的各种信息 甚至该field下的所有terms
   */
  private final Fields[] subs;
  /**
   * 代表对应segment下总共有多少doc  以及doc从哪里开始  (maxDoc 应该是还没瘦身过的 也就是虽然delDoc已经被处理 但是没有整理docId)
   */
  private final ReaderSlice[] subSlices;
  /**
   * 存储某个field 对应的所有term
   */
  private final Map<String,Terms> terms = new ConcurrentHashMap<>();

  /**
   * Sole constructor.
   */
  public MultiFields(Fields[] subs, ReaderSlice[] subSlices) {
    this.subs = subs;
    this.subSlices = subSlices;
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  @Override
  public Iterator<String> iterator() {
    Iterator<String> subIterators[] = new Iterator[subs.length];
    for(int i=0;i<subs.length;i++) {
      subIterators[i] = subs[i].iterator();
    }
    return new MergedIterator<>(subIterators);
  }

  /**
   * 将参与merge的每个 segment下抽取该field的terms信息 并合并
   * @param field
   * @return
   * @throws IOException
   */
  @Override
  public Terms terms(String field) throws IOException {
    // 先尝试从缓存中获取
    Terms result = terms.get(field);
    if (result != null)
      return result;


    // Lazy init: first time this field is requested, we
    // create & add to terms:
    final List<Terms> subs2 = new ArrayList<>();
    final List<ReaderSlice> slices2 = new ArrayList<>();

    // Gather all sub-readers that share this field
    for(int i=0;i<subs.length;i++) {
      // 获取每个segment 下该field的terms
      final Terms terms = subs[i].terms(field);
      // 只有该segment下确实有该field的信息时  才获取terms 并且参与合并     此时subs[?].terms 就是 FieldReader
      if (terms != null) {
        subs2.add(terms);
        slices2.add(subSlices[i]);
      }
    }
    // 代表在所有参与merge的segment下都没有该field信息
    if (subs2.size() == 0) {
      result = null;
      // don't cache this case with an unbounded cache, since the number of fields that don't exist
      // is unbounded.
    } else {
      // 将多个 term整合后返回
      result = new MultiTerms(subs2.toArray(Terms.EMPTY_ARRAY),
          slices2.toArray(ReaderSlice.EMPTY_ARRAY));
      terms.put(field, result);
    }

    return result;
  }

  @Override
  public int size() {
    return -1;
  }

}

