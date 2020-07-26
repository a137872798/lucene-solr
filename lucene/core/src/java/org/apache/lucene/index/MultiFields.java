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
 */
public final class MultiFields extends Fields {
  /**
   * 每个对象都是参与merge的某个segment下所有field
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
      // 这里的 fields 已经做过处理了 只能够得到同一format的field
      subIterators[i] = subs[i].iterator();
    }
    return new MergedIterator<>(subIterators);
  }

  /**
   * 读取某个field下所有的term
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
      final Terms terms = subs[i].terms(field);
      if (terms != null) {
        subs2.add(terms);
        slices2.add(subSlices[i]);
      }
    }
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

