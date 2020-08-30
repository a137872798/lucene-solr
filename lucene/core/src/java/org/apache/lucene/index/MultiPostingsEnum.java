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

import org.apache.lucene.util.BytesRef;

/**
 * Exposes {@link PostingsEnum}, merged from {@link PostingsEnum}
 * API of sub-segments.
 *
 * @lucene.experimental
 * 创建一个整合了多个 posting 的对象
 */

public final class MultiPostingsEnum extends PostingsEnum {
  private final MultiTermsEnum parent;

  /**
   * 每次在使用前会将此时正在处理的term对应的所有 postingEnum的数据填充到这里 (多个segment下同一field下term有可能会相同  所以这里使用一个数组)
   */
  final PostingsEnum[] subPostingsEnums;
  /**
   * 存储 PostingsEnum 与 下标的bean对象
   */
  private final EnumWithSlice[] subs;
  int numSubs;
  int upto;
  PostingsEnum current;
  int currentBase;
  int doc = -1;

  /** Sole constructor.
   * @param parent The {@link MultiTermsEnum} that created us.   代表该位置信息针对的是哪一field下的 term迭代器
   * @param subReaderCount How many sub-readers are being merged.   代表由多少个segment合并
   * */
  public MultiPostingsEnum(MultiTermsEnum parent, int subReaderCount) {
    this.parent = parent;
    subPostingsEnums = new PostingsEnum[subReaderCount];
    this.subs = new EnumWithSlice[subReaderCount];
    for (int i = 0; i < subs.length; i++) {
      subs[i] = new EnumWithSlice();
    }
  }

  /** Returns {@code true} if this instance can be reused by
   *  the provided {@link MultiTermsEnum}. */
  public boolean canReuse(MultiTermsEnum parent) {
    return this.parent == parent;
  }

  /**
   * Re-use and reset this instance on the provided slices.
   * 此时subs中已经填充了 postingsEnum的数据 这里进行初始化
   */
  public MultiPostingsEnum reset(final EnumWithSlice[] subs, final int numSubs) {
    this.numSubs = numSubs;
    for(int i=0;i<numSubs;i++) {
      this.subs[i].postingsEnum = subs[i].postingsEnum;
      this.subs[i].slice = subs[i].slice;
    }
    upto = -1;
    doc = -1;
    current = null;
    return this;
  }

  /** How many sub-readers we are merging.
   *  @see #getSubs */
  public int getNumSubs() {
    return numSubs;
  }

  /** Returns sub-readers we are merging. */
  public EnumWithSlice[] getSubs() {
    return subs;
  }

  @Override
  public int freq() throws IOException {
    assert current != null;
    return current.freq();
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    assert target > doc;
    while(true) {
      if (current != null) {
        final int doc;
        if (target < currentBase) {
          // target was in the previous slice but there was no matching doc after it
          doc = current.nextDoc();
        } else {
          doc = current.advance(target-currentBase);
        }
        if (doc == NO_MORE_DOCS) {
          current = null;
        } else {
          return this.doc = doc + currentBase;
        }
      } else if (upto == numSubs-1) {
        return this.doc = NO_MORE_DOCS;
      } else {
        upto++;
        current = subs[upto].postingsEnum;
        currentBase = subs[upto].slice.start;
      }
    }
  }

  /**
   * 可以看到 postings的信息本身是没有大小概念的  所以不需要借助二叉堆排序  而是挨个遍历位置
   * @return
   * @throws IOException
   */
  @Override
  public int nextDoc() throws IOException {
    while(true) {
      if (current == null) {
        if (upto == numSubs-1) {
          return this.doc = NO_MORE_DOCS;
        } else {
          upto++;
          current = subs[upto].postingsEnum;
          currentBase = subs[upto].slice.start;
        }
      }

      final int doc = current.nextDoc();
      if (doc != NO_MORE_DOCS) {
        return this.doc = currentBase + doc;
      } else {
        current = null;
      }
    }
  }

  @Override
  public int nextPosition() throws IOException {
    return current.nextPosition();
  }

  @Override
  public int startOffset() throws IOException {
    return current.startOffset();
  }

  @Override
  public int endOffset() throws IOException {
    return current.endOffset();
  }

  @Override
  public BytesRef getPayload() throws IOException {
    return current.getPayload();
  }

  // TODO: implement bulk read more efficiently than super
  /**
   * Holds a {@link PostingsEnum} along with the
   * corresponding {@link ReaderSlice}.
   * 包裹 迭代位置信息的 PostingsEnum
   *  */
  public final static class EnumWithSlice {
    /** {@link PostingsEnum} for this sub-reader. */
    public PostingsEnum postingsEnum;

    /** {@link ReaderSlice} describing how this sub-reader
     *  fits into the composite reader. */
    // 关联的分片信息
    public ReaderSlice slice;

    EnumWithSlice() {
    }
    
    @Override
    public String toString() {
      return slice.toString()+":"+ postingsEnum;
    }
  }
  
  @Override
  public long cost() {
    long cost = 0;
    for (int i = 0; i < numSubs; i++) {
      cost += subs[i].postingsEnum.cost();
    }
    return cost;
  }
  
  @Override
  public String toString() {
    return "MultiDocsAndPositionsEnum(" + Arrays.toString(getSubs()) + ")";
  }
}

