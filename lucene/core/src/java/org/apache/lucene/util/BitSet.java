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
package org.apache.lucene.util;


import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * Base implementation for a bit set.
 * @lucene.internal
 * 使用一个 set实现 bits
 * bits 模拟了一个 bit[]  包含2个基础api  通过传入index 判断目标位置上的bit是否设置   以及判断该 bit[] 的长度
 */
public abstract class BitSet implements Bits, Accountable {

  /** Build a {@link BitSet} from the content of the provided {@link DocIdSetIterator}.
   *  NOTE: this will fully consume the {@link DocIdSetIterator}. */
  // 通过 docId 迭代器和 maxDoc 初始化一个 bitSet
  public static BitSet of(DocIdSetIterator it, int maxDoc) throws IOException {
    // 该迭代器内 总计有多少 docId
    final long cost = it.cost();
    // 以128作为分割线    如果maxDoc 本身比较大  那么创建的位图数组就会很大  这有可能会导致大量内存被浪费
    // lucene 创建了一个压缩的位图对象 先将doc本身映射到一个小数组上 在这基础上 在设置占位符  这样实际创建的数组大小就变成了原来的 1/64
    final int threshold = maxDoc >>> 7;
    BitSet set;
    // docId 区间在 128之内的创建一个稀疏的 bitSet
    if (cost < threshold) {
      set = new SparseFixedBitSet(maxDoc);
    } else {
      // 如果 docId跨度比较大的话 会创建一个普通的bitSet
      set = new FixedBitSet(maxDoc);
    }
    // 使用该迭代器填充 set
    set.or(it);
    return set;
  }

  /** Set the bit at <code>i</code>. */
  // 填充指定下标的数据    因为bit 实际上只有2个状态 一个是已经设置值 一个未设置值
  public abstract void set(int i);

  /** Clear the bit at <code>i</code>. */
  // 清除bit 对应位置的值
  public abstract void clear(int i);

  /** Clears a range of bits.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   *                 清除指定范围内所有的bit
   */
  public abstract void clear(int startIndex, int endIndex);

  /**
   * Return the number of bits that are set.
   * NOTE: this method is likely to run in linear time
   * 返回当前 set 内已经设置的bit数量
   */
  public abstract int cardinality();

  /**
   * Return an approximation of the cardinality of this set. Some
   * implementations may trade accuracy for speed if they have the ability to
   * estimate the cardinality of the set without iterating over all the data.
   * The default implementation returns {@link #cardinality()}.
   * 获取一个近似的基数值   计算该值有2个方向 一个是追求速度 那么可能返回的值准确性相对较差  还有一个是追求准确性 那么耗费的时间可能会长些
   */
  public int approximateCardinality() {
    return cardinality();
  }

  /** Returns the index of the last set bit before or on the index specified.
   *  -1 is returned if there are no more set bits.
   *  返回索引前一个bit所在的位置
   */
  public abstract int prevSetBit(int index);

  /** Returns the index of the first set bit starting at the index specified.
   *  {@link DocIdSetIterator#NO_MORE_DOCS} is returned if there are no more set bits.
   *  返回索引后一个bit所在的位置
   */
  public abstract int nextSetBit(int index);

  /** Assert that the current doc is -1. */
  // 检查是否还能继续获取 docId
  protected final void checkUnpositioned(DocIdSetIterator iter) {
    if (iter.docID() != -1) {
      throw new IllegalStateException("This operation only works with an unpositioned iterator, got current position = " + iter.docID());
    }
  }

  /** Does in-place OR of the bits provided by the iterator. The state of the
   *  iterator after this operation terminates is undefined. */
  // 不断的迭代 docId 并且在 bit[] 对应的位置设置值
  public void or(DocIdSetIterator iter) throws IOException {
    checkUnpositioned(iter);
    for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
      set(doc);
    }
  }

}
