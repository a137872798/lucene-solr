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

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * {@link DocIdSet} implementation inspired from http://roaringbitmap.org/
 *
 * The space is divided into blocks of 2^16 bits and each block is encoded
 * independently. In each block, if less than 2^12 bits are set, then
 * documents are simply stored in a short[]. If more than 2^16-2^12 bits are
 * set, then the inverse of the set is encoded in a simple short[]. Otherwise
 * a {@link FixedBitSet} is used.
 *
 * @lucene.internal
 * 该容器 基于 RoaringBitMap 实现
 * 该位图的核心是3个container
 */
public class RoaringDocIdSet extends DocIdSet {

  // Number of documents in a block  一个块的默认大小是 65535
  private static final int BLOCK_SIZE = 1 << 16;
  // The maximum length for an array, beyond that point we switch to a bitset
  private static final int MAX_ARRAY_LENGTH = 1 << 12;
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(RoaringDocIdSet.class);

  /** A builder of {@link RoaringDocIdSet}s. */
  public static class Builder {

    /**
     * 记录当前最大的 doc编号
     */
    private final int maxDoc;
    /**
     * 存储经由flush()生成的数据  每个元素对应一个block的数据   写入的doc 本身会按照block大小进行分块  然后根据划分到每个块的数据大小 又生成合适大小的存储对象 比如直接基于数组 或者基于位图对象
     * 该数组的下标就是 block 同时 block从0开始
     */
    private final DocIdSet[] sets;

    /**
     * 记录总的数据量
     */
    private int cardinality;
    private int lastDocId;
    /**
     * 当前指向的block
     */
    private int currentBlock;
    /**
     * 标记当前 block的大小
     */
    private int currentBlockCardinality;

    // We start by filling the buffer and when it's full we copy the content of
    // the buffer to the FixedBitSet and put further documents in that bitset
    // 该 buffer中存储 4096 个数据  对应  RoaringBitmap.ArrayContainer
    private final short[] buffer;
    /**
     * 每次以相对比较小的数据块作为存储容器  当数据跨度超过某个值时 将数据先存储到 sets[] 中 并更新新数据的起点 以及生成对应的存储容器
     */
    private FixedBitSet denseBuffer;

    /** Sole constructor. */
    // 首先指定该位图 即将占用的位数  每记录一个docId 使用1位
    public Builder(int maxDoc) {
      this.maxDoc = maxDoc;
      // 向上取整
      sets = new DocIdSet[(maxDoc + (1 << 16) - 1) >>> 16];
      lastDocId = -1;
      currentBlock = -1;
      buffer = new short[MAX_ARRAY_LENGTH];
    }

    /**
     * 每当切换到一个新的block时 就会触发一次刷盘
     */
    private void flush() {
      assert currentBlockCardinality <= BLOCK_SIZE;
      // 代表此block中 只写入了 4096个数据  (这样就会直接分配在 buffer中  否则会创建一个 denseBuffer 并将数据全部转移到该位图中)
      if (currentBlockCardinality <= MAX_ARRAY_LENGTH) {
        // Use sparse encoding
        assert denseBuffer == null;
        // 当 currentBlock 还是0的时候就不需要写入任何数据
        if (currentBlockCardinality > 0) {
          // 生成一个 short[] 用于存放当前已经写入的所有 docId  并存储到sets[]中   正常写入的情况 buffer内部数据是连续的(以currentBlockCardinality++ 作为下标填入数据)
          sets[currentBlock] = new ShortArrayDocIdSet(ArrayUtil.copyOfSubArray(buffer, 0, currentBlockCardinality));
        }
      } else {
        // 代表数据通过一个位图来创建  还要注意的是  buffer内部的数据不需要做清理 在新的block中 执行插入 会直接覆盖旧数据 并且可以按照currentBlockCardinality 判断插入了多少数据
        assert denseBuffer != null;
        assert denseBuffer.cardinality() == currentBlockCardinality;
        // 代表当前不是最后一个block  如果是最后一个 可能分配的大小没有满65536    换言之  中间的block 对应的denseBuffer大小刚好是一个block
        // 并且 当前剩余的空间 小于 4096
        if (denseBuffer.length() == BLOCK_SIZE && BLOCK_SIZE - currentBlockCardinality < MAX_ARRAY_LENGTH) {
          // Doc ids are very dense, inverse the encoding
          // 当该block 只有不到 4096的数据没有被分配  这里要做特殊处理
          final short[] excludedDocs = new short[BLOCK_SIZE - currentBlockCardinality];
          // 将内部的数据进行反转
          denseBuffer.flip(0, denseBuffer.length());
          int excludedDoc = -1;
          for (int i = 0; i < excludedDocs.length; ++i) {
            // 相当于从后往前遍历
            excludedDoc = denseBuffer.nextSetBit(excludedDoc + 1);
            assert excludedDoc != DocIdSetIterator.NO_MORE_DOCS;
            // 逆向填满该容器
            excludedDocs[i] = (short) excludedDoc;
          }
          assert excludedDoc + 1 == denseBuffer.length() || denseBuffer.nextSetBit(excludedDoc + 1) == DocIdSetIterator.NO_MORE_DOCS;
          // 这里使用特殊的装饰器 修饰内部的 ShortArrayDocIdSet     就是一个"非" 容器 用于判断某个值不存在于容器中
          // 为了提升遍历的速度吗
          sets[currentBlock] = new NotDocIdSet(BLOCK_SIZE, new ShortArrayDocIdSet(excludedDocs));
        } else {
          // Neither sparse nor super dense, use a fixed bit set
          // 将内部的位图作为存储数据的容器  生成 BitDocIdSet 因为该位图本身分配的大小已经收到限制 所以不需要使用SparseFixedBitSet
          sets[currentBlock] = new BitDocIdSet(denseBuffer, currentBlockCardinality);
        }
        // 将当前引用置空    不过在 BitDocIdSet中 内部引用已经指向了这个位图对象了 所以位图对象本身不会被回收
        denseBuffer = null;
      }

      cardinality += currentBlockCardinality;
      // 代表分配的小容器
      denseBuffer = null;
      // 每次刷盘重置 currentBlockCardinality
      currentBlockCardinality = 0;
    }

    /**
     * Add a new doc-id to this builder.
     * NOTE: doc ids must be added in order.
     * 将某个文档号 存储到位图对象中
     */
    public Builder add(int docId) {
      // 初始状态 lastDocId 和 currentBlock 都是-1

      // 只允许递增的插入doc
      if (docId <= lastDocId) {
        throw new IllegalArgumentException("Doc ids must be added in-order, got " + docId + " which is <= lastDocID=" + lastDocId);
      }
      // 写入数据前 首先判断 docId 是否超过65535 是的话代表切换到下一个block 那么数据要进行 flush 并重置一些相关参数
      // 当写入的数据 是该block下的前4096个 直接存储到 buffer[] 中
      // 否则创建一个 denseBuffer(一个位图对象) 并将数据存储到该位图中 (之前buffer中的数据会转存到 denseBuffer中)

      // 每个  block的大小是65535
      final int block = docId >>> 16;
      // 首次 必然会触发 flush
      // 代表切换到了下一个block  那么需要将之前的数据刷盘
      if (block != currentBlock) {
        // we went to a different block, let's flush what we buffered and start from fresh
        // 刷盘并更新当前指向的block
        flush();
        currentBlock = block;
      }

      // 覆盖之前写入的数据
      if (currentBlockCardinality < MAX_ARRAY_LENGTH) {
        // 这里的转换 暗藏了 & 0xFFFF 的操作 也就是只保留相对偏移量
        buffer[currentBlockCardinality] = (short) docId;
      } else {
        // 当超过4096 时 开始创建真正的位图对象来替代 ArrayContainer
        // 对应 RoaringBitMap.BitmapContainer
        if (denseBuffer == null) {
          // the buffer is full, let's move to a fixed bit set
          // 因为一开始就知道 最大的文档号是多少 所以可以计算该block下原本需要分配多少doc   (包含了上面的4096个数)
          final int numBits = Math.min(1 << 16, maxDoc - (block << 16));
          denseBuffer = new FixedBitSet(numBits);
          // 将之前的数据转移到  bitmap中  因为进入到这里的情况就是写满了 4096 个数据 所以内部的数据肯定都是属于这个block的
          // (完全覆盖了上一轮的数据 从源码中可以看到没有 buffer内部数据的清理逻辑 都是做覆盖)
          for (short doc : buffer) {
            denseBuffer.set(doc & 0xFFFF);
          }
        }
        // 填入 本次的数据
        denseBuffer.set(docId & 0xFFFF);
      }

      // 在插入前4096个数据时 都是直接插入到 buffer中
      lastDocId = docId;
      currentBlockCardinality += 1;
      return this;
    }

    /** Add the content of the provided {@link DocIdSetIterator}. */
    // 将一组数据存储到 docIdSet中
    public Builder add(DocIdSetIterator disi) throws IOException {
      for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
        add(doc);
      }
      return this;
    }

    /** Build an instance. */
    // 将内部所有的DocIdSet 包装成 RoaringDocIdSet
    public RoaringDocIdSet build() {
      flush();
      return new RoaringDocIdSet(sets, cardinality);
    }

  }

  /**
   * {@link DocIdSet} implementation that can store documents up to 2^16-1 in a short[].
   * 基于一个 short[] 来存储 docId
   */
  private static class ShortArrayDocIdSet extends DocIdSet {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(ShortArrayDocIdSet.class);

    private final short[] docIDs;

    private ShortArrayDocIdSet(short[] docIDs) {
      this.docIDs = docIDs;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(docIDs);
    }

    /**
     * 返回一个用于迭代内部 docId 的迭代器
     */
    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new DocIdSetIterator() {

        int i = -1; // this is the index of the current document in the array
        int doc = -1; // 代表当前指向的文档号

        /**
         * 传入下标返回对应的值
         * @param i
         * @return
         */
        private int docId(int i) {
          return docIDs[i] & 0xFFFF;
        }

        /**
         * 返回下一个id
         * @return
         * @throws IOException
         */
        @Override
        public int nextDoc() throws IOException {
          if (++i >= docIDs.length) {
            return doc = NO_MORE_DOCS;
          }
          return doc = docId(i);
        }

        @Override
        public int docID() {
          return doc;
        }

        /**
         * 该迭代器内部一共包含多少个数据
         * @return
         */
        @Override
        public long cost() {
          return docIDs.length;
        }

        /**
         * 基于二分查找 快速定位到某个值
         * @param target
         * @return
         * @throws IOException
         */
        @Override
        public int advance(int target) throws IOException {
          // binary search
          int lo = i + 1;
          int hi = docIDs.length - 1;
          while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final int midDoc = docId(mid);
            if (midDoc < target) {
              lo = mid + 1;
            } else {
              hi = mid - 1;
            }
          }
          if (lo == docIDs.length) {
            i = docIDs.length;
            return doc = NO_MORE_DOCS;
          } else {
            i = lo;
            return doc = docId(i);
          }
        }
      };
    }

  }

  private final DocIdSet[] docIdSets;
  private final int cardinality;
  private final long ramBytesUsed;

  /**
   * 该对象只能通过builder 来创建
   * @param docIdSets   以block的形式避免创建大内存的位图对象
   * @param cardinality   代表该set内部数据总长度
   */
  private RoaringDocIdSet(DocIdSet[] docIdSets, int cardinality) {
    this.docIdSets = docIdSets;
    long ramBytesUsed = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(docIdSets);
    for (DocIdSet set : this.docIdSets) {
      if (set != null) {
        ramBytesUsed += set.ramBytesUsed();
      }
    }
    this.ramBytesUsed = ramBytesUsed;
    this.cardinality = cardinality;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public DocIdSetIterator iterator() throws IOException {
    if (cardinality == 0) {
      return null;
    }
    return new Iterator();
  }

  /**
   * 该迭代器可以遍历内部所有的  DocIdSet
   */
  private class Iterator extends DocIdSetIterator {

    /**
     * 当前遍历到第几个块  从0 开始
     */
    int block;
    /**
     * 当前block 对应的迭代器
     */
    DocIdSetIterator sub = null;
    int doc;

    Iterator() throws IOException {
      doc = -1;
      block = -1;
      sub = DocIdSetIterator.empty();
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      final int subNext = sub.nextDoc();
      if (subNext == NO_MORE_DOCS) {
        // 惰性生成迭代器
        return firstDocFromNextBlock();
      }
      // 返回的是相对偏移量  需要通过 | 运算获得正确的偏移量
      return doc = (block << 16) | subNext;
    }

    /**
     * 寻找目标docId 对应的下标
     * @param target
     * @return
     * @throws IOException
     */
    @Override
    public int advance(int target) throws IOException {
      // 找到 docId 所在的 block
      final int targetBlock = target >>> 16;
      // 代表需要切换迭代器
      if (targetBlock != block) {
        block = targetBlock;
        if (block >= docIdSets.length) {
          sub = null;
          return doc = NO_MORE_DOCS;
        }
        if (docIdSets[block] == null) {
          return firstDocFromNextBlock();
        }
        // 切换迭代器
        sub = docIdSets[block].iterator();
      }
      // 切换成相对偏移量后 找到在位图中的下标
      final int subNext = sub.advance(target & 0xFFFF);
      if (subNext == NO_MORE_DOCS) {
        return firstDocFromNextBlock();
      }
      // 转换回绝对偏移量
      return doc = (block << 16) | subNext;
    }

    /**
     * 找到下一个可用的block
     * @return
     * @throws IOException
     */
    private int firstDocFromNextBlock() throws IOException {
      while (true) {
        block += 1;
        if (block >= docIdSets.length) {
          sub = null;
          return doc = NO_MORE_DOCS;
        } else if (docIdSets[block] != null) {
          sub = docIdSets[block].iterator();
          final int subNext = sub.nextDoc();
          assert subNext != NO_MORE_DOCS;
          return doc = (block << 16) | subNext;
        }
      }
    }

    @Override
    public long cost() {
      return cardinality;
    }

  }

  /** Return the exact number of documents that are contained in this set. */
  public int cardinality() {
    return cardinality;
  }

  @Override
  public String toString() {
    return "RoaringDocIdSet(cardinality=" + cardinality + ")";
  }
}
