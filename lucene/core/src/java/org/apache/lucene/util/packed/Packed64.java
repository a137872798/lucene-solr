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
package org.apache.lucene.util.packed;


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Space optimized random access capable array of values with a fixed number of
 * bits/value. Values are packed contiguously.
 * <p>
 * The implementation strives to perform as fast as possible under the
 * constraint of contiguous bits, by avoiding expensive operations. This comes
 * at the cost of code clarity.
 * <p>
 * Technical details: This implementation is a refinement of a non-branching
 * version. The non-branching get and set methods meant that 2 or 4 atomics in
 * the underlying array were always accessed, even for the cases where only
 * 1 or 2 were needed. Even with caching, this had a detrimental effect on
 * performance.
 * Related to this issue, the old implementation used lookup tables for shifts
 * and masks, which also proved to be a bit slower than calculating the shifts
 * and masks on the fly.
 * See https://issues.apache.org/jira/browse/LUCENE-4062 for details.
 * 最紧凑的实现方式 也就是数据以最小的bits 写入到内存中   而一般写入的单位是一个block  也就存在某个完整的值实际上跨越了2个 block的可能
 * 相对的 Packed64SingleBlock 能够确保某个值不会跨越2个block  不过这也就会存在内存浪费了
 */
class Packed64 extends PackedInts.MutableImpl {
  static final int BLOCK_SIZE = 64; // 32 = int, 64 = long
  static final int BLOCK_BITS = 6; // The #bits representing BLOCK_SIZE
  static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

  /**
   * Values are stores contiguously in the blocks array.
   * 当中每个long值 上面的每一位 都包含了还原每个值所需的必备信息  也就是存储的最小单位 block 就是 64位 也就是long[]中的某个值
   */
  private final long[] blocks;
  /**
   * A right-aligned mask of width BitsPerValue used by {@link #get(int)}.
   */
  private final long maskRight;
  /**
   * Optimization: Saves one lookup in {@link #get(int)}.
   */
  private final int bpvMinusBlockSize;

  /**
   * Creates an array with the internal structures adjusted for the given
   * limits and initialized to 0.
   * @param valueCount   the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   */
  public Packed64(int valueCount, int bitsPerValue) {
    super(valueCount, bitsPerValue);
    // 默认采用紧凑的方式
    final PackedInts.Format format = PackedInts.Format.PACKED;
    // 计算valueCount 一共需要多少个long存储 实际上就是 转换为以bit作为基本存储单位后 再 /64 这样long型变量的每个位数都有意义
    final int longCount = format.longCount(PackedInts.VERSION_CURRENT, valueCount, bitsPerValue);
    this.blocks = new long[longCount];
    // >>> 右边会补0
    maskRight = ~0L << (BLOCK_SIZE-bitsPerValue) >>> (BLOCK_SIZE-bitsPerValue);
    // bitsPerValue - 64    得到的是个负数
    bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
  }

  /**
   * Creates an array with content retrieved from the given DataInput.
   * @param in       a DataInput, positioned at the start of Packed64-content.
   * @param valueCount  the number of elements.
   * @param bitsPerValue the number of bits available for any given value.
   * @throws java.io.IOException if the values for the backing array could not
   *                             be retrieved.
   */
  public Packed64(int packedIntsVersion, DataInput in, int valueCount, int bitsPerValue)
                                                            throws IOException {
    super(valueCount, bitsPerValue);
    final PackedInts.Format format = PackedInts.Format.PACKED;
    final long byteCount = format.byteCount(packedIntsVersion, valueCount, bitsPerValue); // to know how much to read
    final int longCount = format.longCount(PackedInts.VERSION_CURRENT, valueCount, bitsPerValue); // to size the array
    blocks = new long[longCount];
    // read as many longs as we can
    for (int i = 0; i < byteCount / 8; ++i) {
      blocks[i] = in.readLong();
    }
    final int remaining = (int) (byteCount % 8);
    if (remaining != 0) {
      // read the last bytes
      long lastLong = 0;
      for (int i = 0; i < remaining; ++i) {
        lastLong |= (in.readByte() & 0xFFL) << (56 - i * 8);
      }
      blocks[blocks.length - 1] = lastLong;
    }
    maskRight = ~0L << (BLOCK_SIZE-bitsPerValue) >>> (BLOCK_SIZE-bitsPerValue);
    bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
  }

  /**
   * @param index the position of the value.
   * @return the value at the given index.
   * 按照指定的偏移量读取数据
   */
  @Override
  public long get(final int index) {
    // The abstract index in a bit stream
    // 首先在保存时每个值都要占 这么多bit  所以搜索的时候 下标*bitsPerValue 就能够跳跃到该值对应的起点
    final long majorBitPos = (long)index * bitsPerValue;
    // The index in the backing long-array
    // 这里 除以一个block的大小 就获得了 block的下标  意味着这个值当前处在第几个block
    // 在 Packed64 中一个 block 就是64位
    final int elementPos = (int)(majorBitPos >>> BLOCK_BITS);
    // The number of value-bits in the second long
    // 该值是用来判断是否跨越了block
    // bpvMinusBlockSize =   bitsPerValue - 64
    //   majorBitPos & MOD_MASK 相当于对64取余 然后加上bitPerValue 代表从目标偏移量读取该值后 终点所在的位置 如果小于64 代表没有跨越到下个block
    // 代表终点在第二个block的什么位置  如果是负数 代表总和没有超过64
    final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

    // 代表整个值可以通过读取一个block获得
    if (endBits <= 0) { // Single block
        // 找到对应的block后  -endBits 就是终点指针到 block的结尾还有多少位   然后再与掩码做运算 就获得了目标值转换成的位
      return (blocks[elementPos] >>> -endBits) & maskRight;
    }
    // Two blocks
    // 代表发生了block的跨越
    return ((blocks[elementPos] << endBits)
        | (blocks[elementPos+1] >>> (BLOCK_SIZE - endBits)))
        & maskRight;    // 通过掩码来去除最高位一些无关的值 (超过bitPerValue位的部分)
  }

    /**
     * 从指定的数组中获取数据
     * @param index  代表 arr的下标
     * @param arr   本次要存储的所有数据
     * @param off
     * @param len
     * @return
     */
  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    len = Math.min(len, valueCount - index);
    assert off + len <= arr.length;

    final int originalIndex = index;
    // 找到对应的解码器
    final PackedInts.Decoder decoder = BulkOperation.of(PackedInts.Format.PACKED, bitsPerValue);

    // go to the next block where the value does not span across two blocks
    // 有余数  代表无法快捷的大块读取  必须小块小块读取
    final int offsetInBlocks = index % decoder.longValueCount();
    if (offsetInBlocks != 0) {
      //  而该小块 可能正好在一个block中 也可能刚好经过2个block
      for (int i = offsetInBlocks; i < decoder.longValueCount() && len > 0; ++i) {
        // 将读取结果填充到数组中
        arr[off++] = get(index++);
        --len;
      }
      // 返回读取了多少数据
      if (len == 0) {
        return index - originalIndex;
      }
    }

    // bulk get   这里代表可以大块的读取
//    assert index % decoder.longValueCount() == 0;
    // 找到 block下标
    int blockIndex = (int) (((long) index * bitsPerValue) >>> BLOCK_BITS);
//    assert (((long)index * bitsPerValue) & MOD_MASK) == 0;

    // 代表会读取几个block  (大块读取)
    final int iterations = len / decoder.longValueCount();
    // 按照一个 block(64位) 为单位直接批量读取
    decoder.decode(blocks, blockIndex, arr, off, iterations);
    // 计算读取到了哪
    final int gotValues = iterations * decoder.longValueCount();
    index += gotValues;
    len -= gotValues;
    assert len >= 0;

    if (index > originalIndex) {
      // stay at the block boundary
      return index - originalIndex;
    } else {
      // no progress so far => already at a block boundary but no full block to get
      assert index == originalIndex;
      return super.get(index, arr, off, len);
    }
  }

  /**
   * 将某个long值写入
   * @param index where the value should be positioned.  该值在数组中对应的下标
   * @param value a value conforming to the constraints set by the array.
   */
  @Override
  public void set(final int index, final long value) {
    // The abstract index in a contiguous bit stream
    // 找到这个值的 起始bit
    final long majorBitPos = (long)index * bitsPerValue;
    // The index in the backing long-array
    // 定位到block的下标
    final int elementPos = (int)(majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
    // The number of value-bits in the second long
    // 如果大于0代表最终位置会停留在第二个block的哪里
    // 如果计算出的 majorBitPos 为0 比如说bitsPerValue是8 那么 endBits 就是-56
    final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

    if (endBits <= 0) { // Single block
        // blocks[elementPos] &  ~(maskRight << -endBits) 保留高endBits位     （maskRight 只是 00000...1111 最后 bitsPerValue是1）
      blocks[elementPos] = blocks[elementPos] &  ~(maskRight << -endBits)
         | (value << -endBits);
      return;
    }
    // Two blocks
    // endBits 是整数 代表结尾在 第二个block的位置
    blocks[elementPos] = blocks[elementPos] &  ~(maskRight >>> endBits)
        | (value >>> endBits);  // 代表只有 原本的长度 - endBits会用到
    // 低位写到第二个block的最前面
    blocks[elementPos+1] = blocks[elementPos+1] &  (~0L >>> endBits)
        | (value << (BLOCK_SIZE - endBits));
  }

  /**
   * 代表往该数组中填充数据
   * @param index  代表从数组的第几个开始填充
   * @param arr pending[]   内部存放了所有待压缩的数据
   * @param off   当前正准备写入第几个  应该与index一致
   * @param len  原始数据中还未写入的长度
   * @return
   */
  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    // index 会从0开始 直到 (valueCount - 1)
    assert index >= 0 && index < valueCount;
    len = Math.min(len, valueCount - index);
    assert off + len <= arr.length;

    // 记录起始偏移量
    final int originalIndex = index;
    // 根据每个value消耗的bit值生成对应的转换器 或者说编码器
    final PackedInts.Encoder encoder = BulkOperation.of(PackedInts.Format.PACKED, bitsPerValue);

    // go to the next block where the value does not span across two blocks
    // 如果余数为0 代表刚好从某个block的起点开始 否则就是往一个已经包含部分数据的block继续填充数据 这时就有可能发生某个值填写在2个block上
    final int offsetInBlocks = index % encoder.longValueCount();
    // 此时该偏移量没有对应某个long的开始  剩下的那部分单独填充
    if (offsetInBlocks != 0) {
      // 这里从余数开始  直到 满足了longValueCount 也就代表着此时刚好切换到一个新的block
      for (int i = offsetInBlocks; i < encoder.longValueCount() && len > 0; ++i) {
        set(index++, arr[off++]);
        --len;
      }
      // 代表数据全部写完了 返回写入的长度
      if (len == 0) {
        return index - originalIndex;
      }
    }

    // 按照整块来填充   这里起点一定是某个long的开始
    // bulk set
    assert index % encoder.longValueCount() == 0;
    // 获取block的下标
    int blockIndex = (int) (((long) index * bitsPerValue) >>> BLOCK_BITS);
    assert (((long)index * bitsPerValue) & MOD_MASK) == 0;
    // 总计要放入几个block中
    final int iterations = len / encoder.longValueCount();
    encoder.encode(arr, off, blocks, blockIndex, iterations);
    final int setValues = iterations * encoder.longValueCount();
    index += setValues;
    len -= setValues;
    assert len >= 0;

    if (index > originalIndex) {
      // stay at the block boundary
      return index - originalIndex;
    } else {
      // no progress so far => already at a block boundary but no full block to get
      assert index == originalIndex;
      return super.set(index, arr, off, len);
    }
  }

  @Override
  public String toString() {
    return "Packed64(bitsPerValue=" + bitsPerValue + ",size="
            + size() + ",blocks=" + blocks.length + ")";
  }

  /**
   * 返回该对象占用的总byte数
   * @return
   */
  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 3 * Integer.BYTES   // bpvMinusBlockSize,valueCount,bitsPerValue
        + Long.BYTES          // maskRight
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // blocks ref
        + RamUsageEstimator.sizeOf(blocks);
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    assert PackedInts.unsignedBitsRequired(val) <= getBitsPerValue();
    assert fromIndex <= toIndex;

    // minimum number of values that use an exact number of full blocks
    final int nAlignedValues = 64 / gcd(64, bitsPerValue);
    final int span = toIndex - fromIndex;
    if (span <= 3 * nAlignedValues) {
      // there needs be at least 2 * nAlignedValues aligned values for the
      // block approach to be worth trying
      super.fill(fromIndex, toIndex, val);
      return;
    }

    // fill the first values naively until the next block start
    final int fromIndexModNAlignedValues = fromIndex % nAlignedValues;
    if (fromIndexModNAlignedValues != 0) {
      for (int i = fromIndexModNAlignedValues; i < nAlignedValues; ++i) {
        set(fromIndex++, val);
      }
    }
    assert fromIndex % nAlignedValues == 0;

    // compute the long[] blocks for nAlignedValues consecutive values and
    // use them to set as many values as possible without applying any mask
    // or shift
    final int nAlignedBlocks = (nAlignedValues * bitsPerValue) >> 6;
    final long[] nAlignedValuesBlocks;
    {
      Packed64 values = new Packed64(nAlignedValues, bitsPerValue);
      for (int i = 0; i < nAlignedValues; ++i) {
        values.set(i, val);
      }
      nAlignedValuesBlocks = values.blocks;
      assert nAlignedBlocks <= nAlignedValuesBlocks.length;
    }
    final int startBlock = (int) (((long) fromIndex * bitsPerValue) >>> 6);
    final int endBlock = (int) (((long) toIndex * bitsPerValue) >>> 6);
    for (int  block = startBlock; block < endBlock; ++block) {
      final long blockValue = nAlignedValuesBlocks[block % nAlignedBlocks];
      blocks[block] = blockValue;
    }

    // fill the gap
    for (int i = (int) (((long) endBlock << 6) / bitsPerValue); i < toIndex; ++i) {
      set(i, val);
    }
  }

  private static int gcd(int a, int b) {
    if (a < b) {
      return gcd(b, a);
    } else if (b == 0) {
      return a;
    } else {
      return gcd(b, a % b);
    }
  }

  @Override
  public void clear() {
    Arrays.fill(blocks, 0L);
  }
}
