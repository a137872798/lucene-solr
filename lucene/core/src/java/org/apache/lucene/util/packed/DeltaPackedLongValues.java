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


import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts.Reader;

/**
 * 仅存储差值
 */
class DeltaPackedLongValues extends PackedLongValues {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DeltaPackedLongValues.class);

  /**
   * 存放了 每个page 的最小值  同时存储到block前 pag的值都减去了这个最小值   用于节省内存开销
   */
  final long[] mins;

  DeltaPackedLongValues(int pageShift, int pageMask, Reader[] values, long[] mins, long size, long ramBytesUsed) {
    super(pageShift, pageMask, values, size, ramBytesUsed);
    assert values.length == mins.length;
    this.mins = mins;
  }

  /**
   * 加获取结果时 需要 补上之前减去的值
   * @param block  该值用于定位 block
   * @param element   代表某个block下数据的下标
   * @return
   */
  @Override
  long get(int block, int element) {
    return mins[block] + values[block].get(element);
  }

  @Override
  int decodeBlock(int block, long[] dest) {
    final int count = super.decodeBlock(block, dest);
    final long min = mins[block];
    for (int i = 0; i < count; ++i) {
      dest[i] += min;
    }
    return count;
  }

  /**
   * 该对象用于构建 差值存储对象
   */
  static class Builder extends PackedLongValues.Builder {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Builder.class);

    /**
     * 每个 block 对应一个min值   代表某个page 下所有long值中的最小值  同时存储到 packed中的数据 都是在减去了该值的基础上
     */
    long[] mins;

    /**
     * @param pageSize 声明了每个页的大小
     * @param acceptableOverheadRatio  允许使用多少额外的内存存储数据
     */
    Builder(int pageSize, float acceptableOverheadRatio) {
      super(pageSize, acceptableOverheadRatio);
      mins = new long[values.length];
      ramBytesUsed += RamUsageEstimator.sizeOf(mins);
    }

    @Override
    long baseRamBytesUsed() {
      return BASE_RAM_BYTES_USED;
    }

    @Override
    public DeltaPackedLongValues build() {
      finish();
      pending = null;
      final PackedInts.Reader[] values = ArrayUtil.copyOfSubArray(this.values, 0, valuesOff);
      final long[] mins = ArrayUtil.copyOfSubArray(this.mins, 0, valuesOff);
      final long ramBytesUsed = DeltaPackedLongValues.BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(values) + RamUsageEstimator.sizeOf(mins);
      return new DeltaPackedLongValues(pageShift, pageMask, values, mins, size, ramBytesUsed);
    }

    /**
     * 覆盖了 父类的 压缩方法
     * @param values                  本次pending数组存储的数据 这些数据是待处理的
     * @param numValues               代表 values 内部要压缩的长度
     * @param block                   生成的 reader 对象 下标
     * @param acceptableOverheadRatio 代表在原有的基础上愿意多使用多少内存  如果使用 700% 也就是 完全不做压缩  因为原本压缩后就是 1/8
     */
    @Override
    void pack(long[] values, int numValues, int block, float acceptableOverheadRatio) {
      long min = values[0];
      // 存储最小值
      for (int i = 1; i < numValues; ++i) {
        min = Math.min(min, values[i]);
      }
      // 将每个值变成了差值
      for (int i = 0; i < numValues; ++i) {
        values[i] -= min;
      }
      super.pack(values, numValues, block, acceptableOverheadRatio);
      mins[block] = min;
    }

    /**
     * 在扩容的时候 额外为 mins 进行扩容
     * @param newBlockCount
     */
    @Override
    void grow(int newBlockCount) {
      super.grow(newBlockCount);
      ramBytesUsed -= RamUsageEstimator.sizeOf(mins);
      mins = ArrayUtil.growExact(mins, newBlockCount);
      ramBytesUsed += RamUsageEstimator.sizeOf(mins);
    }

  }

}
