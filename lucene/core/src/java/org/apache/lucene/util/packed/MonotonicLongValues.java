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


import static org.apache.lucene.util.packed.MonotonicBlockPackedReader.expected;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts.Reader;

/**
 * 用于存储单调变化的数据   该对象使得压缩的值都变成一样的了 而到了上层 再抽取出最小值(实际上所有值都一样  那么就变成了 一直在存储0) 0只需要1位就可以存储
 * 实际上这里忽略了精度丢失
 */
class MonotonicLongValues extends DeltaPackedLongValues {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MonotonicLongValues.class);

  final float[] averages;

  MonotonicLongValues(int pageShift, int pageMask, Reader[] values, long[] mins, float[] averages, long size, long ramBytesUsed) {
    super(pageShift, pageMask, values, mins, size, ramBytesUsed);
    assert values.length == averages.length;
    this.averages = averages;
  }

  @Override
  long get(int block, int element) {
    // 原始值全都变成了 mins[block]
    return expected(mins[block], averages[block], element) + values[block].get(element);
  }

  @Override
  int decodeBlock(int block, long[] dest) {
    final int count = super.decodeBlock(block, dest);
    final float average = averages[block];
    for (int i = 0; i < count; ++i) {
      dest[i] += expected(0, average, i);
    }
    return count;
  }

  /**
   * 该对象构建内部数据单调变化的容器
   */
  static class Builder extends DeltaPackedLongValues.Builder {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Builder.class);

    float[] averages;

    /**
     * @param pageSize 声明了每个页的大小
     * @param acceptableOverheadRatio  允许使用多少额外的内存存储数据
     */
    Builder(int pageSize, float acceptableOverheadRatio) {
      super(pageSize, acceptableOverheadRatio);
      averages = new float[values.length];
      ramBytesUsed += RamUsageEstimator.sizeOf(averages);
    }

    @Override
    long baseRamBytesUsed() {
      return BASE_RAM_BYTES_USED;
    }

    @Override
    public MonotonicLongValues build() {
      finish();
      pending = null;
      final PackedInts.Reader[] values = ArrayUtil.copyOfSubArray(this.values, 0, valuesOff);
      final long[] mins = ArrayUtil.copyOfSubArray(this.mins, 0, valuesOff);
      final float[] averages = ArrayUtil.copyOfSubArray(this.averages, 0, valuesOff);
      final long ramBytesUsed = MonotonicLongValues.BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(values) + RamUsageEstimator.sizeOf(mins)
          + RamUsageEstimator.sizeOf(averages);
      return new MonotonicLongValues(pageShift, pageMask, values, mins, averages, size, ramBytesUsed);
    }

    @Override
    void pack(long[] values, int numValues, int block, float acceptableOverheadRatio) {
      // 这里获得一个斜率
      final float average = numValues == 1 ? 0 : (float) (values[numValues - 1] - values[0]) / (numValues - 1);
      // 同一个page下的数据 使用同一个斜率做处理
      for (int i = 0; i < numValues; ++i) {
        values[i] -= expected(0, average, i);
      }
      super.pack(values, numValues, block, acceptableOverheadRatio);
      averages[block] = average;
    }

    /**
     * 同样的在扩容时  为 average 扩容
     * @param newBlockCount
     */
    @Override
    void grow(int newBlockCount) {
      super.grow(newBlockCount);
      ramBytesUsed -= RamUsageEstimator.sizeOf(averages);
      averages = ArrayUtil.growExact(averages, newBlockCount);
      ramBytesUsed += RamUsageEstimator.sizeOf(averages);
    }

  }

}
