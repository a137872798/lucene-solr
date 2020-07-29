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

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.RamUsageEstimator;

/**     
 * Implements {@link PackedInts.Mutable}, but grows the
 * bit count of the underlying packed ints on-demand.
 * <p>Beware that this class will accept to set negative values but in order
 * to do this, it will grow the number of bits per value to 64.
 *
 * <p>@lucene.internal</p>
 * 包装后具备扩容能力
 */
public class GrowableWriter extends PackedInts.Mutable {

  private long currentMask;
  /**
   * 该对象才是真正负责存储数据的
   */
  private PackedInts.Mutable current;
  private final float acceptableOverheadRatio;

  /**
   * @param startBitsPerValue       the initial number of bits per value, may grow depending on the data      每个数据会占用多少bit
   * @param valueCount              the number of values      一共要存储多少数据
   * @param acceptableOverheadRatio an acceptable overhead ratio
   */
  public GrowableWriter(int startBitsPerValue, int valueCount, float acceptableOverheadRatio) {
    this.acceptableOverheadRatio = acceptableOverheadRatio;
    // 生成一个符合条件的存储对象
    current = PackedInts.getMutable(valueCount, startBitsPerValue, this.acceptableOverheadRatio);
    currentMask = mask(current.getBitsPerValue());
  }

  /**
   * 计算掩码值
   * @param bitsPerValue
   * @return
   */
  private static long mask(int bitsPerValue) {
    return bitsPerValue == 64 ? ~0L : PackedInts.maxValue(bitsPerValue);
  }

  // 剩下的操作都是通过 委托给current实现的  它们都是非并发安全的 看来是在单线程中使用
  @Override
  public long get(int index) {
    return current.get(index);
  }

  @Override
  public int size() {
    return current.size();
  }

  @Override
  public int getBitsPerValue() {
    return current.getBitsPerValue();
  }

  public PackedInts.Mutable getMutable() {
    return current;
  }

  /**
   * 检测是否有足够的空间
   * @param value
   */
  private void ensureCapacity(long value) {
    // 如果 写入的值与一开始创建时  估算的perBit一样就不需要处理
    if ((value & currentMask) == value) {
      return;
    }
    // 代表需要进行扩容
    // 判断该数据会占用多少bit   等价于 Math.max(1, 64 - Long.numberOfLeadingZeros(bits))
    final int bitsRequired = PackedInts.unsignedBitsRequired(value);
    assert bitsRequired > current.getBitsPerValue();
    // 获取之前预估的容量大小
    final int valueCount = size();
    // 按照新的 bitsRequired 重新生成packed对象
    PackedInts.Mutable next = PackedInts.getMutable(valueCount, bitsRequired, acceptableOverheadRatio);
    // 将之前的数据拷贝进去
    PackedInts.copy(current, 0, next, 0, valueCount, PackedInts.DEFAULT_BUFFER_SIZE);
    current = next;
    // 重新计算掩码
    currentMask = mask(current.getBitsPerValue());
  }

  @Override
  public void set(int index, long value) {
    ensureCapacity(value);
    current.set(index, value);
  }

  @Override
  public void clear() {
    current.clear();
  }

  public GrowableWriter resize(int newSize) {
    GrowableWriter next = new GrowableWriter(getBitsPerValue(), newSize, acceptableOverheadRatio);
    final int limit = Math.min(size(), newSize);
    PackedInts.copy(current, 0, next, 0, limit, PackedInts.DEFAULT_BUFFER_SIZE);
    return next;
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    return current.get(index, arr, off, len);
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    long max = 0;
    for (int i = off, end = off + len; i < end; ++i) {
      // bitwise or is nice because either all values are positive and the
      // or-ed result will require as many bits per value as the max of the
      // values, or one of them is negative and the result will be negative,
      // forcing GrowableWriter to use 64 bits per value
      max |= arr[i];
    }
    ensureCapacity(max);
    return current.set(index, arr, off, len);
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    ensureCapacity(val);
    current.fill(fromIndex, toIndex, val);
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF
        + Long.BYTES
        + Float.BYTES)
        + current.ramBytesUsed();
  }

  @Override
  public void save(DataOutput out) throws IOException {
    current.save(out);
  }

}
