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


import static org.apache.lucene.util.packed.PackedInts.checkBlockSize;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Utility class to compress integers into a {@link LongValues} instance.
 * 该对象本身可以以压缩方式来存储一个 long[]  每当填满一个long[] 时 会根据内部所需的bit数来压缩成少数long值 并保存在一个叫做packed64的对象中 可以随时从中还原数据
 */
public class PackedLongValues extends LongValues implements Accountable {

    /**
     * 计算该对象会消耗的内存
     */
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PackedLongValues.class);

    /**
     * 代表一个默认页的大小
     */
    static final int DEFAULT_PAGE_SIZE = 1024;

    static final int MIN_PAGE_SIZE = 64;
    // More than 1M doesn't really makes sense with these appending buffers
    // since their goal is to try to have small numbers of bits per value
    static final int MAX_PAGE_SIZE = 1 << 20;

    /**
     * Return a new {@link Builder} that will compress efficiently positive integers.
     */
    // 通过传入一个预期的页大小 以及 一个比率对象 返回一个builder
    public static PackedLongValues.Builder packedBuilder(int pageSize, float acceptableOverheadRatio) {
        return new PackedLongValues.Builder(pageSize, acceptableOverheadRatio);
    }

    /**
     * @see #packedBuilder(int, float)
     */
    public static PackedLongValues.Builder packedBuilder(float acceptableOverheadRatio) {
        return packedBuilder(DEFAULT_PAGE_SIZE, acceptableOverheadRatio);
    }

    /**
     * Return a new {@link Builder} that will compress efficiently integers that
     * are close to each other.
     */
    public static PackedLongValues.Builder deltaPackedBuilder(int pageSize, float acceptableOverheadRatio) {
        return new DeltaPackedLongValues.Builder(pageSize, acceptableOverheadRatio);
    }

    /**
     * @see #deltaPackedBuilder(int, float)
     */
    public static PackedLongValues.Builder deltaPackedBuilder(float acceptableOverheadRatio) {
        return deltaPackedBuilder(DEFAULT_PAGE_SIZE, acceptableOverheadRatio);
    }

    /**
     * Return a new {@link Builder} that will compress efficiently integers that
     * would be a monotonic function of their index.
     */
    public static PackedLongValues.Builder monotonicBuilder(int pageSize, float acceptableOverheadRatio) {
        return new MonotonicLongValues.Builder(pageSize, acceptableOverheadRatio);
    }

    /**
     * @see #monotonicBuilder(int, float)
     */
    public static PackedLongValues.Builder monotonicBuilder(float acceptableOverheadRatio) {
        return monotonicBuilder(DEFAULT_PAGE_SIZE, acceptableOverheadRatio);
    }

    final PackedInts.Reader[] values;
    final int pageShift  // 代表 page 转换成二进制后 最右边有几个0
            , pageMask;  // pageSize - 1
    private final long size;  // 每当写入一个数值 size都会+1 这里代表写入了多少数值
    private final long ramBytesUsed;

    PackedLongValues(int pageShift, int pageMask, PackedInts.Reader[] values, long size, long ramBytesUsed) {
        this.pageShift = pageShift;
        this.pageMask = pageMask;
        this.values = values;
        this.size = size;
        this.ramBytesUsed = ramBytesUsed;
    }

    /**
     * Get the number of values in this array.
     */
    public final long size() {
        return size;
    }

    /**
     * 从指定的block中读取数据
     *
     * @param block
     * @param dest
     * @return
     */
    int decodeBlock(int block, long[] dest) {
        final PackedInts.Reader vals = values[block];
        final int size = vals.size();  // 代表该reader是由多少个原始数值压缩得到的
        for (int k = 0; k < size; ) {
            k += vals.get(k, dest, k, size - k);
        }
        return size;
    }

    /**
     * 通过传入 原始数值的下标 内部会转换成 压缩后的起始下标 并读取数据
     *
     * @param block
     * @param element
     * @return
     */
    long get(int block, int element) {
        return values[block].get(element);
    }

    @Override
    public final long get(long index) {
        assert index >= 0 && index < size();
        final int block = (int) (index >> pageShift);   // 相当于 下标除以页数  就可以得到页数的下标
        final int element = (int) (index & pageMask);  // 转换成页的下标 也就是想要读取第几个数据
        return get(block, element);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    /**
     * Return an iterator over the values of this array.
     */
    public Iterator iterator() {
        return new Iterator();
    }

    /**
     * An iterator over long values.
     */
    // 返回一个迭代器对象 用于从block中读取数据
    final public class Iterator {

        final long[] currentValues;
        int vOff  // 代表当前读取到第几个block
                , pOff;
        int currentCount; // number of entries of the current page

        Iterator() {
            currentValues = new long[pageMask + 1];
            vOff = pOff = 0;
            fillBlock();
        }

        /**
         * 在初始化的时候 预先加载数据
         */
        private void fillBlock() {
            // 代表没有数据可读了  当前数量为0
            if (vOff == values.length) {
                currentCount = 0;
            } else {
                // 从block中读取数据 并填充到 currentValues 中  currentCount 代表当前有多少可用的数据
                currentCount = decodeBlock(vOff, currentValues);
                assert currentCount > 0;
            }
        }

        /**
         * Whether or not there are remaining values.
         */
        public final boolean hasNext() {
            return pOff < currentCount;
        }

        /**
         * Return the next long in the buffer.
         */
        public final long next() {
            assert hasNext();
            // 从数组中读取某个值
            long result = currentValues[pOff++];
            // 一旦读取到某个 block的末尾 立即重新读取一批数据
            if (pOff == currentCount) {
                // block下标移动
                vOff += 1;
                // 重置 currentValues的下标
                pOff = 0;
                fillBlock();
            }
            return result;
        }

    }

    /** A Builder for a {@link PackedLongValues} instance. */
    /**
     * 通过该对象不断的填充数据  每当满足一个page的长度 就进行打包  也就是生成一个block  每次写入RAM是以block为单位的
     */
    public static class Builder implements Accountable {

        /**
         * 这个就是 block的大小
         */
        private static final int INITIAL_PAGE_COUNT = 16;
        /**
         * 该对象会占用的内存  (已经考虑了对象对齐填充)
         */
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Builder.class);

        final int pageShift, pageMask;
        final float acceptableOverheadRatio;
        /**
         * 写入的数据都会填充到该数组 同时一旦满足一个页大小 就会将数据写入到RAM中
         */
        long[] pending;
        long size;

        PackedInts.Reader[] values;
        /**
         * 用于记录该对象会使用的 RAM 大小   也就是一共占用多少字节
         */
        long ramBytesUsed;
        int valuesOff;
        /**
         * 记录当前写入 pending[] 的偏移量
         */
        int pendingOff;

        /**
         * 在初始化该对象时 规定页大小 和允许的比率
         *
         * @param pageSize
         * @param acceptableOverheadRatio
         */
        Builder(int pageSize, float acceptableOverheadRatio) {
            pageShift = checkBlockSize(pageSize, MIN_PAGE_SIZE, MAX_PAGE_SIZE);
            // 每当超过该大小就 写入到RAM
            pageMask = pageSize - 1;
            this.acceptableOverheadRatio = acceptableOverheadRatio;
            // 默认情况会创建16个页
            values = new PackedInts.Reader[INITIAL_PAGE_COUNT];
            pending = new long[pageSize];
            valuesOff = 0;
            pendingOff = 0;
            size = 0;
            // 将引用长度替换成数组长度  并重新计算一个对象会使用的 RAM
            ramBytesUsed = baseRamBytesUsed() + RamUsageEstimator.sizeOf(pending) + RamUsageEstimator.shallowSizeOf(values);
        }

        /**
         * Build a {@link PackedLongValues} instance that contains values that
         * have been added to this builder. This operation is destructive.
         */
        // 代表内部数据已经填充好了 这时构建出PackedLongValues 对象
        public PackedLongValues build() {
            // 将剩余数据打包
            finish();
            pending = null;
            // 因为数组本身可能被扩容过了 或者 values 本身就没有填充满 那么只截取有效的部分
            final PackedInts.Reader[] values = ArrayUtil.copyOfSubArray(this.values, 0, valuesOff);
            // 计算一共使用了多少 byte
            final long ramBytesUsed = PackedLongValues.BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
            return new PackedLongValues(pageShift, pageMask, values, size, ramBytesUsed);
        }

        long baseRamBytesUsed() {
            return BASE_RAM_BYTES_USED;
        }

        @Override
        public final long ramBytesUsed() {
            return ramBytesUsed;
        }

        /**
         * Return the number of elements that have been added to this builder.
         */
        public final long size() {
            return size;
        }

        /**
         * Add a new element to this builder.
         */
        // 添加一个long值 内部   因为该对象实际上是一个long[]
        public Builder add(long l) {
            if (pending == null) {
                throw new IllegalStateException("Cannot be reused after build()");
            }
            // 代表填满一个page的大小了 需要做特殊处理
            if (pendingOff == pending.length) {
                // check size
                // 默认情况会存储16组压缩数据
                if (values.length == valuesOff) {
                    // 对数组进行扩容 (默认在原有基础上增加 1/8)  param1 代表扩容后的最小大小   param2 声明了数组的元素类型所占字节数
                    final int newLength = ArrayUtil.oversize(valuesOff + 1, 8);
                    // 对数组进行扩容 同时重置那个标识
                    grow(newLength);
                }
                // 每当填满一个pending[]时 应该是要进行压缩了
                pack();
            }
            // 将数据填充到 block数组中  每当写入的数据满时 pending内的数据 压缩 并且将pendingOff置0
            pending[pendingOff++] = l;
            // 记录当前写入了多少 long值
            size += 1;
            return this;
        }

        /**
         * 将剩余数据压缩
         */
        final void finish() {
            if (pendingOff > 0) {
                if (values.length == valuesOff) {
                    grow(valuesOff + 1);
                }
                pack();
            }
        }

        /**
         * 开始压缩数据   每次pending 满的时候 才会往 values[] 中填入什么东西
         */
        private void pack() {
            pack(pending, pendingOff, valuesOff, acceptableOverheadRatio);
            // 计算当前使用的总byte数
            ramBytesUsed += values[valuesOff].ramBytesUsed();
            valuesOff += 1;
            // reset pending buffer
            // 打包完成后就重置 pending指针 这样下次调用add 又会覆盖 pending[] 中的数据
            pendingOff = 0;
        }

        /**
         * 将一组数据打包
         *
         * @param values                  本次pending数组存储的数据 这些数据是待处理的
         * @param numValues               等于 values.size
         * @param block                   打包后生成的 mutable 是第几个
         * @param acceptableOverheadRatio 代表在原有的基础上愿意多使用多少内存  如果使用 700% 也就是 完全不做压缩  因为原本压缩后就是 1/8
         */
        void pack(long[] values, int numValues, int block, float acceptableOverheadRatio) {
            assert numValues > 0;
            // compute max delta
            // 计算出 该数组中的最小值和最大值
            long minValue = values[0];
            long maxValue = values[0];
            for (int i = 1; i < numValues; ++i) {
                minValue = Math.min(minValue, values[i]);
                maxValue = Math.max(maxValue, values[i]);
            }

            // build a new packed reader
            // 代表long[] 中每个元素都是0
            if (minValue == 0 && maxValue == 0) {
                // 如果本次填入的数据都是0 那么构建一个空的reader对象
                // block 也就是本次要设置的 values的下标
                this.values[block] = new PackedInts.NullReader(numValues);
            } else {
                // 预测最大值需要占用多少bit   long的最大值 也就是64位
                final int bitsRequired = minValue < 0 ? 64 : PackedInts.bitsRequired(maxValue);
                // 返回一个压缩对象  注意 pending[]填满后 经过对数值的比量 会返回一个packed64对象 该对象内部以位来存储数据
                final PackedInts.Mutable mutable = PackedInts.getMutable(numValues, bitsRequired, acceptableOverheadRatio);
                for (int i = 0; i < numValues; ) {

                    i += mutable.set(i, values, i, numValues - i);
                }
                // 当数据存储结束后 将values[] 对应下标设置成reader对象
                this.values[block] = mutable;
            }
        }

        /**
         * 按照新数组大小  或者说块的大小 进行扩容
         *
         * @param newBlockCount
         */
        void grow(int newBlockCount) {
            ramBytesUsed -= RamUsageEstimator.shallowSizeOf(values);
            // 实际扩容的方法
            values = ArrayUtil.growExact(values, newBlockCount);
            // 看来builder 对象适合在单线程访问
            ramBytesUsed += RamUsageEstimator.shallowSizeOf(values);
        }

    }

}
