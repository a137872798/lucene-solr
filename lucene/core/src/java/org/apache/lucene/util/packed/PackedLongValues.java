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
     * 构建一个 差值存储对象  首先要求 数据是单调递增的 之后每次存储的值都是上一次的差值
     */
    public static PackedLongValues.Builder monotonicBuilder(int pageSize, float acceptableOverheadRatio) {
        return new MonotonicLongValues.Builder(pageSize, acceptableOverheadRatio);
    }

    /**
     * 使用差值存储规则创建  PackedLong 对象
     * @see #monotonicBuilder(int, float)
     * @param acceptableOverheadRatio  代表使用额外的内存存储数据  压缩的越多 还原耗时就越长
     */
    public static PackedLongValues.Builder monotonicBuilder(float acceptableOverheadRatio) {
        return monotonicBuilder(DEFAULT_PAGE_SIZE, acceptableOverheadRatio);
    }

    /**
     * 该组对象 用于将压缩后的数据 以正常的方式 读取出来
     */
    final PackedInts.Reader[] values;
    final int pageShift  // 代表 page 转换成二进制后 最右边有几个0
            , pageMask;  // pageSize - 1
    private final long size;  // 代表内部总计有多少个 long值
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
        final int size = vals.size();  // 内部有多少数据
        for (int k = 0; k < size; ) {
            // 将所有数据还原 并写入到 dest中
            k += vals.get(k, dest, k, size - k);
        }
        return size;
    }

    /**
     *
     * @param block  该值用于定位 block
     * @param element   代表某个block下数据的下标
     * @return
     */
    long get(int block, int element) {
        return values[block].get(element);
    }

    /**
     *
     * @param index   绝对偏移量
     * @return
     */
    @Override
    public final long get(long index) {
        assert index >= 0 && index < size();
        // 定位到 block
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
     * 该对象负责遍历 所有reader 内部的数据
     */
    public Iterator iterator() {
        return new Iterator();
    }

    /**
     * An iterator over long values.
     */
    // 返回一个迭代器对象 用于从block中读取数据
    final public class Iterator {

        // 当前 reader 读取出来的数据都填装到这个数组中
        final long[] currentValues;
        int vOff, pOff;   // vOff 对应 reader对象的偏移量   pOff 对应 currentValues 的偏移量

        // 当前 数组内部数据总量
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
         * 数据会先写入到这个数组中  每当填满该数组时 进行一次压缩
         */
        long[] pending;
        long size;

        /**
         * 该组reader 对象负责从某个地方读取数据
         */
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
         *s
         * @param pageSize
         * @param acceptableOverheadRatio
         */
        Builder(int pageSize, float acceptableOverheadRatio) {
            // 确保 page 大小合理  并返回 2的x次
            pageShift = checkBlockSize(pageSize, MIN_PAGE_SIZE, MAX_PAGE_SIZE);
            // 生成掩码
            pageMask = pageSize - 1;
            this.acceptableOverheadRatio = acceptableOverheadRatio;
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
         * 构建该对象 冻结内部的数据
         */
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
         * 将数据 写入到这个压缩的数据结构中
         */
        public Builder add(long l) {
            if (pending == null) {
                throw new IllegalStateException("Cannot be reused after build()");
            }
            // 代表填满一个page的大小了 需要做特殊处理
            if (pendingOff == pending.length) {
                // check size
                // 每当创建了一个 压缩结构  相应的就要初始化对应的reader 对象   当reader对象已经写满时 需要对reader对象进行扩容
                if (values.length == valuesOff) {
                    // 对数组进行扩容 (默认在原有基础上增加 1/8)  param1 代表扩容后的最小大小   param2 声明了数组的元素类型所占字节数
                    final int newLength = ArrayUtil.oversize(valuesOff + 1, 8);
                    grow(newLength);
                }
                // 将 pending[] 内部的数据进行压缩
                pack();
            }
            // 将数据填充到 block数组中
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
         * 开始压缩数据
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
         * 压缩 values 内部的数据  实际上就是将每个long值进行拆解  按照位存储
         *
         * @param values                  本次pending数组存储的数据 这些数据是待处理的
         * @param numValues               代表 values 内部要压缩的长度
         * @param block                   生成的 reader 对象 下标
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
                // reader 对象本身的作用是读取写入的 long值的 既然写入的都是0 直接模拟一个空对象 该方法调用 get(index) 总是返回0
                this.values[block] = new PackedInts.NullReader(numValues);
            } else {
                // 如果每次写入的值 大小还要变化 那么可能还会需要额外的信息来记录下一个long值占用了多少位
                // 比如 length之类的  所以这里干脆就直接按照最大值占用的 位数作为统一的位数
                final int bitsRequired = minValue < 0 ? 64 : PackedInts.bitsRequired(maxValue);
                // 按照这个预期的位数 创建压缩对象   实际上每个 reader对象都只填充一次  并且下次 pending 填满时才创建新的 reader 对象
                final PackedInts.Mutable mutable = PackedInts.getMutable(numValues, bitsRequired, acceptableOverheadRatio);
                for (int i = 0; i < numValues; ) {
                    i += mutable.set(i, values, i, numValues - i);
                }
                // 填充 reader
                this.values[block] = mutable;
            }
        }

        /**
         * 对reader数组进行扩容   每个reader对象 负责读取一定范围的 压缩结构数据
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
