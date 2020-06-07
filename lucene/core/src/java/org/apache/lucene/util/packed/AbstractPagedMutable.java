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
import static org.apache.lucene.util.packed.PackedInts.numBlocks;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Base implementation for {@link PagedMutable} and {@link PagedGrowableWriter}.
 *
 * @lucene.internal 作为 PagedMutable 和 PagedGrowableWriter 的骨架类
 * 该对象以细化到位的粒度进行数据存储 同时 支持扩容功能
 */
public abstract class AbstractPagedMutable<T extends AbstractPagedMutable<T>> extends LongValues implements Accountable {

    /**
     * 一个block的最小长度为 64
     */
    static final int MIN_BLOCK_SIZE = 1 << 6;
    /**
     * 最大长度为 1073741824
     */
    static final int MAX_BLOCK_SIZE = 1 << 30;

    /**
     * 初始大小
     */
    final long size;
    final int pageShift;
    final int pageMask;
    /**
     * 实际上就是page内部的数据
     */
    final PackedInts.Mutable[] subMutables;
    /**
     * 存储每个数据占用多少位
     */
    final int bitsPerValue;

    /**
     * @param bitsPerValue 填入的每个值占用多少bit
     * @param size         初始大小
     * @param pageSize     每页存储多少数据
     */
    AbstractPagedMutable(int bitsPerValue, long size, int pageSize) {
        this.bitsPerValue = bitsPerValue;
        this.size = size;
        // 返回从下往上数有多少个0   他这里是不是有潜规则 比如pageSize是2的n次  否则>>>pageShift 时应该是计算不出页数的
        pageShift = checkBlockSize(pageSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
        // 获取掩码
        pageMask = pageSize - 1;
        // 计算有多少 block     size代表总长度  pageSize 是每个page的大小
        final int numPages = numBlocks(size, pageSize);
        // 数组可以使用抽象类声明   那么每个page 对应一个Mutable
        subMutables = new PackedInts.Mutable[numPages];
    }

    /**
     * 填满内部的所有数据
     */
    protected final void fillPages() {
        // 计算一共存在多少个page
        final int numPages = numBlocks(size, pageSize());
        for (int i = 0; i < numPages; ++i) {
            // do not allocate for more entries than necessary on the last page
            // 最后一页取余数
            final int valueCount = i == numPages - 1 ? lastPageSize(size) : pageSize();
            subMutables[i] = newMutable(valueCount, bitsPerValue);
        }
    }

    /**
     * 根据目标页存储的 value 数量 以及每个value 对应的位数 创建 数据包
     *
     * @param valueCount
     * @param bitsPerValue
     * @return
     */
    protected abstract PackedInts.Mutable newMutable(int valueCount, int bitsPerValue);

    /**
     * 最后一页的大小
     *
     * @param size 总大小
     * @return
     */
    final int lastPageSize(long size) {
        final int sz = indexInPage(size);
        // 就是取余
        return sz == 0 ? pageSize() : sz;
    }

    final int pageSize() {
        return pageMask + 1;
    }

    /**
     * The number of values.
     */
    public final long size() {
        return size;
    }

    /**
     * 传入目标偏移量 计算页数
     * @param index
     * @return
     */
    final int pageIndex(long index) {
        return (int) (index >>> pageShift);
    }

    /**
     * 计算某页中 index的大小 也就是取余运算
     *
     * @param index
     * @return
     */
    final int indexInPage(long index) {
        return (int) index & pageMask;
    }

    @Override
    public final long get(long index) {
        assert index >= 0 && index < size : "index=" + index + " size=" + size;
        // 计算页数
        final int pageIndex = pageIndex(index);
        // 该页有多少数据
        final int indexInPage = indexInPage(index);
        return subMutables[pageIndex].get(indexInPage);
    }

    /**
     * Set value at <code>index</code>.
     */
    public final void set(long index, long value) {
        assert index >= 0 && index < size;
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        subMutables[pageIndex].set(indexInPage, value);
    }

    protected long baseRamBytesUsed() {
        return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                + RamUsageEstimator.NUM_BYTES_OBJECT_REF
                + Long.BYTES
                + 3 * Integer.BYTES;
    }

    @Override
    public long ramBytesUsed() {
        long bytesUsed = RamUsageEstimator.alignObjectSize(baseRamBytesUsed());
        bytesUsed += RamUsageEstimator.alignObjectSize(RamUsageEstimator.shallowSizeOf(subMutables));
        for (PackedInts.Mutable gw : subMutables) {
            bytesUsed += gw.ramBytesUsed();
        }
        return bytesUsed;
    }

    /**
     * 创建一个未填充的副本
     * @param newSize
     * @return
     */
    protected abstract T newUnfilledCopy(long newSize);

    /**
     * Create a new copy of size <code>newSize</code> based on the content of
     * this buffer. This method is much more efficient than creating a new
     * instance and copying values one by one.
     * 进行扩容
     */
    public final T resize(long newSize) {
        final T copy = newUnfilledCopy(newSize);
        final int numCommonPages = Math.min(copy.subMutables.length, subMutables.length);
        final long[] copyBuffer = new long[1024];
        for (int i = 0; i < copy.subMutables.length; ++i) {
            // 如果是最后一页 那么valueCount 不同
            final int valueCount = i == copy.subMutables.length - 1 ? lastPageSize(newSize) : pageSize();
            final int bpv = i < numCommonPages ? subMutables[i].getBitsPerValue() : this.bitsPerValue;
            copy.subMutables[i] = newMutable(valueCount, bpv);
            // 拷贝之前的数据
            if (i < numCommonPages) {
                final int copyLength = Math.min(valueCount, subMutables[i].size());
                PackedInts.copy(subMutables[i], 0, copy.subMutables[i], 0, copyLength, copyBuffer);
            }
        }
        return copy;
    }

    /**
     * Similar to {@link ArrayUtil#grow(long[], int)}.
     * 进行扩容
     */
    public final T grow(long minSize) {
        assert minSize >= 0;
        if (minSize <= size()) {
            @SuppressWarnings("unchecked") final T result = (T) this;
            return result;
        }
        // 默认扩容 1/3
        long extra = minSize >>> 3;
        if (extra < 3) {
            extra = 3;
        }
        final long newSize = minSize + extra;
        return resize(newSize);
    }

    /**
     * Similar to {@link ArrayUtil#grow(long[])}.
     */
    public final T grow() {
        return grow(size() + 1);
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "(size=" + size() + ",pageSize=" + pageSize() + ")";
    }
}
