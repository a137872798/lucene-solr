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

import org.apache.lucene.util.packed.PackedInts.Mutable;

/**
 * A {@link PagedGrowableWriter}. This class slices data into fixed-size blocks
 * which have independent numbers of bits per value and grow on-demand.
 * <p>You should use this class instead of the {@link PackedLongValues} related ones only when
 * you need random write-access. Otherwise this class will likely be slower and
 * less memory-efficient.
 *
 * @lucene.internal 将数据拆分并存入 大小固定的 block中
 * 只有当需要随机访问时 才应该使用 PackedLongValues  而非 PagedGrowableWriter
 * 并且该对象的读取可能会更耗时一些  不过更加节省内存
 */
public final class PagedGrowableWriter extends AbstractPagedMutable<PagedGrowableWriter> {

    /**
     * 允许付出的额外开销
     */
    final float acceptableOverheadRatio;

    /**
     * Create a new {@link PagedGrowableWriter} instance.
     *
     * @param size                    the number of values to store.
     * @param pageSize                the number of values per page
     * @param startBitsPerValue       the initial number of bits per value
     * @param acceptableOverheadRatio an acceptable overhead ratio
     */
    public PagedGrowableWriter(long size, int pageSize,
                               int startBitsPerValue, float acceptableOverheadRatio) {
        this(size, pageSize, startBitsPerValue, acceptableOverheadRatio, true);
    }

    /**
     * @param size
     * @param pageSize                默认为 1<<27     注意这里传入的 pageSize应当为2的幂次
     * @param startBitsPerValue       每个值会占用多少bit    默认是8
     * @param acceptableOverheadRatio 默认情况下不适用额外的内存  但是在读取时会更耗时一些  对应 PackedInts.COMPACT
     * @param fillPages               默认情况下为true
     */
    PagedGrowableWriter(long size, int pageSize, int startBitsPerValue, float acceptableOverheadRatio, boolean fillPages) {
        super(startBitsPerValue, size, pageSize);
        this.acceptableOverheadRatio = acceptableOverheadRatio;
        if (fillPages) {
            fillPages();
        }
    }

    /**
     * 为每个 Mutable槽创建对象
     *
     * @param valueCount
     * @param bitsPerValue
     * @return
     */
    @Override
    protected Mutable newMutable(int valueCount, int bitsPerValue) {
        return new GrowableWriter(bitsPerValue, valueCount, acceptableOverheadRatio);
    }

    /**
     * 返回一个未填充的对象
     * @param newSize
     * @return
     */
    @Override
    protected PagedGrowableWriter newUnfilledCopy(long newSize) {
        return new PagedGrowableWriter(newSize, pageSize(), bitsPerValue, acceptableOverheadRatio, false);
    }

    @Override
    protected long baseRamBytesUsed() {
        return super.baseRamBytesUsed() + Float.BYTES;
    }

}
