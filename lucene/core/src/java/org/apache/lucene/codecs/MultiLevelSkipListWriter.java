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
package org.apache.lucene.codecs;


import java.io.IOException;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.MathUtil;

/**
 * This abstract class writes skip lists with multiple levels.
 *
 * <pre>
 *
 * Example for skipInterval = 3:
 *                                                     c            (skip level 2)
 *                 c                 c                 c            (skip level 1)
 *     x     x     x     x     x     x     x     x     x     x      (skip level 0)
 * d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d  (posting list)
 *     3     6     9     12    15    18    21    24    27    30     (df)
 *
 * d - document
 * x - skip data
 * c - skip data with child pointer
 *
 * Skip level i contains every skipInterval-th entry from skip level i-1.
 * Therefore the number of entries on level i is: floor(df / ((skipInterval ^ (i + 1))).
 *
 * Each skip entry on a level {@code i>0} contains a pointer to the corresponding skip entry in list i-1.
 * This guarantees a logarithmic amount of skips to find the target document.
 *
 * While this class takes care of writing the different skip levels,
 * subclasses must define the actual format of the skip data.
 * </pre>
 *
 * @lucene.experimental 代表一个多层级的跳跃表
 */

public abstract class MultiLevelSkipListWriter {
    /**
     * number of levels in this skip list
     */
    // 总计存在几个级别
    protected final int numberOfSkipLevels;

    /**
     * the skip interval in the list with level = 0
     * 下层 是上一层元素的多少倍
     */
    private final int skipInterval;

    /**
     * skipInterval used for level &gt; 0
     */
    private final int skipMultiplier;

    /**
     * for every skip level a different buffer is used
     */
    // 用于存储每一层的数据  上层的数据会存储下层的指针
    private ByteBuffersDataOutput[] skipBuffer;

    /**
     * Creates a {@code MultiLevelSkipListWriter}.
     *
     * @param skipInterval   对应block的大小     在Lucene84PositionsWriter的构造函数中可以看到  使用BLOCK_SIZE 来初始化跳跃表的 skipInterval  也就是每当  Lucene84PositionsWriter 写入一定数量的doc时 会将数据存储到跳跃表中
     *                       也刚好在跳跃表中对应了一个 block的大小
     * @param skipMultiplier 默认为8
     * @param maxSkipLevels  跳跃表最大层级   默认为10
     * @param df             当前段预备写入多少doc
     */
    protected MultiLevelSkipListWriter(int skipInterval, int skipMultiplier, int maxSkipLevels, int df) {
        this.skipInterval = skipInterval;
        this.skipMultiplier = skipMultiplier;

        int numberOfSkipLevels;
        // calculate the maximum number of skip levels for this document frequency
        // 这里开始提前计算跳跃表的层级了
        if (df <= skipInterval) {
            numberOfSkipLevels = 1;
        } else {
            // df / skipInterval 代表预估要使用多少块   每上升一级就要 / skipMultiplier  这里在预估最多需要多少level
            numberOfSkipLevels = 1 + MathUtil.log(df / skipInterval, skipMultiplier);
        }

        // make sure it does not exceed maxSkipLevels
        if (numberOfSkipLevels > maxSkipLevels) {
            numberOfSkipLevels = maxSkipLevels;
        }
        this.numberOfSkipLevels = numberOfSkipLevels;
    }

    /**
     * Creates a {@code MultiLevelSkipListWriter}, where
     * {@code skipInterval} and {@code skipMultiplier} are
     * the same.
     */
    protected MultiLevelSkipListWriter(int skipInterval, int maxSkipLevels, int df) {
        this(skipInterval, skipInterval, maxSkipLevels, df);
    }

    /**
     * Allocates internal skip buffers.
     */
    protected void init() {
        // 每个元素对应一个 output  该对象本身是按照层级存储的
        /**
         * [1]  [[[[]]]]  output
         * [2]  [[[[[[[[]]]]]]]]]  output
         * [3] [[[[[[[[[[[[[]]]]]]]]]]]]]]  output
         */
        skipBuffer = new ByteBuffersDataOutput[numberOfSkipLevels];
        for (int i = 0; i < numberOfSkipLevels; i++) {
            skipBuffer[i] = ByteBuffersDataOutput.newResettableInstance();
        }
    }

    /**
     * Creates new buffers or empties the existing ones
     */
    protected void resetSkip() {
        if (skipBuffer == null) {
            // 初始化多层 out结构
            init();
        } else {
            // ByteBuffersDataOutput.newResettableInstance() 该函数分配的 buffer 具备重复利用的能力
            for (int i = 0; i < skipBuffer.length; i++) {
                skipBuffer[i].reset();
            }
        }
    }

    /**
     * Subclasses must implement the actual skip data encoding in this method.
     *
     * @param level      the level skip data shall be writing for
     * @param skipBuffer the skip buffer to write to
     */
    protected abstract void writeSkipData(int level, DataOutput skipBuffer) throws IOException;

    /**
     * Writes the current skip data to the buffers. The current document frequency determines
     * the max level is skip data is to be written to.
     *
     * @param df the current document frequency
     * @throws IOException If an I/O error occurs
     * 代表此时处理到 term关联的第几个 doc
     */
    public void bufferSkip(int df) throws IOException {

        // 整个跳跃表结构是这样的  在最下层 存储了所有的 block 然后每当满足 skipMultiplier 个block时 会在上层增加一个 block作为索引项

        assert df % skipInterval == 0;
        int numLevels = 1;
        // 转换成 block的下标   因为传入的doc 必然能被blockSize(skipInterval) 整除
        df /= skipInterval;

        // determine max level
        // 计算数据总计要写入几层
        while ((df % skipMultiplier) == 0 && numLevels < numberOfSkipLevels) {
            numLevels++;
            df /= skipMultiplier;
        }

        long childPointer = 0;

        // 从下级开始往上写数据
        for (int level = 0; level < numLevels; level++) {
            writeSkipData(level, skipBuffer[level]);

            // 获取此时该层已经写入的数据量
            long newChildPointer = skipBuffer[level].size();

            if (level != 0) {
                // store child pointers for all levels except the lowest
                // 除了最低层外 其他层要写入下层的偏移量  (体现跳跃表的特性 查询数据时 就按照该值定位)
                skipBuffer[level].writeVLong(childPointer);
            }

            //remember the childPointer for the next level
            childPointer = newChildPointer;
        }
    }

    /**
     * Writes the buffered skip lists to the given output.
     *
     * @param output the IndexOutput the skip lists shall be written to
     * @return the pointer the skip list starts
     * 将数据写入到  output中
     */
    public long writeSkip(IndexOutput output) throws IOException {
        long skipPointer = output.getFilePointer();
        // System.out.println("skipper.writeSkip fp=" + skipPointer);
        if (skipBuffer == null || skipBuffer.length == 0) return skipPointer;

        // 从最高层开始
        for (int level = numberOfSkipLevels - 1; level > 0; level--) {
            long length = skipBuffer[level].size();
            if (length > 0) {
                // 针对每层 先写入长度信息 再写入数据
                output.writeVLong(length);
                skipBuffer[level].copyTo(output);
            }
        }
        // 最后一层直接写入数据
        skipBuffer[0].copyTo(output);

        return skipPointer;
    }
}
