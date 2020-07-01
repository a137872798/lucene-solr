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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.DataOutput;

/**
 * 该对象 将数据以位的方式写入到 输出流中
 */
abstract class AbstractBlockPackedWriter {

    /**
     * 一个 block 对应一个 long
     */
    static final int MIN_BLOCK_SIZE = 64;
    static final int MAX_BLOCK_SIZE = 1 << (30 - 3);
    static final int MIN_VALUE_EQUALS_0 = 1 << 0;
    static final int BPV_SHIFT = 1;

    // same as DataOutput.writeVLong but accepts negative values
    static void writeVLong(DataOutput out, long i) throws IOException {
        int k = 0;
        while ((i & ~0x7FL) != 0L && k++ < 8) {
            out.writeByte((byte) ((i & 0x7FL) | 0x80L));
            i >>>= 7;
        }
        out.writeByte((byte) i);
    }

    /**
     * 输出目标
     */
    protected DataOutput out;
    /**
     * 存储原始数据
     */
    protected final long[] values;
    /**
     * 存储压缩后的数据
     */
    protected byte[] blocks;
    /**
     * 记录out的偏移量
     * 推测每次 flush 该值会清零
     */
    protected int off;
    /**
     * 记录总计写入多少数据
     */
    protected long ord;
    protected boolean finished;

    /**
     * Sole constructor.
     *
     * @param blockSize the number of values of a single block, must be a multiple of <code>64</code>
     */
    public AbstractBlockPackedWriter(DataOutput out, int blockSize) {
        checkBlockSize(blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
        reset(out);
        values = new long[blockSize];
    }

    /**
     * Reset this writer to wrap <code>out</code>. The block size remains unchanged.
     */
    public void reset(DataOutput out) {
        assert out != null;
        this.out = out;
        off = 0;
        ord = 0L;
        finished = false;
    }

    private void checkNotFinished() {
        if (finished) {
            throw new IllegalStateException("Already finished");
        }
    }

    /**
     * Append a new long.
     */
    public void add(long l) throws IOException {
        checkNotFinished();
        // 代表当前已经写满了 先执行刷盘操作
        if (off == values.length) {
            flush();
        }
        values[off++] = l;
        ++ord;
    }

    // For testing only  测试用方法 忽略
    void addBlockOfZeros() throws IOException {
        checkNotFinished();
        if (off != 0 && off != values.length) {
            throw new IllegalStateException("" + off);
        }
        if (off == values.length) {
            flush();
        }
        Arrays.fill(values, 0);
        off = values.length;
        ord += values.length;
    }

    /**
     * Flush all buffered data to disk. This instance is not usable anymore
     * after this method has been called until {@link #reset(DataOutput)} has
     * been called.
     */
    public void finish() throws IOException {
        checkNotFinished();
        if (off > 0) {
            flush();
        }
        finished = true;
    }

    /**
     * Return the number of values which have been added.
     */
    public long ord() {
        return ord;
    }

    protected abstract void flush() throws IOException;

    /**
     * 标记当前写入的数据需要多少位
     * @param bitsRequired
     * @throws IOException
     */
    protected final void writeValues(int bitsRequired) throws IOException {
        // 创建对应的编码对象  该对象可以将数据拆解成位 写入
        final PackedInts.Encoder encoder = PackedInts.getEncoder(PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, bitsRequired);
        final int iterations = values.length / encoder.byteValueCount();
        final int blockSize = encoder.byteBlockCount() * iterations;
        if (blocks == null || blocks.length < blockSize) {
            blocks = new byte[blockSize];
        }
        if (off < values.length) {
            Arrays.fill(values, off, values.length, 0L);
        }
        // 一次将 values内部的数据全部写入 blocks中
        encoder.encode(values, 0, blocks, 0, iterations);
        // 计算当前有多少byte 要写入
        final int blockCount = (int) PackedInts.Format.PACKED.byteCount(PackedInts.VERSION_CURRENT, off, bitsRequired);
        out.writeBytes(blocks, blockCount);
    }

}
