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


/**
 * Non-specialized {@link BulkOperation} for {@link PackedInts.Format#PACKED}.
 * 该对象会以 位的形式写入
 */
class BulkOperationPacked extends BulkOperation {

    /**
     * 代表写入时 每个值占多少bit    如果是一组数 选取其中会占用最多bit作为该值
     */
    private final int bitsPerValue;

    private final int longBlockCount;
    /**
     * 当 block以 long作为数据载体时 允许存储多少个值  比如 bitsPerValue = 8 代表一个long 变量可以存储8个值   如果 bitsPerValue = 1 代表可以存储64个
     */
    private final int longValueCount;
    private final int byteBlockCount;
    private final int byteValueCount;

    private final long mask;
    private final int intMask;

    /**
     * 代表每个值会使用多少bit
     *
     * @param bitsPerValue
     */
    public BulkOperationPacked(int bitsPerValue) {
        this.bitsPerValue = bitsPerValue;
        assert bitsPerValue > 0 && bitsPerValue <= 64;
        int blocks = bitsPerValue;

        while ((blocks & 1) == 0) {
            blocks >>>= 1;
        }
        // 这个好像是在求公倍数  也就是 (longValueCount*bitsPerValue) 必须是64的倍数
        this.longBlockCount = blocks;
        this.longValueCount = 64 * longBlockCount / bitsPerValue;
        int byteBlockCount = 8 * longBlockCount;
        int byteValueCount = longValueCount;
        while ((byteBlockCount & 1) == 0 && (byteValueCount & 1) == 0) {
            byteBlockCount >>>= 1;
            byteValueCount >>>= 1;
        }
        this.byteBlockCount = byteBlockCount;
        this.byteValueCount = byteValueCount;
        if (bitsPerValue == 64) {
            this.mask = ~0L;
        } else {
            // 掩码含义 和 Packed64 的掩码一致
            this.mask = (1L << bitsPerValue) - 1;
        }
        this.intMask = (int) mask;
        assert longValueCount * bitsPerValue == 64 * longBlockCount;
    }

    @Override
    public int longBlockCount() {
        return longBlockCount;
    }

    @Override
    public int longValueCount() {
        return longValueCount;
    }

    @Override
    public int byteBlockCount() {
        return byteBlockCount;
    }

    @Override
    public int byteValueCount() {
        return byteValueCount;
    }

    /**
     * 将block内部的数据读取出来 填充到 values 中
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start reading blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start writing values
     * @param iterations   controls how much data to decode
     */
    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values,
                       int valuesOffset, int iterations) {
        int bitsLeft = 64;
        for (int i = 0; i < longValueCount * iterations; ++i) {
            bitsLeft -= bitsPerValue;
            if (bitsLeft < 0) {
                values[valuesOffset++] =
                        ((blocks[blocksOffset++] & ((1L << (bitsPerValue + bitsLeft)) - 1)) << -bitsLeft)
                                | (blocks[blocksOffset] >>> (64 + bitsLeft));
                bitsLeft += 64;
            } else {
                values[valuesOffset++] = (blocks[blocksOffset] >>> bitsLeft) & mask;
            }
        }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, long[] values,
                       int valuesOffset, int iterations) {
        long nextValue = 0L;
        int bitsLeft = bitsPerValue;
        for (int i = 0; i < iterations * byteBlockCount; ++i) {
            final long bytes = blocks[blocksOffset++] & 0xFFL;
            if (bitsLeft > 8) {
                // just buffer
                bitsLeft -= 8;
                nextValue |= bytes << bitsLeft;
            } else {
                // flush
                int bits = 8 - bitsLeft;
                values[valuesOffset++] = nextValue | (bytes >>> bits);
                while (bits >= bitsPerValue) {
                    bits -= bitsPerValue;
                    values[valuesOffset++] = (bytes >>> bits) & mask;
                }
                // then buffer
                bitsLeft = bitsPerValue - bits;
                nextValue = (bytes & ((1L << bits) - 1)) << bitsLeft;
            }
        }
        assert bitsLeft == bitsPerValue;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values,
                       int valuesOffset, int iterations) {
        if (bitsPerValue > 32) {
            throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
        }
        int bitsLeft = 64;
        for (int i = 0; i < longValueCount * iterations; ++i) {
            bitsLeft -= bitsPerValue;
            if (bitsLeft < 0) {
                values[valuesOffset++] = (int)
                        (((blocks[blocksOffset++] & ((1L << (bitsPerValue + bitsLeft)) - 1)) << -bitsLeft)
                                | (blocks[blocksOffset] >>> (64 + bitsLeft)));
                bitsLeft += 64;
            } else {
                values[valuesOffset++] = (int) ((blocks[blocksOffset] >>> bitsLeft) & mask);
            }
        }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, int[] values,
                       int valuesOffset, int iterations) {
        int nextValue = 0;
        int bitsLeft = bitsPerValue;
        for (int i = 0; i < iterations * byteBlockCount; ++i) {
            final int bytes = blocks[blocksOffset++] & 0xFF;
            if (bitsLeft > 8) {
                // just buffer
                bitsLeft -= 8;
                nextValue |= bytes << bitsLeft;
            } else {
                // flush
                int bits = 8 - bitsLeft;
                values[valuesOffset++] = nextValue | (bytes >>> bits);
                while (bits >= bitsPerValue) {
                    bits -= bitsPerValue;
                    values[valuesOffset++] = (bytes >>> bits) & intMask;
                }
                // then buffer
                bitsLeft = bitsPerValue - bits;
                nextValue = (bytes & ((1 << bits) - 1)) << bitsLeft;
            }
        }
        assert bitsLeft == bitsPerValue;
    }

    /**
     * @param values       the values buffer    待写入到压缩结构的数据
     * @param valuesOffset the offset where to start reading values    values 从哪里开始压缩
     * @param blocks       the long blocks that hold packed integer values   存储压缩结果的容器
     * @param blocksOffset the offset where to start writing blocks    从哪个block 开始写入
     * @param iterations   controls how much data to encode
     */
    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks,
                       int blocksOffset, int iterations) {
        // 即将写入 blocks的值
        long nextBlock = 0;
        int bitsLeft = 64;
        // longValueCount * iterations 代表总计要写入多少数据
        for (int i = 0; i < longValueCount * iterations; ++i) {
            // 每次分配足以容纳一个数值的空间
            bitsLeft -= bitsPerValue;
            if (bitsLeft > 0) {
                // 拼接上每次截取的数据     数据是从高位开始写入 走向低位
                nextBlock |= values[valuesOffset++] << bitsLeft;
                // 刚好写满了这个 block
            } else if (bitsLeft == 0) {
                // 直接拼接上最后几个位 就可以
                nextBlock |= values[valuesOffset++];
                // 这里重置相关标识
                blocks[blocksOffset++] = nextBlock;
                nextBlock = 0;
                bitsLeft = 64;
            } else {
                // 也就是 bitsLeft < 0  本次分配的位 不仅抢占了 nextBlock 剩余的空间 还会抢占下个block 部分空间
                // values[valuesOffset] >>> -bitsLeft  代表截取 某高几位  因为低位 要放到下一个block
                nextBlock |= values[valuesOffset] >>> -bitsLeft;
                blocks[blocksOffset++] = nextBlock;
                // 占用下一个block的部分位置
                nextBlock = (values[valuesOffset++] & ((1L << -bitsLeft) - 1)) << (64 + bitsLeft);
                bitsLeft += 64;
            }
        }
    }

    /**
     * 这里变成 以 int为 单位 换算成位后写入到 block中 基本与上边一致 不同点就是 int 值多做了一个切换成 long的动作
     * @param values       the values buffer
     * @param valuesOffset the offset where to start reading values
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start writing blocks
     * @param iterations   controls how much data to encode
     */
    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks,
                       int blocksOffset, int iterations) {
        long nextBlock = 0;
        int bitsLeft = 64;
        for (int i = 0; i < longValueCount * iterations; ++i) {
            bitsLeft -= bitsPerValue;
            if (bitsLeft > 0) {
                nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL) << bitsLeft;
            } else if (bitsLeft == 0) {
                nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL);
                blocks[blocksOffset++] = nextBlock;
                nextBlock = 0;
                bitsLeft = 64;
            } else { // bitsLeft < 0
                nextBlock |= (values[valuesOffset] & 0xFFFFFFFFL) >>> -bitsLeft;
                blocks[blocksOffset++] = nextBlock;
                nextBlock = (values[valuesOffset++] & ((1L << -bitsLeft) - 1)) << (64 + bitsLeft);
                bitsLeft += 64;
            }
        }
    }

    /**
     * 存储的 block 变成了以 byte 为单位
     * @param values       the values buffer
     * @param valuesOffset the offset where to start reading values
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start writing blocks
     * @param iterations   controls how much data to encode
     */
    @Override
    public void encode(long[] values, int valuesOffset, byte[] blocks,
                       int blocksOffset, int iterations) {
        int nextBlock = 0;
        int bitsLeft = 8;
        for (int i = 0; i < byteValueCount * iterations; ++i) {
            final long v = values[valuesOffset++];
            assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
            // 代表不需要切换 block 直接写入就好
            if (bitsPerValue < bitsLeft) {
                // just buffer
                nextBlock |= v << (bitsLeft - bitsPerValue);
                bitsLeft -= bitsPerValue;
            } else {
                // flush as many blocks as possible
                // bitsPerValue 代表 long值中有多少有效位    bitsPerValue - bitsLeft 就是除了本block之外 还有多少位没有写入block中
                int bits = bitsPerValue - bitsLeft;
                // 这里填充 最高bits位
                blocks[blocksOffset++] = (byte) (nextBlock | (v >>> bits));
                // 每有8位 填充一个block
                while (bits >= 8) {
                    bits -= 8;
                    blocks[blocksOffset++] = (byte) (v >>> bits);
                }
                // then buffer
                bitsLeft = 8 - bits;
                // 将最低的几位 放在 block的高位
                nextBlock = (int) ((v & ((1L << bits) - 1)) << bitsLeft);
            }
        }
        assert bitsLeft == 8;
    }

    @Override
    public void encode(int[] values, int valuesOffset, byte[] blocks,
                       int blocksOffset, int iterations) {
        int nextBlock = 0;
        int bitsLeft = 8;
        for (int i = 0; i < byteValueCount * iterations; ++i) {
            final int v = values[valuesOffset++];
            assert PackedInts.bitsRequired(v & 0xFFFFFFFFL) <= bitsPerValue;
            if (bitsPerValue < bitsLeft) {
                // just buffer
                nextBlock |= v << (bitsLeft - bitsPerValue);
                bitsLeft -= bitsPerValue;
            } else {
                // flush as many blocks as possible
                int bits = bitsPerValue - bitsLeft;
                blocks[blocksOffset++] = (byte) (nextBlock | (v >>> bits));
                while (bits >= 8) {
                    bits -= 8;
                    blocks[blocksOffset++] = (byte) (v >>> bits);
                }
                // then buffer
                bitsLeft = 8 - bits;
                nextBlock = (v & ((1 << bits) - 1)) << bitsLeft;
            }
        }
        assert bitsLeft == 8;
    }

}
