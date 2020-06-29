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
package org.apache.lucene.util;


import java.util.Arrays;
import java.util.List;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

/**
 * Class that Posting and PostingVector use to write byte
 * streams into shared fixed-size byte[] arrays.  The idea
 * is to allocate slices of increasing lengths For
 * example, the first slice is 5 bytes, the next slice is
 * 14, etc.  We start by writing our bytes into the first
 * 5 bytes.  When we hit the end of the slice, we allocate
 * the next slice and then write the address of the new
 * slice into the last 4 bytes of the previous slice (the
 * "forwarding address").
 * <p>
 * Each slice is filled with 0's initially, and we mark
 * the end with a non-zero byte.  This way the methods
 * that are writing into the slice don't need to record
 * its length and instead allocate a new slice once they
 * hit a non-zero byte.
 *
 * @lucene.internal
 * 该对象的实现机理与 IntBlockPool 相近
 **/
public final class ByteBlockPool implements Accountable {
    /**
     * 预估该对象本身会占用多少byte
     */
    private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(ByteBlockPool.class);

    public final static int BYTE_BLOCK_SHIFT = 15;
    /**
     * 这是一个block的大小  默认为  32768
     */
    public final static int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;
    public final static int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;

    /**
     * Abstract class for allocating and freeing byte
     * blocks.
     */
    public abstract static class Allocator {
        /**
         * 单个block的大小
         */
        protected final int blockSize;

        public Allocator(int blockSize) {
            this.blockSize = blockSize;
        }

        public abstract void recycleByteBlocks(byte[][] blocks, int start, int end);

        public void recycleByteBlocks(List<byte[]> blocks) {
            final byte[][] b = blocks.toArray(new byte[blocks.size()][]);
            recycleByteBlocks(b, 0, b.length);
        }

        /**
         * 按默认大小 创建一个 byte[]
         *
         * @return
         */
        public byte[] getByteBlock() {
            return new byte[blockSize];
        }
    }

    /**
     * A simple {@link Allocator} that never recycles.
     */
    // 该类不具备回收功能
    public static final class DirectAllocator extends Allocator {

        public DirectAllocator() {
            this(BYTE_BLOCK_SIZE);
        }

        public DirectAllocator(int blockSize) {
            super(blockSize);
        }

        @Override
        public void recycleByteBlocks(byte[][] blocks, int start, int end) {
        }
    }

    /**
     * A simple {@link Allocator} that never recycles, but
     * tracks how much total RAM is in use.
     */
    // 内部包含一个计数器 记录使用了多少byte
    public static class DirectTrackingAllocator extends Allocator {
        private final Counter bytesUsed;

        public DirectTrackingAllocator(Counter bytesUsed) {
            this(BYTE_BLOCK_SIZE, bytesUsed);
        }

        public DirectTrackingAllocator(int blockSize, Counter bytesUsed) {
            super(blockSize);
            this.bytesUsed = bytesUsed;
        }

        @Override
        public byte[] getByteBlock() {
            bytesUsed.addAndGet(blockSize);
            return new byte[blockSize];
        }

        @Override
        public void recycleByteBlocks(byte[][] blocks, int start, int end) {
            bytesUsed.addAndGet(-((end - start) * blockSize));
            for (int i = start; i < end; i++) {
                blocks[i] = null;
            }
        }
    }

    ;

    /**
     * array of buffers currently used in the pool. Buffers are allocated if
     * needed don't modify this outside of this class.
     * 该对象的初衷是 无法在一开始确定要分配多大的内存  所以采用了 二维数组的方式  将多个block连接起来 生成一个逻辑上的大数组
     */
    public byte[][] buffers = new byte[10][];

    /**
     * index into the buffers array pointing to the current buffer used as the head
     * 一维数组的偏移量  对应的block 就是  byte[] buffer
     */
    private int bufferUpto = -1;                        // Which buffer we are upto
    /**
     * Where we are in head buffer
     * 当前block的相对偏移量
     */
    public int byteUpto = BYTE_BLOCK_SIZE;

    /**
     * Current head buffer
     * 当前指向的 block
     */
    public byte[] buffer;
    /**
     * Current head offset
     * 当前block的head的起始偏移量
     */
    public int byteOffset = -BYTE_BLOCK_SIZE;

    /**
     * pool对象就是通过该对象分配内存的
     */
    private final Allocator allocator;

    public ByteBlockPool(Allocator allocator) {
        this.allocator = allocator;
    }

    /**
     * Resets the pool to its initial state reusing the first buffer and fills all
     * buffers with <code>0</code> bytes before they reused or passed to
     * {@link Allocator#recycleByteBlocks(byte[][], int, int)}. Calling
     * {@link ByteBlockPool#nextBuffer()} is not needed after reset.
     */
    public void reset() {
        reset(true, true);
    }

    /**
     * Expert: Resets the pool to its initial state reusing the first buffer. Calling
     * {@link ByteBlockPool#nextBuffer()} is not needed after reset.
     *
     * @param zeroFillBuffers if <code>true</code> the buffers are filled with <code>0</code>.
     *                        This should be set to <code>true</code> if this pool is used with slices.
     * @param reuseFirst      if <code>true</code> the first buffer will be reused and calling
     *                        {@link ByteBlockPool#nextBuffer()} is not needed after reset iff the
     *                        block pool was used before ie. {@link ByteBlockPool#nextBuffer()} was called before.
     *                        重置池内的数据
     */
    public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
        // 代表当前指针指向了某个对象
        if (bufferUpto != -1) {
            // We allocated at least one buffer

            // 在此之前的 buffer都用0 填充
            if (zeroFillBuffers) {
                for (int i = 0; i < bufferUpto; i++) {
                    // Fully zero fill buffers that we fully used
                    Arrays.fill(buffers[i], (byte) 0);
                }
                // Partial zero fill the final buffer
                // 将当前使用的偏移量之前的数据都置0
                Arrays.fill(buffers[bufferUpto], 0, byteUpto, (byte) 0);
            }

            if (bufferUpto > 0 || !reuseFirst) {
                // 根据入参判断是否从1开始重置
                final int offset = reuseFirst ? 1 : 0;
                // Recycle all but the first buffer
                // 回收 以 offset 开始到 bufferUpto的所有 byte[]   (不包含end对应的值)
                allocator.recycleByteBlocks(buffers, offset, 1 + bufferUpto);
                // 将后面的 byte[]置空
                Arrays.fill(buffers, offset, 1 + bufferUpto, null);
            }
            if (reuseFirst) {
                // Re-use the first buffer
                // 重置相关指针
                bufferUpto = 0;
                byteUpto = 0;
                byteOffset = 0;
                buffer = buffers[0];
            } else {
                // 直接将相关指针指向无效值
                bufferUpto = -1;
                byteUpto = BYTE_BLOCK_SIZE;
                byteOffset = -BYTE_BLOCK_SIZE;
                buffer = null;
            }
        }
    }

    /**
     * Advances the pool to its next buffer. This method should be called once
     * after the constructor to initialize the pool. In contrast to the
     * constructor a {@link ByteBlockPool#reset()} call will advance the pool to
     * its first buffer immediately.
     * 获取池化对象的下一个 buffer
     */
    public void nextBuffer() {
        // 进行扩容
        if (1 + bufferUpto == buffers.length) {
            byte[][] newBuffers = new byte[ArrayUtil.oversize(buffers.length + 1,
                    NUM_BYTES_OBJECT_REF)][];
            System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
            buffers = newBuffers;
        }
        // 通过分配器获取一个新的byte[]
        buffer = buffers[1 + bufferUpto] = allocator.getByteBlock();
        bufferUpto++;

        // 将有关单个数组内的偏移量置空
        byteUpto = 0;
        byteOffset += BYTE_BLOCK_SIZE;
    }

    /**
     * Allocates a new slice with the given size.
     *
     * @see ByteBlockPool#FIRST_LEVEL_SIZE
     * 在某个block内部预先分配一块内存  被叫做分片 同时分片本身还可以组成一个 链表结构
     */
    public int newSlice(final int size) {
        // 代表剩余空间不足 创建一个新的buffer
        if (byteUpto > BYTE_BLOCK_SIZE - size)
            nextBuffer();
        // 返回该分片的起点
        final int upto = byteUpto;
        // 移动到对应的位置
        byteUpto += size;
        // 这个是分片的特殊标记 代表当前位置是该分片的末尾  同时通过位运算还可以得到此次分配的内存块大小
        buffer[byteUpto - 1] = 16;
        return upto;
    }

    // Size of each slice.  These arrays should be at most 16
    // elements (index is encoded with 4 bits).  First array
    // is just a compact way to encode X+1 with a max.  Second
    // array is the length of each slice, ie first slice is 5
    // bytes, next slice is 14 bytes, etc.

    /**
     * An array holding the offset into the {@link ByteBlockPool#LEVEL_SIZE_ARRAY}
     * to quickly navigate to the next slice level.
     * 分片的级别
     */
    public final static int[] NEXT_LEVEL_ARRAY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};

    /**
     * An array holding the level sizes for byte slices.
     * 不同级别对应的分片大小
     */
    public final static int[] LEVEL_SIZE_ARRAY = {5, 14, 20, 30, 40, 40, 80, 80, 120, 200};

    /**
     * The first level size for new slices
     *
     * @see ByteBlockPool#newSlice(int)
     */
    public final static int FIRST_LEVEL_SIZE = LEVEL_SIZE_ARRAY[0];

    /**
     * Creates a new byte slice with the given starting size and
     * returns the slices offset in the pool.
     * 创建分片对象
     */
    public int allocSlice(final byte[] slice, final int upto) {

        // 每16作为一个级别
        final int level = slice[upto] & 15;
        // 找到该级别对应的大小
        final int newLevel = NEXT_LEVEL_ARRAY[level];
        final int newSize = LEVEL_SIZE_ARRAY[newLevel];

        // Maybe allocate another block
        // 当前block空间不足
        if (byteUpto > BYTE_BLOCK_SIZE - newSize) {
            nextBuffer();
        }

        final int newUpto = byteUpto;
        // 计算绝对偏移量
        final int offset = newUpto + byteOffset;
        // 直接指向目标位置
        byteUpto += newSize;

        // Copy forward the past 3 bytes (which we are about
        // to overwrite with the forwarding address):
        // 从新起点开始 将之前最后的3个值拷贝过去
        buffer[newUpto] = slice[upto - 3];
        buffer[newUpto + 1] = slice[upto - 2];
        buffer[newUpto + 2] = slice[upto - 1];

        // Write forwarding address at end of last slice:
        // 使用4个位置来存储 下一个分片的地址   跟 IntBlockPool 不一样  需要使用4个byte来存储int值
        slice[upto - 3] = (byte) (offset >>> 24);
        slice[upto - 2] = (byte) (offset >>> 16);
        slice[upto - 1] = (byte) (offset >>> 8);
        slice[upto] = (byte) offset;

        // Write new level:
        buffer[byteUpto - 1] = (byte) (16 | newLevel);

        // 前3个位置因为 写入了上个分片的值 所以不能使用
        return newUpto + 3;
    }

    /**
     * Fill the provided {@link BytesRef} with the bytes at the specified offset/length slice.
     * This will avoid copying the bytes, if the slice fits into a single block; otherwise, it uses
     * the provided {@link BytesRefBuilder} to copy bytes over.
     * 将指定位置的block的数据拷贝出来 生成 BytesRef  （引用指向同一地址）
     */
    void setBytesRef(BytesRefBuilder builder, BytesRef result, long offset, int length) {
        result.length = length;

        int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
        byte[] buffer = buffers[bufferIndex];
        int pos = (int) (offset & BYTE_BLOCK_MASK);

        // 定位到block后 发现读取动作不会跨block 那么直接读取就好
        if (pos + length <= BYTE_BLOCK_SIZE) {
            // common case where the slice lives in a single block: just reference the buffer directly without copying
            // 不经过拷贝 而是指向同一地址
            result.bytes = buffer;
            result.offset = pos;
        } else {
            // uncommon case: the slice spans at least 2 blocks, so we must copy the bytes:
            // 扩容
            builder.grow(length);
            // 将result 指向扩容后的数组
            result.bytes = builder.get().bytes;
            result.offset = 0;
            // 采用拷贝数据的方式
            readBytes(offset, result.bytes, 0, length);
        }
    }

    /**
     * Fill in a BytesRef from term's length & bytes encoded in
     * byte block
     * @param term
     * @param textStart  这是一个词的起点  term的存放特性是先存一个长度 然后存字符
     */
    public void setBytesRef(BytesRef term, int textStart) {
        // 将 term.bytes 指向了block   这样可以避免数据拷贝
        final byte[] bytes = term.bytes = buffers[textStart >> BYTE_BLOCK_SHIFT];
        int pos = textStart & BYTE_BLOCK_MASK;
        // 代表只有低7位有数据  也就是长度只有一个byte
        if ((bytes[pos] & 0x80) == 0) {
            // 第一位是长度
            term.length = bytes[pos];
            // 这里指定读取的起点  配合 length 就可以读取到数据
            term.offset = pos + 1;
        } else {
            // length is 2 bytes
            // 代表表示长度的数据 超过一个字节
            term.length = (bytes[pos] & 0x7f) + ((bytes[pos + 1] & 0xff) << 7);
            term.offset = pos + 2;
        }
        assert term.length >= 0;
    }

    /**
     * Appends the bytes in the provided {@link BytesRef} at
     * the current position.
     * 将bytes 内部的数据追加到 block中
     */
    public void append(final BytesRef bytes) {
        int bytesLeft = bytes.length;
        int offset = bytes.offset;
        while (bytesLeft > 0) {
            // 当前block的剩余空间
            int bufferLeft = BYTE_BLOCK_SIZE - byteUpto;
            if (bytesLeft < bufferLeft) {
                // 当前byte有足够的空间
                // fits within current buffer
                System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bytesLeft);
                byteUpto += bytesLeft;
                break;
            } else {
                // fill up this buffer and move to next one
                // 代表当前byte[] 空间不足 选择先拷贝一部分
                if (bufferLeft > 0) {
                    System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bufferLeft);
                }
                // 切换到下一个byte[] 继续读取数据
                nextBuffer();
                bytesLeft -= bufferLeft;
                offset += bufferLeft;
            }
        }
    }

    /**
     * Reads bytes out of the pool starting at the given offset with the given
     * length into the given byte array at offset <code>off</code>.
     * <p>Note: this method allows to copy across block boundaries.</p>
     * 将当前byte[] 内的数据读取到 bytes[] 中
     */
    public void readBytes(final long offset, final byte bytes[], int bytesOffset, int bytesLength) {
        int bytesLeft = bytesLength;
        int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
        int pos = (int) (offset & BYTE_BLOCK_MASK);
        while (bytesLeft > 0) {
            // 从给定偏移量定位到的byte[] 开始读取数据
            byte[] buffer = buffers[bufferIndex++];
            // 计算能够读取多少长度
            int chunk = Math.min(bytesLeft, BYTE_BLOCK_SIZE - pos);
            System.arraycopy(buffer, pos, bytes, bytesOffset, chunk);
            bytesOffset += chunk;
            bytesLeft -= chunk;
            pos = 0;
        }
    }

    /**
     * Set the given {@link BytesRef} so that its content is equal to the
     * {@code ref.length} bytes starting at {@code offset}. Most of the time this
     * method will set pointers to internal data-structures. However, in case a
     * value crosses a boundary, a fresh copy will be returned.
     * On the contrary to {@link #setBytesRef(BytesRef, int)}, this does not
     * expect the length to be encoded with the data.
     * 读取block的数据
     */
    public void setRawBytesRef(BytesRef ref, final long offset) {
        int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
        int pos = (int) (offset & BYTE_BLOCK_MASK);
        if (pos + ref.length <= BYTE_BLOCK_SIZE) {
            ref.bytes = buffers[bufferIndex];
            ref.offset = pos;
        } else {
            ref.bytes = new byte[ref.length];
            ref.offset = 0;
            readBytes(offset, ref.bytes, 0, ref.length);
        }
    }

    /**
     * Read a single byte at the given {@code offset}.
     * 从池中 通过绝对偏移量 定位到某个buffer 并返回byte
     */
    public byte readByte(long offset) {
        int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
        int pos = (int) (offset & BYTE_BLOCK_MASK);
        byte[] buffer = buffers[bufferIndex];
        return buffer[pos];
    }

    @Override
    public long ramBytesUsed() {
        long size = BASE_RAM_BYTES;
        size += RamUsageEstimator.sizeOfObject(buffer);
        size += RamUsageEstimator.shallowSizeOf(buffers);
        for (byte[] buf : buffers) {
            if (buf == buffer) {
                continue;
            }
            size += RamUsageEstimator.sizeOfObject(buf);
        }
        return size;
    }
}

