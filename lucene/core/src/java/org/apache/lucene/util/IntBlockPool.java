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

/**
 * A pool for int blocks similar to {@link ByteBlockPool}
 *
 * @lucene.internal
 */
public final class IntBlockPool {
    public final static int INT_BLOCK_SHIFT = 13;
    public final static int INT_BLOCK_SIZE = 1 << INT_BLOCK_SHIFT;
    public final static int INT_BLOCK_MASK = INT_BLOCK_SIZE - 1;

    /**
     * Abstract class for allocating and freeing int
     * blocks.
     */
    public abstract static class Allocator {
        protected final int blockSize;

        public Allocator(int blockSize) {
            this.blockSize = blockSize;
        }

        public abstract void recycleIntBlocks(int[][] blocks, int start, int end);

        public int[] getIntBlock() {
            return new int[blockSize];
        }
    }

    /**
     * A simple {@link Allocator} that never recycles.
     */
    public static final class DirectAllocator extends Allocator {

        /**
         * Creates a new {@link DirectAllocator} with a default block size
         */
        public DirectAllocator() {
            super(INT_BLOCK_SIZE);
        }

        @Override
        public void recycleIntBlocks(int[][] blocks, int start, int end) {
        }
    }

    /**
     * array of buffers currently used in the pool. Buffers are allocated if needed don't modify this outside of this class
     */
    public int[][] buffers = new int[10][];

    /**
     * index into the buffers array pointing to the current buffer used as the head
     */
    // 记录当前指向的 一维数组下标
    private int bufferUpto = -1;
    /**
     * Pointer to the current position in head buffer
     * 下一个将要写入的偏移量
     */
    public int intUpto = INT_BLOCK_SIZE;
    /**
     * Current head buffer
     */
    public int[] buffer;
    /**
     * Current head offset
     */
    // 当前 block头部的偏移量  当创建第一个buffer时 该值为0
    public int intOffset = -INT_BLOCK_SIZE;

    private final Allocator allocator;

    /**
     * Creates a new {@link IntBlockPool} with a default {@link Allocator}.
     *
     * @see IntBlockPool#nextBuffer()
     */
    public IntBlockPool() {
        this(new DirectAllocator());
    }

    /**
     * Creates a new {@link IntBlockPool} with the given {@link Allocator}.
     *
     * @see IntBlockPool#nextBuffer()
     * 该对象通过一个 内存分配器来初始化  allocator包含一个回收内存的方法 避免频繁创建内存的开销
     */
    public IntBlockPool(Allocator allocator) {
        this.allocator = allocator;
    }

    /**
     * Resets the pool to its initial state reusing the first buffer. Calling
     * {@link IntBlockPool#nextBuffer()} is not needed after reset.
     */
    public void reset() {
        this.reset(true, true);
    }

    /**
     * Expert: Resets the pool to its initial state reusing the first buffer.
     *
     * @param zeroFillBuffers if <code>true</code> the buffers are filled with <code>0</code>.
     *                        This should be set to <code>true</code> if this pool is used with
     *                        {@link SliceWriter}.
     * @param reuseFirst      if <code>true</code> the first buffer will be reused and calling
     *                        {@link IntBlockPool#nextBuffer()} is not needed after reset iff the
     *                        block pool was used before ie. {@link IntBlockPool#nextBuffer()} was called before.
     */
    public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
        if (bufferUpto != -1) {
            // We allocated at least one buffer

            if (zeroFillBuffers) {
                for (int i = 0; i < bufferUpto; i++) {
                    // Fully zero fill buffers that we fully used
                    // 将block中所有数据都置0
                    Arrays.fill(buffers[i], 0);
                }
                // Partial zero fill the final buffer
                // 这里将地址置0
                Arrays.fill(buffers[bufferUpto], 0, intUpto, 0);
            }

            if (bufferUpto > 0 || !reuseFirst) {
                final int offset = reuseFirst ? 1 : 0;
                // Recycle all but the first buffer
                allocator.recycleIntBlocks(buffers, offset, 1 + bufferUpto);
                // 被回收的情况下 释放引用
                Arrays.fill(buffers, offset, bufferUpto + 1, null);
            }
            if (reuseFirst) {
                // Re-use the first buffer
                bufferUpto = 0;
                intUpto = 0;
                intOffset = 0;
                buffer = buffers[0];
            } else {
                bufferUpto = -1;
                intUpto = INT_BLOCK_SIZE;
                intOffset = -INT_BLOCK_SIZE;
                buffer = null;
            }
        }
    }

    /**
     * Advances the pool to its next buffer. This method should be called once
     * after the constructor to initialize the pool. In contrast to the
     * constructor a {@link IntBlockPool#reset()} call will advance the pool to
     * its first buffer immediately.
     * 切换到下个buffer
     */
    public void nextBuffer() {
        // 一维数组每个元素连接到一个一个大的 buffer 对象  这里的拷贝实际上 只是拷贝了buffer的地址 所以比起直接拷贝buffer的数据 会减少很多的性能开销
        if (1 + bufferUpto == buffers.length) {
            int[][] newBuffers = new int[(int) (buffers.length * 1.5)][];
            System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
            buffers = newBuffers;
        }
        // 分配一个新的内存块 同时保存该block的地址   同时更新当前buffer指向的block
        buffer = buffers[1 + bufferUpto] = allocator.getIntBlock();
        // 一维数组下标 +1
        bufferUpto++;

        intUpto = 0;
        intOffset += INT_BLOCK_SIZE;
    }

    /**
     * Creates a new int slice with the given starting size and returns the slices offset in the pool.
     *
     * @see SliceReader
     * 这里根据指定大小 开辟一个分片
     */
    private int newSlice(final int size) {
        // 等同于 intUpto + size > INT_BLOCK_SIZE   也就是当前buffer的空间不足以分配分片
        if (intUpto > INT_BLOCK_SIZE - size) {
            nextBuffer();
            assert assertSliceBuffer(buffer);
        }

        // 正常情况 移动二维数组的下标  如果此时刚好切换到下一个buffer 那么该值会重置成0
        final int upto = intUpto;
        // 增加这个下标 使得pool本身无法使用之前的数据了  而返回的偏移量又恰好是创建的分片的起点  那么调用者就是根据返回的偏移量作为起点来写入数据
        intUpto += size;
        // 将分配的分片的最后一个位置标记成1
        // 1 代表 NEXT_LEVEL_ARRAY.index + 1   当换算成level的时候会将 1-1 并去NEXT_LEVEL_ARRAY 找level 就会变成 NEXT_LEVEL_ARRAY[1-1]
        buffer[intUpto - 1] = 1;
        return upto;
    }

    private static final boolean assertSliceBuffer(int[] buffer) {
        int count = 0;
        for (int i = 0; i < buffer.length; i++) {
            count += buffer[i]; // for slices the buffer must only have 0 values
        }
        return count == 0;
    }


    // no need to make this public unless we support different sizes
    // TODO make the levels and the sizes configurable
    /**
     * An array holding the offset into the {@link IntBlockPool#LEVEL_SIZE_ARRAY}
     * to quickly navigate to the next slice level.
     * 每个block的大小是固定的  但是从外部看过去 buffers 被连接成一个大的数组  这里按需分配内存  不同的级别对应不同的大小
     */
    private final static int[] NEXT_LEVEL_ARRAY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};

    /**
     * An array holding the level sizes for int slices.
     */
    private final static int[] LEVEL_SIZE_ARRAY = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024};

    /**
     * The first level size for new slices
     */
    private final static int FIRST_LEVEL_SIZE = LEVEL_SIZE_ARRAY[0];

    /**
     * Allocates a new slice from the given offset
     *
     * @param slice       目标block
     * @param sliceOffset block的起始偏移量
     *                    基于之前分片区域的未节点 所设置的分片大小 创建新的分片
     *                    默认情况下 每次存入的level都是一样的  所以每次分配等大的分片
     */
    private int allocSlice(final int[] slice, final int sliceOffset) {
        // 读取目标位置对应的level  一个新的分片对象对应的值为 1
        final int level = slice[sliceOffset];
        // 找到对应的level
        final int newLevel = NEXT_LEVEL_ARRAY[level - 1];
        // 找到level对应的分片大小   也就是前2次分配的大小实际上都是2  分配多少大小不是重点
        final int newSize = LEVEL_SIZE_ARRAY[newLevel];
        // Maybe allocate another block
        // 当前buffer 不足分配  需要切换到下一个buffer  注意了这里分片是不会跨buffer的 并且最大的一个分片也只有 1024 确保单个分片不会大于一个block的大小
        if (intUpto > INT_BLOCK_SIZE - newSize) {
            nextBuffer();
            assert assertSliceBuffer(buffer);
        }

        // 返回本次分片的起点
        final int newUpto = intUpto;
        // 将当前偏移量 + 当前block头部的绝对偏移量  得到在大块数组的偏移量
        final int offset = newUpto + intOffset;
        // 更新相对偏移量
        intUpto += newSize;
        // Write forwarding address at end of last slice:
        // 将上一个分片的末尾值 由level 更换成下一个分片的起点 这样就间接形成一个 slice链表
        slice[sliceOffset] = offset;

        // Write new level:
        // 更新对应位置的 level   这样当往该分片写入数据时 分片空间不足时 会基于新的level分配大小
        buffer[intUpto - 1] = newLevel;

        return newUpto;
    }

    /**
     * A {@link SliceWriter} that allows to write multiple integer slices into a given {@link IntBlockPool}.
     *
     * @lucene.internal 该对象将 pool模拟的大块数组 以分片为单位写入数据
     * @see SliceReader
     */
    public static class SliceWriter {

        /**
         * 这是 pool 所抽象的大数组的绝对偏移量
         */
        private int offset;
        private final IntBlockPool pool;


        public SliceWriter(IntBlockPool pool) {
            this.pool = pool;
        }

        /**
         *
         */
        public void reset(int sliceOffset) {
            this.offset = sliceOffset;
        }

        /**
         * Writes the given value into the slice and resizes the slice if needed
         * 先通过 startNewSlice 创建一个分片对象 并占用pool的存储空间后 再在分配好的分片空间内写入数据   这样不会影响到pool本身
         */
        public void writeInt(int value) {
            // 将偏移量换算成一维数组的下标
            int[] ints = pool.buffers[offset >> INT_BLOCK_SHIFT];
            assert ints != null;
            // 计算二维数组的偏移量
            int relativeOffset = offset & INT_BLOCK_MASK;
            // 这里代表分片的大小不够用了  需要分配更大的分片
            if (ints[relativeOffset] != 0) {
                // End of slice; allocate a new one
                // 指向新的分片的 相对偏移量
                relativeOffset = pool.allocSlice(ints, relativeOffset);
                ints = pool.buffer;
                // 计算得到大数组的绝对偏移量
                offset = relativeOffset + pool.intOffset;
            }
            // 正常情况下就是直接将int 写入到int数组中 同时增加偏移量    在这个SliceWriter中 底层的二维数组是一个大数组  (隐藏了换算的细节) 这样就不需要在一开始就分配这么大的内存块 而是每次按需分配
            // 不足的情况下 pool会利用 allocate 分配新的内存块  然后更新relativeOffset 变成新的block的下一个写入的偏移量
            ints[relativeOffset] = value;
            offset++;
        }

        /**
         * starts a new slice and returns the start offset. The returned value
         * should be used as the start offset to initialize a {@link SliceReader}.
         * 每次开启分片时 都是先按照最小的尺寸创建
         */
        public int startNewSlice() {
            return offset = pool.newSlice(FIRST_LEVEL_SIZE) + pool.intOffset;
        }

        /**
         * Returns the offset of the currently written slice. The returned value
         * should be used as the end offset to initialize a {@link SliceReader} once
         * this slice is fully written or to reset the this writer if another slice
         * needs to be written.
         */
        public int getCurrentOffset() {
            return offset;
        }
    }

    /**
     * A {@link SliceReader} that can read int slices written by a {@link SliceWriter}
     *
     * @lucene.internal
     * 该对象负责从 slice链表中读取数据
     */
    public static final class SliceReader {

        /**
         * 从该池中读取数据
         */
        private final IntBlockPool pool;
        /**
         * 当前block的相对偏移量
         */
        private int upto;
        /**
         * pool 中 block 下标
         */
        private int bufferUpto;
        /**
         * 当前block header的偏移量
         */
        private int bufferOffset;
        /**
         * 当前正在读取的block
         */
        private int[] buffer;
        /**
         * 当前分片的终点
         */
        private int limit;
        private int level;
        /**
         * 最多允许扫描到pool的哪个位置  (因为分片本身是一个链表 会发生跳跃  下一个分片的起点可能就超过了end 那么不允许读取)
         */
        private int end;

        /**
         * Creates a new {@link SliceReader} on the given pool
         */
        public SliceReader(IntBlockPool pool) {
            this.pool = pool;
        }

        /**
         * Resets the reader to a slice give the slices absolute start and end offset in the pool
         */
        public void reset(int startOffset, int endOffset) {
            // 计算 block的下标
            bufferUpto = startOffset / INT_BLOCK_SIZE;
            // block起点的绝对偏移量
            bufferOffset = bufferUpto * INT_BLOCK_SIZE;
            this.end = endOffset;
            upto = startOffset;
            level = 1;

            buffer = pool.buffers[bufferUpto];
            upto = startOffset & INT_BLOCK_MASK;

            // 默认情况下使用最小的分片大小
            final int firstSize = IntBlockPool.LEVEL_SIZE_ARRAY[0];
            if (startOffset + firstSize >= endOffset) {
                // There is only this one slice to read
                // 如果limit 超过了end 缩小limit
                limit = endOffset & INT_BLOCK_MASK;
            } else {
                // 将limit指向当前 分片的末尾
                limit = upto + firstSize - 1;
            }

        }

        /**
         * Returns <code>true</code> iff the current slice is fully read. If this
         * method returns <code>true</code> {@link SliceReader#readInt()} should not
         * be called again on this slice.
         * 判断是否读取到末尾了
         */
        public boolean endOfSlice() {
            assert upto + bufferOffset <= end;
            return upto + bufferOffset == end;
        }

        /**
         * Reads the next int from the current slice and returns it.
         *
         * @see SliceReader#endOfSlice()
         * 从目标偏移量开始 读取一个数据
         */
        public int readInt() {
            assert !endOfSlice();
            assert upto <= limit;
            // 读取找到下一个分片
            if (upto == limit)
                nextSlice();
            // 移动偏移量 并返回结果
            return buffer[upto++];
        }

        /**
         * 切换到下一个分片   分片本身是一个链表结构 最后一个元素存储的值为下一个分片起点在大数组中的绝对偏移量
         */
        private void nextSlice() {
            // Skip to our next slice
            // 记录了下一个分片的起点
            final int nextIndex = buffer[limit];
            // 实际上还是自己啊 应该是在调用方有特殊用法
            level = NEXT_LEVEL_ARRAY[level - 1];
            // 找到匹配的分片大小
            final int newSize = LEVEL_SIZE_ARRAY[level];

            // 找到下一个block
            bufferUpto = nextIndex / INT_BLOCK_SIZE;
            // 找到 block的起始偏移量
            bufferOffset = bufferUpto * INT_BLOCK_SIZE;

            buffer = pool.buffers[bufferUpto];
            upto = nextIndex & INT_BLOCK_MASK;

            // 如果该分片的终点 超过了end
            if (nextIndex + newSize >= end) {
                // We are advancing to the final slice
                assert end - nextIndex > 0;
                // 缩小limit的值
                limit = end - bufferOffset;
            } else {
                // This is not the final slice (subtract 4 for the
                // forwarding address at the end of this new slice)
                limit = upto + newSize - 1;
            }
        }
    }
}

