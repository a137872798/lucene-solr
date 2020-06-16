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
 * @lucene.internal 一个池化对象  用于存储 int block
 */
public final class IntBlockPool {
    public final static int INT_BLOCK_SHIFT = 13;
    public final static int INT_BLOCK_SIZE = 1 << INT_BLOCK_SHIFT;
    public final static int INT_BLOCK_MASK = INT_BLOCK_SIZE - 1;

    /**
     * Abstract class for allocating and freeing int
     * blocks.
     */
    // 该对象在设置了 块的大小后 会根据该大小分配 内存 并且具备回收能力
    public abstract static class Allocator {
        protected final int blockSize;

        public Allocator(int blockSize) {
            this.blockSize = blockSize;
        }

        /**
         * 回收一组 block
         *
         * @param blocks
         * @param start
         * @param end
         */
        public abstract void recycleIntBlocks(int[][] blocks, int start, int end);

        /**
         * 默认情况下 会创建一个新的数组
         *
         * @return
         */
        public int[] getIntBlock() {
            return new int[blockSize];
        }
    }

    /**
     * A simple {@link Allocator} that never recycles.
     */
    // 简单实现  不会回收内存
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
    // 默认情况下 池中存储10个block
    public int[][] buffers = new int[10][];

    /**
     * index into the buffers array pointing to the current buffer used as the head
     */
    // 记录当前使用的buffer下标
    private int bufferUpto = -1;
    /**
     * Pointer to the current position in head buffer
     */
    // 当前buffer写入的偏移量
    public int intUpto = INT_BLOCK_SIZE;
    /**
     * Current head buffer
     */
    // 当前指向的 block
    public int[] buffer;
    /**
     * Current head offset
     */
    // 绝对偏移量
    public int intOffset = -INT_BLOCK_SIZE;

    /**
     * pool使用的内存分配器
     */
    private final Allocator allocator;

    /**
     * Creates a new {@link IntBlockPool} with a default {@link Allocator}.
     *
     * @see IntBlockPool#nextBuffer()
     * 当没有指定内存分配器的时候 会使用默认的实现 也就是无法回收内存的那个
     */
    public IntBlockPool() {
        this(new DirectAllocator());
    }

    /**
     * Creates a new {@link IntBlockPool} with the given {@link Allocator}.
     *
     * @see IntBlockPool#nextBuffer()
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
     *                        {@link SliceWriter}.    代表用0填充所有的空位
     * @param reuseFirst      if <code>true</code> the first buffer will be reused and calling
     *                        {@link IntBlockPool#nextBuffer()} is not needed after reset iff the
     *                        block pool was used before ie. {@link IntBlockPool#nextBuffer()} was called before.
     */
    public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
        // 代表当前指向了某个有效的buffer 那么才有回收的必要
        if (bufferUpto != -1) {
            // We allocated at least one buffer

            if (zeroFillBuffers) {
                // 从初始位置开始到目标位置 将所有的值重置成 0
                for (int i = 0; i < bufferUpto; i++) {
                    // Fully zero fill buffers that we fully used
                    Arrays.fill(buffers[i], 0);
                }
                // Partial zero fill the final buffer
                // 当前buffer 从0到目标偏移量进行重置
                Arrays.fill(buffers[bufferUpto], 0, intUpto, 0);
            }

            // 这里要回收之前的内存
            if (bufferUpto > 0 || !reuseFirst) {
                // 根据是否要重用 第一个buffer 决定起始偏移量
                final int offset = reuseFirst ? 1 : 0;
                // Recycle all but the first buffer
                allocator.recycleIntBlocks(buffers, offset, 1 + bufferUpto);
                // 将引用置空
                Arrays.fill(buffers, offset, bufferUpto + 1, null);
            }
            // 指向第一个 buffer
            if (reuseFirst) {
                // Re-use the first buffer
                bufferUpto = 0;
                intUpto = 0;
                intOffset = 0;
                buffer = buffers[0];
                // 置空相关指针
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
        // 代表切换到末尾了 需要进行扩容
        if (1 + bufferUpto == buffers.length) {
            int[][] newBuffers = new int[(int) (buffers.length * 1.5)][];
            System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
            buffers = newBuffers;
        }
        // 使用分配器分配一个新的内存块 同时移动指针
        buffer = buffers[1 + bufferUpto] = allocator.getIntBlock();
        bufferUpto++;

        intUpto = 0;
        intOffset += INT_BLOCK_SIZE;
    }

    /**
     * Creates a new int slice with the given starting size and returns the slices offset in the pool.
     * 创建指定大小的分片 并且返回的是buffer的偏移量
     *
     * @see SliceReader
     * 创建一个分片对象
     */
    private int newSlice(final int size) {
        //  等同于 intUpto + size > INT_BLOCK_SIZE
        if (intUpto > INT_BLOCK_SIZE - size) {
            nextBuffer();
            assert assertSliceBuffer(buffer);
        }

        final int upto = intUpto;
        // 更新偏移量
        intUpto += size;
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
     * 指定某个buffer 并从指定的偏移量开始 分配分片
     * 可以看到 Pool中每个block的大小是相同的 但是每次使用分片时 分配的大小却是不同的
     */
    private int allocSlice(final int[] slice, final int sliceOffset) {
        // 找到SliceWriter上次写入的值
        final int level = slice[sliceOffset];
        // 返回一个等级 并获取对应的长度
        final int newLevel = NEXT_LEVEL_ARRAY[level - 1];
        final int newSize = LEVEL_SIZE_ARRAY[newLevel];
        // Maybe allocate another block
        // 当前空间不足 所以创建一个新buffer
        if (intUpto > INT_BLOCK_SIZE - newSize) {
            nextBuffer();
            assert assertSliceBuffer(buffer);
        }

        final int newUpto = intUpto;
        final int offset = newUpto + intOffset;
        intUpto += newSize;
        // Write forwarding address at end of last slice:
        slice[sliceOffset] = offset;

        // Write new level:
        buffer[intUpto - 1] = newLevel;

        return newUpto;
    }

    /**
     * A {@link SliceWriter} that allows to write multiple integer slices into a given {@link IntBlockPool}.
     *
     * @lucene.internal 该对象支持写入多个分片到 pool中
     * @see SliceReader
     */
    public static class SliceWriter {

        /**
         * 记录当前偏移量 每写入一个int 加1
         */
        private int offset;
        private final IntBlockPool pool;


        public SliceWriter(IntBlockPool pool) {
            this.pool = pool;
        }

        /**
         * 重置分片偏移量
         */
        public void reset(int sliceOffset) {
            this.offset = sliceOffset;
        }

        /**
         * Writes the given value into the slice and resizes the slice if needed
         * 写入一个int值
         */
        public void writeInt(int value) {
            // 通过当前偏移量定位到 预备写入的buffer
            int[] ints = pool.buffers[offset >> INT_BLOCK_SHIFT];
            assert ints != null;
            // 计算相对偏移量
            int relativeOffset = offset & INT_BLOCK_MASK;
            if (ints[relativeOffset] != 0) {
                // End of slice; allocate a new one
                relativeOffset = pool.allocSlice(ints, relativeOffset);
                // 上面可能会创建一个新的 buffer 所以这里要更新ints  可能 relativeOffset 也是指向下一个buffer
                ints = pool.buffer;
                offset = relativeOffset + pool.intOffset;
            }
            // 在给定的位置写入值
            ints[relativeOffset] = value;
            offset++;
        }

        /**
         * starts a new slice and returns the start offset. The returned value
         * should be used as the start offset to initialize a {@link SliceReader}.
         * 创建一个新的分片
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
     * 该对象对应 SliceWriter 负责读取分片数据
     */
    public static final class SliceReader {

        // 从pool中读取数据
        private final IntBlockPool pool;
        private int upto;
        private int bufferUpto;
        private int bufferOffset;
        private int[] buffer;
        private int limit;
        private int level;
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
            bufferUpto = startOffset / INT_BLOCK_SIZE;
            bufferOffset = bufferUpto * INT_BLOCK_SIZE;
            this.end = endOffset;
            upto = startOffset;
            level = 1;

            buffer = pool.buffers[bufferUpto];
            upto = startOffset & INT_BLOCK_MASK;

            final int firstSize = IntBlockPool.LEVEL_SIZE_ARRAY[0];
            if (startOffset + firstSize >= endOffset) {
                // There is only this one slice to read
                limit = endOffset & INT_BLOCK_MASK;
            } else {
                limit = upto + firstSize - 1;
            }

        }

        /**
         * Returns <code>true</code> iff the current slice is fully read. If this
         * method returns <code>true</code> {@link SliceReader#readInt()} should not
         * be called again on this slice.
         */
        public boolean endOfSlice() {
            assert upto + bufferOffset <= end;
            return upto + bufferOffset == end;
        }

        /**
         * Reads the next int from the current slice and returns it.
         *
         * @see SliceReader#endOfSlice()
         */
        public int readInt() {
            assert !endOfSlice();
            assert upto <= limit;
            if (upto == limit)
                nextSlice();
            return buffer[upto++];
        }

        private void nextSlice() {
            // Skip to our next slice
            final int nextIndex = buffer[limit];
            level = NEXT_LEVEL_ARRAY[level - 1];
            final int newSize = LEVEL_SIZE_ARRAY[level];

            bufferUpto = nextIndex / INT_BLOCK_SIZE;
            bufferOffset = bufferUpto * INT_BLOCK_SIZE;

            buffer = pool.buffers[bufferUpto];
            upto = nextIndex & INT_BLOCK_MASK;

            if (nextIndex + newSize >= end) {
                // We are advancing to the final slice
                assert end - nextIndex > 0;
                limit = end - bufferOffset;
            } else {
                // This is not the final slice (subtract 4 for the
                // forwarding address at the end of this new slice)
                limit = upto + newSize - 1;
            }
        }
    }
}

