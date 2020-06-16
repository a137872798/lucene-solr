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

import org.apache.lucene.util.IntBlockPool.Allocator;


/**
 * A {@link Allocator} implementation that recycles unused int
 * blocks in a buffer and reuses them in subsequent calls to
 * {@link #getIntBlock()}.
 * <p>
 * Note: This class is not thread-safe
 * </p>
 * @lucene.internal
 * 该对象具备回收 block的能力
 */
public final class RecyclingIntBlockAllocator extends Allocator {

  /**
   * 维护被回收的 block的引用
   */
  private int[][] freeByteBlocks;
  /**
   * 最大允许回收的数量
   */
  private final int maxBufferedBlocks;
  /**
   * 当前已经回收了多少buffer
   */
  private int freeBlocks = 0;
  /**
   * 一个计数器对象
   */
  private final Counter bytesUsed;
  /**
   * 默认情况最多允许缓存 64个block
   */
  public static final int DEFAULT_BUFFERED_BLOCKS = 64;

  /**
   * Creates a new {@link RecyclingIntBlockAllocator}
   * 
   * @param blockSize
   *          the block size in bytes  初始化的时候要规定block的大小
   * @param maxBufferedBlocks
   *          maximum number of buffered int block   指定允许缓存的block大小
   * @param bytesUsed
   *          {@link Counter} reference counting internally allocated bytes   计数器对象
   */
  public RecyclingIntBlockAllocator(int blockSize, int maxBufferedBlocks,
      Counter bytesUsed) {
    super(blockSize);
    // 一开始就创建好一维数组
    freeByteBlocks = new int[maxBufferedBlocks][];
    this.maxBufferedBlocks = maxBufferedBlocks;
    this.bytesUsed = bytesUsed;
  }

  /**
   * Creates a new {@link RecyclingIntBlockAllocator}.
   * 
   * @param blockSize
   *          the size of each block returned by this allocator
   * @param maxBufferedBlocks
   *          maximum number of buffered int blocks
   */
  public RecyclingIntBlockAllocator(int blockSize, int maxBufferedBlocks) {
    this(blockSize, maxBufferedBlocks, Counter.newCounter(false));
  }

  /**
   * Creates a new {@link RecyclingIntBlockAllocator} with a block size of
   * {@link IntBlockPool#INT_BLOCK_SIZE}, upper buffered docs limit of
   * {@link #DEFAULT_BUFFERED_BLOCKS} ({@value #DEFAULT_BUFFERED_BLOCKS}).
   * 
   */
  public RecyclingIntBlockAllocator() {
    this(IntBlockPool.INT_BLOCK_SIZE, 64, Counter.newCounter(false));
  }

  /**
   * 获取一个block 如果有缓存的block 返回缓存的block  否则重新创建一个
   * @return
   */
  @Override
  public int[] getIntBlock() {
    if (freeBlocks == 0) {
      bytesUsed.addAndGet(blockSize*Integer.BYTES);
      return new int[blockSize];
    }
    final int[] b = freeByteBlocks[--freeBlocks];
    // 记得将引用置空
    freeByteBlocks[freeBlocks] = null;
    return b;
  }

  @Override
  public void recycleIntBlocks(int[][] blocks, int start, int end) {
    // 计算允许回收的量
    final int numBlocks = Math.min(maxBufferedBlocks - freeBlocks, end - start);
    final int size = freeBlocks + numBlocks;
    if (size >= freeByteBlocks.length) {
      final int[][] newBlocks = new int[ArrayUtil.oversize(size,
          RamUsageEstimator.NUM_BYTES_OBJECT_REF)][];
      System.arraycopy(freeByteBlocks, 0, newBlocks, 0, freeBlocks);
      freeByteBlocks = newBlocks;
    }
    final int stop = start + numBlocks;
    for (int i = start; i < stop; i++) {
      // 使用该对象维护引用 并将源数组的引用释放掉
      freeByteBlocks[freeBlocks++] = blocks[i];
      blocks[i] = null;
    }
    for (int i = stop; i < end; i++) {
      blocks[i] = null;
    }
    bytesUsed.addAndGet(-(end - stop) * (blockSize * Integer.BYTES));
    assert bytesUsed.get() >= 0;
  }

  /**
   * @return the number of currently buffered blocks
   */
  public int numBufferedBlocks() {
    return freeBlocks;
  }

  /**
   * @return the number of bytes currently allocated by this {@link Allocator}
   */
  public long bytesUsed() {
    return bytesUsed.get();
  }

  /**
   * @return the maximum number of buffered byte blocks
   */
  public int maxBufferedBlocks() {
    return maxBufferedBlocks;
  }

  /**
   * Removes the given number of int blocks from the buffer if possible.
   * 
   * @param num
   *          the number of int blocks to remove
   * @return the number of actually removed buffers
   * 释放指定数量的 block
   */
  public int freeBlocks(int num) {
    assert num >= 0 : "free blocks must be >= 0 but was: "+ num;
    final int stop;
    final int count;
    if (num > freeBlocks) {
      stop = 0;
      count = freeBlocks;
    } else {
      stop = freeBlocks - num;
      count = num;
    }
    while (freeBlocks > stop) {
      freeByteBlocks[--freeBlocks] = null;
    }
    bytesUsed.addAndGet(-count*blockSize*Integer.BYTES);
    assert bytesUsed.get() >= 0;
    return count;
  }
}