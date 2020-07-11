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


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Retrieves an instance previously written by {@link DirectMonotonicWriter}.
 * @see DirectMonotonicWriter 
 */
public final class DirectMonotonicReader extends LongValues implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DirectMonotonicReader.class);

  /** An instance that always returns {@code 0}. */
  private static final LongValues EMPTY = new LongValues() {

    @Override
    public long get(long index) {
      return 0;
    }

  };

  /** In-memory metadata that needs to be kept around for
   *  {@link DirectMonotonicReader} to read data from disk. */
  // 将数据读取出来后 存放在 Meta对象中
  public static class Meta implements Accountable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Meta.class);

    final int blockShift;
    final int numBlocks;
    final long[] mins;
    final float[] avgs;
    final byte[] bpvs;
    final long[] offsets;

    Meta(long numValues, int blockShift) {
      this.blockShift = blockShift;
      // 获取块的下标
      long numBlocks = numValues >>> blockShift;
      if ((numBlocks << blockShift) < numValues) {
        numBlocks += 1;
      }
      this.numBlocks = (int) numBlocks;
      this.mins = new long[this.numBlocks];
      this.avgs = new float[this.numBlocks];
      this.bpvs = new byte[this.numBlocks];
      this.offsets = new long[this.numBlocks];
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(mins)
          + RamUsageEstimator.sizeOf(avgs)
          + RamUsageEstimator.sizeOf(bpvs)
          + RamUsageEstimator.sizeOf(offsets);
    }
  }

  /** Load metadata from the given {@link IndexInput}.
   *  @see DirectMonotonicReader#getInstance(Meta, RandomAccessInput) */
  public static Meta loadMeta(IndexInput metaIn, long numValues, int blockShift) throws IOException {
    // 初始化内部的数组   blockShift 用于计算block的大小  每个block大小是 1<<blockShift
    Meta meta = new Meta(numValues, blockShift);
    // 通过上面的初始化 就能知道 有多少个block
    for (int i = 0; i < meta.numBlocks; ++i) {
      meta.mins[i] = metaIn.readLong();
      meta.avgs[i] = Float.intBitsToFloat(metaIn.readInt());
      // 代表 data 在本次 flush 写入了多少数据
      meta.offsets[i] = metaIn.readLong();
      // 代表增量数据使用了多少位表示  如果是0代表所有数据都是一样的
      meta.bpvs[i] = metaIn.readByte();
    }
    return meta;
  }

  /**
   * Retrieves an instance from the specified slice.
   * 通过一个元数据对象 返回一个 reader 该reader 会根据元数据信息读取  data内的数据
   */
  public static DirectMonotonicReader getInstance(Meta meta, RandomAccessInput data) throws IOException {
    final LongValues[] readers = new LongValues[meta.numBlocks];
    for (int i = 0; i < meta.mins.length; ++i) {
      if (meta.bpvs[i] == 0) {
        // 这里返回的是基本值 在读取的时候 还要加上差值 以及  avg   0 代表data中没有写入任何数据 同时所有的值都是一样的
        readers[i] = EMPTY;
      } else {
        // offsets[i] 偏移量
        readers[i] = DirectReader.getInstance(data, meta.bpvs[i], meta.offsets[i]);
      }
    }

    return new DirectMonotonicReader(meta.blockShift, readers, meta.mins, meta.avgs, meta.bpvs);
  }

  private final int blockShift;
  private final LongValues[] readers;
  private final long[] mins;
  private final float[] avgs;
  private final byte[] bpvs;
  private final int nonZeroBpvs;

  /**
   * 该对象在读取值的时候 会通过 mins 和 avgs 对数据进行还原
   * @param blockShift
   * @param readers
   * @param mins
   * @param avgs
   * @param bpvs
   */
  private DirectMonotonicReader(int blockShift, LongValues[] readers, long[] mins, float[] avgs, byte[] bpvs) {
    this.blockShift = blockShift;
    this.readers = readers;
    this.mins = mins;
    this.avgs = avgs;
    this.bpvs = bpvs;
    if (readers.length != mins.length || readers.length != avgs.length || readers.length != bpvs.length) {
      throw new IllegalArgumentException();
    }
    // ZeroBpvs 代表某批写入的数值 完全一样
    // bpvs 代表某批写入的数值 每个占用多少bit
    int nonZeroBpvs = 0;
    for (byte b : bpvs) {
      if (b != 0) {
        nonZeroBpvs++;
      }
    }
    this.nonZeroBpvs = nonZeroBpvs;
  }

  /**
   * 通过传入某个偏移量得到具体的值
   * @param index
   * @return
   */
  @Override
  public long get(long index) {
    final int block = (int) (index >>> blockShift);
    final long blockIndex = index & ((1 << blockShift) - 1);
    final long delta = readers[block].get(blockIndex);
    return mins[block] + (long) (avgs[block] * blockIndex) + delta;
  }

  /** Get lower/upper bounds for the value at a given index without hitting the direct reader. */
  private long[] getBounds(long index) {
    final int block = Math.toIntExact(index >>> blockShift);
    final long blockIndex = index & ((1 << blockShift) - 1);
    final long lowerBound = mins[block] + (long) (avgs[block] * blockIndex);
    final long upperBound = lowerBound + (1L << bpvs[block]) - 1;
    if (bpvs[block] == 64 || upperBound < lowerBound) { // overflow
      return new long[] { Long.MIN_VALUE, Long.MAX_VALUE };
    } else {
      return new long[] { lowerBound, upperBound };
    }
  }

  /**
   * Return the index of a key if it exists, or its insertion point otherwise
   * like {@link Arrays#binarySearch(long[], int, int, long)}.
   *
   * @see Arrays#binarySearch(long[], int, int, long)
   * 通过传入 key 进行二分查找
   */
  public long binarySearch(long fromIndex, long toIndex, long key) {
    if (fromIndex < 0 || fromIndex > toIndex) {
      throw new IllegalArgumentException("fromIndex=" + fromIndex + ",toIndex=" + toIndex);
    }
    long lo = fromIndex;
    long hi = toIndex - 1;

    while (lo <= hi) {
      final long mid = (lo + hi) >>> 1;
      // Try to run as many iterations of the binary search as possible without
      // hitting the direct readers, since they might hit a page fault.
      final long[] bounds = getBounds(mid);
      if (bounds[1] < key) {
        lo = mid + 1;
      } else if (bounds[0] > key) {
        hi = mid - 1;
      } else {
        final long midVal = get(mid);
        if (midVal < key) {
          lo = mid + 1;
        } else if (midVal > key) {
          hi = mid - 1;
        } else {
          return mid;
        }
      }
    }

    return -1 - lo;
  }

  @Override
  public long ramBytesUsed() {
    // Don't include meta, which should be accounted separately
    return BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(readers) +
        // Assume empty objects for the readers
        nonZeroBpvs * RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER);
  }

}
