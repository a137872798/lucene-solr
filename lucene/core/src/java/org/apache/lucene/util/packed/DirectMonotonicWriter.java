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

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;

/**
 * Write monotonically-increasing sequences of integers. This writer splits
 * data into blocks and then for each block, computes the average slope, the
 * minimum value and only encode the delta from the expected value using a
 * {@link DirectWriter}.
 * 
 * @see DirectMonotonicReader
 * @lucene.internal
 * 按照单调趋势写入数据 (单调递增 or 单调递减)
 * FieldsIndexWriter 写入数据时 就是依靠该对象
 */
public final class DirectMonotonicWriter {

  public static final int MIN_BLOCK_SHIFT = 2;
  public static final int MAX_BLOCK_SHIFT = 22;

  /**
   * 2个输出流 分别代表了 元数据信息 和 实际的数据信息
   */
  final IndexOutput meta;
  final IndexOutput data;
  final long numValues;
  /**
   * dataOutput 的基础偏移量  记录当前文件已经写入了多少byte
   */
  final long baseDataPointer;
  final long[] buffer;
  /**
   * 类似于 buffer的偏移量
   */
  int bufferSize;
  /**
   * 记录add()调用次数  或者说写入的数据总数
   */
  long count;
  boolean finished;

  /**
   *
   * @param metaOut
   * @param dataOut
   * @param numValues     标明了一共要写入多少数据
   * @param blockShift     bufferSize = 1 << blockShift
   */
  DirectMonotonicWriter(IndexOutput metaOut, IndexOutput dataOut, long numValues, int blockShift) {
    if (blockShift < MIN_BLOCK_SHIFT || blockShift > MAX_BLOCK_SHIFT) {
      throw new IllegalArgumentException("blockShift must be in [" + MIN_BLOCK_SHIFT + "-" + MAX_BLOCK_SHIFT + "], got " + blockShift);
    }
    if (numValues < 0) {
      throw new IllegalArgumentException("numValues can't be negative, got " + numValues);
    }
    final long numBlocks = numValues == 0 ? 0 : ((numValues - 1) >>> blockShift) + 1;
    if (numBlocks > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("blockShift is too low for the provided number of values: blockShift=" + blockShift +
          ", numValues=" + numValues + ", MAX_ARRAY_LENGTH=" + ArrayUtil.MAX_ARRAY_LENGTH);
    }
    this.meta = metaOut;
    this.data = dataOut;
    this.numValues = numValues;
    final int blockSize = 1 << blockShift;
    // 允许 buffer < numValues  并且在继续写入前会先将之前的数据持久化
    this.buffer = new long[(int) Math.min(numValues, blockSize)];
    this.bufferSize = 0;
    this.baseDataPointer = dataOut.getFilePointer();
  }

  /**
   * 当 buffer数据写满时 就需要先将内部的数据持久化
   * @throws IOException
   */
  private void flush() throws IOException {
    assert bufferSize != 0;

    // 这里减期望有什么用???
    final float avgInc = (float) ((double) (buffer[bufferSize-1] - buffer[0]) / Math.max(1, bufferSize - 1));
    for (int i = 0; i < bufferSize; ++i) {
      final long expected = (long) (avgInc * (long) i);
      buffer[i] -= expected;
    }

    long min = buffer[0];
    for (int i = 1; i < bufferSize; ++i) {
      min = Math.min(buffer[i], min);
    }

    // 再通过差值策略 进一步节省内存开销
    long maxDelta = 0;
    for (int i = 0; i < bufferSize; ++i) {
      buffer[i] -= min;
      // use | will change nothing when it comes to computing required bits
      // but has the benefit of working fine with negative values too
      // (in case of overflow)
      maxDelta |= buffer[i];
    }

    // 写入用于还原数据的2个关键变量  min 和 avg
    meta.writeLong(min);
    meta.writeInt(Float.floatToIntBits(avgInc));
    // 代表偏移量信息
    meta.writeLong(data.getFilePointer() - baseDataPointer);
    // 代表所有的值都是一样的 也就是 数据的变化是平稳的
    if (maxDelta == 0) {
      meta.writeByte((byte) 0);
    } else {
      // 要表达该值需要多少位  比如 4需要2位  8需要3位
      final int bitsRequired = DirectWriter.unsignedBitsRequired(maxDelta);
      // 这里使用 packed 来存储数据
      DirectWriter writer = DirectWriter.getInstance(data, bufferSize, bitsRequired);
      for (int i = 0; i < bufferSize; ++i) {
        writer.add(buffer[i]);
      }
      writer.finish();
      meta.writeByte((byte) bitsRequired);
    }
    bufferSize = 0;
  }

  long previous = Long.MIN_VALUE;

  /** Write a new value. Note that data might not make it to storage until
   * {@link #finish()} is called.
   *  @throws IllegalArgumentException if values don't come in order */
  // 将数据写入到 buffer 中   并且写入的数据 必然是单调递增的
  public void add(long v) throws IOException {
    if (v < previous) {
      throw new IllegalArgumentException("Values do not come in order: " + previous + ", " + v);
    }
    if (bufferSize == buffer.length) {
      // 写不下时 先将之前的数据刷盘
      flush();
    }
    // 如果超量了 那么在flush之后 应该重置了 bufferSize
    buffer[bufferSize++] = v;
    // 更新上一个值 并且下次写入的值 必须要大于该值
    previous = v;
    count++;
  }

  /** This must be called exactly once after all values have been {@link #add(long) added}. */
  public void finish() throws IOException {
    if (count != numValues) {
      throw new IllegalStateException("Wrong number of values added, expected: " + numValues + ", got: " + count);
    }
    if (finished) {
      throw new IllegalStateException("#finish has been called already");
    }
    // 只要有剩余数据 就进行持久化
    if (bufferSize > 0) {
      flush();
    }
    finished = true;
  }

  /** Returns an instance suitable for encoding {@code numValues} into monotonic
   *  blocks of 2<sup>{@code blockShift}</sup> values. Metadata will be written
   *  to {@code metaOut} and actual data to {@code dataOut}. */
  public static DirectMonotonicWriter getInstance(IndexOutput metaOut, IndexOutput dataOut, long numValues, int blockShift) {
    return new DirectMonotonicWriter(metaOut, dataOut, numValues, blockShift);
  }

}
