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


import org.apache.lucene.store.DataOutput;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

// Packs high order byte first, to match
// IndexOutput.writeInt/Long/Short byte order
// 代表一个紧凑的对象
// 该对象会以紧凑的方式将数据写入到 output中
final class PackedWriter extends PackedInts.Writer {

  boolean finished;
  /**
   * 指定了写入的格式  默认情况下是 Packed 代表紧凑的  也就是 (valueCount * bitsPerValue)/8 = byteCount
   */
  final PackedInts.Format format;
  final BulkOperation encoder;
  final byte[] nextBlocks;
  final long[] nextValues;
  final int iterations;
  int off;
  int written;

  /**
   *
   * @param format
   * @param out
   * @param valueCount  预计要写入多少数据
   * @param bitsPerValue   每个数据要占用多少位
   * @param mem
   */
  PackedWriter(PackedInts.Format format, DataOutput out, int valueCount, int bitsPerValue, int mem) {
    super(out, valueCount, bitsPerValue);
    this.format = format;
    // 根据格式和 每个value所占bit 生成一个 执行大量填充操作的 operation
    encoder = BulkOperation.of(format, bitsPerValue);
    iterations = encoder.computeIterations(valueCount, mem);
    nextBlocks = new byte[iterations * encoder.byteBlockCount()];
    nextValues = new long[iterations * encoder.byteValueCount()];
    off = 0;
    written = 0;
    finished = false;
  }

  @Override
  protected PackedInts.Format getFormat() {
    return format;
  }

  @Override
  public void add(long v) throws IOException {
    assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
    assert !finished;
    if (valueCount != -1 && written >= valueCount) {
      throw new EOFException("Writing past end of stream");
    }
    nextValues[off++] = v;
    if (off == nextValues.length) {
      flush();
    }
    ++written;
  }

  @Override
  public void finish() throws IOException {
    assert !finished;
    if (valueCount != -1) {
      // 如果写入的值没有到一开始预估的  用0填充
      while (written < valueCount) {
        add(0L);
      }
    }
    flush();
    finished = true;
  }

  private void flush() throws IOException {
    // 将写入的 int 转换成 以 byte 为单位
    encoder.encode(nextValues, 0, nextBlocks, 0, iterations);
    final int blockCount = (int) format.byteCount(PackedInts.VERSION_CURRENT, off, bitsPerValue);
    out.writeBytes(nextBlocks, blockCount);
    Arrays.fill(nextValues, 0L);
    off = 0;
  }

  @Override
  public int ord() {
    return written - 1;
  }
}
