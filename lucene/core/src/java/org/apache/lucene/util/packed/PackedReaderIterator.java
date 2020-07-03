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


import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.LongsRef;

/**
 * 该对象按照位读取数据
 */
final class PackedReaderIterator extends PackedInts.ReaderIteratorImpl {

  final int packedIntsVersion;
  final PackedInts.Format format;
  final BulkOperation bulkOperation;
  final byte[] nextBlocks;
  final LongsRef nextValues;
  final int iterations;
  int position;

  /**
   *
   * @param format
   * @param packedIntsVersion
   * @param valueCount  代表该对象总计有多少个值
   * @param bitsPerValue
   * @param in
   * @param mem
   */
  PackedReaderIterator(PackedInts.Format format, int packedIntsVersion, int valueCount, int bitsPerValue, DataInput in, int mem) {
    super(valueCount, bitsPerValue, in);
    this.format = format;
    this.packedIntsVersion = packedIntsVersion;
    bulkOperation = BulkOperation.of(format, bitsPerValue);
    iterations = bulkOperation.computeIterations(valueCount, mem);
    assert valueCount == 0 || iterations > 0;
    nextBlocks = new byte[iterations * bulkOperation.byteBlockCount()];
    // 创建存储多个结果的 数组
    nextValues = new LongsRef(new long[iterations * bulkOperation.byteValueCount()], 0, 0);
    nextValues.offset = nextValues.longs.length;
    position = -1;
  }

  /**
   *
   * @param count  代表往下读取几个值  每个值在创建时已经声明了占用多少位
   * @return
   * @throws IOException
   */
  @Override
  public LongsRef next(int count) throws IOException {
    assert nextValues.length >= 0;
    assert count > 0;
    assert nextValues.offset + nextValues.length <= nextValues.longs.length;
    
    nextValues.offset += nextValues.length;

    final int remaining = valueCount - position - 1;
    if (remaining <= 0) {
      throw new EOFException();
    }
    count = Math.min(remaining, count);

    // 代表第一次加载数据
    if (nextValues.offset == nextValues.longs.length) {
      // 代表装载所有的数据 需要多少个 byte
      final long remainingBlocks = format.byteCount(packedIntsVersion, remaining, bitsPerValue);
      final int blocksToRead = (int) Math.min(remainingBlocks, nextBlocks.length);
      // 将数据按位读取出来 并填满 nextBlocks
      in.readBytes(nextBlocks, 0, blocksToRead);
      // 这里读取出来的还是数据块  现在才要根据位将 byte数据转换成 存入的值
      if (blocksToRead < nextBlocks.length) {
        // 剩余的部分用0 填充
        Arrays.fill(nextBlocks, blocksToRead, nextBlocks.length, (byte) 0);
      }

      // 将byte数据解码后 存入到nextValues 中
      bulkOperation.decode(nextBlocks, 0, nextValues.longs, 0, iterations);
      nextValues.offset = 0;
    }

    nextValues.length = Math.min(nextValues.longs.length - nextValues.offset, count);
    position += nextValues.length;
    return nextValues;
  }

  @Override
  public int ord() {
    return position;
  }

}
