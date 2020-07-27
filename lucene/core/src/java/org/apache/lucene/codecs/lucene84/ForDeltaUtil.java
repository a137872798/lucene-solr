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
package org.apache.lucene.codecs.lucene84;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Utility class to encode/decode increasing sequences of 128 integers.
 */
public class ForDeltaUtil {

  // IDENTITY_PLUS_ONE[i] == i+1
  private static final long[] IDENTITY_PLUS_ONE = new long[ForUtil.BLOCK_SIZE];
  static {
    for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
      IDENTITY_PLUS_ONE[i] = i+1;
    }
  }

  private static void prefixSumOfOnes(long[] arr, long base) {
    System.arraycopy(IDENTITY_PLUS_ONE, 0, arr, 0, ForUtil.BLOCK_SIZE);
    // This loop gets auto-vectorized
    for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
      arr[i] += base;
    }
  }

  private final ForUtil forUtil;

  ForDeltaUtil(ForUtil forUtil) {
    this.forUtil = forUtil;
  }

  /**
   * Encode deltas of a strictly monotonically increasing sequence of integers.
   * The provided {@code longs} are expected to be deltas between consecutive values.
   * @param longs 每个docId 相较上一个有效docId的增量
   * @param out 数据写入的目标输出流 一般对应索引文件
   */
  void encodeDeltas(long[] longs, DataOutput out) throws IOException {
    // 代表所有增量值都是1  也就是docId 顺序增加  这种特殊情况单纯保存一个 0
    if (longs[0] == 1 && PForUtil.allEqual(longs)) { // happens with very dense postings
      out.writeByte((byte) 0);
    } else {
      long or = 0;
      for (long l : longs) {
        // 这里是预估最大需要多少位存储
        or |= l;
      }
      assert or != 0;
      final int bitsPerValue = PackedInts.bitsRequired(or);
      // 先写入一个描述存储位大小的信息
      out.writeByte((byte) bitsPerValue);
      // 通过一系列诡异的处理后 写入到out中
      forUtil.encode(longs, bitsPerValue, out);
    }
  }

  /**
   * Decode deltas, compute the prefix sum and add {@code base} to all decoded longs.
   */
  void decodeAndPrefixSum(DataInput in, long base, long[] longs) throws IOException {
    final int bitsPerValue = Byte.toUnsignedInt(in.readByte());
    if (bitsPerValue == 0) {
      prefixSumOfOnes(longs, base);
    } else {
      forUtil.decodeAndPrefixSum(bitsPerValue, in, base, longs);
    }
  }

  /**
   * Skip a sequence of 128 longs.
   */
  void skip(DataInput in) throws IOException {
    final int bitsPerValue = Byte.toUnsignedInt(in.readByte());
    if (bitsPerValue != 0) {
      in.skipBytes(forUtil.numBytes(bitsPerValue));
    }
  }

}
