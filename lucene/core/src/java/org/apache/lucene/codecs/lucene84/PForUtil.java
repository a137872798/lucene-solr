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
import java.util.Arrays;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Utility class to encode sequences of 128 small positive integers.
 */
final class PForUtil {

  static boolean allEqual(long[] l) {
    for (int i = 1; i < ForUtil.BLOCK_SIZE; ++i) {
      if (l[i] != l[0]) {
        return false;
      }
    }
    return true;
  }

  private final ForUtil forUtil;

  PForUtil(ForUtil forUtil) {
    this.forUtil = forUtil;
  }

  /**
   * Encode 128 integers from {@code longs} into {@code out}.
   * 将128个long值编码后写入out
   */
  void encode(long[] longs, DataOutput out) throws IOException {
    // At most 3 exceptions  这里尝试记录最大的4个值
    final long[] top4 = new long[4];
    Arrays.fill(top4, -1L);
    for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
      if (longs[i] > top4[0]) {
        top4[0] = longs[i];
        Arrays.sort(top4); // For only 4 entries we just sort on every iteration instead of maintaining a PQ
      }
    }

    // 最大值需要多少位存储
    final int maxBitsRequired = PackedInts.bitsRequired(top4[3]);
    // We store the patch on a byte, so we can't decrease the number of bits required by more than 8
    // 最少需要多少位
    final int patchedBitsRequired =  Math.max(PackedInts.bitsRequired(top4[0]), maxBitsRequired - 8);
    int numExceptions = 0;
    // 只要有某个值超过了这个限制 针对这些值就需要额外的信息存储
    final long maxUnpatchedValue = (1L << patchedBitsRequired) - 1;
    for (int i = 1; i < 4; ++i) {
      if (top4[i] > maxUnpatchedValue) {
        numExceptions++;
      }
    }
    // 每个异常信息会需要2个byte
    final byte[] exceptions = new byte[numExceptions*2];
    // 只有当 top4[0] 所需要的位数与 max 一样时 numExceptions 才有可能等于0
    if (numExceptions > 0) {
      int exceptionCount = 0;
      for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
        // 处理每个超过该限制值的数
        if (longs[i] > (1L << patchedBitsRequired) - 1) {
          // 第一个byte记录下标
          exceptions[exceptionCount*2] = (byte) i;
          // 第二个记录超过了多少位
          exceptions[exceptionCount*2+1] = (byte) (longs[i] >>> patchedBitsRequired);
          // 这里只会保留 最小限度的位数
          longs[i] &= maxUnpatchedValue;
          exceptionCount++;
        }
      }
      assert exceptionCount == numExceptions : exceptionCount + " " + numExceptions;
    }

    // 代表在处理之后 所有值都是一致的
    if (allEqual(longs) && maxBitsRequired <= 8) {
      for (int i = 0; i < numExceptions; ++i) {
        // 将数据还原
        exceptions[2*i + 1] = (byte) (Byte.toUnsignedLong(exceptions[2*i + 1]) << patchedBitsRequired);
      }
      // numExceptions 最大也只能是4   4需要3位来标识  (3可以用2位来表示)
      out.writeByte((byte) (numExceptions << 5));
      // 因为所有值都是一样的 只要写入一个值就好
      out.writeVLong(longs[0]);
    } else {
      // 特殊处理生成token
      final int token = (numExceptions << 5) | patchedBitsRequired;
      out.writeByte((byte) token);
      // 剩余数据采用该方式进行写入 (也就是尽可能将数据以位为单位存储 不细看了)
      forUtil.encode(longs, patchedBitsRequired, out);
    }
    // 将特殊值写入
    out.writeBytes(exceptions, exceptions.length);
  }

  /**
   * Decode 128 integers into {@code ints}.
   */
  void decode(DataInput in, long[] longs) throws IOException {
    final int token = Byte.toUnsignedInt(in.readByte());
    final int bitsPerValue = token & 0x1f;
    final int numExceptions = token >>> 5;
    if (bitsPerValue == 0) {
      Arrays.fill(longs, 0, ForUtil.BLOCK_SIZE, in.readVLong());
    } else {
      forUtil.decode(bitsPerValue, in, longs);
    }
    for (int i = 0; i < numExceptions; ++i) {
      longs[Byte.toUnsignedInt(in.readByte())] |= Byte.toUnsignedLong(in.readByte()) << bitsPerValue;
    }
  }

  /**
   * Skip 128 integers.
   */
  void skip(DataInput in) throws IOException {
    final int token = Byte.toUnsignedInt(in.readByte());
    final int bitsPerValue = token & 0x1f;
    final int numExceptions = token >>> 5;
    if (bitsPerValue == 0) {
      in.readVLong();
      in.skipBytes((numExceptions << 1));
    } else {
      in.skipBytes(forUtil.numBytes(bitsPerValue) + (numExceptions << 1));
    }
  }

}
