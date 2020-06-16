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



/**
 * Non-specialized {@link BulkOperation} for {@link PackedInts.Format#PACKED}.
 * 这个对象就是负责进行编解码的核心骨架类
 */
class BulkOperationPacked extends BulkOperation {

  /**
   * 代表写入时 每个值占多少bit    如果是一组数 选取其中会占用最多bit作为该值
   */
  private final int bitsPerValue;

  /**
   * 总计有多少 block
   */
  private final int longBlockCount;
  /**
   * 代表 64bit  按照 bitsPerValue 划分后的值
   * 如果用index 来索引 那么index 每+1 就代表增加 bitsPerValue
   * index & longValueCount = 0 就代表  index & （64/bitPerValue） = 0  也就代表刚好是某个block的起点 那么就可以大块的读取
   */
  private final int longValueCount;
  private final int byteBlockCount;
  private final int byteValueCount;
  private final long mask;
  private final int intMask;

  /**
   * 代表每个值会使用多少bit
   * @param bitsPerValue
   */
  public BulkOperationPacked(int bitsPerValue) {
    this.bitsPerValue = bitsPerValue;
    assert bitsPerValue > 0 && bitsPerValue <= 64;
    int blocks = bitsPerValue;

    while ((blocks & 1) == 0) {
      blocks >>>= 1;
    }
    // 假设以8bit 来考虑
    // block 就是1
    this.longBlockCount = blocks;
    // 这个就是8  代表一个 block 可以写入8个值
    this.longValueCount = 64 * longBlockCount / bitsPerValue;
    int byteBlockCount = 8 * longBlockCount;
    int byteValueCount = longValueCount;
    while ((byteBlockCount & 1) == 0 && (byteValueCount & 1) == 0) {
      byteBlockCount >>>= 1;
      byteValueCount >>>= 1;
    }
    // 这2个都会变成1
    this.byteBlockCount = byteBlockCount;
    this.byteValueCount = byteValueCount;
    if (bitsPerValue == 64) {
      this.mask = ~0L;
    } else {
      // 掩码含义 和 Packed64 的掩码一致
      this.mask = (1L << bitsPerValue) - 1;
    }
    this.intMask = (int) mask;
    assert longValueCount * bitsPerValue == 64 * longBlockCount;
  }

  @Override
  public int longBlockCount() {
    return longBlockCount;
  }

  @Override
  public int longValueCount() {
    return longValueCount;
  }

  @Override
  public int byteBlockCount() {
    return byteBlockCount;
  }

  @Override
  public int byteValueCount() {
    return byteValueCount;
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft < 0) {
        values[valuesOffset++] =
            ((blocks[blocksOffset++] & ((1L << (bitsPerValue + bitsLeft)) - 1)) << -bitsLeft)
            | (blocks[blocksOffset] >>> (64 + bitsLeft));
        bitsLeft += 64;
      } else {
        values[valuesOffset++] = (blocks[blocksOffset] >>> bitsLeft) & mask;
      }
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    long nextValue = 0L;
    int bitsLeft = bitsPerValue;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final long bytes = blocks[blocksOffset++] & 0xFFL;
      if (bitsLeft > 8) {
        // just buffer
        bitsLeft -= 8;
        nextValue |= bytes << bitsLeft;
      } else {
        // flush
        int bits = 8 - bitsLeft;
        values[valuesOffset++] = nextValue | (bytes >>> bits);
        while (bits >= bitsPerValue) {
          bits -= bitsPerValue;
          values[valuesOffset++] = (bytes >>> bits) & mask;
        }
        // then buffer
        bitsLeft = bitsPerValue - bits;
        nextValue = (bytes & ((1L << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == bitsPerValue;
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft < 0) {
        values[valuesOffset++] = (int)
            (((blocks[blocksOffset++] & ((1L << (bitsPerValue + bitsLeft)) - 1)) << -bitsLeft)
            | (blocks[blocksOffset] >>> (64 + bitsLeft)));
        bitsLeft += 64;
      } else {
        values[valuesOffset++] = (int) ((blocks[blocksOffset] >>> bitsLeft) & mask);
      }
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    int nextValue = 0;
    int bitsLeft = bitsPerValue;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final int bytes = blocks[blocksOffset++] & 0xFF;
      if (bitsLeft > 8) {
        // just buffer
        bitsLeft -= 8;
        nextValue |= bytes << bitsLeft;
      } else {
        // flush
        int bits = 8 - bitsLeft;
        values[valuesOffset++] = nextValue | (bytes >>> bits);
        while (bits >= bitsPerValue) {
          bits -= bitsPerValue;
          values[valuesOffset++] = (bytes >>> bits) & intMask;
        }
        // then buffer
        bitsLeft = bitsPerValue - bits;
        nextValue = (bytes & ((1 << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == bitsPerValue;
  }

  /**
   * 将数据转换成bit 并填写到 blocks[] 中
   * @param values       the values buffer
   * @param valuesOffset the offset where to start reading values
   * @param blocks       the long blocks that hold packed integer values
   * @param blocksOffset the offset where to start writing blocks
   * @param iterations   controls how much data to encode
   */
  @Override
  public void encode(long[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int bitsLeft = 64;
    // 每当移动longValueCount 就代表切换到下一个block  比如 64位 与8位 那么 longValueCount 就是8  代表填写8次后 自动切换到下一个block
    for (int i = 0; i < longValueCount * iterations; ++i) {
      // 每次填写一个值是 就移动对应的位数 这样做 << 运算时很容易就拿到想要的位数
      bitsLeft -= bitsPerValue;
      if (bitsLeft > 0) {
        // 拼接上每次截取的数据
        nextBlock |= values[valuesOffset++] << bitsLeft;
      // 代表填满了一个 block 那么准备切换到下一个block
      } else if (bitsLeft == 0) {
        nextBlock |= values[valuesOffset++];
        // 将生成的结果保存到block中 同时移动 blocksOffset 这样下次就会存入新的block
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        bitsLeft = 64;
      } else { // bitsLeft < 0
        // 可能 64位 不能被bitsPerValue整除
        nextBlock |= values[valuesOffset] >>> -bitsLeft;
        blocks[blocksOffset++] = nextBlock;
        // 占用下一个block的部分位置
        nextBlock = (values[valuesOffset++] & ((1L << -bitsLeft) - 1)) << (64 + bitsLeft);
        bitsLeft += 64;
      }
    }
  }

  @Override
  public void encode(int[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft > 0) {
        nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL) << bitsLeft;
      } else if (bitsLeft == 0) {
        nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL);
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        bitsLeft = 64;
      } else { // bitsLeft < 0
        nextBlock |= (values[valuesOffset] & 0xFFFFFFFFL) >>> -bitsLeft;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = (values[valuesOffset++] & ((1L << -bitsLeft) - 1)) << (64 + bitsLeft);
        bitsLeft += 64;
      }
    }
  }

  @Override
  public void encode(long[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    int nextBlock = 0;
    int bitsLeft = 8;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final long v = values[valuesOffset++];
      assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
      if (bitsPerValue < bitsLeft) {
        // just buffer
        nextBlock |= v << (bitsLeft - bitsPerValue);
        bitsLeft -= bitsPerValue;
      } else {
        // flush as many blocks as possible
        int bits = bitsPerValue - bitsLeft;
        blocks[blocksOffset++] = (byte) (nextBlock | (v >>> bits));
        while (bits >= 8) {
          bits -= 8;
          blocks[blocksOffset++] = (byte) (v >>> bits);
        }
        // then buffer
        bitsLeft = 8 - bits;
        nextBlock = (int) ((v & ((1L << bits) - 1)) << bitsLeft);
      }
    }
    assert bitsLeft == 8;
  }

  @Override
  public void encode(int[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    int nextBlock = 0;
    int bitsLeft = 8;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final int v = values[valuesOffset++];
      assert PackedInts.bitsRequired(v & 0xFFFFFFFFL) <= bitsPerValue;
      if (bitsPerValue < bitsLeft) {
        // just buffer
        nextBlock |= v << (bitsLeft - bitsPerValue);
        bitsLeft -= bitsPerValue;
      } else {
        // flush as many blocks as possible
        int bits = bitsPerValue - bitsLeft;
        blocks[blocksOffset++] = (byte) (nextBlock | (v >>> bits));
        while (bits >= 8) {
          bits -= 8;
          blocks[blocksOffset++] = (byte) (v >>> bits);
        }
        // then buffer
        bitsLeft = 8 - bits;
        nextBlock = (v & ((1 << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == 8;
  }

}
