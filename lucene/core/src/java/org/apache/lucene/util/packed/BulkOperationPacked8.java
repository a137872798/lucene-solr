// This file has been automatically generated, DO NOT EDIT

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
 * Efficient sequential read/write of packed integers.
 * 代表写入的数据 需要8位表示  也就是写入的数值在 0~255 之间
 */
final class BulkOperationPacked8 extends BulkOperationPacked {

  public BulkOperationPacked8() {
    super(8);
  }

  // 父类方法本身可以实现解码 但是这里简化了逻辑

  /**
   *
   * @param blocks
   * @param blocksOffset   block的偏移量
   * @param values
   * @param valuesOffset
   * @param iterations
   */
  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      // 在定位到这个block 之后
      final long block = blocks[blocksOffset++];
      // 每个 block 存储了 8个byte
      for (int shift = 56; shift >= 0; shift -= 8) {
        values[valuesOffset++] = (int) ((block >>> shift) & 255);
      }
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int j = 0; j < iterations; ++j) {
      // 刚好以byte形式存储 那么直接取值就好
      values[valuesOffset++] = blocks[blocksOffset++] & 0xFF;
    }
  }

  /**
   *
   * @param blocks  存储了所有的block  该对象只是作为一个reader 负责读数据就好
   * @param blocksOffset  代表从block的第几个开始读
   * @param values  存放读取的结果
   * @param valuesOffset  从哪里开始存
   * @param iterations  代表会读取几个 block
   */
  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      // 找到对应的block
      final long block = blocks[blocksOffset++];
      // 因为该对象已经确定会以 8 bit 为单位存数据 所以取的时候就 8bit 的取
      for (int shift = 56; shift >= 0; shift -= 8) {
        values[valuesOffset++] = (block >>> shift) & 255;
      }
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int j = 0; j < iterations; ++j) {
      values[valuesOffset++] = blocks[blocksOffset++] & 0xFF;
    }
  }

}
