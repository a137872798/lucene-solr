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
package org.apache.lucene.index;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ByteBlockPool;



/**
 * Class to write byte streams into slices of shared
 * byte[].  This is used by DocumentsWriter to hold the
 * posting list for many terms in RAM.
 * 不同于 IntBlockPool   分片写入和读取对象是分离出来的
 * 该对象负责将数据写入到 pool 中
 */

final class ByteSliceWriter extends DataOutput {

  /**
   * 当前 分片所在的 block
   */
  private byte[] slice;
  private int upto;
  /**
   * 该对象是一个内存池
   */
  private final ByteBlockPool pool;

  /**
   * 当前 block 的起点偏移量
   */
  int offset0;

  public ByteSliceWriter(ByteBlockPool pool) {
    this.pool = pool;
  }

  /**
   * Set up the writer to write at address.
   * 指定一个创建分配的地址
   */
  public void init(int address) {
    slice = pool.buffers[address >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert slice != null;
    upto = address & ByteBlockPool.BYTE_BLOCK_MASK;
    offset0 = address;
    assert upto < slice.length;
  }

  /** Write byte into byte slice stream */
  @Override
  public void writeByte(byte b) {
    assert slice != null;
    // 原本每次只写入一个 byte 当快要用完这个byte的时候 会多写一个位置
    // 这里代表发现分片的空间不足了 于是申请一个新的分片   同时这个位置上的数据还记录了期望分配的slice大小
    if (slice[upto] != 0) {
      // 返回新的block写入的起点位置
      upto = pool.allocSlice(slice, upto);
      slice = pool.buffer;
      offset0 = pool.byteOffset;
      assert slice != null;
    }
    slice[upto++] = b;
    assert upto != slice.length;
  }

  /**
   * 将数组内的数据写入到 slice中
   * @param b the bytes to write
   * @param offset the offset in the byte array
   * @param len
   */
  @Override
  public void writeBytes(final byte[] b, int offset, final int len) {
    final int offsetEnd = offset + len;
    while(offset < offsetEnd) {
      if (slice[upto] != 0) {
        // End marker
        upto = pool.allocSlice(slice, upto);
        slice = pool.buffer;
        offset0 = pool.byteOffset;
      }

      slice[upto++] = b[offset++];
      assert upto != slice.length;
    }
  }

  public int getAddress() {
    return upto + (offset0 & DocumentsWriterPerThread.BYTE_BLOCK_NOT_MASK);
  }
}