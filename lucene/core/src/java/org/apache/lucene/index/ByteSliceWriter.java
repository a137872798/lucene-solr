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
 */

final class ByteSliceWriter extends DataOutput {

  private byte[] slice;
  private int upto;
  /**
   * 该对象是一个内存池
   */
  private final ByteBlockPool pool;

  int offset0;

  public ByteSliceWriter(ByteBlockPool pool) {
    this.pool = pool;
  }

  /**
   * Set up the writer to write at address.
   * address 在通过位运算后 获得了一个下标
   */
  public void init(int address) {
    // 定位到一个 byte[]
    slice = pool.buffers[address >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert slice != null;
    // 这好像是一个写入的起点  为什么要这样定义
    upto = address & ByteBlockPool.BYTE_BLOCK_MASK;
    offset0 = address;
    assert upto < slice.length;
  }

  /** Write byte into byte slice stream */
  @Override
  public void writeByte(byte b) {
    assert slice != null;
    // 代表起点已经被写入了某个值
    if (slice[upto] != 0) {
      upto = pool.allocSlice(slice, upto);
      slice = pool.buffer;
      offset0 = pool.byteOffset;
      assert slice != null;
    }
    // 最终结果就是 将值写入到 byte[] 中 上面应该是申请空位的
    slice[upto++] = b;
    assert upto != slice.length;
  }

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