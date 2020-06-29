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


import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ByteBlockPool;

/* IndexInput that knows how to read the byte slices written
 * by Posting and PostingVector.  We read the bytes in
 * each slice until we hit the end of that slice at which
 * point we read the forwarding address of the next slice
 * and then jump to it.*/
// 该对象负责从 pool中读取分片数据    创建分片的好处是根据level 提前分配一块连续的 大小适当的内存
final class ByteSliceReader extends DataInput {

  /**
   * 分片被创建在这个 pool 中
   */
  ByteBlockPool pool;
  int bufferUpto;
  byte[] buffer;
  /**
   * 在某个block的相对偏移量  当读取到 这个block的末尾时 代表 eof
   */
  public int upto;
  /**
   * 代表还允许写入的长度
   */
  int limit;
  /**
   * 这个 level 指slice 分配的内存大小
   */
  int level;
  public int bufferOffset;

  /**
   * 这是绝对偏移量
   */
  public int endIndex;

  /**
   *
   * @param pool
   * @param startIndex   pool的绝对偏移量
   * @param endIndex    pool的绝对偏移量
   */
  public void init(ByteBlockPool pool, int startIndex, int endIndex) {

    assert endIndex-startIndex >= 0;
    assert startIndex >= 0;
    assert endIndex >= 0;

    this.pool = pool;
    this.endIndex = endIndex;

    // 一开始的 bufferOffset 之类的指针类属性都是根据 startIndex 初始化的   并且随着数据的读取 buffer会往后切换 并且无法倒退 也就是数据只能读取一次

    // 默认情况 按最小单位创建分片
    level = 0;
    // 通过去尾法 可以得到 block的下标
    bufferUpto = startIndex / ByteBlockPool.BYTE_BLOCK_SIZE;
    // 计算block 的起点偏移量
    bufferOffset = bufferUpto * ByteBlockPool.BYTE_BLOCK_SIZE;
    buffer = pool.buffers[bufferUpto];
    upto = startIndex & ByteBlockPool.BYTE_BLOCK_MASK;

    // 这是最小的分片大小
    final int firstSize = ByteBlockPool.LEVEL_SIZE_ARRAY[0];

    // 代表已经到末尾了
    if (startIndex+firstSize >= endIndex) {
      // There is only this one slice to read
      limit = endIndex & ByteBlockPool.BYTE_BLOCK_MASK;
    } else
      // 第一个分片的大小为5   但是有4个byte 要用来存储连接到下一个分片的信息
      limit = upto+firstSize-4;
  }

  /**
   * 正常读取的时候 无法判断 此时读取到末尾了 还是需要跳到下一个分片  这时就要配合该方法
   * @return
   */
  public boolean eof() {
    assert upto + bufferOffset <= endIndex;
    return upto + bufferOffset == endIndex;
  }

  @Override
  public byte readByte() {
    assert !eof();
    assert upto <= limit;
    if (upto == limit)
      // 代表该block的
      nextSlice();
    // 正常情况下 往下读取数据就好
    return buffer[upto++];
  }

  /**
   * 将内部的数据写入到 output 中
   * @param out
   * @return
   * @throws IOException
   */
  public long writeTo(DataOutput out) throws IOException {
    long size = 0;
    while(true) {
      // 当写入到末尾时 退出循环
      if (limit + bufferOffset == endIndex) {
        assert endIndex - bufferOffset >= upto;
        out.writeBytes(buffer, upto, limit-upto);
        size += limit-upto;
        break;
      } else {
        // 从当前位置写入到 limit
        out.writeBytes(buffer, upto, limit-upto);
        size += limit-upto;
        // 切换到下一个分片
        nextSlice();
      }
    }

    return size;
  }

  /**
   * 切换到下一个 block 同时更新 bufferOffset limit等
   */
  public void nextSlice() {

    // Skip to our next slice
    // 这里通过4个byte 来存储 下一个分片的起始偏移量   跟 IntBlockPool 一个套路
    final int nextIndex = ((buffer[limit]&0xff)<<24) + ((buffer[1+limit]&0xff)<<16) + ((buffer[2+limit]&0xff)<<8) + (buffer[3+limit]&0xff);

    // 每次分配的大小都会递增  直到第9级的时候 level 每次都会被重新赋值成9 也就是分片不会超过这个大小了
    level = ByteBlockPool.NEXT_LEVEL_ARRAY[level];
    final int newSize = ByteBlockPool.LEVEL_SIZE_ARRAY[level];

    // 找到分片的起点
    bufferUpto = nextIndex / ByteBlockPool.BYTE_BLOCK_SIZE;
    bufferOffset = bufferUpto * ByteBlockPool.BYTE_BLOCK_SIZE;

    buffer = pool.buffers[bufferUpto];
    upto = nextIndex & ByteBlockPool.BYTE_BLOCK_MASK;

    if (nextIndex + newSize >= endIndex) {
      // We are advancing to the final slice
      assert endIndex - nextIndex > 0;
      limit = endIndex - bufferOffset;
    } else {
      // This is not the final slice (subtract 4 for the
      // forwarding address at the end of this new slice)
      limit = upto+newSize-4;
    }
  }

  /**
   * 实际上将内部的数据 写入到 b 中  跟 writeTo 差不多
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len the number of bytes to read
   */
  @Override
  public void readBytes(byte[] b, int offset, int len) {
    while(len > 0) {
      final int numLeft = limit-upto;
      if (numLeft < len) {
        // Read entire slice
        System.arraycopy(buffer, upto, b, offset, numLeft);
        offset += numLeft;
        len -= numLeft;
        nextSlice();
      } else {
        // This slice is the last one
        System.arraycopy(buffer, upto, b, offset, len);
        upto += len;
        break;
      }
    }
  }
}