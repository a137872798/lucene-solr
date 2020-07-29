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
package org.apache.lucene.util.fst;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

// TODO: merge with PagedBytes, except PagedBytes doesn't
// let you read while writing which FST needs
// 该对象内部存储了一组用于写入 数据的block    该对象在 fst中被使用
class BytesStore extends DataOutput implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(BytesStore.class)
      + RamUsageEstimator.shallowSizeOfInstance(ArrayList.class);

  /**
   * 每个block 都是一个 byte[]  一个byte代表8bit 也就是每个block大小 都是8的倍数
   */
  private final List<byte[]> blocks = new ArrayList<>();

  /**
   * 等价于 1 << blockBits
   */
  private final int blockSize;
  /**
   * 看来应该是 8的倍数 也就是每个 block 都是由一个 或多个byte 组成的
   */
  private final int blockBits;
  /**
   * 根据 size 计算出来的掩码
   */
  private final int blockMask;

  /**
   * 当前数到 list的哪个block
   */
  private byte[] current;

  private int nextWrite;

  /**
   * 每个block 占多少bit
   * @param blockBits
   */
  public BytesStore(int blockBits) {
    this.blockBits = blockBits;
    // 通过位运算计算 block的大小
    blockSize = 1 << blockBits;
    blockMask = blockSize-1;
    nextWrite = blockSize;
  }

  /** Pulls bytes from the provided IndexInput.  */
  // 通过一个输入流来初始化store 对象
  public BytesStore(DataInput in, long numBytes, int maxBlockSize) throws IOException {
    int blockSize = 2;
    int blockBits = 1;
    // 每次 bit 增加一位 同时 blockSize 翻一倍
    while(blockSize < numBytes && blockSize < maxBlockSize) {
      blockSize *= 2;
      blockBits++;
    }
    this.blockBits = blockBits;
    this.blockSize = blockSize;
    this.blockMask = blockSize-1;
    long left = numBytes;
    while(left > 0) {
      final int chunk = (int) Math.min(blockSize, left);
      byte[] block = new byte[chunk];
      // 从输入流中读取数据
      in.readBytes(block, 0, block.length);
      blocks.add(block);
      left -= chunk;
    }

    // So .getPosition still works
    nextWrite = blocks.get(blocks.size()-1).length;
  }

  /** Absolute write byte; you must ensure dest is &lt; max
   *  position written so far. */
  // dest 代表写入的目标位置   然后b 代表写入的值
  public void writeByte(long dest, byte b) {
    int blockIndex = (int) (dest >> blockBits);
    byte[] block = blocks.get(blockIndex);
    block[(int) (dest & blockMask)] = b;
  }

  @Override
  public void writeByte(byte b) {
    // 如果当前指针已经指向末尾了 就创建一个新的 block 继续写入数据
    if (nextWrite == blockSize) {
      current = new byte[blockSize];
      blocks.add(current);
      nextWrite = 0;
    }
    current[nextWrite++] = b;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int len) {
    while (len > 0) {
      //代表当前block还有多少空间可用
      int chunk = blockSize - nextWrite;
      if (len <= chunk) {
        assert b != null;
        assert current != null;
        System.arraycopy(b, offset, current, nextWrite, len);
        nextWrite += len;
        break;
      } else {
        // 代表该block空间不够 还需要创建一个新的
        if (chunk > 0) {
          System.arraycopy(b, offset, current, nextWrite, chunk);
          offset += chunk;
          len -= chunk;
        }
        current = new byte[blockSize];
        blocks.add(current);
        nextWrite = 0;
      }
    }
  }

  int getBlockBits() {
    return blockBits;
  }

  /** Absolute writeBytes without changing the current
   *  position.  Note: this cannot "grow" the bytes, so you
   *  must only call it on already written parts.
   * @param offset  代表该数组从哪里开始拷贝
   */
  void writeBytes(long dest, byte[] b, int offset, int len) {
    //System.out.println("  BS.writeBytes dest=" + dest + " offset=" + offset + " len=" + len);
    assert dest + len <= getPosition(): "dest=" + dest + " pos=" + getPosition() + " len=" + len;

    // Note: weird: must go "backwards" because copyBytes
    // calls us with overlapping src/dest.  If we
    // go forwards then we overwrite bytes before we can
    // copy them:

    /*
    int blockIndex = dest >> blockBits;
    int upto = dest & blockMask;
    byte[] block = blocks.get(blockIndex);
    while (len > 0) {
      int chunk = blockSize - upto;
      System.out.println("    cycle chunk=" + chunk + " len=" + len);
      if (len <= chunk) {
        System.arraycopy(b, offset, block, upto, len);
        break;
      } else {
        System.arraycopy(b, offset, block, upto, chunk);
        offset += chunk;
        len -= chunk;
        blockIndex++;
        block = blocks.get(blockIndex);
        upto = 0;
      }
    }
    */

    final long end = dest + len;
    // 同样 先换算block的下标 然后根据 掩码计算偏移量
    int blockIndex = (int) (end >> blockBits);
    int downTo = (int) (end & blockMask);
    if (downTo == 0) {
      blockIndex--;
      downTo = blockSize;
    }
    byte[] block = blocks.get(blockIndex);

    while (len > 0) {
      //System.out.println("    cycle downTo=" + downTo + " len=" + len);
      if (len <= downTo) {
        //System.out.println("      final: offset=" + offset + " len=" + len + " dest=" + (downTo-len));
        System.arraycopy(b, offset, block, downTo-len, len);
        break;
      } else {
        len -= downTo;
        //System.out.println("      partial: offset=" + (offset + len) + " len=" + downTo + " dest=0");
        System.arraycopy(b, offset + len, block, 0, downTo);
        blockIndex--;
        block = blocks.get(blockIndex);
        downTo = blockSize;
      }
    }
  }

  /** Absolute copy bytes self to self, without changing the
   *  position. Note: this cannot "grow" the bytes, so must
   *  only call it on already written parts. */
  public void copyBytes(long src, long dest, int len) {
    //System.out.println("BS.copyBytes src=" + src + " dest=" + dest + " len=" + len);
    assert src < dest;

    // Note: weird: must go "backwards" because copyBytes
    // calls us with overlapping src/dest.  If we
    // go forwards then we overwrite bytes before we can
    // copy them:

    /*
    int blockIndex = src >> blockBits;
    int upto = src & blockMask;
    byte[] block = blocks.get(blockIndex);
    while (len > 0) {
      int chunk = blockSize - upto;
      System.out.println("  cycle: chunk=" + chunk + " len=" + len);
      if (len <= chunk) {
        writeBytes(dest, block, upto, len);
        break;
      } else {
        writeBytes(dest, block, upto, chunk);
        blockIndex++;
        block = blocks.get(blockIndex);
        upto = 0;
        len -= chunk;
        dest += chunk;
      }
    }
    */

    // 本次写入的终点
    long end = src + len;

    // 这里是定位终点在哪个block
    int blockIndex = (int) (end >> blockBits);
    int downTo = (int) (end & blockMask);
    // 如果刚好写换到下一个  block 那么回到上个block的末尾
    if (downTo == 0) {
      blockIndex--;
      downTo = blockSize;
    }
    byte[] block = blocks.get(blockIndex);

    while (len > 0) {
      //System.out.println("  cycle downTo=" + downTo);
      // 没有跨越block 可以直接写入数据
      if (len <= downTo) {
        //System.out.println("    finish");
        writeBytes(dest, block, downTo-len, len);
        break;
      } else {
        //System.out.println("    partial");
        len -= downTo;
        writeBytes(dest + len, block, 0, downTo);
        blockIndex--;
        block = blocks.get(blockIndex);
        downTo = blockSize;
      }
    }
  }

  /** Copies bytes from this store to a target byte array. */
  // 从当前block[] 中拷贝数据到目标数组
  public void copyBytes(long src, byte[] dest, int offset, int len) {
    int blockIndex = (int) (src >> blockBits);
    int upto = (int) (src & blockMask);
    byte[] block = blocks.get(blockIndex);
    while (len > 0) {
      int chunk = blockSize - upto;
      if (len <= chunk) {
        System.arraycopy(block, upto, dest, offset, len);
        break;
      } else {
        System.arraycopy(block, upto, dest, offset, chunk);
        blockIndex++;
        block = blocks.get(blockIndex);
        upto = 0;
        len -= chunk;
        offset += chunk;
      }
    }
  }

  /** Writes an int at the absolute position without
   *  changing the current pointer. */
  // 在指定的位置写入一个int值
  public void writeInt(long pos, int value) {
    int blockIndex = (int) (pos >> blockBits);
    int upto = (int) (pos & blockMask);
    byte[] block = blocks.get(blockIndex);
    // 好像 blockBits 总是8的倍数 所以可以放心的每次 右移8位
    int shift = 24;
    // 将 intValue 拆解成4个byte 写入
    for(int i=0;i<4;i++) {
      // 从高位写入
      block[upto++] = (byte) (value >> shift);
      shift -= 8;
      // 代表这个block写满了 切换到下一个
      if (upto == blockSize) {
        upto = 0;
        blockIndex++;
        block = blocks.get(blockIndex);
      }
    }
  }

  /** Reverse from srcPos, inclusive, to destPos, inclusive. */
  // 将 src 到 dest 的数据反转
  public void reverse(long srcPos, long destPos) {
    assert srcPos < destPos;
    assert destPos < getPosition();
    //System.out.println("reverse src=" + srcPos + " dest=" + destPos);

    int srcBlockIndex = (int) (srcPos >> blockBits);
    int src = (int) (srcPos & blockMask);
    byte[] srcBlock = blocks.get(srcBlockIndex);

    int destBlockIndex = (int) (destPos >> blockBits);
    int dest = (int) (destPos & blockMask);
    byte[] destBlock = blocks.get(destBlockIndex);
    //System.out.println("  srcBlock=" + srcBlockIndex + " destBlock=" + destBlockIndex);

    // 向上取整
    int limit = (int) (destPos - srcPos + 1)/2;
    for(int i=0;i<limit;i++) {
      //System.out.println("  cycle src=" + src + " dest=" + dest);
      // 很基础的一个交换函数
      byte b = srcBlock[src];
      srcBlock[src] = destBlock[dest];
      destBlock[dest] = b;

      src++;
      if (src == blockSize) {
        srcBlockIndex++;
        srcBlock = blocks.get(srcBlockIndex);
        //System.out.println("  set destBlock=" + destBlock + " srcBlock=" + srcBlock);
        src = 0;
      }

      dest--;
      if (dest == -1) {
        destBlockIndex--;
        destBlock = blocks.get(destBlockIndex);
        //System.out.println("  set destBlock=" + destBlock + " srcBlock=" + srcBlock);
        dest = blockSize-1;
      }
    }
  }

  public void skipBytes(int len) {
    while (len > 0) {
      int chunk = blockSize - nextWrite;
      if (len <= chunk) {
        nextWrite += len;
        break;
      } else {
        // 要跳过的长度 超过了当前block的可用长度 构建一个新的 block 并添加到列表中
        len -= chunk;
        current = new byte[blockSize];
        blocks.add(current);
        nextWrite = 0;
      }
    }
  }

  /**
   * 返回当前存储的位置
   * @return
   */
  public long getPosition() {
    return ((long) blocks.size()-1) * blockSize + nextWrite;
  }

  /** Pos must be less than the max position written so far!
   *  Ie, you cannot "grow" the file with this! */
  // 截取超过该长度的部分
  public void truncate(long newLen) {
    assert newLen <= getPosition();
    assert newLen >= 0;
    int blockIndex = (int) (newLen >> blockBits);
    nextWrite = (int) (newLen & blockMask);
    if (nextWrite == 0) {
      blockIndex--;
      nextWrite = blockSize;
    }
    // 将起点后面所有的block 截去
    blocks.subList(blockIndex+1, blocks.size()).clear();
    if (newLen == 0) {
      current = null;
    } else {
      current = blocks.get(blockIndex);
    }
    assert newLen == getPosition();
  }

  /**
   * 当FST.finish() 触发时 会转发到该方法
   */
  public void finish() {
    if (current != null) {
      // 这里创建了一个 等大的数组 将元素拷贝进去
      byte[] lastBuffer = new byte[nextWrite];
      System.arraycopy(current, 0, lastBuffer, 0, nextWrite);
      // 替换原来的数组
      blocks.set(blocks.size()-1, lastBuffer);
      current = null;
    }
  }

  /** Writes all of our bytes to the target {@link DataOutput}. */
  // 将仓库中的数据 转移到 output 中
  public void writeTo(DataOutput out) throws IOException {
    for(byte[] block : blocks) {
      out.writeBytes(block, 0, block.length);
    }
  }

  /**
   * TODO 这里有关 FST的先跳过
   * @return
   */
  public FST.BytesReader getForwardReader() {
    if (blocks.size() == 1) {
      return new ForwardBytesReader(blocks.get(0));
    }
    return new FST.BytesReader() {
      private byte[] current;
      private int nextBuffer;
      private int nextRead = blockSize;

      @Override
      public byte readByte() {
        if (nextRead == blockSize) {
          current = blocks.get(nextBuffer++);
          nextRead = 0;
        }
        return current[nextRead++];
      }

      @Override
      public void skipBytes(long count) {
        setPosition(getPosition() + count);
      }

      @Override
      public void readBytes(byte[] b, int offset, int len) {
        while(len > 0) {
          int chunkLeft = blockSize - nextRead;
          if (len <= chunkLeft) {
            System.arraycopy(current, nextRead, b, offset, len);
            nextRead += len;
            break;
          } else {
            if (chunkLeft > 0) {
              System.arraycopy(current, nextRead, b, offset, chunkLeft);
              offset += chunkLeft;
              len -= chunkLeft;
            }
            current = blocks.get(nextBuffer++);
            nextRead = 0;
          }
        }
      }

      @Override
      public long getPosition() {
        return ((long) nextBuffer-1)*blockSize + nextRead;
      }

      @Override
      public void setPosition(long pos) {
        int bufferIndex = (int) (pos >> blockBits);
        nextBuffer = bufferIndex+1;
        current = blocks.get(bufferIndex);
        nextRead = (int) (pos & blockMask);
        assert getPosition() == pos;
      }

      @Override
      public boolean reversed() {
        return false;
      }
    };
  }

  /**
   * 将内部数据 以反向的方式读取
   * @return
   */
  public FST.BytesReader getReverseReader() {
    return getReverseReader(true);
  }

  /**
   * 获取一个反向读取的对象
   * @param allowSingle  是否支持处理只有一个block的情况
   * @return
   */
  FST.BytesReader getReverseReader(boolean allowSingle) {
    // 如果内部只有一个元素 那么直接返回一个基于 byte[] 的反向reader
    if (allowSingle && blocks.size() == 1) {
      return new ReverseBytesReader(blocks.get(0));
    }
    return new FST.BytesReader() {
      // 默认情况下指向的是第一个 byte[]
      private byte[] current = blocks.size() == 0 ? null : blocks.get(0);
      // 对应blocks下标
      private int nextBuffer = -1;
      // 对应byte[] 下标
      private int nextRead = 0;

      @Override
      public byte readByte() {
        // 代表此时已经读取完这个数组 那么就获取上一个数组 并重置nextRead
        if (nextRead == -1) {
          // 获取上一个 byte[]
          current = blocks.get(nextBuffer--);
          nextRead = blockSize-1;
        }
        // 因为是反向读取 所以 nextRead --
        return current[nextRead--];
      }

      @Override
      public void skipBytes(long count) {
        setPosition(getPosition() - count);
      }

      /**
       * 读取数据到b 中
       * @param b the array to read bytes into
       * @param offset the offset in the array to start storing bytes
       * @param len the number of bytes to read
       */
      @Override
      public void readBytes(byte[] b, int offset, int len) {
        for(int i=0;i<len;i++) {
          b[offset+i] = readByte();
        }
      }

      @Override
      public long getPosition() {
        return ((long) nextBuffer+1)*blockSize + nextRead;
      }

      /**
       * 更新当前的指针
       * 初始化该对象后 必须通过该方法后才能正常使用
       * @param pos
       */
      @Override
      public void setPosition(long pos) {
        // NOTE: a little weird because if you
        // setPosition(0), the next byte you read is
        // bytes[0] ... but I would expect bytes[-1] (ie,
        // EOF)...?
        // 将指针换算成下标
        int bufferIndex = (int) (pos >> blockBits);
        // 因为是从后往前遍历 所以下一个buffer 是当前buffer - 1
        nextBuffer = bufferIndex-1;
        current = blocks.get(bufferIndex);
        // 略小于pos 代表下一个要被读取的元素
        nextRead = (int) (pos & blockMask);
        assert getPosition() == pos: "pos=" + pos + " getPos()=" + getPosition();
      }

      @Override
      public boolean reversed() {
        return true;
      }
    };
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES_USED;
    for (byte[] block : blocks) {
      size += RamUsageEstimator.sizeOf(block);
    }
    return size;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(numBlocks=" + blocks.size() + ")";
  }
}
