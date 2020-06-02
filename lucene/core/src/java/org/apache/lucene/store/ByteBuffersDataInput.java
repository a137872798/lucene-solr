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
package org.apache.lucene.store;

import java.io.EOFException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A {@link DataInput} implementing {@link RandomAccessInput} and reading data from a
 * list of {@link ByteBuffer}s.
 * 基于BB 的输入流对象
 */
public final class ByteBuffersDataInput extends DataInput implements Accountable, RandomAccessInput {
  /**
   * 一个 BB 数组   内部是只读对象
   */
  private final ByteBuffer[] blocks;
  /**
   * 代表每个 block 会占用多少bit
   */
  private final int blockBits;
  private final int blockMask;
  private final long size;
  /**
   * 偏移量  就是拷贝BB 时 slice相关的
   */
  private final long offset;

  private long pos;

  /**
   * Read data from a set of contiguous buffers. All data buffers except for the last one 
   * must have an identical remaining number of bytes in the buffer (that is a power of two). The last
   * buffer can be of an arbitrary remaining length.
   */
  public ByteBuffersDataInput(List<ByteBuffer> buffers) {
    // 进行参数校验
    ensureAssumptions(buffers);

    this.blocks = buffers.stream().map(buf -> buf.asReadOnlyBuffer()).toArray(ByteBuffer[]::new);

    if (blocks.length == 1) {
      this.blockBits = 32;
      this.blockMask = ~0;
    } else {
      // 代表一个 block 由多少个 byte组成
      final int blockBytes = determineBlockPage(buffers);
      // 比如一个 byteBuffer 对象由 8个byte 组成 那么 每个 blockBits 为 3  (2^3 = 8)
      // 当寻找下标时 将pos / 2^3  刚好就代表 / 8byte
      // 也就是 pos >>> blockBits
      this.blockBits = Integer.numberOfTrailingZeros(blockBytes);
      this.blockMask = (1 << blockBits) - 1;
    }

    // 计算以 byte  为单位总长度
    this.size = Arrays.stream(blocks).mapToLong(block -> block.remaining()).sum();

    // The initial "position" of this stream is shifted by the position of the first block.
    this.offset = blocks[0].position();
    this.pos = offset;
  }

  public long size() {
    return size;
  }

  @Override
  public long ramBytesUsed() {
    // Return a rough estimation for allocated blocks. Note that we do not make
    // any special distinction for what the type of buffer is (direct vs. heap-based).
    return RamUsageEstimator.NUM_BYTES_OBJECT_REF * blocks.length + 
           Arrays.stream(blocks).mapToLong(buf -> buf.capacity()).sum();
  }

  @Override
  public byte readByte() throws EOFException {
    try {
      ByteBuffer block = blocks[blockIndex(pos)];
      byte v = block.get(blockOffset(pos));
      pos++;
      return v;
    } catch (IndexOutOfBoundsException e) {
      if (pos >= size()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  /**
   * Reads exactly {@code len} bytes into the given buffer. The buffer must have
   * enough remaining limit.
   * 
   * If there are fewer than {@code len} bytes in the input, {@link EOFException} 
   * is thrown.
   * 将数据读取出来并写入到 给与的 BB 中
   */
  public void readBytes(ByteBuffer buffer, int len) throws EOFException {
    try {
      while (len > 0) {
        // 常见副本对象
        ByteBuffer block = blocks[blockIndex(pos)].duplicate();
        int blockOffset = blockOffset(pos);
        block.position(blockOffset);
        int chunk = Math.min(len, block.remaining());
        if (chunk == 0) {
          throw new EOFException();
        }

        // Update pos early on for EOF detection on output buffer, then try to get buffer content.
        pos += chunk;
        block.limit(blockOffset + chunk);
        buffer.put(block);

        len -= chunk;
      }
    } catch (BufferUnderflowException | ArrayIndexOutOfBoundsException e) {
      if (pos >= size()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  @Override
  public void readBytes(byte[] arr, int off, int len) throws EOFException {
    try {
      while (len > 0) {
        ByteBuffer block = blocks[blockIndex(pos)].duplicate();
        block.position(blockOffset(pos));
        int chunk = Math.min(len, block.remaining());
        if (chunk == 0) {
          throw new EOFException();
        }

        // Update pos early on for EOF detection, then try to get buffer content.
        pos += chunk;
        block.get(arr, off, chunk);

        len -= chunk;
        off += chunk;
      }
    } catch (BufferUnderflowException | ArrayIndexOutOfBoundsException e) {
      if (pos >= size()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  @Override
  public byte readByte(long pos) {
    pos += offset;
    return blocks[blockIndex(pos)].get(blockOffset(pos));
  }

  @Override
  public short readShort(long pos) {
    long absPos = offset + pos;
    int blockOffset = blockOffset(absPos);
    // 代表可以直接在该block中读取所有数据
    if (blockOffset + Short.BYTES <= blockMask) {
      return blocks[blockIndex(absPos)].getShort(blockOffset);
    } else {
      // 将当前block 的数据作为高位  下一个block的数据作为低位
      return (short) ((readByte(pos    ) & 0xFF) << 8 | 
                      (readByte(pos + 1) & 0xFF));
    }
  }

  @Override
  public int readInt(long pos) {
    long absPos = offset + pos;
    int blockOffset = blockOffset(absPos);
    if (blockOffset + Integer.BYTES <= blockMask) {
      return blocks[blockIndex(absPos)].getInt(blockOffset);
    } else {
      return ((readByte(pos    )       ) << 24 |
              (readByte(pos + 1) & 0xFF) << 16 |
              (readByte(pos + 2) & 0xFF) << 8  |
              (readByte(pos + 3) & 0xFF));
    }
  }

  @Override
  public long readLong(long pos) {
    long absPos = offset + pos;
    int blockOffset = blockOffset(absPos);
    if (blockOffset + Long.BYTES <= blockMask) {
      return blocks[blockIndex(absPos)].getLong(blockOffset);
    } else {
      return (((long) readInt(pos)) << 32) | (readInt(pos + 4) & 0xFFFFFFFFL);
    }
  }

  public long position() {
    return pos - offset;
  }

  public void seek(long position) throws EOFException {
    this.pos = position + offset;
    if (position > size()) {
      this.pos = size();
      throw new EOFException();
    }
  }
  
  public ByteBuffersDataInput slice(long offset, long length) {
    if (offset < 0 || length < 0 || offset + length > this.size) {
      throw new IllegalArgumentException(String.format(Locale.ROOT,
          "slice(offset=%s, length=%s) is out of bounds: %s",
          offset, length, this));
    }

    return new ByteBuffersDataInput(sliceBufferList(Arrays.asList(this.blocks), offset, length));
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT,
        "%,d bytes, block size: %,d, blocks: %,d, position: %,d%s",
        size(),
        blockSize(),
        blocks.length,
        position(),
        offset == 0 ? "" : String.format(Locale.ROOT, " [offset: %,d]", offset));
  }

  /**
   * 根据pos 计算block的下标
   * @param pos
   * @return
   */
  private final int blockIndex(long pos) {
    return Math.toIntExact(pos >> blockBits);
  }

  /**
   * 对应 每个block下 byte的偏移量
   * @param pos
   * @return
   */
  private final int blockOffset(long pos) {
    return (int) pos & blockMask;
  }  
  
  private int blockSize() {
    return 1 << blockBits;
  }

  private static final boolean isPowerOfTwo(int v) {
    return (v & (v - 1)) == 0;
  }

  /**
   * 校验数据是否合法
   * @param buffers
   */
  private static void ensureAssumptions(List<ByteBuffer> buffers) {
    if (buffers.isEmpty()) {
      throw new IllegalArgumentException("Buffer list must not be empty.");
    }

    if (buffers.size() == 1) {
      // Special case of just a single buffer, conditions don't apply.
    } else {
      // 根据 BB 的容量 获取block的大小
      final int blockPage = determineBlockPage(buffers);
      
      // First buffer decides on block page length.
      if (!isPowerOfTwo(blockPage)) {
        throw new IllegalArgumentException("The first buffer must have power-of-two position() + remaining(): 0x"
            + Integer.toHexString(blockPage));
      }

      // Any block from 2..last-1 should have the same page size.
      for (int i = 1, last = buffers.size() - 1; i < last; i++) {
        ByteBuffer buffer = buffers.get(i);
        if (buffer.position() != 0) {
          throw new IllegalArgumentException("All buffers except for the first one must have position() == 0: " + buffer);
        }
        // 最后一个 BB 允许不写满
        if (i != last && buffer.remaining() != blockPage) {
          throw new IllegalArgumentException("Intermediate buffers must share an identical remaining() power-of-two block size: 0x" 
              + Integer.toHexString(blockPage));
        }
      }
    }
  }

  /**
   * 根据  BB 的容量 获取每个block的容量
   * @param buffers
   * @return
   */
  static int determineBlockPage(List<ByteBuffer> buffers) {
    ByteBuffer first = buffers.get(0);
    final int blockPage = Math.toIntExact((long) first.position() + first.remaining());
    return blockPage;
  }

  /**
   * 创建一个分片
   * @param buffers
   * @param offset 使用给定的偏移量 然后分片对象会以该值作为 起点   + pos 才会得到 absPos(绝对偏移量)
   * @param length
   * @return
   */
  private static List<ByteBuffer> sliceBufferList(List<ByteBuffer> buffers, long offset, long length) {
    ensureAssumptions(buffers);

    if (buffers.size() == 1) {
      ByteBuffer cloned = buffers.get(0).asReadOnlyBuffer();
      cloned.position(Math.toIntExact(cloned.position() + offset));
      cloned.limit(Math.toIntExact(cloned.position() + length));
      return Arrays.asList(cloned);
    } else {
      long absStart = buffers.get(0).position() + offset;
      long absEnd = absStart + length;

      int blockBytes = ByteBuffersDataInput.determineBlockPage(buffers);
      int blockBits = Integer.numberOfTrailingZeros(blockBytes);
      long blockMask = (1L << blockBits) - 1;

      int endOffset = Math.toIntExact(absEnd & blockMask);

      ArrayList<ByteBuffer> cloned = 
        buffers.subList(Math.toIntExact(absStart / blockBytes), 
                        Math.toIntExact(absEnd / blockBytes + (endOffset == 0 ? 0 : 1)))
          .stream()
          .map(buf -> buf.asReadOnlyBuffer())
          .collect(Collectors.toCollection(ArrayList::new));

      if (endOffset == 0) {
        cloned.add(ByteBuffer.allocate(0));
      }

      cloned.get(0).position(Math.toIntExact(absStart & blockMask));
      cloned.get(cloned.size() - 1).limit(endOffset);
      return cloned;
    }
  }  
}
