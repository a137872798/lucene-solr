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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.UnicodeUtil;

/**
 * A {@link DataOutput} storing data in a list of {@link ByteBuffer}s.
 * 通过 byteBuffer 来存储数据
 * 而 DataOutput抽象的含义是普通的数据输出流    IndexOutput 代表数据本身有一个偏移量的概念 和 filePoint()
 * 所以一般都是针对 文件 才使用 IndexOutput
 */
public final class ByteBuffersDataOutput extends DataOutput implements Accountable {
  private final static ByteBuffer EMPTY = ByteBuffer.allocate(0);
  private final static byte [] EMPTY_BYTE_ARRAY = {};

  public final static IntFunction<ByteBuffer> ALLOCATE_BB_ON_HEAP = ByteBuffer::allocate;

  /**
   * A singleton instance of "no-reuse" buffer strategy.
   */
  public final static Consumer<ByteBuffer> NO_REUSE = (bb) -> {
    throw new RuntimeException("reset() is not allowed on this buffer.");
  };

  /**
   * An implementation of a {@link ByteBuffer} allocation and recycling policy.
   * The blocks are recycled if exactly the same size is requested, otherwise
   * they're released to be GCed.
   * 避免频繁GC
   */
  public final static class ByteBufferRecycler {
    /**
     * 存储对象 以便复用
     */
    private final ArrayDeque<ByteBuffer> reuse = new ArrayDeque<>();
    /**
     * 当BB池中 没有byteBuffer可分配时 通过该函数分配一个对象
     */
    private final IntFunction<ByteBuffer> delegate;

    public ByteBufferRecycler(IntFunction<ByteBuffer> delegate) {
      this.delegate = Objects.requireNonNull(delegate);
    }

    public ByteBuffer allocate(int size) {
      while (!reuse.isEmpty()) {
        ByteBuffer bb = reuse.removeFirst();
        // If we don't have a buffer of exactly the requested size, discard it.
        if (bb.remaining() == size) {
          return bb;
        }
      }

      return delegate.apply(size);        
    }

    public void reuse(ByteBuffer buffer) {
      // 重置相关指针后 重新回到队列
      buffer.rewind();
      reuse.addLast(buffer);
    }
  }

  /**
   * 代表以 2的10次位 为一个block
   */
  public final static int DEFAULT_MIN_BITS_PER_BLOCK = 10; // 1024 B
  public final static int DEFAULT_MAX_BITS_PER_BLOCK = 26; //   64 MB

  /**
   * Maximum number of blocks at the current {@link #blockBits} block size
   * before we increase the block size (and thus decrease the number of blocks).
   */
  final static int MAX_BLOCKS_BEFORE_BLOCK_EXPANSION = 100;

  /**
   * Maximum block size: {@code 2^bits}.
   * 每个块最多会使用多少位
   */
  private final int maxBitsPerBlock;

  /**
   * {@link ByteBuffer} supplier.
   */
  private final IntFunction<ByteBuffer> blockAllocate;

  /**
   * {@link ByteBuffer} recycler on {@link #reset}.
   */
  private final Consumer<ByteBuffer> blockReuse;

  /**
   * Current block size: {@code 2^bits}.
   * 当前block 已经写入了多少bit
   */
  private int blockBits;

  /**
   * Blocks storing data.
   */
  private final ArrayDeque<ByteBuffer> blocks = new ArrayDeque<>();

  /**
   * The current-or-next write block.
   */
  private ByteBuffer currentBlock = EMPTY;

  /**
   * 基由一个预估的大小来创建 输出流    这里的输出流内部维护了一组 BB 对象 每次写入数据就是往BB 写入
   * 这里 block 等同于 BB
   * @param expectedSize   预计每次存入的数据大小至少是
   */
  public ByteBuffersDataOutput(long expectedSize) {
    this(computeBlockSizeBitsFor(expectedSize), DEFAULT_MAX_BITS_PER_BLOCK, ALLOCATE_BB_ON_HEAP, NO_REUSE);
  }

  public ByteBuffersDataOutput() {
    this(DEFAULT_MIN_BITS_PER_BLOCK, DEFAULT_MAX_BITS_PER_BLOCK, ALLOCATE_BB_ON_HEAP, NO_REUSE);
  }

  public ByteBuffersDataOutput(int minBitsPerBlock,    // 每个块最小会使用多少bit
                               int maxBitsPerBlock,
                               IntFunction<ByteBuffer> blockAllocate,
                               Consumer<ByteBuffer> blockReuse) {
    if (minBitsPerBlock < 10 ||
        minBitsPerBlock > maxBitsPerBlock ||
        maxBitsPerBlock > 31) {
      throw new IllegalArgumentException(String.format(Locale.ROOT,
          "Invalid arguments: %s %s",
          minBitsPerBlock,
          maxBitsPerBlock));
    }
    this.maxBitsPerBlock = maxBitsPerBlock;
    this.blockBits = minBitsPerBlock;
    this.blockAllocate = Objects.requireNonNull(blockAllocate, "Block allocator must not be null.");
    this.blockReuse = Objects.requireNonNull(blockReuse, "Block reuse must not be null.");
  }

  /**
   * 正常情况下直接往block中插入数据
   * @param b
   */
  @Override
  public void writeByte(byte b) {
    if (!currentBlock.hasRemaining()) {
      // 创建一个新的block
      appendBlock();
    }
    currentBlock.put(b);
  }

  @Override
  public void writeBytes(byte[] src, int offset, int length) {
    assert length >= 0;
    while (length > 0) {
      if (!currentBlock.hasRemaining()) {
        appendBlock();
      }

      int chunk = Math.min(currentBlock.remaining(), length);
      currentBlock.put(src, offset, chunk);
      length -= chunk;
      offset += chunk;
    }
  }

  @Override
  public void writeBytes(byte[] b, int length) {
    writeBytes(b, 0, length);
  }

  public void writeBytes(byte[] b) {
    writeBytes(b, 0, b.length);
  }

  /**
   * 将某个BB中的数据写入到输出流中
   * @param buffer
   */
  public void writeBytes(ByteBuffer buffer) {
    // 单独维护一份新的指针不影响之前的对象
    buffer = buffer.duplicate();
    int length = buffer.remaining();
    while (length > 0) {
      if (!currentBlock.hasRemaining()) {
        appendBlock();
      }

      // 如果 buffer 内部的数据长度 超过了一个block 那么可能还要继续创建 block 所以是一个循环
      int chunk = Math.min(currentBlock.remaining(), length);
      buffer.limit(buffer.position() + chunk);
      currentBlock.put(buffer);

      length -= chunk;
    }
  }

  /**
   * Return a list of read-only view of {@link ByteBuffer} blocks over the 
   * current content written to the output.
   */
  public ArrayList<ByteBuffer> toBufferList() {
    ArrayList<ByteBuffer> result = new ArrayList<>(Math.max(blocks.size(), 1));
    if (blocks.isEmpty()) {
      result.add(EMPTY);
    } else {
      for (ByteBuffer bb : blocks) {
        bb = bb.asReadOnlyBuffer().flip();
        result.add(bb);
      }
    }
    return result;
  }

  /**
   * Returns a list of writeable blocks over the (source) content buffers.
   * 
   * This method returns the raw content of source buffers that may change over the lifetime 
   * of this object (blocks can be recycled or discarded, for example). Most applications 
   * should favor calling {@link #toBufferList()} which returns a read-only <i>view</i> over 
   * the content of the source buffers.
   * 
   * The difference between {@link #toBufferList()} and {@link #toWriteableBufferList()} is that
   * read-only view of source buffers will always return {@code false} from {@link ByteBuffer#hasArray()}
   * (which sometimes may be required to avoid double copying).
   * 创建一份拷贝对象 它们共享同一个 bytBuffer
   */
  public ArrayList<ByteBuffer> toWriteableBufferList() {
    ArrayList<ByteBuffer> result = new ArrayList<>(Math.max(blocks.size(), 1));
    if (blocks.isEmpty()) {
      result.add(EMPTY);
    } else {
      for (ByteBuffer bb : blocks) {
        bb = bb.duplicate().flip();
        result.add(bb);
      }
    }
    return result;
  }

  /**
   * Return a {@link ByteBuffersDataInput} for the set of current buffers ({@link #toBufferList()}). 
   */
  public ByteBuffersDataInput toDataInput() {
    return new ByteBuffersDataInput(toBufferList());
  }

  /**
   * Return a contiguous array with the current content written to the output. The returned
   * array is always a copy (can be mutated).
   *
   * If the {@link #size()} of the underlying buffers exceeds maximum size of Java array, an
   * {@link RuntimeException} will be thrown.
   */
  public byte[] toArrayCopy() {
    if (blocks.size() == 0) {
      return EMPTY_BYTE_ARRAY;
    }

    // We could try to detect single-block, array-based ByteBuffer here
    // and use Arrays.copyOfRange, but I don't think it's worth the extra
    // instance checks.
    long size = size();
    if (size > Integer.MAX_VALUE) {
      throw new RuntimeException("Data exceeds maximum size of a single byte array: " + size);
    }

    byte [] arr = new byte[Math.toIntExact(size())];
    int offset = 0;
    for (ByteBuffer bb : toBufferList()) {
      int len = bb.remaining();
      bb.get(arr, offset, len);
      offset += len;
    }
    return arr;
  }  

  /**
   * Copy the current content of this object into another {@link DataOutput}.
   */
  public void copyTo(DataOutput output) throws IOException {
    for (ByteBuffer bb : blocks) {
      if (bb.hasArray()) {
        output.writeBytes(bb.array(), bb.arrayOffset(), bb.position());
      } else {
        bb = bb.asReadOnlyBuffer().flip();
        // 如果 BB 是基于对外内存的  那么创建一个 输入流对象负责读取数据 并将数据转移到 output中
        output.copyBytes(new ByteBuffersDataInput(Collections.singletonList(bb)), bb.remaining());
      }
    }
  }

  /**
   * @return The number of bytes written to this output so far.
   * 返回所有 block的总长
   */
  public long size() {
    long size = 0;
    int blockCount = blocks.size();
    if (blockCount >= 1) {
      long fullBlockSize = (blockCount - 1L) * blockSize();
      long lastBlockSize = blocks.getLast().position();
      size = fullBlockSize + lastBlockSize;
    }
    return size;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT,
        "%,d bytes, block size: %,d, blocks: %,d",
        size(),
        blockSize(),
        blocks.size());
  }

  // Specialized versions of writeXXX methods that break execution into
  // fast/ slow path if the result would fall on the current block's 
  // boundary.
  // 
  // We also remove the IOException from methods because it (theoretically)
  // cannot be thrown from byte buffers.

  // 下面都是同样的思路

  /**
   * 如果无法直接写入 short 通过super.writeShort 会间接触发 writeByte 这样就会在内存不足时 尝试创建新的block
   * @param v
   */
  @Override
  public void writeShort(short v) {
    try {
      if (currentBlock.remaining() >= Short.BYTES) {
        currentBlock.putShort(v);
      } else {
        super.writeShort(v);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void writeInt(int v) {
    try {
      if (currentBlock.remaining() >= Integer.BYTES) {
        currentBlock.putInt(v);
      } else {
        super.writeInt(v);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }    
  }

  @Override
  public void writeLong(long v) {
    try {
      if (currentBlock.remaining() >= Long.BYTES) {
        currentBlock.putLong(v);
      } else {
        super.writeLong(v);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }    
  }

  /**
   * 如果写入的是字符串类型
   * @param v
   */
  @Override
  public void writeString(String v) {
    try {
      // 每一个窗口 最多由 多少 char组成
      final int MAX_CHARS_PER_WINDOW = 1024;
      // 代表本次写入长度小于一个窗口
      if (v.length() <= MAX_CHARS_PER_WINDOW) {
        final BytesRef utf8 = new BytesRef(v);
        writeVInt(utf8.length);
        writeBytes(utf8.bytes, utf8.offset, utf8.length);
      } else {
        writeVInt(UnicodeUtil.calcUTF16toUTF8Length(v, 0, v.length()));
        final byte [] buf = new byte [UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * MAX_CHARS_PER_WINDOW];
        UTF16toUTF8(v, 0, v.length(), buf, (len) -> {
          writeBytes(buf, 0, len);
        });
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }    
  }
  
  @Override
  public void writeMapOfStrings(Map<String, String> map) {
    try {
      super.writeMapOfStrings(map);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
  
  @Override
  public void writeSetOfStrings(Set<String> set) {
    try {
      super.writeSetOfStrings(set);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }      
  }

  @Override
  public long ramBytesUsed() {
    // Return a rough estimation for allocated blocks. Note that we do not make
    // any special distinction for direct memory buffers.
    return RamUsageEstimator.NUM_BYTES_OBJECT_REF * blocks.size() + 
           blocks.stream().mapToLong(buf -> buf.capacity()).sum();
  }

  /**
   * This method resets this object to a clean (zero-size) state and
   * publishes any currently allocated buffers for reuse to the reuse strategy
   * provided in the constructor.
   * 
   * Sharing byte buffers for reads and writes is dangerous and will very likely
   * lead to hard-to-debug issues, use with great care.
   */
  public void reset() {
    if (blockReuse != NO_REUSE) {
      blocks.forEach(blockReuse);
    }
    blocks.clear();
    currentBlock = EMPTY;
  }

  /**
   * @return Returns a new {@link ByteBuffersDataOutput} with the {@link #reset()} capability. 
   */
  // TODO: perhaps we can move it out to an utility class (as a supplier of preconfigured instances?) 
  public static ByteBuffersDataOutput newResettableInstance() {
    ByteBuffersDataOutput.ByteBufferRecycler reuser = new ByteBuffersDataOutput.ByteBufferRecycler(
        ByteBuffersDataOutput.ALLOCATE_BB_ON_HEAP); 
    return new ByteBuffersDataOutput(
        ByteBuffersDataOutput.DEFAULT_MIN_BITS_PER_BLOCK,
        ByteBuffersDataOutput.DEFAULT_MAX_BITS_PER_BLOCK,
        reuser::allocate,
        reuser::reuse);
  }

  private int blockSize() {
    return 1 << blockBits;
  }

  /**
   * 创建一个新的 block
   */
  private void appendBlock() {
    // 当block数量已经超过一个阈值时 先判断 还有没有空闲的 位可以写入
    if (blocks.size() >= MAX_BLOCKS_BEFORE_BLOCK_EXPANSION && blockBits < maxBitsPerBlock) {
      rewriteToBlockSize(blockBits + 1);
      if (blocks.getLast().hasRemaining()) {
        return;
      }
    }

    // 这里是正常情况  创建一个新的block  实际上是没有block数量限制的
    final int requiredBlockSize = 1 << blockBits;
    currentBlock = blockAllocate.apply(requiredBlockSize);
    assert currentBlock.capacity() == requiredBlockSize;
    blocks.add(currentBlock);
  }

  /**
   * 将原先的数据填充到一个新的size的 block中
   * @param targetBlockBits
   */
  private void rewriteToBlockSize(int targetBlockBits) {
    assert targetBlockBits <= maxBitsPerBlock;

    // We copy over data blocks to an output with one-larger block bit size.
    // We also discard references to blocks as we're copying to allow GC to
    // clean up partial results in case of memory pressure.
    // 以一个新的位数大小来创建 输出流
    ByteBuffersDataOutput cloned = new ByteBuffersDataOutput(targetBlockBits, targetBlockBits, blockAllocate, NO_REUSE);
    ByteBuffer block;
    // 将当前所有 block的数据 都转移到 cloned 中
    while ((block = blocks.pollFirst()) != null) {
      block.flip();
      cloned.writeBytes(block);
      if (blockReuse != NO_REUSE) {
        blockReuse.accept(block);
      }
    }

    assert blocks.isEmpty();
    this.blockBits = targetBlockBits;
    blocks.addAll(cloned.blocks);
  }

  /**
   * 根据给与的 byte 计算每个 块使用多少bit
   * @param bytes
   * @return
   */
  private static int computeBlockSizeBitsFor(long bytes) {
    long powerOfTwo = BitUtil.nextHighestPowerOfTwo(bytes / MAX_BLOCKS_BEFORE_BLOCK_EXPANSION);
    if (powerOfTwo == 0) {
      return DEFAULT_MIN_BITS_PER_BLOCK;
    }
    
    int blockBits = Long.numberOfTrailingZeros(powerOfTwo);
    blockBits = Math.min(blockBits, DEFAULT_MAX_BITS_PER_BLOCK);
    blockBits = Math.max(blockBits, DEFAULT_MIN_BITS_PER_BLOCK);
    return blockBits;
  }

  // TODO: move this block-based conversion to UnicodeUtil.
  
  private static final long HALF_SHIFT = 10;
  private static final int SURROGATE_OFFSET = 
      Character.MIN_SUPPLEMENTARY_CODE_POINT - 
      (UnicodeUtil.UNI_SUR_HIGH_START << HALF_SHIFT) - UnicodeUtil.UNI_SUR_LOW_START;

  /**
   * A consumer-based UTF16-UTF8 encoder (writes the input string in smaller buffers.).
   */
  private static int UTF16toUTF8(final CharSequence s, 
                                 final int offset,
                                 final int length,
                                 byte[] buf,
                                 IntConsumer bufferFlusher) {
    int utf8Len = 0;
    int j = 0;    
    for (int i = offset, end = offset + length; i < end; i++) {
      final int chr = (int) s.charAt(i);

      if (j + 4 >= buf.length) {
        bufferFlusher.accept(j);
        utf8Len += j;
        j = 0;
      }

      if (chr < 0x80)
        buf[j++] = (byte) chr;
      else if (chr < 0x800) {
        buf[j++] = (byte) (0xC0 | (chr >> 6));
        buf[j++] = (byte) (0x80 | (chr & 0x3F));
      } else if (chr < 0xD800 || chr > 0xDFFF) {
        buf[j++] = (byte) (0xE0 | (chr >> 12));
        buf[j++] = (byte) (0x80 | ((chr >> 6) & 0x3F));
        buf[j++] = (byte) (0x80 | (chr & 0x3F));
      } else {
        // A surrogate pair. Confirm valid high surrogate.
        if (chr < 0xDC00 && (i < end - 1)) {
          int utf32 = (int) s.charAt(i + 1);
          // Confirm valid low surrogate and write pair.
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = (chr << 10) + utf32 + SURROGATE_OFFSET;
            i++;
            buf[j++] = (byte) (0xF0 | (utf32 >> 18));
            buf[j++] = (byte) (0x80 | ((utf32 >> 12) & 0x3F));
            buf[j++] = (byte) (0x80 | ((utf32 >> 6) & 0x3F));
            buf[j++] = (byte) (0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // Replace unpaired surrogate or out-of-order low surrogate
        // with substitution character.
        buf[j++] = (byte) 0xEF;
        buf[j++] = (byte) 0xBF;
        buf[j++] = (byte) 0xBD;
      }
    }

    bufferFlusher.accept(j);
    utf8Len += j;

    return utf8Len;
  }
}
