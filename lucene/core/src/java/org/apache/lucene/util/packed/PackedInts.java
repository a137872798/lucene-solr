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


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Simplistic compression for array of unsigned long values.
 * Each value is {@code >= 0} and {@code <=} a specified maximum value.  The
 * values are stored as packed ints, with each value
 * consuming a fixed number of bits.
 *
 * @lucene.internal
 * 用于压缩 无符号的long值
 * 整齐的整数
 * 也就是每个值 都使用相同的bits
 */
public class PackedInts {

  /**
   * At most 700% memory overhead, always select a direct implementation.
   * 使用700%的内存开销 作为默认选择
   */
  public static final float FASTEST = 7f;

  /**
   * At most 50% memory overhead, always select a reasonably fast implementation.
   */
  public static final float FAST = 0.5f;

  /**
   * At most 25% memory overhead.
   */
  public static final float DEFAULT = 0.25f;

  /**
   * No memory overhead at all, but the returned implementation may be slow.
   * 不适用额外内存 但是可能很慢
   */
  public static final float COMPACT = 0f;

  /**
   * Default amount of memory to use for bulk operations.
   */
  public static final int DEFAULT_BUFFER_SIZE = 1024; // 1K

  /**
   * 编码名称为  整齐的整数
   */
  public final static String CODEC_NAME = "PackedInts";
  /**
   * 代表一种单调的趋势  没有出现锯齿状
   */
  public static final int VERSION_MONOTONIC_WITHOUT_ZIGZAG = 2;
  // 初始状态 和当前版本号 都是2
  public final static int VERSION_START = VERSION_MONOTONIC_WITHOUT_ZIGZAG;
  public final static int VERSION_CURRENT = VERSION_MONOTONIC_WITHOUT_ZIGZAG;

  /**
   * Check the validity of a version number.
   * 默认情况下 只允许版本号 2
   */
  public static void checkVersion(int version) {
    if (version < VERSION_START) {
      throw new IllegalArgumentException("Version is too old, should be at least " + VERSION_START + " (got " + version + ")");
    } else if (version > VERSION_CURRENT) {
      throw new IllegalArgumentException("Version is too new, should be at most " + VERSION_CURRENT + " (got " + version + ")");
    }
  }

  /**
   * A format to write packed ints.
   * 代表采用哪种格式写入
   *
   * @lucene.internal
   */
  public enum Format {
    /**
     * Compact format, all bits are written contiguously.
     * 代表紧凑模式    数据写入时本身会以block为单位  而一个block代表64bit  如果在紧凑模式下 某个值可能会出现跨block
     * 但是 确保不会出现跨block的 PackedSingleBlock 已经被弃用了
     */
    PACKED(0) {
      /**
       * 代表写入期望的值 需要消耗多少 byte
       * @param packedIntsVersion
       * @param valueCount   数值的数量
       * @param bitsPerValue   每个值会使用多少byte      一个byte = 8 bit 所以这里要/8
       * @return
       */
      @Override
      public long byteCount(int packedIntsVersion, int valueCount, int bitsPerValue) {
        return (long) Math.ceil((double) valueCount * bitsPerValue / 8);      
      }

    },

    /**
     * A format that may insert padding bits to improve encoding and decoding
     * speed. Since this format doesn't support all possible bits per value, you
     * should never use it directly, but rather use
     * {@link PackedInts#fastestFormatAndBits(int, int, float)} to find the
     * format that best suits your needs.
     * @deprecated Use {@link #PACKED} instead.
     * 目前仅支持 整齐模式
     */
    @Deprecated
    PACKED_SINGLE_BLOCK(1) {

      @Override
      public int longCount(int packedIntsVersion, int valueCount, int bitsPerValue) {
        final int valuesPerBlock = 64 / bitsPerValue;
        return (int) Math.ceil((double) valueCount / valuesPerBlock);
      }

      @Override
      public boolean isSupported(int bitsPerValue) {
        return Packed64SingleBlock.isSupported(bitsPerValue);
      }

      @Override
      public float overheadPerValue(int bitsPerValue) {
        assert isSupported(bitsPerValue);
        final int valuesPerBlock = 64 / bitsPerValue;
        final int overhead = 64 % bitsPerValue;
        return (float) overhead / valuesPerBlock;
      }

    };

    /**
     * Get a format according to its ID.
     * 根据 id 找到对应的格式化对象
     */
    public static Format byId(int id) {
      for (Format format : Format.values()) {
        if (format.getId() == id) {
          return format;
        }
      }
      throw new IllegalArgumentException("Unknown format id: " + id);
    }

    private Format(int id) {
      this.id = id;
    }

    public int id;

    /**
     * Returns the ID of the format.
     */
    public int getId() {
      return id;
    }

    /**
     * Computes how many byte blocks are needed to store <code>values</code>
     * values of size <code>bitsPerValue</code>.
     * 因为  非整齐模式已经被弃用了  可以认为该方法就是通过 整齐模式那个对象来触发的
     */
    public long byteCount(int packedIntsVersion, int valueCount, int bitsPerValue) {
      assert bitsPerValue >= 0 && bitsPerValue <= 64 : bitsPerValue;
      // assume long-aligned
      return 8L * longCount(packedIntsVersion, valueCount, bitsPerValue);
    }

    /**
     * Computes how many long blocks are needed to store <code>values</code>
     * values of size <code>bitsPerValue</code>.
     * 计算传入的一系列值 一共需要多少bit来存储
     */
    public int longCount(int packedIntsVersion, int valueCount, int bitsPerValue) {
      assert bitsPerValue >= 0 && bitsPerValue <= 64 : bitsPerValue;
      // 这里 得到的是写入 valueCount  总计需要消耗多少个 byte
      final long byteCount = byteCount(packedIntsVersion, valueCount, bitsPerValue);
      assert byteCount < 8L * Integer.MAX_VALUE;
      // 如果是8的倍数 直接 /8
      if ((byteCount % 8) == 0) {
        return (int) (byteCount / 8);
      } else {
        // 向上进1 确保空间足够
        return (int) (byteCount / 8 + 1);
      }
    }

    /**
     * Tests whether the provided number of bits per value is supported by the
     * format.
     * 每个值所使用的 bit 必须在 1-64 之内  假设使用最大的类型 也就是long  8byte  也就是 64bit
     */
    public boolean isSupported(int bitsPerValue) {
      return bitsPerValue >= 1 && bitsPerValue <= 64;
    }

    /**
     * Returns the overhead per value, in bits.
     */
    public float overheadPerValue(int bitsPerValue) {
      assert isSupported(bitsPerValue);
      return 0f;
    }

    /**
     * Returns the overhead ratio (<code>overhead per value / bits per value</code>).
     */
    public final float overheadRatio(int bitsPerValue) {
      assert isSupported(bitsPerValue);
      return overheadPerValue(bitsPerValue) / bitsPerValue;
    }
  }

  /**
   * Simple class that holds a format and a number of bits per value.
   * 该对象包含了使用的格式 以及 每个值消耗多少bit
   */
  public static class FormatAndBits {
    public final Format format;
    public final int bitsPerValue;
    public FormatAndBits(Format format, int bitsPerValue) {
      this.format = format;
      this.bitsPerValue = bitsPerValue;
    }

    @Override
    public String toString() {
      return "FormatAndBits(format=" + format + " bitsPerValue=" + bitsPerValue + ")";
    }
  }

  /**
   * Try to find the {@link Format} and number of bits per value that would
   * restore from disk the fastest reader whose overhead is less than
   * <code>acceptableOverheadRatio</code>.
   * <p>
   * The <code>acceptableOverheadRatio</code> parameter makes sense for
   * random-access {@link Reader}s. In case you only plan to perform
   * sequential access on this stream later on, you should probably use
   * {@link PackedInts#COMPACT}.
   * <p>
   * If you don't know how many values you are going to write, use
   * <code>valueCount = -1</code>.
   * 根据 传入的 value 和 每个value使用的bit  以及允许的开销比率 生成符合条件的 FormatAndBits
   */
  public static FormatAndBits fastestFormatAndBits(int valueCount, int bitsPerValue, float acceptableOverheadRatio) {
    if (valueCount == -1) {
      valueCount = Integer.MAX_VALUE;
    }

    // 避免数据越界
    acceptableOverheadRatio = Math.max(COMPACT, acceptableOverheadRatio);
    acceptableOverheadRatio = Math.min(FASTEST, acceptableOverheadRatio);
    // 允许采用的额外开销值   如果采用 FASTEST 也就是 多使用700% 加上原本的 100% 实际上就是 800% 而一般情况下按照bit来拆解也就是变成 1/8
    // 这样相当于不做任何的编码
    float acceptableOverheadPerValue = acceptableOverheadRatio * bitsPerValue; // in bits

    // 计算最大开销   分为原始开销和 额外开销
    int maxBitsPerValue = bitsPerValue + (int) acceptableOverheadPerValue;

    int actualBitsPerValue = -1;

    // rounded number of bits per value are usually the fastest
    // 这里其实还是局限了  额外开销
    if (bitsPerValue <= 8 && maxBitsPerValue >= 8) {
      actualBitsPerValue = 8;
    } else if (bitsPerValue <= 16 && maxBitsPerValue >= 16) {
      actualBitsPerValue = 16;
    } else if (bitsPerValue <= 32 && maxBitsPerValue >= 32) {
      actualBitsPerValue = 32;
    } else if (bitsPerValue <= 64 && maxBitsPerValue >= 64) {
      actualBitsPerValue = 64;
    } else {
      actualBitsPerValue = bitsPerValue;
    }

    return new FormatAndBits(Format.PACKED, actualBitsPerValue);
  }

  /**
   * A decoder for packed integers.
   * 为紧凑的integers 解码
   */
  public static interface Decoder {

    /**
     * The minimum number of long blocks to encode in a single iteration, when
     * using long encoding.
     */
    int longBlockCount();

    /**
     * The number of values that can be stored in {@link #longBlockCount()} long
     * blocks.
     */
    int longValueCount();

    /**
     * The minimum number of byte blocks to encode in a single iteration, when
     * using byte encoding.
     */
    int byteBlockCount();

    /**
     * The number of values that can be stored in {@link #byteBlockCount()} byte
     * blocks.
     */
    int byteValueCount();

    /**
     * Read <code>iterations * blockCount()</code> blocks from <code>blocks</code>,
     * decode them and write <code>iterations * valueCount()</code> values into
     * <code>values</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start reading blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start writing values
     * @param iterations   controls how much data to decode
     */
    void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations);

    /**
     * Read <code>8 * iterations * blockCount()</code> blocks from <code>blocks</code>,
     * decode them and write <code>iterations * valueCount()</code> values into
     * <code>values</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start reading blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start writing values
     * @param iterations   controls how much data to decode
     */
    void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations);

    /**
     * Read <code>iterations * blockCount()</code> blocks from <code>blocks</code>,
     * decode them and write <code>iterations * valueCount()</code> values into
     * <code>values</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start reading blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start writing values
     * @param iterations   controls how much data to decode
     */
    void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations);

    /**
     * Read <code>8 * iterations * blockCount()</code> blocks from <code>blocks</code>,
     * decode them and write <code>iterations * valueCount()</code> values into
     * <code>values</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start reading blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start writing values
     * @param iterations   controls how much data to decode
     */
    void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations);

  }

  /**
   * An encoder for packed integers.
   */
  public static interface Encoder {

    /**
     * The minimum number of long blocks to encode in a single iteration, when
     * using long encoding.
     */
    int longBlockCount();

    /**
     * The number of values that can be stored in {@link #longBlockCount()} long
     * blocks.
     */
    int longValueCount();

    /**
     * The minimum number of byte blocks to encode in a single iteration, when
     * using byte encoding.
     */
    int byteBlockCount();

    /**
     * The number of values that can be stored in {@link #byteBlockCount()} byte
     * blocks.
     */
    int byteValueCount();

    /**
     * Read <code>iterations * valueCount()</code> values from <code>values</code>,
     * encode them and write <code>iterations * blockCount()</code> blocks into
     * <code>blocks</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start writing blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start reading values
     * @param iterations   controls how much data to encode
     */
    void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations);

    /**
     * Read <code>iterations * valueCount()</code> values from <code>values</code>,
     * encode them and write <code>8 * iterations * blockCount()</code> blocks into
     * <code>blocks</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start writing blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start reading values
     * @param iterations   controls how much data to encode
     */
    void encode(long[] values, int valuesOffset, byte[] blocks, int blocksOffset, int iterations);

    /**
     * Read <code>iterations * valueCount()</code> values from <code>values</code>,
     * encode them and write <code>iterations * blockCount()</code> blocks into
     * <code>blocks</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start writing blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start reading values
     * @param iterations   controls how much data to encode
     */
    void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations);

    /**
     * Read <code>iterations * valueCount()</code> values from <code>values</code>,
     * encode them and write <code>8 * iterations * blockCount()</code> blocks into
     * <code>blocks</code>.
     *
     * @param blocks       the long blocks that hold packed integer values
     * @param blocksOffset the offset where to start writing blocks
     * @param values       the values buffer
     * @param valuesOffset the offset where to start reading values
     * @param iterations   controls how much data to encode
     */
    void encode(int[] values, int valuesOffset, byte[] blocks, int blocksOffset, int iterations);

  }

  /**
   * A read-only random access array of positive integers.
   * @lucene.internal
   * 该对象 仅负责从RAM 中读取数据  注意读取的数据类型是 long
   */
  public static abstract class Reader implements Accountable {

    /** Get the long at the given index. Behavior is undefined for out-of-range indices. */
    // 通过传入一个下标 返回对应的long 值
    public abstract long get(int index);

    /**
     * Bulk get: read at least one and at most <code>len</code> longs starting
     * from <code>index</code> into <code>arr[off:off+len]</code> and return
     * the actual number of values that have been read.
     * 大块的读取 大批量
     */
    public int get(int index, long[] arr, int off, int len) {
      assert len > 0 : "len must be > 0 (got " + len + ")";
      assert index >= 0 && index < size();
      assert off + len <= arr.length;

      // 简单的将数据读取到 long[] 中
      final int gets = Math.min(size() - index, len);
      for (int i = index, o = off, end = index + gets; i < end; ++i, ++o) {
        arr[o] = get(i);
      }
      return gets;
    }

    /**
     * @return the number of values.
     * 返回这组数据的长度
     */
    public abstract int size();
  }

  /**
   * Run-once iterator interface, to decode previously saved PackedInts.
   * 只允许单次访问的迭代器
   */
  public static interface ReaderIterator {
    /** Returns next value */
    long next() throws IOException;
    /** Returns at least 1 and at most <code>count</code> next values,
     * the returned ref MUST NOT be modified */
    // 代表往下读取多少数据 count 最少为1  所以结果用long[] 来填装
    LongsRef next(int count) throws IOException;
    /** Returns number of bits per value */
    // 内部每个值占用多少bit
    int getBitsPerValue();
    /** Returns number of values */
    // 返回内部有多少个元素
    int size();
    /** Returns the current position */
    // 返回当前迭代到的光标
    int ord();
  }

  /**
   * 读取数据的迭代器
   */
  static abstract class ReaderIteratorImpl implements ReaderIterator {

    /**
     * 简单的看作一个输入流 可以从内部读取数据
     */
    protected final DataInput in;
    /**
     * 读取的每个值会占用多少bit
     */
    protected final int bitsPerValue;
    /**
     * 该对象一共有多少个value
     */
    protected final int valueCount;

    protected ReaderIteratorImpl(int valueCount, int bitsPerValue, DataInput in) {
      this.in = in;
      this.bitsPerValue = bitsPerValue;
      this.valueCount = valueCount;
    }

    @Override
    public long next() throws IOException {
      // 也就是实际的读取逻辑是由子类完成 应该就是从 dataInput中读取
      LongsRef nextValues = next(1);
      assert nextValues.length > 0;
      // nextValues.offset 代表当前 long[] 读取到哪个位置
      final long result = nextValues.longs[nextValues.offset];
      // 因为发生了读取动作 所以 offset +1
      ++nextValues.offset;
      // 减少对应的值
      --nextValues.length;
      return result;
    }

    @Override
    public int getBitsPerValue() {
      return bitsPerValue;
    }

    @Override
    public int size() {
      return valueCount;
    }
  }

  /**
   * A packed integer array that can be modified.
   * @lucene.internal
   * 代表一个整齐的 int数组 并且是可以改变的
   */
  public static abstract class Mutable extends Reader {

    /**
     * @return the number of bits used to store any given value.
     *         Note: This does not imply that memory usage is
     *         {@code bitsPerValue * #values} as implementations are free to
     *         use non-space-optimal packing of bits.
     */
    public abstract int getBitsPerValue();

    /**
     * Set the value at the given index in the array.
     * @param index where the value should be positioned.
     * @param value a value conforming to the constraints set by the array.
     *              这也就是可变性的体现 通过index定位到数据后 可以替换
     */
    public abstract void set(int index, long value);

    /**
     * Bulk set: set at least one and at most <code>len</code> longs starting
     * at <code>off</code> in <code>arr</code> into this mutable, starting at
     * <code>index</code>. Returns the actual number of values that have been
     * set.
     */
    public int set(int index, long[] arr, int off, int len) {
      assert len > 0 : "len must be > 0 (got " + len + ")";
      assert index >= 0 && index < size();
      len = Math.min(len, size() - index);
      assert off + len <= arr.length;

      for (int i = index, o = off, end = index + len; i < end; ++i, ++o) {
        set(i, arr[o]);
      }
      return len;
    }

    /**
     * Fill the mutable from <code>fromIndex</code> (inclusive) to
     * <code>toIndex</code> (exclusive) with <code>val</code>.
     * 使用指定的值填满容器
     */
    public void fill(int fromIndex, int toIndex, long val) {
      assert val <= maxValue(getBitsPerValue());
      assert fromIndex <= toIndex;
      for (int i = fromIndex; i < toIndex; ++i) {
        set(i, val);
      }
    }

    /**
     * Sets all values to 0.
     * 将所有值都重置成0
     */
    public void clear() {
      fill(0, size(), 0);
    }

    /**
     * Save this mutable into <code>out</code>. Instantiating a reader from
     * the generated data will return a reader with the same number of bits
     * per value.
     * 将内部数据转移到 dataOutput中
     * 在 lucene中 很多代表数据的存储对象都携带了  bitsPerValue属性
     */
    public void save(DataOutput out) throws IOException {
      // 先包装成一个 writer对象
      Writer writer = getWriterNoHeader(out, getFormat(), size(), getBitsPerValue(), DEFAULT_BUFFER_SIZE);
      writer.writeHeader();
      // 将所有数据写入
      for (int i = 0; i < size(); ++i) {
        writer.add(get(i));
      }
      // 刷盘/持久化
      writer.finish();
    }

    /** The underlying format. */
    // 使用整齐整数的形式
    Format getFormat() {
      return Format.PACKED;
    }

  }

  /**
   * A simple base for Readers that keeps track of valueCount and bitsPerValue.
   * @lucene.internal
   */
  static abstract class ReaderImpl extends Reader {
    protected final int valueCount;

    protected ReaderImpl(int valueCount) {
      this.valueCount = valueCount;
    }

    @Override
    public abstract long get(int index);

    @Override
    public final int size() {
      return valueCount;
    }
  }

  /**
   * 代表一个内部值可以改变的int[]
   */
  static abstract class MutableImpl extends Mutable {

    protected final int valueCount;
    protected final int bitsPerValue;

    protected MutableImpl(int valueCount, int bitsPerValue) {
      this.valueCount = valueCount;
      assert bitsPerValue > 0 && bitsPerValue <= 64 : "bitsPerValue=" + bitsPerValue;
      this.bitsPerValue = bitsPerValue;
    }

    @Override
    public final int getBitsPerValue() {
      return bitsPerValue;
    }

    @Override
    public final int size() {
      return valueCount;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(valueCount=" + valueCount + ",bitsPerValue=" + bitsPerValue + ")";
    }
  }

  /** A {@link Reader} which has all its values equal to 0 (bitsPerValue = 0). */
  // 该对象代表内部long[] 的值都是0
  public static final class NullReader extends Reader {

    private final int valueCount;

    /** Sole constructor. */
    public NullReader(int valueCount) {
      this.valueCount = valueCount;
    }

    @Override
    public long get(int index) {
      return 0;
    }

    @Override
    public int get(int index, long[] arr, int off, int len) {
      assert len > 0 : "len must be > 0 (got " + len + ")";
      assert index >= 0 && index < valueCount;
      len = Math.min(len, valueCount - index);
      Arrays.fill(arr, off, off + len, 0);
      return len;
    }

    @Override
    public int size() {
      return valueCount;
    }

      /**
       * 预估本对象会使用的byte数 也就是在对象头的基础上增加一个 int字段的长度
       * @return
       */
    @Override
    public long ramBytesUsed() {
      return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES);
    }
  }

  /** A write-once Writer.
   * @lucene.internal
   * 用于写入数据的 对象 只能写入一次
   */
  public static abstract class Writer {
    /**
     * 实际上是操纵该对象  writer只是隐藏了内部细节
     */
    protected final DataOutput out;
    protected final int valueCount;
    protected final int bitsPerValue;

    protected Writer(DataOutput out, int valueCount, int bitsPerValue) {
      assert bitsPerValue <= 64;
      assert valueCount >= 0 || valueCount == -1;
      this.out = out;
      this.valueCount = valueCount;
      this.bitsPerValue = bitsPerValue;
    }

    void writeHeader() throws IOException {
      assert valueCount != -1;
      CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
      out.writeVInt(bitsPerValue);
      out.writeVInt(valueCount);
      out.writeVInt(getFormat().getId());
    }

    /** The format used to serialize values. */
    protected abstract PackedInts.Format getFormat();

    /** Add a value to the stream. */
    public abstract void add(long v) throws IOException;

    /** The number of bits per value. */
    public final int bitsPerValue() {
      return bitsPerValue;
    }

    /** Perform end-of-stream operations. */
    public abstract void finish() throws IOException;

    /**
     * Returns the current ord in the stream (number of values that have been
     * written so far minus one).
     */
    public abstract int ord();
  }

  /**
   * Get a {@link Decoder}.
   *
   * @param format         the format used to store packed ints
   * @param version        the compatibility version
   * @param bitsPerValue   the number of bits per value
   * @return a decoder
   */
  public static Decoder getDecoder(Format format, int version, int bitsPerValue) {
    checkVersion(version);
    return BulkOperation.of(format, bitsPerValue);
  }

  /**
   * Get an {@link Encoder}.
   *
   * @param format         the format used to store packed ints
   * @param version        the compatibility version
   * @param bitsPerValue   the number of bits per value
   * @return an encoder
   */
  public static Encoder getEncoder(Format format, int version, int bitsPerValue) {
    checkVersion(version);
    return BulkOperation.of(format, bitsPerValue);
  }

  /**
   * Expert: Restore a {@link Reader} from a stream without reading metadata at
   * the beginning of the stream. This method is useful to restore data from
   * streams which have been created using
   * {@link PackedInts#getWriterNoHeader(DataOutput, Format, int, int, int)}.
   *
   * @param in           the stream to read data from, positioned at the beginning of the packed values
   * @param format       the format used to serialize
   * @param version      the version used to serialize the data
   * @param valueCount   how many values the stream holds
   * @param bitsPerValue the number of bits per value
   * @return             a Reader
   * @throws IOException If there is a low-level I/O error
   * @see PackedInts#getWriterNoHeader(DataOutput, Format, int, int, int)
   * @lucene.internal
   */
  public static Reader getReaderNoHeader(DataInput in, Format format, int version,
      int valueCount, int bitsPerValue) throws IOException {
    checkVersion(version);
    switch (format) {
      case PACKED_SINGLE_BLOCK:
        return Packed64SingleBlock.create(in, valueCount, bitsPerValue);
      case PACKED:
        return new Packed64(version, in, valueCount, bitsPerValue);
      default:
        throw new AssertionError("Unknown Writer format: " + format);
    }
  }

  /**
   * Restore a {@link Reader} from a stream.
   *
   * @param in           the stream to read data from
   * @return             a Reader
   * @throws IOException If there is a low-level I/O error
   * @lucene.internal
   */
  public static Reader getReader(DataInput in) throws IOException {
    final int version = CodecUtil.checkHeader(in, CODEC_NAME, VERSION_START, VERSION_CURRENT);
    final int bitsPerValue = in.readVInt();
    assert bitsPerValue > 0 && bitsPerValue <= 64: "bitsPerValue=" + bitsPerValue;
    final int valueCount = in.readVInt();
    final Format format = Format.byId(in.readVInt());

    return getReaderNoHeader(in, format, version, valueCount, bitsPerValue);
  }

  /**
   * Expert: Restore a {@link ReaderIterator} from a stream without reading
   * metadata at the beginning of the stream. This method is useful to restore
   * data from streams which have been created using
   * {@link PackedInts#getWriterNoHeader(DataOutput, Format, int, int, int)}.
   *
   * @param in           the stream to read data from, positioned at the beginning of the packed values
   * @param format       the format used to serialize
   * @param version      the version used to serialize the data
   * @param valueCount   how many values the stream holds
   * @param bitsPerValue the number of bits per value
   * @param mem          how much memory the iterator is allowed to use to read-ahead (likely to speed up iteration)
   * @return             a ReaderIterator
   * @see PackedInts#getWriterNoHeader(DataOutput, Format, int, int, int)
   * @lucene.internal
   */
  public static ReaderIterator getReaderIteratorNoHeader(DataInput in, Format format, int version,
      int valueCount, int bitsPerValue, int mem) {
    checkVersion(version);
    return new PackedReaderIterator(format, version, valueCount, bitsPerValue, in, mem);
  }

  /**
   * Retrieve PackedInts as a {@link ReaderIterator}
   * @param in positioned at the beginning of a stored packed int structure.
   * @param mem how much memory the iterator is allowed to use to read-ahead (likely to speed up iteration)
   * @return an iterator to access the values
   * @throws IOException if the structure could not be retrieved.
   * @lucene.internal
   */
  public static ReaderIterator getReaderIterator(DataInput in, int mem) throws IOException {
    final int version = CodecUtil.checkHeader(in, CODEC_NAME, VERSION_START, VERSION_CURRENT);
    final int bitsPerValue = in.readVInt();
    assert bitsPerValue > 0 && bitsPerValue <= 64: "bitsPerValue=" + bitsPerValue;
    final int valueCount = in.readVInt();
    final Format format = Format.byId(in.readVInt());
    return getReaderIteratorNoHeader(in, format, version, valueCount, bitsPerValue, mem);
  }

  /**
   * Expert: Construct a direct {@link Reader} from a stream without reading
   * metadata at the beginning of the stream. This method is useful to restore
   * data from streams which have been created using
   * {@link PackedInts#getWriterNoHeader(DataOutput, Format, int, int, int)}.
   * <p>
   * The returned reader will have very little memory overhead, but every call
   * to {@link Reader#get(int)} is likely to perform a disk seek.
   *
   * @param in           the stream to read data from
   * @param format       the format used to serialize
   * @param version      the version used to serialize the data
   * @param valueCount   how many values the stream holds
   * @param bitsPerValue the number of bits per value
   * @return a direct Reader
   * @lucene.internal
   */
  public static Reader getDirectReaderNoHeader(final IndexInput in, Format format,
      int version, int valueCount, int bitsPerValue) {
    checkVersion(version);
    switch (format) {
      case PACKED:
        return new DirectPackedReader(bitsPerValue, valueCount, in);
      case PACKED_SINGLE_BLOCK:
        return new DirectPacked64SingleBlockReader(bitsPerValue, valueCount, in);
      default:
        throw new AssertionError("Unknown format: " + format);
    }
  }

  /**
   * Construct a direct {@link Reader} from an {@link IndexInput}. This method
   * is useful to restore data from streams which have been created using
   * {@link PackedInts#getWriter(DataOutput, int, int, float)}.
   * <p>
   * The returned reader will have very little memory overhead, but every call
   * to {@link Reader#get(int)} is likely to perform a disk seek.
   *
   * @param in           the stream to read data from
   * @return a direct Reader
   * @throws IOException If there is a low-level I/O error
   * @lucene.internal
   */
  public static Reader getDirectReader(IndexInput in) throws IOException {
    final int version = CodecUtil.checkHeader(in, CODEC_NAME, VERSION_START, VERSION_CURRENT);
    final int bitsPerValue = in.readVInt();
    assert bitsPerValue > 0 && bitsPerValue <= 64: "bitsPerValue=" + bitsPerValue;
    final int valueCount = in.readVInt();
    final Format format = Format.byId(in.readVInt());
    return getDirectReaderNoHeader(in, format, version, valueCount, bitsPerValue);
  }
  
  /**
   * Create a packed integer array with the given amount of values initialized
   * to 0. the valueCount and the bitsPerValue cannot be changed after creation.
   * All Mutables known by this factory are kept fully in RAM.
   * <p>
   * Positive values of <code>acceptableOverheadRatio</code> will trade space
   * for speed by selecting a faster but potentially less memory-efficient
   * implementation. An <code>acceptableOverheadRatio</code> of
   * {@link PackedInts#COMPACT} will make sure that the most memory-efficient
   * implementation is selected whereas {@link PackedInts#FASTEST} will make sure
   * that the fastest implementation is selected.
   *
   * @param valueCount   the number of elements   代表block中 元素的数量
   * @param bitsPerValue the number of bits available for any given value  每个值使用的bit数
   * @param acceptableOverheadRatio an acceptable overhead   每个值接收的开销比率
   *        ratio per value
   * @return a mutable packed integer array
   * @lucene.internal
   */
  public static Mutable getMutable(int valueCount,
      int bitsPerValue, float acceptableOverheadRatio) {
    // 生成一个存储格式对象 比如紧凑模式 Packed
    final FormatAndBits formatAndBits = fastestFormatAndBits(valueCount, bitsPerValue, acceptableOverheadRatio);
    return getMutable(valueCount, formatAndBits.bitsPerValue, formatAndBits.format);
  }

  /** Same as {@link #getMutable(int, int, float)} with a pre-computed number
   *  of bits per value and format.
   *  @lucene.internal */
  public static Mutable getMutable(int valueCount,
      int bitsPerValue, PackedInts.Format format) {
    assert valueCount >= 0;
    switch (format) {
      case PACKED_SINGLE_BLOCK:
        return Packed64SingleBlock.create(valueCount, bitsPerValue);
      case PACKED:
        return new Packed64(valueCount, bitsPerValue);
      default:
        throw new AssertionError();
    }
  }

  /**
   * Expert: Create a packed integer array writer for the given output, format,
   * value count, and number of bits per value.
   * <p>
   * The resulting stream will be long-aligned. This means that depending on
   * the format which is used, up to 63 bits will be wasted. An easy way to
   * make sure that no space is lost is to always use a <code>valueCount</code>
   * that is a multiple of 64.
   * <p>
   * This method does not write any metadata to the stream, meaning that it is
   * your responsibility to store it somewhere else in order to be able to
   * recover data from the stream later on:
   * <ul>
   *   <li><code>format</code> (using {@link Format#getId()}),</li>
   *   <li><code>valueCount</code>,</li>
   *   <li><code>bitsPerValue</code>,</li>
   *   <li>{@link #VERSION_CURRENT}.</li>
   * </ul>
   * <p>
   * It is possible to start writing values without knowing how many of them you
   * are actually going to write. To do this, just pass <code>-1</code> as
   * <code>valueCount</code>. On the other hand, for any positive value of
   * <code>valueCount</code>, the returned writer will make sure that you don't
   * write more values than expected and pad the end of stream with zeros in
   * case you have written less than <code>valueCount</code> when calling
   * {@link Writer#finish()}.
   * <p>
   * The <code>mem</code> parameter lets you control how much memory can be used
   * to buffer changes in memory before flushing to disk. High values of
   * <code>mem</code> are likely to improve throughput. On the other hand, if
   * speed is not that important to you, a value of <code>0</code> will use as
   * little memory as possible and should already offer reasonable throughput.
   *
   * @param out          the data output
   * @param format       the format to use to serialize the values
   * @param valueCount   the number of values
   * @param bitsPerValue the number of bits per value
   * @param mem          how much memory (in bytes) can be used to speed up serialization
   * @return             a Writer
   * @see PackedInts#getReaderIteratorNoHeader(DataInput, Format, int, int, int, int)
   * @see PackedInts#getReaderNoHeader(DataInput, Format, int, int, int)
   * @lucene.internal
   */
  public static Writer getWriterNoHeader(
      DataOutput out, Format format, int valueCount, int bitsPerValue, int mem) {
    return new PackedWriter(format, out, valueCount, bitsPerValue, mem);
  }

  /**
   * Create a packed integer array writer for the given output, format, value
   * count, and number of bits per value.
   * <p>
   * The resulting stream will be long-aligned. This means that depending on
   * the format which is used under the hoods, up to 63 bits will be wasted.
   * An easy way to make sure that no space is lost is to always use a
   * <code>valueCount</code> that is a multiple of 64.
   * <p>
   * This method writes metadata to the stream, so that the resulting stream is
   * sufficient to restore a {@link Reader} from it. You don't need to track
   * <code>valueCount</code> or <code>bitsPerValue</code> by yourself. In case
   * this is a problem, you should probably look at
   * {@link #getWriterNoHeader(DataOutput, Format, int, int, int)}.
   * <p>
   * The <code>acceptableOverheadRatio</code> parameter controls how
   * readers that will be restored from this stream trade space
   * for speed by selecting a faster but potentially less memory-efficient
   * implementation. An <code>acceptableOverheadRatio</code> of
   * {@link PackedInts#COMPACT} will make sure that the most memory-efficient
   * implementation is selected whereas {@link PackedInts#FASTEST} will make sure
   * that the fastest implementation is selected. In case you are only interested
   * in reading this stream sequentially later on, you should probably use
   * {@link PackedInts#COMPACT}.
   *
   * @param out          the data output
   * @param valueCount   the number of values
   * @param bitsPerValue the number of bits per value
   * @param acceptableOverheadRatio an acceptable overhead ratio per value
   * @return             a Writer
   * @throws IOException If there is a low-level I/O error
   * @lucene.internal
   */
  public static Writer getWriter(DataOutput out,
      int valueCount, int bitsPerValue, float acceptableOverheadRatio)
    throws IOException {
    assert valueCount >= 0;

    final FormatAndBits formatAndBits = fastestFormatAndBits(valueCount, bitsPerValue, acceptableOverheadRatio);
    final Writer writer = getWriterNoHeader(out, formatAndBits.format, valueCount, formatAndBits.bitsPerValue, DEFAULT_BUFFER_SIZE);
    writer.writeHeader();
    return writer;
  }

  /** Returns how many bits are required to hold values up
   *  to and including maxValue
   *  NOTE: This method returns at least 1.
   * @param maxValue the maximum value that should be representable.
   * @return the amount of bits needed to represent values from 0 to maxValue.
   * @lucene.internal
   * 估算某个long值需要至少多少bit来表达
   */
  public static int bitsRequired(long maxValue) {
    if (maxValue < 0) {
      throw new IllegalArgumentException("maxValue must be non-negative (got: " + maxValue + ")");
    }
    return unsignedBitsRequired(maxValue);
  }

  /** Returns how many bits are required to store <code>bits</code>,
   * interpreted as an unsigned value.
   * NOTE: This method returns at least 1.
   * @lucene.internal
   */
  public static int unsignedBitsRequired(long bits) {
    // Long.numberOfLeadingZeros 代表该值按二进制使用有多少个0
    return Math.max(1, 64 - Long.numberOfLeadingZeros(bits));
  }

  /**
   * Calculates the maximum unsigned long that can be expressed with the given
   * number of bits.
   * @param bitsPerValue the number of bits available for any given value.
   * @return the maximum value for the given bits.
   * @lucene.internal
   */
  public static long maxValue(int bitsPerValue) {
    return bitsPerValue == 64 ? Long.MAX_VALUE : ~(~0L << bitsPerValue);
  }

  /**
   * Copy <code>src[srcPos:srcPos+len]</code> into
   * <code>dest[destPos:destPos+len]</code> using at most <code>mem</code>
   * bytes.
   */
  public static void copy(Reader src, int srcPos, Mutable dest, int destPos, int len, int mem) {
    assert srcPos + len <= src.size();
    assert destPos + len <= dest.size();
    final int capacity = mem >>> 3;
    if (capacity == 0) {
      for (int i = 0; i < len; ++i) {
        dest.set(destPos++, src.get(srcPos++));
      }
    } else if (len > 0) {
      // use bulk operations
      final long[] buf = new long[Math.min(capacity, len)];
      copy(src, srcPos, dest, destPos, len, buf);
    }
  }

  /** Same as {@link #copy(Reader, int, Mutable, int, int, int)} but using a pre-allocated buffer. */
  static void copy(Reader src, int srcPos, Mutable dest, int destPos, int len, long[] buf) {
    assert buf.length > 0;
    int remaining = 0;
    while (len > 0) {
      final int read = src.get(srcPos, buf, remaining, Math.min(len, buf.length - remaining));
      assert read > 0;
      srcPos += read;
      len -= read;
      remaining += read;
      final int written = dest.set(destPos, buf, 0, remaining);
      assert written > 0;
      destPos += written;
      if (written < remaining) {
        System.arraycopy(buf, written, buf, 0, remaining - written);
      }
      remaining -= written;
    }
    while (remaining > 0) {
      final int written = dest.set(destPos, buf, 0, remaining);
      destPos += written;
      remaining -= written;
      System.arraycopy(buf, written, buf, 0, remaining);
    }
  }

  /** Check that the block size is a power of 2, in the right bounds, and return
   *  its log in base 2.
   *  确保使用的页大小是合法的 */
  static int checkBlockSize(int blockSize, int minBlockSize, int maxBlockSize) {
    if (blockSize < minBlockSize || blockSize > maxBlockSize) {
      throw new IllegalArgumentException("blockSize must be >= " + minBlockSize + " and <= " + maxBlockSize + ", got " + blockSize);
    }
    if ((blockSize & (blockSize - 1)) != 0) {
      throw new IllegalArgumentException("blockSize must be a power of two, got " + blockSize);
    }
    return Integer.numberOfTrailingZeros(blockSize);
  }

  /** Return the number of blocks required to store <code>size</code> values on
   *  <code>blockSize</code>. */
  static int numBlocks(long size, int blockSize) {
    final int numBlocks = (int) (size / blockSize) + (size % blockSize == 0 ? 0 : 1);
    if ((long) numBlocks * blockSize < size) {
      throw new IllegalArgumentException("size is too large for this block size");
    }
    return numBlocks;
  }

}
