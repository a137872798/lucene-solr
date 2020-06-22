/*
 * LZ4 Library
 * Copyright (c) 2011-2016, Yann Collet
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this
 *   list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.lucene.util.compress;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * LZ4 compression and decompression routines.
 *
 * https://github.com/lz4/lz4/tree/dev/lib
 * http://fastcompression.blogspot.fr/p/lz4.html
 *
 * The high-compression option is a simpler version of the one of the original
 * algorithm, and only retains a better hash table that remembers about more
 * occurrences of a previous 4-bytes sequence, and removes all the logic about
 * handling of the case when overlapping matches are found.
 * LZ4 压缩算法  一个是基于速度 一个是基于压缩率
 */
public final class LZ4 {

  private LZ4() {}

  static final int MEMORY_USAGE = 14;
  /**
   * 代表至少要4个相同的byte 才进行压缩
   */
  static final int MIN_MATCH = 4; // minimum length of a match
  /**
   * 值通过hash算法发生冲突后  允许2个值的最大差值
   */
  static final int MAX_DISTANCE = 1 << 16; // maximum distance of a reference
  /**
   * 没有重复 首次出现的字节流     当重复的字符串本身比较小时
   */
  static final int LAST_LITERALS = 5; // the last 5 bytes must be encoded as literals
  static final int HASH_LOG_HC = 15; // log size of the dictionary for compressHC
  static final int HASH_TABLE_SIZE_HC = 1 << HASH_LOG_HC;
  static final int OPTIMAL_ML = 0x0F + 4 - 1; // match length that doesn't require an additional byte


  private static int hash(int i, int hashBits) {
    return (i * -1640531535) >>> (32 - hashBits);
  }

  private static int hashHC(int i) {
    return hash(i, HASH_LOG_HC);
  }

  /**
   * 从当前位置开始 读取总计4个byte 拼接成int
   * @param buf
   * @param i
   * @return
   */
  private static int readInt(byte[] buf, int i) {
    return ((buf[i] & 0xFF) << 24) | ((buf[i+1] & 0xFF) << 16) | ((buf[i+2] & 0xFF) << 8) | (buf[i+3] & 0xFF);
  }

  /**
   * 从一个byte数组的 2个点开始读取数据 并返回相同的长度
   * @param b
   * @param o1
   * @param o2
   * @param limit
   * @return
   */
  private static int commonBytes(byte[] b, int o1, int o2, int limit) {
    assert o1 < o2;
    // never -1 because lengths always differ
    return Arrays.mismatch(b, o1, limit, b, o2, limit);
  }

  /**
   * Decompress at least <code>decompressedLen</code> bytes into
   * <code>dest[dOff:]</code>. Please note that <code>dest</code> must be large
   * enough to be able to hold <b>all</b> decompressed data (meaning that you
   * need to know the total decompressed length).
   * 从input中读取数据 并解压到目标数组
   */
  public static int decompress(DataInput compressed, int decompressedLen, byte[] dest) throws IOException {
    int dOff = 0;
    final int destEnd = dest.length;

    do {
      // literals
      final int token = compressed.readByte() & 0xFF;
      // token 高4位代表 literalLen
      int literalLen = token >>> 4;

      if (literalLen != 0) {
        // 如果literalLen 等于 15 代表可能超过了15 需要读取额外的空间  如果刚好是15 那么会用一个0占位 所以解析逻辑一致
        if (literalLen == 0x0F) {
          byte len;
          while ((len = compressed.readByte()) == (byte) 0xFF) {
            literalLen += 0xFF;
          }
          literalLen += len & 0xFF;
        }
        // 这里已经将 literal的部分读取出来了
        compressed.readBytes(dest, dOff, literalLen);
        dOff += literalLen;
      }

      // 代表当前长度超过了预计读取的长度 那么直接返回
      if (dOff >= decompressedLen) {
        break;
      }

      // matchs
      // 这里使用 16位保存了 2个相同的字符串的起点差值
      final int matchDec = (compressed.readByte() & 0xFF) | ((compressed.readByte() & 0xFF) << 8);
      assert matchDec > 0;

      // 低4位记录了 压缩的长度  下面的逻辑同 literalLen
      int matchLen = token & 0x0F;
      if (matchLen == 0x0F) {
        int len;
        while ((len = compressed.readByte()) == (byte) 0xFF) {
          matchLen += 0xFF;
        }
        matchLen += len & 0xFF;
      }
      // 因为该部分忽略了 MIN_MATCH 所以这里要补上
      matchLen += MIN_MATCH;

      // copying a multiple of 8 bytes can make decompression from 5% to 10% faster   这里提高性能的同时会导致数据有部分丢失 不过在lucene的场景中允许这种情况
      // 向下变成8的倍数  比如25 -> 24 | 15 -> 8 | 17 -> 16
      final int fastLen = (matchLen + 7) & 0xFFFFFFF8;
      // 前半部分 对应   11111   中 1111 与 1111 重复   也就是重叠的情况
      // 后半部分代表 存储还原数据的数组容量不足 那么只会还原部分数据
      if (matchDec < matchLen || dOff + fastLen > destEnd) {
        // overlap -> naive incremental copy
        // 此时 end指向重叠的末尾  也就是o2的末尾   重叠的情况实际上就是出现重复的数据本身有一定自重复的规则  比如 1212121212  所以只要保存一部分数据就能自动生成后面的数据 在存储时压缩率会更高
        for (int ref = dOff - matchDec, end = dOff + matchLen; dOff < end; ++ref, ++dOff) {
          dest[dOff] = dest[ref];
        }
      } else {
        // no overlap -> arraycopy
        // 没有重叠
        // 每当还原某部分的字符串时 可以认为之前的数据都已经完成还原了   所以 dOff- matchDec 总是能定位到拷贝的起点 拷贝的长度 略小于 matchLen
        System.arraycopy(dest, dOff - matchDec, dest, dOff, fastLen);
        dOff += matchLen;
      }
    } while (dOff < decompressedLen);

    return dOff;
  }

  /**
   * 当literalLen/matchLen  超过15时 会继续写入长度信息
   * @param l
   * @param out
   * @throws IOException
   */
  private static void encodeLen(int l, DataOutput out) throws IOException {
    while (l >= 0xFF) {
      out.writeByte((byte) 0xFF);
      l -= 0xFF;
    }
    // 如果刚好是15 会直接写入 0
    out.writeByte((byte) l);
  }

  /**
   * 当生成token后 将token和 literal 存储到 out中
   * @param bytes
   * @param token
   * @param anchor
   * @param literalLen
   * @param out
   * @throws IOException
   */
  private static void encodeLiterals(byte[] bytes, int token, int anchor, int literalLen, DataOutput out) throws IOException {
    // 首先写入token  token经过特殊处理 只会存储8位
    out.writeByte((byte) token);

    // encode literal length
    // 代表实际上 无重复字符串的长度超过了 token分配的4位长度
    if (literalLen >= 0x0F) {
      // 那么就需要额外的空间去存储长度   实际上压缩的核心就是利用滑动窗口 + hashTable 至于如何存储压缩后的数据 就是一种协定  这里假定压缩的字符串都不会很长 所以使用一个byte(token) 来存储元数据信息
      // 而一旦长度超过15时 无法使用单个byte来表示 这时才使用额外的内存来存储元数据信息  也就是一种乐观策略 每次假定被压缩的长度不会太大
      encodeLen(literalLen - 0x0F, out);
    }

    // encode literals
    // 从bytes的anchor为起点 将literalLen长度的数据写入到out中
    out.writeBytes(bytes, anchor, literalLen);
  }

  private static void encodeLastLiterals(byte[] bytes, int anchor, int literalLen, DataOutput out) throws IOException {
    // 看来低4位为0 代表本次是最后一次写入
    final int token = Math.min(literalLen, 0x0F) << 4;
    encodeLiterals(bytes, token, anchor, literalLen, out);
  }

  /**
   * 匹配代表的是 字符串o1 与 o2的匹配
   * @param bytes  存储数据的容器
   * @param anchor  当一次压缩完成后 新的起点
   * @param matchRef  o1的起点   该值与锚点没有必然联系  即使锚点已经前进到一个新的位置 o2还是允许与之前的数据发生重复
   * @param matchOff  o2的起点
   * @param matchLen   本次匹配的最大长度
   * @param out    将压缩后的结果写入到这里
   * @throws IOException
   */
  private static void encodeSequence(byte[] bytes, int anchor, int matchRef, int matchOff, int matchLen, DataOutput out) throws IOException {
    // 在重复之前的字符串需要原封不动的写入    锚点的作用就是确定需要存储的字符串  锚点的数据要么已经存储到out中 要么就是被压缩过了 (处理过)   因为锚点之后的数据可以与锚点之前的数据发生重复
    // 为了不再重复写入 所以通过锚点作分割
    final int literalLen = matchOff - anchor;

    assert matchLen >= 4;
    // encode token
    // 这个token 同时维护了 有关 literal的长度 以及匹配的长度    低4位最多只存储 15  这时代表匹配的长度超过了 15(也可能刚好相等)   literal同理 最多只存储15 超过15 也按15计算
    final int token = (Math.min(literalLen, 0x0F) << 4) | Math.min(matchLen - 4, 0x0F);
    // 存储token 以及 literal   这里如果literal的长度超过了15 会使用额外的空间来存储长度信息
    encodeLiterals(bytes, token, anchor, literalLen, out);

    // 下面的部分是关于如何写入重复的部分
    // encode match dec
    // 2个重复的字符串之间的长度  这个值跟 literalLen 是没有关系的  并且可以认为这之间的数据都已经写入到out中  可能是以literal的形式写入(原封不动的写入) 也可能有部分数据已经做过压缩处理
    final int matchDec = matchOff - matchRef;
    assert matchDec > 0 && matchDec < 1 << 16;
    // 这里默认2个重复字符串之间的长度不会超过 1<<16 因为在get() 方法中 即使2个字符串重复 但是距离超过了 1<<16 还是认为不可压缩
    out.writeByte((byte) matchDec);
    out.writeByte((byte) (matchDec >>> 8));

    // encode match len
    // 如果匹配的长度 本身小于 0x0f 那么直接从token中获取即可  如果超过了 那么就需要额外的空间来存储  注意token中存储的matchLen 是减去 MIN_MATCH
    if (matchLen >= MIN_MATCH + 0x0F) {
      encodeLen(matchLen - 0x0F - MIN_MATCH, out);
    }
  }

  /**
   * A record of previous occurrences of sequences of 4 bytes.
   *
   */
  static abstract class HashTable {

    /** Reset this hash table in order to compress the given content. */
    abstract void reset(byte[] b, int off, int len);

    /**
     * Advance the cursor to {@off} and return an index that stored the same
     * 4 bytes as {@code b[o:o+4)}. This may only be called on strictly
     * increasing sequences of offsets. A return value of {@code -1} indicates
     * that no other index could be found. */
    abstract int get(int off);

    /**
     * Return an index that less than {@code off} and stores the same 4
     * bytes. Unlike {@link #get}, it doesn't need to be called on increasing
     * offsets. A return value of {@code -1} indicates that no other index could
     * be found. */
    abstract int previous(int off);

    // For testing
    abstract boolean assertReset();
  }

  /**
   * Simple lossy {@link HashTable} that only stores the last ocurrence for
   * each hash on {@code 2^14} bytes of memory.
   * 速度优先的 LZ4 hash桶
   */
  public static final class FastCompressionHashTable extends HashTable {

    private byte[] bytes;
    /**
     * 基础值
     */
    private int base;
    /**
     * 记录上次写入的偏移量
     */
    private int lastOff;
    /**
     * 最多允许写入的偏移量  对应 get(offset)
     */
    private int end;
    /**
     * 这个是计算hash值相关的一个因子
     */
    private int hashLog;
    /**
     * Packed64 系列的特点就是在一开始声明了存入的数据将会占用多少位  然后通过指定偏移量后 读取对应的位数生成值
     */
    private PackedInts.Mutable hashTable;

    /** Sole constructor */
    public FastCompressionHashTable() {}

    /**
     * 通过传入的bytes 重置内部数据
     * @param bytes
     * @param off
     * @param len
     */
    @Override
    void reset(byte[] bytes, int off, int len) {
      Objects.checkFromIndexSize(off, len, bytes.length);
      this.bytes = bytes;
      // 用于计算绝对偏移量的槽
      this.base = off;
      this.lastOff = off - 1;
      this.end = off + len;
      // 这里用的是差值规则  可以假设 off 当前很大 那么存储off 可能会占用很多位   为了节省空间每次只存储差值 比如存1 就代表 off + 1的位置占用了 slot
      // 这里通过提前获取最大的差值所占用的位来创建 packedInts
      final int bitsPerOffset = PackedInts.bitsRequired(len - LAST_LITERALS);
      final int bitsPerOffsetLog = 32 - Integer.numberOfLeadingZeros(bitsPerOffset - 1);
      hashLog = MEMORY_USAGE + 3 - bitsPerOffsetLog;
      // 基于目标位 创建 packedIns 对象
      if (hashTable == null || hashTable.size() < 1 << hashLog || hashTable.getBitsPerValue() < bitsPerOffset) {
        hashTable = PackedInts.getMutable(1 << hashLog, bitsPerOffset, PackedInts.DEFAULT);
      } else {
        // Avoid calling hashTable.clear(), this makes it costly to compress many short sequences otherwise.
        // Instead, get() checks that references are less than the current offset.
        get(off); // this sets the hashTable for the first 4 bytes as a side-effect
      }
    }

    /**
     * 根据偏移量 返回某个值
     * @param off
     * @return
     */
    @Override
    int get(int off) {
      assert off > lastOff;
      assert off < end;

      // LZ4 压缩算法实现就是每次读取4个byte的长度
      final int v = readInt(bytes, off);
      // 计算读取的值的hash     当hash冲突时 选择覆盖之前的数据 那么可能部分数据已经出现重复了 却没办法压缩 不过概率比较低 这里就体现了fast 在追求高压缩率的 table中 hash冲突的偏移量以一种链表形式关联
      // 这样就能确保尽量不丢失所有可能的重复字符串
      final int h = hash(v, hashLog);

      // 从hash桶中读取数据    Packed64 当中存储的某个值在一开始就规定了占用多少位  然后读取数据时 变成读取固定的位 而不是按照类型读取对应的长度(比如int读取4个byte的长度)
      // 因为利用了差值规则 所以要加上base(off) 还原成绝对偏移量
      final int ref = base + (int) hashTable.get(h);
      // 写入数据  写入的是一个相对偏移量  对应上面换算成绝对偏移量的地方
      hashTable.set(h, off - base);
      // 记录上一次写入的偏移量
      lastOff = off;

      // 本次偏移量比之前大
      // 并且差值要小于一个允许的最大差值  并且2个偏移量在bytes中读取出来的值要一样
      if (ref < off && off - ref < MAX_DISTANCE && readInt(bytes, ref) == v) {
        // 返回上一个冲突的偏移量
        return ref;
      } else {
        return -1;
      }
    }

    @Override
    public int previous(int off) {
      return -1;
    }

    @Override
    boolean assertReset() {
      return true;
    }

  }

  /**
   * A higher-precision {@link HashTable}. It stores up to 256 occurrences of
   * 4-bytes sequences in the last {@code 2^16} bytes, which makes it much more
   * likely to find matches than {@link FastCompressionHashTable}.
   * 追求高压缩率
   */
  public static final class HighCompressionHashTable extends HashTable {
    private static final int MAX_ATTEMPTS = 256;
    private static final int MASK = MAX_DISTANCE - 1;

    private byte[] bytes;
    private int base;
    private int next;
    private int end;
    /**
     * 这里没有使用 PacketInts 这种数据结构  而是使用一个普通的数组
     */
    private final int[] hashTable;
    /**
     * 这里利用增量值提供了一种链式追踪的功能  比如目标偏移量对应的增量值为 x  那么将off -x 就得到上一个hash冲突的值 同样使用 off-x 可以找到更前面的冲突的值
     */
    private final short[] chainTable;
    private int attempts = 0;

    /** Sole constructor */
    public HighCompressionHashTable() {
      hashTable = new int[HASH_TABLE_SIZE_HC];
      Arrays.fill(hashTable, -1);
      chainTable = new short[MAX_DISTANCE];
      Arrays.fill(chainTable, (short) 0xFFFF);
    }

    /**
     * 重置对象
     * @param bytes
     * @param off
     * @param len
     */
    @Override
    void reset(byte[] bytes, int off, int len) {
      Objects.checkFromIndexSize(off, len, bytes.length);
      if (end - base < chainTable.length) {
        // The last call to compress was done on less than 64kB, let's not reset
        // the hashTable and only reset the relevant parts of the chainTable.
        // This helps avoid slowing down calling compress() many times on short
        // inputs.
        int startOffset = base & MASK;
        int endOffset = end == 0 ? 0 : ((end - 1) & MASK) + 1;
        if (startOffset < endOffset) {
          Arrays.fill(chainTable, startOffset, endOffset, (short) 0xFFFF);
        } else {
          Arrays.fill(chainTable, 0, endOffset, (short) 0xFFFF);
          Arrays.fill(chainTable, startOffset, chainTable.length, (short) 0xFFFF);
        }
      } else {
        // The last call to compress was done on a large enough amount of data
        // that it's fine to reset both tables
        Arrays.fill(hashTable, -1);
        Arrays.fill(chainTable, (short) 0xFFFF);
      }
      this.bytes = bytes;
      this.base = off;
      this.next = off;
      this.end = off + len;
    }

    /**
     * 开始检测是否出现重复
     * @param off
     * @return
     */
    @Override
    int get(int off) {
      assert off > next;
      assert off < end;

      // 如果next 小于 off 将之前的值都填充 (确保之前的链式结构顺序生成)
      for (; next < off; next++) {
        addHash(next);
      }

      // 读取本次off 对应的偏移量
      final int v = readInt(bytes, off);
      final int h = hashHC(v);

      attempts = 0;
      int ref = hashTable[h];
      // 如果此次找到的值 大于传入的 off 则忽略 (认为没有找到)
      if (ref >= off) {
        // remainder from a previous call to compress()
        return -1;
      }
      for (int min = Math.max(base, off - MAX_DISTANCE + 1);
          ref >= min && attempts < MAX_ATTEMPTS;  // 返回的目标值大于 最小值 同时尝试次数小于最大值
          ref -= chainTable[ref & MASK] & 0xFFFF, attempts++) {    // 类似于线性探测法 此时没有找到时 找到对应的冲突值 并重新读取数据判断是否重复
        if (readInt(bytes, ref) == v) {
          return ref;
        }
      }
      return -1;
    }

    private void addHash(int off) {
      final int v = readInt(bytes, off);
      final int h = hashHC(v);
      // 当hash冲突时 生成增量值
      int delta = off - hashTable[h];
      // 当增量值异常时  重置为MAX_DISTANCE - 1
      if (delta <= 0 || delta >= MAX_DISTANCE) {
        delta = MAX_DISTANCE - 1;
      }
      // 这里填充的是增量值
      chainTable[off & MASK] = (short) delta;
      // 这里记录的是绝对偏移量 没有运用差值规则  因为int[] 可以直接存放int值
      hashTable[h] = off;
    }

    /**
     * 找到上一个相同值所在的偏移量
     * @param off
     * @return
     */
    @Override
    int previous(int off) {
      final int v = readInt(bytes, off);
      for (int ref = off - (chainTable[off & MASK] & 0xFFFF);
          ref >= base && attempts < MAX_ATTEMPTS;
          ref -= chainTable[ref & MASK] & 0xFFFF, attempts++ ) {
        if (readInt(bytes, ref) == v) {
          return ref;
        }
      }
      return -1;
    }

    @Override
    boolean assertReset() {
      for (int i = 0; i < chainTable.length; ++i) {
        assert chainTable[i] == (short) 0xFFFF : i;
      }
      return true;
    }
  }

  /**
   * Compress <code>bytes[off:off+len]</code> into <code>out</code> using
   * at most 16KB of memory. <code>ht</code> shouldn't be shared across threads
   * but can safely be reused.
   * 针对输入的数据进行压缩
   * 当出现  11111 那么当off 变成1的时候 就会发生重复 (1111 和 1111) 这时写入的 literal为1  实际上也不重要
   */
  public static void compress(byte[] bytes, int off, int len, DataOutput out, HashTable ht) throws IOException {
    Objects.checkFromIndexSize(off, len, bytes.length);

    final int base = off;
    // 记录允许读取的终点
    final int end = off + len;

    // 这个是锚点  作用是记录写入的起点 (不同于重复的起点  主要是确定写入的literal)
    int anchor = off++;

    // 长度至少要大于 最小的一个无重复的字节流 + 最小的重复长度  (小于该长度是无法进行压缩的)
    if (len > LAST_LITERALS + MIN_MATCH) {

      final int limit = end - LAST_LITERALS;
      final int matchLimit = limit - MIN_MATCH;
      // 使用相关参数初始化hash桶
      ht.reset(bytes, base, len);

      // 外层while代表整个压缩流程
      main:
      while (off <= limit) {
        // find a match
        int ref;
        // 内循环的作用是检测重复的部分
        while (true) {
          // 实际上当前长度 不满足 匹配的最小长度时 已经没必要继续循环了
          if (off >= matchLimit) {
            break main;
          }
          // 从off开始 将4个byte 计算hash值后写入hash桶    ref是hash冲突时上一个字符串的偏移量
          ref = ht.get(off);
          // != -1 代表发生了重复
          // 如果为-1 代表没有发生重复 就继续移动滑动窗口
          if (ref != -1) {
            assert ref >= base && ref < off;
            assert readInt(bytes, ref) == readInt(bytes, off);
            break;
          }
          ++off;
        }

        // 进入到这里 代表已经检测到了重复的部分
        // compute match length
        // 在已经满足 最小重复长度的基础上 检测总的匹配长度
        int matchLen = MIN_MATCH + commonBytes(bytes, ref + MIN_MATCH, off + MIN_MATCH, limit);

        // try to find a better match
        // 针对 FastHashTable  previous 总是返回-1   也就是无法进入下面的循环
        // 而针对追求压缩率的 HashTable 这里是找到上一个相同的 重复字符串
        for (int r = ht.previous(ref), min = Math.max(off - MAX_DISTANCE + 1, base); r >= min; r = ht.previous(r)) {
          assert readInt(bytes, r) == readInt(bytes, off);
          // 这里记录最大的重复字符串长度
          int rMatchLen = MIN_MATCH + commonBytes(bytes, r + MIN_MATCH, off + MIN_MATCH, limit);
          if (rMatchLen > matchLen) {
            ref = r;
            matchLen = rMatchLen;
          }
        }

        // 将相关信息包装成一个 sequence 并存储
        encodeSequence(bytes, anchor, ref, off, matchLen, out);
        // 更新当前的偏移量
        off += matchLen;
        // 将锚点更新成 本次压缩的结尾
        anchor = off;
      }
    }

    // 最后一段要写入的作特殊处理
    // last literals   剩余的部分要全部写入
    final int literalLen = end - anchor;
    assert literalLen >= LAST_LITERALS || literalLen == len;
    encodeLastLiterals(bytes, anchor, end - anchor, out);
  }

}
