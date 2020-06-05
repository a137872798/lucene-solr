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
package org.apache.lucene.util;


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * BitSet of fixed length (numBits), backed by accessible ({@link #getBits})
 * long[], accessed with an int index, implementing {@link Bits} and
 * {@link DocIdSet}. If you need to manage more than 2.1B bits, use
 * {@link LongBitSet}.
 * 
 * @lucene.internal
 * 一个最基础的位图对象  不考虑内存开销
 * (如果一开始maxDoc非常大 那么创建的数组也会很大 而实际上占用的位少 那么就造成大量的内存浪费)
 */
public final class FixedBitSet extends BitSet implements Bits, Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

  /**
   * 位图数组 每个元素存储64位数据
   */
  private final long[] bits; // Array of longs holding the bits
  /**
   * 记录当前已经标记了多少位
   */
  private final int numBits; // The number of bits in use
  /**
   * bits.length
   */
  private final int numWords; // The exact number of longs needed to hold numBits (<= bits.length)
  
  /**
   * If the given {@link FixedBitSet} is large enough to hold {@code numBits+1},
   * returns the given bits, otherwise returns a new {@link FixedBitSet} which
   * can hold the requested number of bits.
   * <p>
   * <b>NOTE:</b> the returned bitset reuses the underlying {@code long[]} of
   * the given {@code bits} if possible. Also, calling {@link #length()} on the
   * returned bits may return a value greater than {@code numBits}.
   * 判断传入的参数是否有足够的空间 用于设置该位
   */
  public static FixedBitSet ensureCapacity(FixedBitSet bits, int numBits) {
    if (numBits < bits.numBits) {
      return bits;
    } else {
      // Depends on the ghost bits being clear!
      // (Otherwise, they may become visible in the new instance)
      // 代表传入的 bits空间不足 需要做扩容 + 数据拷贝
      int numWords = bits2words(numBits);
      long[] arr = bits.getBits();
      if (numWords >= arr.length) {
        // 扩容 + 数据拷贝
        arr = ArrayUtil.grow(arr, numWords + 1);
      }
      return new FixedBitSet(arr, arr.length << 6);
    }
  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(int numBits) {
    // 3种情况
    // 1: 64的整数倍  那么取余后 + 1 相当于向上取整
    // 2: 小于64的倍数    那么即使是0 计算出来也至少是1
    // 3: 如果大于64的倍数 也是向上取整
    return ((numBits - 1) >> 6) + 1; // I.e.: get the word-offset of the last bit and add one (make sure to use >> so 0 returns 0!)
  }

  // 下面3个方法套路都类似  都是利用一些位运算

  /**
   * Returns the popcount or cardinality of the intersection of the two sets.
   * Neither set is modified.
   * 返回交集
   */
  public static long intersectionCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    return BitUtil.pop_intersect(a.bits, b.bits, 0, Math.min(a.numWords, b.numWords));
  }

  /**
   * Returns the popcount or cardinality of the union of the two sets. Neither
   * set is modified.
   * 返回并集
   */
  public static long unionCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    long tot = BitUtil.pop_union(a.bits, b.bits, 0, Math.min(a.numWords, b.numWords));
    // 单独计算剩余的部分
    if (a.numWords < b.numWords) {
      tot += BitUtil.pop_array(b.bits, a.numWords, b.numWords - a.numWords);
    } else if (a.numWords > b.numWords) {
      tot += BitUtil.pop_array(a.bits, b.numWords, a.numWords - b.numWords);
    }
    return tot;
  }

  /**
   * Returns the popcount or cardinality of "a and not b" or
   * "intersection(a, not(b))". Neither set is modified.
   */
  public static long andNotCount(FixedBitSet a, FixedBitSet b) {
    // Depends on the ghost bits being clear!
    long tot = BitUtil.pop_andnot(a.bits, b.bits, 0, Math.min(a.numWords, b.numWords));
    if (a.numWords > b.numWords) {
      tot += BitUtil.pop_array(a.bits, b.numWords, a.numWords - b.numWords);
    }
    return tot;
  }

  /**
   * Creates a new LongBitSet.
   * The internally allocated long array will be exactly the size needed to accommodate the numBits specified.
   * @param numBits the number of bits needed
   *                基于目标大小创建位图对象
   */
  public FixedBitSet(int numBits) {
    this.numBits = numBits;
    // 换算成 long的数量  (向上取整)
    bits = new long[bits2words(numBits)];
    numWords = bits.length;
  }

  /**
   * Creates a new LongBitSet using the provided long[] array as backing store.
   * The storedBits array must be large enough to accommodate the numBits specified, but may be larger.
   * In that case the 'extra' or 'ghost' bits must be clear (or they may provoke spurious side-effects)
   * @param storedBits the array to use as backing store 基于一个已经存在的数组创建位图对象
   * @param numBits the number of bits actually needed   记录当前位图一共占用多少个 long
   *
   */
  public FixedBitSet(long[] storedBits, int numBits) {
    this.numWords = bits2words(numBits);
    if (numWords > storedBits.length) {
      throw new IllegalArgumentException("The given long array is too small  to hold " + numBits + " bits");
    }
    this.numBits = numBits;
    this.bits = storedBits;

    assert verifyGhostBitsClear();
  }

  /**
   * Checks if the bits past numBits are clear.
   * Some methods rely on this implicit assumption: search for "Depends on the ghost bits being clear!" 
   * @return true if the bits past numBits are clear.
   */
  private boolean verifyGhostBitsClear() {
    // 默认情况下 这里是不跑的  因为 numWords = bits.length  这里主要是针对扩容的情况
    for (int i = numWords; i < bits.length; i++) {
      if (bits[i] != 0) return false;
    }

    // 只要numBits 不是64的倍数 那么数组必然有空位可使用
    if ((numBits & 0x3f) == 0) return true;

    // -1L 64位全部是1    这里高位全部用1占住了   低位全是0
    // 当左移超过64位时 编译器会自动做取模运算
    long mask = -1L << numBits;
    // 这个方法的含义实际上是判断 mask 到 能被64整出的数之间是否有空间  如果mask本身就存在0 那么 代表numBits 本身就不是64的倍数  当然有空间
    // 那么进入到这里 就代表 mask 全是1  同时最后一个long 必须有空位
    return (bits[numWords - 1] & mask) == 0;
  }
  
  @Override
  public int length() {
    return numBits;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(bits);
  }

  /** Expert. */
  public long[] getBits() {
    return bits;
  }

  /** Returns number of set bits.  NOTE: this visits every
   *  long in the backing bits array, and the result is not
   *  internally cached!
   *  一共占了多少位
   */
  @Override
  public int cardinality() {
    // Depends on the ghost bits being clear!
    return (int) BitUtil.pop_array(bits, 0, numWords);
  }

  /**
   * 通过指定位数 判断对应的位是否被设置
   * @param index index, should be non-negative and &lt; {@link #length()}.
   *        The result of passing negative or out of bounds values is undefined
   *        by this interface, <b>just don't do it!</b>
   * @return
   */
  @Override
  public boolean get(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    long bitmask = 1L << index;
    return (bits[i] & bitmask) != 0;
  }

  public void set(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    // 定位数组下标
    int wordNum = index >> 6;      // div 64
    // 从低位开始往上填充
    long bitmask = 1L << index;
    bits[wordNum] |= bitmask;
  }

  public boolean getAndSet(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;      // div 64
    long bitmask = 1L << index;
    boolean val = (bits[wordNum] & bitmask) != 0;
    bits[wordNum] |= bitmask;
    return val;
  }

  @Override
  public void clear(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;
    long bitmask = 1L << index;
    // x & 0 必然是0
    bits[wordNum] &= ~bitmask;
  }

  public boolean getAndClear(int index) {
    assert index >= 0 && index < numBits: "index=" + index + ", numBits=" + numBits;
    int wordNum = index >> 6;      // div 64
    long bitmask = 1L << index;
    boolean val = (bits[wordNum] & bitmask) != 0;
    bits[wordNum] &= ~bitmask;
    return val;
  }

  /**
   * 从当前位置遍历到下一个已经被设置的位
   * @param index
   * @return
   */
  @Override
  public int nextSetBit(int index) {
    // Depends on the ghost bits being clear!
    assert index >= 0 && index < numBits : "index=" + index + ", numBits=" + numBits;
    int i = index >> 6;
    long word = bits[i] >> index;  // skip all the bits to the right of index

    // 如果刚好在同一个long 中 判断剩余的位是否被设置
    if (word!=0) {
      // numberOfTrailingZeros 代表从最低位往上数 直到遇到1时 经过的位数  index 是一个基础的值
      return index + Long.numberOfTrailingZeros(word);
    }

    /**
     * 需要遍历到下一个long
     */
    while(++i < numWords) {
      word = bits[i];
      if (word != 0) {
        return (i<<6) + Long.numberOfTrailingZeros(word);
      }
    }

    return DocIdSetIterator.NO_MORE_DOCS;
  }

  /**
   * 获取上一个设置的位
   * @param index
   * @return
   */
  @Override
  public int prevSetBit(int index) {
    assert index >= 0 && index < numBits: "index=" + index + " numBits=" + numBits;
    int i = index >> 6;
    final int subIndex = index & 0x3f;  // index within the word
    long word = (bits[i] << (63-subIndex));  // skip all the bits to the left of index

    if (word != 0) {
      return (i << 6) + subIndex - Long.numberOfLeadingZeros(word); // See LUCENE-3197
    }

    while (--i >= 0) {
      word = bits[i];
      if (word !=0 ) {
        return (i << 6) + 63 - Long.numberOfLeadingZeros(word);
      }
    }

    return -1;
  }

  @Override
  public void or(DocIdSetIterator iter) throws IOException {
    if (BitSetIterator.getFixedBitSetOrNull(iter) != null) {
      checkUnpositioned(iter);
      final FixedBitSet bits = BitSetIterator.getFixedBitSetOrNull(iter); 
      or(bits);
    } else {
      super.or(iter);
    }
  }

  /** this = this OR other */
  public void or(FixedBitSet other) {
    or(other.bits, other.numWords);
  }
  
  private void or(final long[] otherArr, final int otherNumWords) {
    assert otherNumWords <= numWords : "numWords=" + numWords + ", otherNumWords=" + otherNumWords;
    final long[] thisArr = this.bits;
    int pos = Math.min(numWords, otherNumWords);
    while (--pos >= 0) {
      thisArr[pos] |= otherArr[pos];
    }
  }
  
  /** this = this XOR other */
  public void xor(FixedBitSet other) {
    xor(other.bits, other.numWords);
  }
  
  /** Does in-place XOR of the bits provided by the iterator. */
  public void xor(DocIdSetIterator iter) throws IOException {
    checkUnpositioned(iter);
    if (BitSetIterator.getFixedBitSetOrNull(iter) != null) {
      final FixedBitSet bits = BitSetIterator.getFixedBitSetOrNull(iter); 
      xor(bits);
    } else {
      int doc;
      while ((doc = iter.nextDoc()) < numBits) {
        flip(doc);
      }
    }
  }

  private void xor(long[] otherBits, int otherNumWords) {
    assert otherNumWords <= numWords : "numWords=" + numWords + ", other.numWords=" + otherNumWords;
    final long[] thisBits = this.bits;
    int pos = Math.min(numWords, otherNumWords);
    while (--pos >= 0) {
      thisBits[pos] ^= otherBits[pos];
    }
  }

  /** returns true if the sets have any elements in common */
  public boolean intersects(FixedBitSet other) {
    // Depends on the ghost bits being clear!
    int pos = Math.min(numWords, other.numWords);
    while (--pos>=0) {
      if ((bits[pos] & other.bits[pos]) != 0) return true;
    }
    return false;
  }

  /** this = this AND other */
  public void and(FixedBitSet other) {
    and(other.bits, other.numWords);
  }
  
  private void and(final long[] otherArr, final int otherNumWords) {
    final long[] thisArr = this.bits;
    int pos = Math.min(this.numWords, otherNumWords);
    while(--pos >= 0) {
      thisArr[pos] &= otherArr[pos];
    }
    if (this.numWords > otherNumWords) {
      Arrays.fill(thisArr, otherNumWords, this.numWords, 0L);
    }
  }

  /** this = this AND NOT other */
  public void andNot(FixedBitSet other) {
    andNot(other.bits, other.numWords);
  }
  
  private void andNot(final long[] otherArr, final int otherNumWords) {
    final long[] thisArr = this.bits;
    int pos = Math.min(this.numWords, otherNumWords);
    while(--pos >= 0) {
      thisArr[pos] &= ~otherArr[pos];
    }
  }

  /**
   * Scans the backing store to check if all bits are clear.
   * The method is deliberately not called "isEmpty" to emphasize it is not low cost (as isEmpty usually is).
   * @return true if all bits are clear.
   */
  public boolean scanIsEmpty() {
    // This 'slow' implementation is still faster than any external one could be
    // (e.g.: (bitSet.length() == 0 || bitSet.nextSetBit(0) == -1))
    // especially for small BitSets
    // Depends on the ghost bits being clear!
    final int count = numWords;
    
    for (int i = 0; i < count; i++) {
      if (bits[i] != 0) return false;
    }
    
    return true;
  }

  /** Flips a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to flip
   */
  public void flip(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits;
    assert endIndex >= 0 && endIndex <= numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex-1) >> 6;

    /*** Grrr, java shifting uses only the lower 6 bits of the count so -1L>>>64 == -1
     * for that reason, make sure not to use endmask if the bits to flip will
     * be zero in the last word (redefine endWord to be the last changed...)
    long startmask = -1L << (startIndex & 0x3f);     // example: 11111...111000
    long endmask = -1L >>> (64-(endIndex & 0x3f));   // example: 00111...111111
    ***/

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex since only the lowest 6 bits are used

    if (startWord == endWord) {
      bits[startWord] ^= (startmask & endmask);
      return;
    }

    bits[startWord] ^= startmask;

    for (int i=startWord+1; i<endWord; i++) {
      bits[i] = ~bits[i];
    }

    bits[endWord] ^= endmask;
  }

  /** Flip the bit at the provided index. */
  public void flip(int index) {
    assert index >= 0 && index < numBits: "index=" + index + " numBits=" + numBits;
    int wordNum = index >> 6;      // div 64
    long bitmask = 1L << index; // mod 64 is implicit
    bits[wordNum] ^= bitmask;
  }

  /** Sets a range of bits
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to set
   */
  public void set(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits : "startIndex=" + startIndex + ", numBits=" + numBits;
    assert endIndex >= 0 && endIndex <= numBits : "endIndex=" + endIndex + ", numBits=" + numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex-1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex since only the lowest 6 bits are used

    if (startWord == endWord) {
      bits[startWord] |= (startmask & endmask);
      return;
    }

    bits[startWord] |= startmask;
    Arrays.fill(bits, startWord+1, endWord, -1L);
    bits[endWord] |= endmask;
  }

  @Override
  public void clear(int startIndex, int endIndex) {
    assert startIndex >= 0 && startIndex < numBits : "startIndex=" + startIndex + ", numBits=" + numBits;
    assert endIndex >= 0 && endIndex <= numBits : "endIndex=" + endIndex + ", numBits=" + numBits;
    if (endIndex <= startIndex) {
      return;
    }

    int startWord = startIndex >> 6;
    int endWord = (endIndex-1) >> 6;

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex since only the lowest 6 bits are used

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
      bits[startWord] &= (startmask | endmask);
      return;
    }

    bits[startWord] &= startmask;
    Arrays.fill(bits, startWord+1, endWord, 0L);
    bits[endWord] &= endmask;
  }

  @Override
  public FixedBitSet clone() {
    long[] bits = new long[this.bits.length];
    System.arraycopy(this.bits, 0, bits, 0, numWords);
    return new FixedBitSet(bits, numBits);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FixedBitSet)) {
      return false;
    }
    FixedBitSet other = (FixedBitSet) o;
    if (numBits != other.numBits) {
      return false;
    }
    // Depends on the ghost bits being clear!
    return Arrays.equals(bits, other.bits);
  }

  @Override
  public int hashCode() {
    // Depends on the ghost bits being clear!
    long h = 0;
    for (int i = numWords; --i>=0;) {
      h ^= bits[i];
      h = (h << 1) | (h >>> 63); // rotate left
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int) ((h>>32) ^ h) + 0x98761234;
  }

  /**
   * Make a copy of the given bits.
   */
  public static FixedBitSet copyOf(Bits bits) {
    if (bits instanceof FixedBits) {
      // restore the original FixedBitSet
      FixedBits fixedBits = (FixedBits) bits;
      bits = new FixedBitSet(fixedBits.bits, fixedBits.length);
    }

    if (bits instanceof FixedBitSet) {
      return ((FixedBitSet)bits).clone();
    } else {
      int length = bits.length();
      FixedBitSet bitSet = new FixedBitSet(length);
      bitSet.set(0, length);
      for (int i = 0; i < length; ++i) {
        if (bits.get(i) == false) {
          bitSet.clear(i);
        }
      }
      return bitSet;
    }
  }

  /**
   * Convert this instance to read-only {@link Bits}.
   * This is useful in the case that this {@link FixedBitSet} is returned as a
   * {@link Bits} instance, to make sure that consumers may not get write access
   * back by casting to a {@link FixedBitSet}.
   * NOTE: Changes to this {@link FixedBitSet} will be reflected on the returned
   * {@link Bits}.
   */
  public Bits asReadOnlyBits() {
    return new FixedBits(bits, numBits);
  }
}
