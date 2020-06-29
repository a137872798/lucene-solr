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


import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.ByteBlockPool.DirectAllocator;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SHIFT;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

/**
 * {@link BytesRefHash} is a special purpose hash-map like data-structure
 * optimized for {@link BytesRef} instances. BytesRefHash maintains mappings of
 * byte arrays to ids (Map&lt;BytesRef,int&gt;) storing the hashed bytes
 * efficiently in continuous storage. The mapping to the id is
 * encapsulated inside {@link BytesRefHash} and is guaranteed to be increased
 * for each added {@link BytesRef}.
 * 
 * <p>
 * Note: The maximum capacity {@link BytesRef} instance passed to
 * {@link #add(BytesRef)} must not be longer than {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2. 
 * The internal storage is limited to 2GB total byte storage.
 * </p>
 * 
 * @lucene.internal
 * 该对象 类似一个hash桶  数据以 bytesRef的形式存储
 */
public final class BytesRefHash implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(BytesRefHash.class) +
      // size of scratch1
      RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) +
      // size of Counter
      RamUsageEstimator.primitiveSizes.get(long.class);

  public static final int DEFAULT_CAPACITY = 16;

  // the following fields are needed by comparator,
  // so package private to prevent access$-methods:
  // 该对象可以理解成逻辑上的一个 大数组
  final ByteBlockPool pool;

  /**
   * id 映射到 pool的起始偏移量
   */
  int[] bytesStart;

  /**
   * 这个对象就是为了调用 equals 需要的临时内存
   */
  private final BytesRef scratch1 = new BytesRef();
  private int hashSize;
  /**
   * 等同于 hashSize >> 1
   */
  private int hashHalfSize;
  private int hashMask;
  /**
   * 当前存储了多少数据
   */
  private int count;
  private int lastCount = -1;
  /**
   * hash 映射到 id
   */
  private int[] ids;
  /**
   * 该对象内部包含一个int[]
   */
  private final BytesStartArray bytesStartArray;
  /**
   * 记录当前对象消耗了多少 byte
   */
  private Counter bytesUsed;

  /**
   * Creates a new {@link BytesRefHash} with a {@link ByteBlockPool} using a
   * {@link DirectAllocator}.
   */
  public BytesRefHash() { 
    this(new ByteBlockPool(new DirectAllocator()));
  }
  
  /**
   * Creates a new {@link BytesRefHash}
   */
  public BytesRefHash(ByteBlockPool pool) {
    this(pool, DEFAULT_CAPACITY, new DirectBytesStartArray(DEFAULT_CAPACITY));
  }

  /**
   * Creates a new {@link BytesRefHash}
   * @param pool 该对象负责存储数据
   * @param capacity hash桶的容量
   * @param bytesStartArray 该对象可以定位到每个term在 pool的位置
   */
  public BytesRefHash(ByteBlockPool pool, int capacity, BytesStartArray bytesStartArray) {
    hashSize = capacity;
    hashHalfSize = hashSize >> 1;
    hashMask = hashSize - 1;
    this.pool = pool;
    ids = new int[hashSize];
    Arrays.fill(ids, -1);
    this.bytesStartArray = bytesStartArray;
    // 初始化一个int数组
    bytesStart = bytesStartArray.init();
    bytesUsed = bytesStartArray.bytesUsed() == null? Counter.newCounter() : bytesStartArray.bytesUsed();
    bytesUsed.addAndGet(hashSize * Integer.BYTES);
  }

  /**
   * Returns the number of {@link BytesRef} values in this {@link BytesRefHash}.
   * 
   * @return the number of {@link BytesRef} values in this {@link BytesRefHash}.
   */
  public int size() {
    return count;
  }

  /**
   * Populates and returns a {@link BytesRef} with the bytes for the given
   * bytesID.
   * <p>
   * Note: the given bytesID must be a positive integer less than the current
   * size ({@link #size()})
   * 
   * @param bytesID
   *          the id
   * @param ref
   *          the {@link BytesRef} to populate
   * 
   * @return the given BytesRef instance populated with the bytes for the given
   *         bytesID
   *         从池对象中读取数据
   */
  public BytesRef get(int bytesID, BytesRef ref) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID < bytesStart.length: "bytesID exceeds byteStart len: " + bytesStart.length;
    pool.setBytesRef(ref, bytesStart[bytesID]);
    return ref;
  }

  /**
   * Returns the ids array in arbitrary order. Valid ids start at offset of 0
   * and end at a limit of {@link #size()} - 1
   * <p>
   * Note: This is a destructive operation. {@link #clear()} must be called in
   * order to reuse this {@link BytesRefHash} instance.
   * </p>
   *
   * @lucene.internal
   * 紧凑化  可能初始化值的时候 ids 不是连续设置的  这里修改成连续
   */
  public int[] compact() {
    assert bytesStart != null : "bytesStart is null - not initialized";
    int upto = 0;
    for (int i = 0; i < hashSize; i++) {
      // 代表目标值已经被设置
      if (ids[i] != -1) {
        if (upto < i) {
          ids[upto] = ids[i];
          ids[i] = -1;
        }
        upto++;
      }
    }

    assert upto == count;
    lastCount = count;
    return ids;
  }

  /**
   * Returns the values array sorted by the referenced byte values.
   * <p>
   * Note: This is a destructive operation. {@link #clear()} must be called in
   * order to reuse this {@link BytesRefHash} instance.
   * </p>
   * TODO  MSB 啥玩意???
   */
  public int[] sort() {
    final int[] compact = compact();
    new StringMSBRadixSorter() {

      BytesRef scratch = new BytesRef();

      @Override
      protected void swap(int i, int j) {
        int tmp = compact[i];
        compact[i] = compact[j];
        compact[j] = tmp;
      }

      @Override
      protected BytesRef get(int i) {
        pool.setBytesRef(scratch, bytesStart[compact[i]]);
        return scratch;
      }

    }.sort(0, count);
    return compact;
  }

  private boolean equals(int id, BytesRef b) {
    pool.setBytesRef(scratch1, bytesStart[id]);
    return scratch1.bytesEquals(b);
  }

  /**
   * 重置内部数据 + 缩容
   * @param targetSize
   * @return
   */
  private boolean shrink(int targetSize) {
    // Cannot use ArrayUtil.shrink because we require power
    // of 2:
    int newSize = hashSize;
    while (newSize >= 8 && newSize / 4 > targetSize) {
      newSize /= 2;
    }
    // 这里长度发生了变化  重新调整内部的结构
    if (newSize != hashSize) {
      bytesUsed.addAndGet(Integer.BYTES * -(hashSize - newSize));
      hashSize = newSize;
      ids = new int[hashSize];
      Arrays.fill(ids, -1);
      hashHalfSize = newSize / 2;
      hashMask = newSize - 1;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Clears the {@link BytesRef} which maps to the given {@link BytesRef}
   * 清理内部数据
   */
  public void clear(boolean resetPool) {
    lastCount = count;
    count = 0;
    if (resetPool) {
      // 释放 block引用  (之前用于读取数据的 byteRef 实际上还是指向block的某个地址  需要等byteRef的引用也释放 才会真正被GC回收)
      pool.reset(false, false); // we don't need to 0-fill the buffers
    }
    bytesStart = bytesStartArray.clear();
    // 缩小成 lastCount
    if (lastCount != -1 && shrink(lastCount)) {
      // shrink clears the hash entries
      return;
    }
    // 将内部数据都置成 -1  上面返回true的时候 同时对ids 做了清理操作
    Arrays.fill(ids, -1);
  }

  public void clear() {
    clear(true);
  }
  
  /**
   * Closes the BytesRefHash and releases all internally used memory
   */
  public void close() {
    clear(true);
    ids = null;
    bytesUsed.addAndGet(Integer.BYTES * -hashSize);
  }

  /**
   * Adds a new {@link BytesRef}
   * 
   * @param bytes
   *          the bytes to hash
   * @return the id the given bytes are hashed if there was no mapping for the
   *         given bytes, otherwise <code>(-(id)-1)</code>. This guarantees
   *         that the return value will always be &gt;= 0 if the given bytes
   *         haven't been hashed before.
   * 
   * @throws MaxBytesLengthExceededException
   *           if the given bytes are {@code > 2 +}
   *           {@link ByteBlockPool#BYTE_BLOCK_SIZE}
   *           将某个 byte[] 写入到hash桶中
   */
  public int add(BytesRef bytes) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    final int length = bytes.length;
    // final position
    // 计算hash值   这里已经使用线性探测法解决冲突了
    final int hashPos = findHash(bytes);
    int e = ids[hashPos];

    // 代表对应的bucket  还没有被抢占 这里抢占位置 并将数据写入到 block中
    if (e == -1) {
      // new entry
      // 这里额外的2个byte 应该是用来存储长度的
      final int len2 = 2 + bytes.length;
      if (len2 + pool.byteUpto > BYTE_BLOCK_SIZE) {
        if (len2 > BYTE_BLOCK_SIZE) {
          throw new MaxBytesLengthExceededException("bytes can be at most "
              + (BYTE_BLOCK_SIZE - 2) + " in length; got " + bytes.length);
        }
        // 当前block不够写 切换到新的 block
        pool.nextBuffer();
      }
      final byte[] buffer = pool.buffer;
      final int bufferUpto = pool.byteUpto;
      // 该数组记录了 bytesId 与 pool绝对偏移量的映射关系
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: "
            + bytesStart.length;
      }
      // count++ 其实就是id
      e = count++;

      // 设置绝对偏移量
      bytesStart[e] = bufferUpto + pool.byteOffset;

      // We first encode the length, followed by the
      // bytes. Length is encoded as vInt, but will consume
      // 1 or 2 bytes at most (we reject too-long terms,
      // above).
      // 当写入的长度小于128时 代表长度可以用一个byte 来表示
      if (length < 128) {
        // 1 byte to store length
        // 先写入长度
        buffer[bufferUpto] = (byte) length;
        pool.byteUpto += length + 1;
        assert length >= 0: "Length must be positive: " + length;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 1,
            length);
      } else {
        // 2 byte to store length
        buffer[bufferUpto] = (byte) (0x80 | (length & 0x7f));
        buffer[bufferUpto + 1] = (byte) ((length >> 7) & 0xff);
        pool.byteUpto += length + 2;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 2,
            length);
      }
      assert ids[hashPos] == -1;
      // 设置hash桶的 bytesId
      ids[hashPos] = e;

      // 当达到半数时 进行扩容
      if (count == hashHalfSize) {
        rehash(2 * hashSize, true);
      }
      return e;
    }

    // 代表当前值已经写入过了
    return -(e + 1);
  }
  
  /**
   * Returns the id of the given {@link BytesRef}.
   * 
   * @param bytes
   *          the bytes to look for
   * 
   * @return the id of the given bytes, or {@code -1} if there is no mapping for the
   *         given bytes.
   */
  public int find(BytesRef bytes) {
    return ids[findHash(bytes)];
  }

  /**
   * 根据 byte[] 计算hash值
   * @param bytes
   * @return
   */
  private int findHash(BytesRef bytes) {
    assert bytesStart != null : "bytesStart is null - not initialized";

    int code = doHash(bytes.bytes, bytes.offset, bytes.length);

    // final position
    int hashPos = code & hashMask;
    int e = ids[hashPos];
    // 这里会先使用线性探测法 解决冲突
    if (e != -1 && !equals(e, bytes)) {
      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      do {
        code++;
        hashPos = code & hashMask;
        e = ids[hashPos];
      } while (e != -1 && !equals(e, bytes));
    }
    
    return hashPos;
  }

  /** Adds a "arbitrary" int offset instead of a BytesRef
   *  term.  This is used in the indexer to hold the hash for term
   *  vectors, because they do not redundantly store the byte[] term
   *  directly and instead reference the byte[] term
   *  already stored by the postings BytesRefHash.  See
   *  add(int textStart) in TermsHashPerField. */
  // 通过指定pool的偏移量 提前预定一个 bytesRef  (虽然并没有写入数据)
  public int addByPoolOffset(int offset) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    // final position
    int code = offset;
    int hashPos = offset & hashMask;
    int e = ids[hashPos];

    // 先解决hash冲突
    if (e != -1 && bytesStart[e] != offset) {
      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      do {
        code++;
        hashPos = code & hashMask;
        e = ids[hashPos];
      } while (e != -1 && bytesStart[e] != offset);
    }
    if (e == -1) {
      // new entry
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: "
            + bytesStart.length;
      }
      e = count++;
      bytesStart[e] = offset;
      assert ids[hashPos] == -1;
      ids[hashPos] = e;

      if (count == hashHalfSize) {
        rehash(2 * hashSize, false);
      }
      return e;
    }
    return -(e + 1);
  }

  /**
   * Called when hash is too small ({@code > 50%} occupied) or too large ({@code < 20%}
   * occupied).
   */
  private void rehash(final int newSize, boolean hashOnData) {
    final int newMask = newSize - 1;
    bytesUsed.addAndGet(Integer.BYTES * (newSize));
    final int[] newHash = new int[newSize];
    Arrays.fill(newHash, -1);
    for (int i = 0; i < hashSize; i++) {
      final int e0 = ids[i];
      if (e0 != -1) {
        int code;
        if (hashOnData) {
          final int off = bytesStart[e0];
          final int start = off & BYTE_BLOCK_MASK;
          final byte[] bytes = pool.buffers[off >> BYTE_BLOCK_SHIFT];
          final int len;
          int pos;
          if ((bytes[start] & 0x80) == 0) {
            // length is 1 byte
            len = bytes[start];
            pos = start + 1;
          } else {
            len = (bytes[start] & 0x7f) + ((bytes[start + 1] & 0xff) << 7);
            pos = start + 2;
          }
          code = doHash(bytes, pos, len);
        } else {
          code = bytesStart[e0];
        }

        int hashPos = code & newMask;
        assert hashPos >= 0;
        if (newHash[hashPos] != -1) {
          // Conflict; use linear probe to find an open slot
          // (see LUCENE-5604):
          do {
            code++;
            hashPos = code & newMask;
          } while (newHash[hashPos] != -1);
        }
        newHash[hashPos] = e0;
      }
    }

    hashMask = newMask;
    bytesUsed.addAndGet(Integer.BYTES * (-ids.length));
    ids = newHash;
    hashSize = newSize;
    hashHalfSize = newSize / 2;
  }

  // TODO: maybe use long?  But our keys are typically short...
  private int doHash(byte[] bytes, int offset, int length) {
    return StringHelper.murmurhash3_x86_32(bytes, offset, length, StringHelper.GOOD_FAST_HASH_SEED);
  }

  /**
   * reinitializes the {@link BytesRefHash} after a previous {@link #clear()}
   * call. If {@link #clear()} has not been called previously this method has no
   * effect.
   */
  public void reinit() {
    if (bytesStart == null) {
      bytesStart = bytesStartArray.init();
    }
    
    if (ids == null) {
      ids = new int[hashSize];
      bytesUsed.addAndGet(Integer.BYTES * hashSize);
    }
  }

  /**
   * Returns the bytesStart offset into the internally used
   * {@link ByteBlockPool} for the given bytesID
   * 
   * @param bytesID
   *          the id to look up
   * @return the bytesStart offset into the internally used
   *         {@link ByteBlockPool} for the given id
   *         返回 某个byte[] 在pool的偏移量
   */
  public int byteStart(int bytesID) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID >= 0 && bytesID < count : bytesID;
    return bytesStart[bytesID];
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES +
        RamUsageEstimator.sizeOfObject(bytesStart) +
        RamUsageEstimator.sizeOfObject(ids) +
        RamUsageEstimator.sizeOfObject(pool);
    return size;
  }

  /**
   * Thrown if a {@link BytesRef} exceeds the {@link BytesRefHash} limit of
   * {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2.
   */
  @SuppressWarnings("serial")
  public static class MaxBytesLengthExceededException extends RuntimeException {
    MaxBytesLengthExceededException(String message) {
      super(message);
    }
  }

  /** Manages allocation of the per-term addresses. */
  // 类似于 hash桶的  bucket[] 也就是定位key 在 存储value的结构中的位置的  可以将hashMap存储value的结构看成是那个链表
  public abstract static class BytesStartArray {
    /**
     * Initializes the BytesStartArray. This call will allocate memory
     * 
     * @return the initialized bytes start array
     * 为存储 keys 开辟空间
     */
    public abstract int[] init();

    /**
     * Grows the {@link BytesStartArray}
     * 
     * @return the grown array
     * 扩容
     */
    public abstract int[] grow();

    /**
     * clears the {@link BytesStartArray} and returns the cleared instance.
     * 
     * @return the cleared instance, this might be <code>null</code>
     * 清理内部数据
     */
    public abstract int[] clear();

    /**
     * A {@link Counter} reference holding the number of bytes used by this
     * {@link BytesStartArray}. The {@link BytesRefHash} uses this reference to
     * track it memory usage
     * 
     * @return a {@link AtomicLong} reference holding the number of bytes used
     *         by this {@link BytesStartArray}.
     */
    public abstract Counter bytesUsed();
  }

  /** A simple {@link BytesStartArray} that tracks
   *  memory allocation using a private {@link Counter}
   *  instance.  */
  public static class DirectBytesStartArray extends BytesStartArray {
    // TODO: can't we just merge this w/
    // TrackingDirectBytesStartArray...?  Just add a ctor
    // that makes a private bytesUsed?

    protected final int initSize;
    private int[] bytesStart;
    private final Counter bytesUsed;
    
    public DirectBytesStartArray(int initSize, Counter counter) {
      this.bytesUsed = counter;
      this.initSize = initSize;      
    }
    
    public DirectBytesStartArray(int initSize) {
      this(initSize, Counter.newCounter());
    }

    /**
     * 将内部数组置null 后返回
     * @return
     */
    @Override
    public int[] clear() {
      return bytesStart = null;
    }

    @Override
    public int[] grow() {
      assert bytesStart != null;
      return bytesStart = ArrayUtil.grow(bytesStart, bytesStart.length + 1);
    }

    @Override
    public int[] init() {
      return bytesStart = new int[ArrayUtil.oversize(initSize, Integer.BYTES)];
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }
}
