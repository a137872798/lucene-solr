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
import java.util.Comparator;
import java.util.Objects;
import java.util.function.IntBinaryOperator;

/**
 * A simple append only random-access {@link BytesRef} array that stores full
 * copies of the appended bytes in a {@link ByteBlockPool}.
 * 
 * 
 * <b>Note: This class is not Thread-Safe!</b>
 * 
 * @lucene.internal
 * @lucene.experimental
 * 存放 一组 byteRef 并且具备将他们排序的能力
 */
public final class BytesRefArray implements SortableBytesRefArray {

  /**
   * 该对象可以看作一个大内存对象 内部有多个 bytesRef   一开始无法正常估算 或者说 提前预分配过大的内存会有弊端  所以lucene 通过池化对象 并以二维数组的形式
   * 将多个bytesRef 连接在一起 形成了一个逻辑上的大内存
   */
  private final ByteBlockPool pool;
  /**
   * 这里记录了每个 bytesRef 的偏移量
   */
  private int[] offsets = new int[1];
  /**
   * 记录写入了多少数据
   */
  private int lastElement = 0;
  /**
   * 当前 bytesRef的 起始偏移量
   */
  private int currentOffset = 0;
  private final Counter bytesUsed;

  /**
   * Creates a new {@link BytesRefArray} with a counter to track allocated bytes
   * @param bytesUsed 用于记录在这里分配了多少内存
   */
  public BytesRefArray(Counter bytesUsed) {
    // 每次使用这个pool对象时  当分配内存的时候 都会间接操作 counter对象 更新数值
    this.pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(
        bytesUsed));
    // 创建首个 buffer
    pool.nextBuffer();
    bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER * Integer.BYTES);
    this.bytesUsed = bytesUsed;
  }
 
  /**
   * Clears this {@link BytesRefArray}
   */
  @Override
  public void clear() {
    lastElement = 0;
    currentOffset = 0;
    // TODO: it's trappy that this does not return storage held by int[] offsets array!
    Arrays.fill(offsets, 0);
    pool.reset(false, true); // no need to 0 fill the buffers we control the allocator
  }
  
  /**
   * Appends a copy of the given {@link BytesRef} to this {@link BytesRefArray}.
   * @param bytes the bytes to append
   * @return the index of the appended bytes   返回值是写入的下标 从0开始
   * 将 bytes的数据 写入到 pool 中
   */
  @Override
  public int append(BytesRef bytes) {
    if (lastElement >= offsets.length) {
      int oldLen = offsets.length;
      offsets = ArrayUtil.grow(offsets, offsets.length + 1);
      bytesUsed.addAndGet((offsets.length - oldLen) * Integer.BYTES);
    }
    // 将bytes 内部的数据写入到  pool 中
    pool.append(bytes);
    offsets[lastElement++] = currentOffset;
    // 更新偏移量 作为下个 bytes的起点 并在下次写入时 存入到 offsets[] 中
    currentOffset += bytes.length;
    return lastElement-1;
  }
  
  /**
   * Returns the current size of this {@link BytesRefArray}
   * @return the current size of this {@link BytesRefArray}
   */
  @Override
  public int size() {
    return lastElement;
  }
  
  /**
   * Returns the <i>n'th</i> element of this {@link BytesRefArray}
   * @param spare a spare {@link BytesRef} instance
   * @param index the elements index to retrieve 
   * @return the <i>n'th</i> element of this {@link BytesRefArray}
   * 通过指定下标 在 offsets[] 中找到偏移量 然后去pool中读取数据
   */
  public BytesRef get(BytesRefBuilder spare, int index) {
    Objects.checkIndex(index, lastElement);
    int offset = offsets[index];
    int length = index == lastElement - 1 ? currentOffset - offset
        : offsets[index + 1] - offset;
    spare.grow(length);
    spare.setLength(length);
    pool.readBytes(offset, spare.bytes(), 0, spare.length());
    return spare.get();
  }

  /** Used only by sort below, to set a {@link BytesRef} with the specified slice, avoiding copying bytes in the common case when the slice
   *  is contained in a single block in the byte block pool.
   * @param index 代表读取第几个 bytesRef
   */
  private void setBytesRef(BytesRefBuilder spare, BytesRef result, int index) {
    Objects.checkIndex(index, lastElement);
    // 这个offset 对应某次写入的 bytes的起始偏移量
    int offset = offsets[index];
    int length;
    // 计算写入bytes的长度
    if (index == lastElement - 1) {
      length = currentOffset - offset;
    } else {
      length = offsets[index + 1] - offset;
    }
    pool.setBytesRef(spare, result, offset, length);
  }


  /**
   * Returns a {@link SortState} representing the order of elements in this array. This is a non-destructive operation.
   * 将该对象内部的数据 按照 cmp对象进行排序
   */
  public SortState sort(final Comparator<BytesRef> comp, final IntBinaryOperator tieComparator) {
    // size() 对应当前写入的 bytes 数量
    final int[] orderedEntries = new int[size()];
    for (int i = 0; i < orderedEntries.length; i++) {
      orderedEntries[i] = i;
    }
    // 该对象使用快速排序算法
    new IntroSorter() {
      @Override
      protected void swap(int i, int j) {
        final int o = orderedEntries[i];
        orderedEntries[i] = orderedEntries[j];
        orderedEntries[j] = o;
      }

      /**
       * 将基于下标值的比较 转换成 内部数据值的比较
       * @param i
       * @param j
       * @return
       */
      @Override
      protected int compare(int i, int j) {
        // 获取存储的 下标值
        final int idx1 = orderedEntries[i], idx2 = orderedEntries[j];
        // 从pool 中将数据读取出来 并设置到 scratchBytes1 ， scratchBytes2 内
        setBytesRef(scratch1, scratchBytes1, idx1);
        setBytesRef(scratch2, scratchBytes2, idx2);
        return compare(idx1, scratchBytes1, idx2, scratchBytes2);
      }
      
      @Override
      protected void setPivot(int i) {
        pivotIndex = orderedEntries[i];
        setBytesRef(pivotBuilder, pivot, pivotIndex);
      }
      
      @Override
      protected int comparePivot(int j) {
        final int index = orderedEntries[j];
        setBytesRef(scratch2, scratchBytes2, index);
        return compare(pivotIndex, pivot, index, scratchBytes2);
      }

      // 先根据 偏移量获取到 写入的值 在比较写入的值之后  当相同时 转而比较 docId
      private int compare(int i1, BytesRef b1, int i2, BytesRef b2) {
        int res = comp.compare(b1, b2);
        return res == 0 ? tieComparator.applyAsInt(i1, i2) : res;
      }

      private int pivotIndex;
      private final BytesRef pivot = new BytesRef();
      private final BytesRef scratchBytes1 = new BytesRef();
      private final BytesRef scratchBytes2 = new BytesRef();
      private final BytesRefBuilder pivotBuilder = new BytesRefBuilder();
      private final BytesRefBuilder scratch1 = new BytesRefBuilder();
      private final BytesRefBuilder scratch2 = new BytesRefBuilder();
    }.sort(0, size());
    // 存储排序结果
    return new SortState(orderedEntries);
  }
  
  /**
   * sugar for {@link #iterator(Comparator)} with a <code>null</code> comparator
   */
  public BytesRefIterator iterator() {
    return iterator((SortState) null);
  }
  
  /**
   * <p>
   * Returns a {@link BytesRefIterator} with point in time semantics. The
   * iterator provides access to all so far appended {@link BytesRef} instances.
   * </p>
   * <p>
   * If a non <code>null</code> {@link Comparator} is provided the iterator will
   * iterate the byte values in the order specified by the comparator. Otherwise
   * the order is the same as the values were appended.
   * </p>
   * <p>
   * This is a non-destructive operation.
   * </p>
   */
  @Override
  public BytesRefIterator iterator(final Comparator<BytesRef> comp) {
    return iterator(sort(comp, (i, j) -> 0));
  }

  /**
   * Returns an {@link IndexedBytesRefIterator} with point in time semantics. The iterator provides access to all
   * so far appended {@link BytesRef} instances. If a non-null sortState is specified then the iterator will iterate
   * the byte values in the order of the sortState; otherwise, the order is the same as the values were appended.
   * @param sortState  定义了 读取数据的 偏移量顺序
   */
  public IndexedBytesRefIterator iterator(final SortState sortState) {
    final int size = size();
    final int[] indices = sortState == null ? null : sortState.indices;
    assert indices == null || indices.length == size : indices.length + " != " + size;

    // 这2个对象是 从pool 读取数据的搬运工
    final BytesRefBuilder spare = new BytesRefBuilder();
    final BytesRef result = new BytesRef();

    return new IndexedBytesRefIterator() {
      int pos = -1;
      int ord = 0;
      @Override
      public BytesRef next() {
        ++pos;
        if (pos < size) {
          // 当 indices 为 null 时 使用插入的顺序 否则按照sortState内的顺序
          ord = indices == null ? pos : indices[pos];
          setBytesRef(spare, result, ord);
          return result;
        }
        return null;
      }

      /**
       * 获取当前的顺序
       * @return
       */
      @Override
      public int ord() {
        return ord;
      }
    };
  }

  /**
   * Used to iterate the elements of an array in a given order.
   */
  public final static class SortState implements Accountable {
    private final int[] indices;

    private SortState(int[] indices) {
      this.indices = indices;
    }

    @Override
    public long ramBytesUsed() {
      return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + indices.length * Integer.BYTES;
    }
  }

  /**
   * An extension of {@link BytesRefIterator} that allows retrieving the index of the current element
   */
  public interface IndexedBytesRefIterator extends BytesRefIterator {
    /**
     * Returns the ordinal position of the element that was returned in the latest call of {@link #next()}.
     * Do not call this method if {@link #next()} is not called yet or the last call returned a null value.
     */
    int ord();
  }
}
