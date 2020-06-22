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
import java.util.HashMap;
import java.util.Map;

/**
 * A ring buffer that tracks the frequency of the integers that it contains.
 * This is typically useful to track the hash codes of popular recently-used
 * items.
 *
 * This data-structure requires 22 bytes per entry on average (between 16 and
 * 28).
 *
 * @lucene.internal
 * 环形缓冲区 也就是利用了轮式算法
 * 这个key 应该就是 docId
 */
public final class FrequencyTrackingRingBuffer implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FrequencyTrackingRingBuffer.class);

  /**
   * 数组大小
   */
  private final int maxSize;
  /**
   * 轮对象
   */
  private final int[] buffer;
  private int position;
  /**
   * 内部维护2个数组 一个存储key 一个存储key对应的频率  通过线性探测法解决冲突
   */
  private final IntBag frequencies;

  /** Create a new ring buffer that will contain at most <code>maxSize</code> items.
   *  This buffer will initially contain <code>maxSize</code> times the
   *  <code>sentinel</code> value. */
  public FrequencyTrackingRingBuffer(int maxSize, int sentinel) {
    if (maxSize < 2) {
      throw new IllegalArgumentException("maxSize must be at least 2");
    }
    this.maxSize = maxSize;
    buffer = new int[maxSize];
    position = 0;
    // 通过指定最大长度 生成存储频率的对象
    frequencies = new IntBag(maxSize);

    // 使用 哨兵填充数组
    Arrays.fill(buffer, sentinel);
    for (int i = 0; i < maxSize; ++i) {
      frequencies.add(sentinel);
    }
    assert frequencies.frequency(sentinel) == maxSize;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED
        + frequencies.ramBytesUsed()
        + RamUsageEstimator.sizeOf(buffer);
  }

  /**
   * Add a new item to this ring buffer, potentially removing the oldest
   * entry from this buffer if it is already full.
   * 存储某个key 同时增加频率   每次add都会伴随一次remove 频率能上去吗???
   */
  public void add(int i) {
    // remove the previous value
    final int removed = buffer[position];
    // 目标位置之前是否有存放数据
    final boolean removedFromBag = frequencies.remove(removed);
    assert removedFromBag;
    // add the new value
    // 正常情况下 每次插入某个值 position 都会增加
    buffer[position] = i;
    frequencies.add(i);
    // increment the position
    position += 1;
    // 轮式结构  重置为0
    if (position == maxSize) {
      position = 0;
    }
  }

  /**
   * Returns the frequency of the provided key in the ring buffer.
   * 获取某个key的频率
   */
  public int frequency(int key) {
    return frequencies.frequency(key);
  }

  // pkg-private for testing
  Map<Integer, Integer> asFrequencyMap() {
    return frequencies.asMap();
  }

  /**
   * A bag of integers.
   * Since in the context of the ring buffer the maximum size is known up-front
   * there is no need to worry about resizing the underlying storage.
   */
  private static class IntBag implements Accountable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IntBag.class);

    private final int[] keys;
    /**
     * 对应key的频率
     */
    private final int[] freqs;

    private final int mask;

    /**
     * 执行 bag的大小
     * @param maxSize
     */
    IntBag(int maxSize) {
      // load factor of 2/3
      int capacity = Math.max(2, maxSize * 3 / 2);
      // round up to the next power of two
      // 就是向上取 2^n
      capacity = Integer.highestOneBit(capacity - 1) << 1;
      assert capacity > maxSize;
      keys = new int[capacity];
      freqs = new int[capacity];
      mask = capacity - 1;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(keys)
          + RamUsageEstimator.sizeOf(freqs);
    }

    /** Return the frequency of the give key in the bag. */
    // 返回key 对应的频率
    int frequency(int key) {
      for (int slot = key & mask; ; slot = (slot + 1) & mask) {
        // 代表插入该key时 没有发生冲突 所以可以正常获取频率
        if (keys[slot] == key) {
          return freqs[slot];
        } else if (freqs[slot] == 0) {
          return 0;
        }
      }
    }

    /** Increment the frequency of the given key by 1 and return its new frequency. */
    int add(int key) {
      // 首先通过与掩码做 & 运算 获取槽
      for (int slot = key & mask; ; slot = (slot + 1) & mask) {
        // 占据坑后 设置频率
        if (freqs[slot] == 0) {
          keys[slot] = key;
          return freqs[slot] = 1;
        // 当前上个占据坑的key 与本次的key 相同 直接增加频率
        } else if (keys[slot] == key) {
          return ++freqs[slot];
        }
        // 当发现冲突时 使用线性探测法解决冲突
      }
    }

    /** Decrement the frequency of the given key by one, or do nothing if the
     *  key is not present in the bag. Returns true iff the key was contained
     *  in the bag. */
    // 移除掉某个key  应该还要将因为冲突而后移的数据 前移  这样才能确保 frequency() 逻辑正确
    boolean remove(int key) {
      for (int slot = key & mask; ; slot = (slot + 1) & mask) {
        if (freqs[slot] == 0) {
          // no such key in the bag
          return false;
        } else if (keys[slot] == key) {
          final int newFreq = --freqs[slot];
          if (newFreq == 0) { // removed
            relocateAdjacentKeys(slot);
          }
          return true;
        }
      }
    }

    /**
     * 调整因线性探测法带来的副作用
     * @param freeSlot
     */
    private void relocateAdjacentKeys(int freeSlot) {
      for (int slot = (freeSlot + 1) & mask; ; slot = (slot + 1) & mask) {
        final int freq = freqs[slot];
        // 代表没有发生冲突 不需要前移
        if (freq == 0) {
          // end of the collision chain, we're done
          break;
        }
        final int key = keys[slot];
        // the slot where <code>key</code> should be if there were no collisions
        // 找到应该存在的slot
        final int expectedSlot = key & mask;
        // if the free slot is between the expected slot and the slot where the
        // key is, then we can relocate there
        if (between(expectedSlot, slot, freeSlot)) {
          keys[freeSlot] = key;
          freqs[freeSlot] = freq;
          // slot is the new free slot
          freqs[slot] = 0;
          freeSlot = slot;
        }
      }
    }

    /** Given a chain of occupied slots between <code>chainStart</code>
     *  and <code>chainEnd</code>, return whether <code>slot</code> is
     *  between the start and end of the chain.
     * @param chainStart 某个值起始在的位置
     * @param chainEnd 因冲突导致当前存放的位置
     * @param slot 当前空缺的位置     目标是将 chainEnd 上的数据移动到 slot上
     */
    private static boolean between(int chainStart, int chainEnd, int slot) {
      if (chainStart <= chainEnd) {
        return chainStart <= slot && slot <= chainEnd;
      } else {
        // the chain is across the end of the array
        // 代表跨轮了
        return slot >= chainStart || slot <= chainEnd;
      }
    }

    Map<Integer, Integer> asMap() {
      Map<Integer, Integer> map = new HashMap<>();
      for (int i = 0; i < keys.length; ++i) {
        if (freqs[i] > 0) {
          map.put(keys[i], freqs[i]);
        }
      }
      return map;
    }

  }

}
