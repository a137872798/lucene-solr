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


import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides a merged sorted view from several sorted iterators.
 * <p>
 * If built with <code>removeDuplicates</code> set to true and an element
 * appears in multiple iterators then it is deduplicated, that is this iterator
 * returns the sorted union of elements.
 * <p>
 * If built with <code>removeDuplicates</code> set to false then all elements
 * in all iterators are returned.
 * <p>
 * Caveats:
 * <ul>
 *   <li>The behavior is undefined if the iterators are not actually sorted.
 *   <li>Null elements are unsupported.
 *   <li>If removeDuplicates is set to true and if a single iterator contains
 *       duplicates then they will not be deduplicated.
 *   <li>When elements are deduplicated it is not defined which one is returned.
 *   <li>If removeDuplicates is set to false then the order in which duplicates
 *       are returned isn't defined.
 * </ul>
 * @lucene.internal
 * 该对象将多个迭代器整合起来
 */
public final class MergedIterator<T extends Comparable<T>> implements Iterator<T> {

  private T current;
  private final TermMergeQueue<T> queue; 
  private final SubIterator<T>[] top;

  /**
   * 遇到相同的数据时是否需要移除 默认为true
   */
  private final boolean removeDuplicates;
  private int numTop;

  @SuppressWarnings({"unchecked","rawtypes"})
  public MergedIterator(Iterator<T>... iterators) {
    this(true, iterators);
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  /**
   * @param removeDuplicates 是否要移除重复的 field
   */
  public MergedIterator(boolean removeDuplicates, Iterator<T>... iterators) {
    this.removeDuplicates = removeDuplicates;
    queue = new TermMergeQueue<>(iterators.length);
    top = new SubIterator[iterators.length];
    int index = 0;
    for (Iterator<T> iterator : iterators) {
      // 确保该迭代器是有效的  (也就是有field信息)
      if (iterator.hasNext()) {
        SubIterator<T> sub = new SubIterator<>();
        sub.current = iterator.next();
        sub.iterator = iterator;
        sub.index = index++;
        queue.add(sub);
      }
    }
  }
  
  @Override
  public boolean hasNext() {
    if (queue.size() > 0) {
      return true;
    }
    
    for (int i = 0; i < numTop; i++) {
      if (top[i].iterator.hasNext()) {
        return true;
      }
    }
    return false;
  }

  /**
   * 获取下一个元素
   * @return
   */
  @Override
  public T next() {
    // restore queue
    // 将之前弹出的所有迭代器重新入堆 并维护堆结构
    pushTop();
    
    // gather equal top elements
    // 此时队列中有数据 从top拉取数据
    if (queue.size() > 0) {
      pullTop();
    } else {
      current = null;
    }
    // 代表此时已经没有数据了
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }
  
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * 初始化时 将数据存入优先队列 在 next() 时 从优先队列中取出已经排序过的top元素
   */
  private void pullTop() {
    assert numTop == 0;
    // 弹出的是剩余的迭代器中 首个元素最小的那个迭代器  注意每次会从堆中弹出
    top[numTop++] = queue.pop();
    if (removeDuplicates) {
      // extract all subs from the queue that have the same top element
      // 尝试将剩余迭代器的最小值取出来做比较 如果大小一致 也弹出 并暂存在top中
      while (queue.size() != 0
             && queue.top().current.equals(top[0].current)) {
        top[numTop++] = queue.pop();
      }
    }
    current = top[0].current;
  }

  /**
   * 将之前取出的迭代器 回填到队列中
   */
  private void pushTop() {
    // call next() on each top, and put back into queue
    for (int i = 0; i < numTop; i++) {
      if (top[i].iterator.hasNext()) {
        // 更新当前元素 并通过比较该元素形成新的堆结构
        top[i].current = top[i].iterator.next();
        queue.add(top[i]);
      } else {
        // no more elements
        top[i].current = null;
      }
    }
    numTop = 0;
  }
  
  private static class SubIterator<I extends Comparable<I>> {
    Iterator<I> iterator;
    I current;
    int index;
  }

  /**
   * 该对象定义了 比较大小的规则
   * @param <C>
   */
  private static class TermMergeQueue<C extends Comparable<C>> extends PriorityQueue<SubIterator<C>> {
    TermMergeQueue(int size) {
      // 规定二叉堆的大小
      super(size);
    }
    
    @Override
    protected boolean lessThan(SubIterator<C> a, SubIterator<C> b) {
      // 先比较field的大小 如果相同再比较 index
      final int cmp = a.current.compareTo(b.current);
      if (cmp != 0) {
        return cmp < 0;
      } else {
        return a.index < b.index;
      }
    }
  }
}
