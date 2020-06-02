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


/** Acts like forever growing T[], but internally uses a
 *  circular buffer to reuse instances of T.
 * 
 *  @lucene.internal */
// 应该是一个轮状的存储容器   这就是环形缓冲区啊
public abstract class RollingBuffer<T extends RollingBuffer.Resettable> {

  /**
   * Implement to reset an instance
   */
  public static interface Resettable {
    public void reset();
  }

  /**
   *
   */
  @SuppressWarnings("unchecked") private T[] buffer = (T[]) new RollingBuffer.Resettable[8];

  // Next array index to write to:  这是一个逻辑偏移量  也就是每到轮底 会重置成0
  private int nextWrite;

  // Next position to write:  这是一个物理偏移量  该指针不会重置 会不断增加
  private int nextPos;

  // How many valid Position are held in the
  // array:  记录当前数组内的元素
  private int count;

  public RollingBuffer() {
    for(int idx=0;idx<buffer.length;idx++) {
      buffer[idx] = newInstance();
    }
  }

  protected abstract T newInstance();

  /**
   * 重置环形缓冲区内所有元素
   */
  public void reset() {
    nextWrite--;
    while (count > 0) {
      if (nextWrite == -1) {
        nextWrite = buffer.length - 1;
      }
      buffer[nextWrite--].reset();
      count--;
    }
    nextWrite = 0;
    nextPos = 0;
    count = 0;
  }

  // For assert:
  private boolean inBounds(int pos) {
    return pos < nextPos && pos >= nextPos - count;
  }

  private int getIndex(int pos) {
    int index = nextWrite - (nextPos - pos);
    if (index < 0) {
      index += buffer.length;
    }
    return index;
  }

  /** Get T instance for this absolute position;
   *  this is allowed to be arbitrarily far "in the
   *  future" but cannot be before the last freeBefore. */
  // 获取指定下标对应的元素
  public T get(int pos) {
    //System.out.println("RA.get pos=" + pos + " nextPos=" + nextPos + " nextWrite=" + nextWrite + " count=" + count);
    while (pos >= nextPos) {
      // 代表当前数量已经满了
      if (count == buffer.length) {
        // 进行扩容
        @SuppressWarnings("unchecked") T[] newBuffer = (T[]) new Resettable[ArrayUtil.oversize(1+count, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        //System.out.println("  grow length=" + newBuffer.length);
        // 这里先写入的是 buffer.length-nextWrite 的部分   后写入之前的部分
        System.arraycopy(buffer, nextWrite, newBuffer, 0, buffer.length-nextWrite);
        System.arraycopy(buffer, 0, newBuffer, buffer.length-nextWrite, nextWrite);
        // 为扩容出来的部分 创建新对象填充空间
        for(int i=buffer.length;i<newBuffer.length;i++) {
          newBuffer[i] = newInstance();
        }
        nextWrite = buffer.length;
        buffer = newBuffer;
      }
      if (nextWrite == buffer.length) {
        nextWrite = 0;
      }
      // Should have already been reset:
      nextWrite++;
      nextPos++;
      count++;
    }
    assert inBounds(pos): "pos=" + pos + " nextPos=" + nextPos + " count=" + count;
    final int index = getIndex(pos);
    //System.out.println("  pos=" + pos + " nextPos=" + nextPos + " -> index=" + index);
    //assert buffer[index].pos == pos;
    return buffer[index];
  }

  /** Returns the maximum position looked up, or -1 if no
  *   position has been looked up since reset/init.  */
  public int getMaxPos() {
    return nextPos-1;
  }

  /** Returns how many active positions are in the buffer. */
  public int getBufferSize() {
    return count;
  }

  public void freeBefore(int pos) {
    final int toFree = count - (nextPos - pos);
    assert toFree >= 0;
    assert toFree <= count: "toFree=" + toFree + " count=" + count;
    int index = nextWrite - count;
    if (index < 0) {
      index += buffer.length;
    }
    for(int i=0;i<toFree;i++) {
      if (index == buffer.length) {
        index = 0;
      }
      //System.out.println("  fb idx=" + index);
      buffer[index].reset();
      index++;
    }
    count -= toFree;
  }
}
