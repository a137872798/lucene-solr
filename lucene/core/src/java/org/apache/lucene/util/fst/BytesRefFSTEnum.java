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
package org.apache.lucene.util.fst;


import java.io.IOException;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** Enumerates all input (BytesRef) + output pairs in an
 *  FST.
 *
  * @lucene.experimental
 * 该对象负责读取 fst内部的数据
*/

public final class BytesRefFSTEnum<T> extends FSTEnum<T> {

  /**
   * 存储的是 某个input  在lucene中存入fst的数据包含一个字面量(实际上就是term) 以及关联的权重值实际上就是(docId)
   */
  private final BytesRef current = new BytesRef(10);
  private final InputOutput<T> result = new InputOutput<>();
  private BytesRef target;

  /** Holds a single input (BytesRef) + output pair. */
  public static class InputOutput<T> {
    public BytesRef input;
    public T output;
  }

  /** doFloor controls the behavior of advance: if it's true
   *  doFloor is true, advance positions to the biggest
   *  term before target.  */
  public BytesRefFSTEnum(FST<T> fst) {
    super(fst);
    result.input = current;
    current.offset = 1;
  }

  public InputOutput<T> current() {
    return result;
  }

  public InputOutput<T> next() throws IOException {
    //System.out.println("  enum.next");
    doNext();
    return setResult();
  }

  /** Seeks to smallest term that's &gt;= target. */
  // 查找大于等于target的最小值
  public InputOutput<T> seekCeil(BytesRef target) throws IOException {
    this.target = target;
    targetLength = target.length;
    super.doSeekCeil();
    return setResult();
  }

  /** Seeks to biggest term that's &lt;= target. */
  // 找到<=target的最大值
  public InputOutput<T> seekFloor(BytesRef target) throws IOException {
    this.target = target;
    targetLength = target.length;
    super.doSeekFloor();
    return setResult();
  }

  /** Seeks to exactly this term, returning null if the term
   *  doesn't exist.  This is faster than using {@link
   *  #seekFloor} or {@link #seekCeil} because it
   *  short-circuits as soon the match is not found. */
  // 从 fst中精确匹配 target的值
  public InputOutput<T> seekExact(BytesRef target) throws IOException {
    this.target = target;
    targetLength = target.length;
    if (doSeekExact()) {
      assert upto == 1+target.length;
      return setResult();
    } else {
      return null;
    }
  }

  @Override
  protected int getTargetLabel() {
    // 代表此时读取到末尾了
    if (upto-1 == target.length) {
      return FST.END_LABEL;
    } else {
      return target.bytes[target.offset + upto - 1] & 0xFF;
    }
  }

  @Override
  protected int getCurrentLabel() {
    // current.offset fixed at 1
    return current.bytes[upto] & 0xFF;
  }

  /**
   * 设置字面量的值  字面量可以按 byte存储 或者int存储
   * @param label
   */
  @Override
  protected void setCurrentLabel(int label) {
    current.bytes[upto] = (byte) label;
  }

  @Override
  protected void grow() {
    current.bytes = ArrayUtil.grow(current.bytes, upto+1);
  }

  private InputOutput<T> setResult() {
    if (upto == 0) {
      return null;
    } else {
      // result.current 就是term数据
      current.length = upto-1;
      // 将权重信息设置到 result上
      result.output = output[upto];
      return result;
    }
  }
}
