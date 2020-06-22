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

package org.apache.lucene.index;

import java.io.IOException;

/**
 * A per-document numeric value.
 * 记录每个文档 以数字方式记录的值
 */
public abstract class NumericDocValues extends DocValuesIterator {
  
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected NumericDocValues() {}

  /**
   * Returns the numeric value for the current document ID.
   * It is illegal to call this method after {@link #advanceExact(int)}
   * returned {@code false}.
   * @return numeric value
   * 配合 advanceExact() 使用
   * 前一个方法判断指定偏移量是否有值  该方法获取当前指针指向的值
   */
  public abstract long longValue() throws IOException;

}
