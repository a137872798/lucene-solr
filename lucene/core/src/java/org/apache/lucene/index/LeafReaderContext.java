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


import java.util.Collections;
import java.util.List;

/**
 * {@link IndexReaderContext} for {@link LeafReader} instances.
 * 代表一个基于 LeafReader的上下文对象
 */
public final class LeafReaderContext extends IndexReaderContext {
  /** The reader's ord in the top-level's leaves array */
  // 记录本节点的顺序
  public final int ord;
  /** The reader's absolute doc base */
  // 对于doc的绝对顺序
  public final int docBase;
  
  private final LeafReader reader;
  /**
   * 该对象也可以有一组叶子节点
   */
  private final List<LeafReaderContext> leaves;
  
  /**
   * Creates a new {@link LeafReaderContext} 
   */    
  LeafReaderContext(CompositeReaderContext parent, LeafReader reader,
                    int ord, int docBase, int leafOrd, int leafDocBase) {
    super(parent, ord, docBase);
    this.ord = leafOrd;
    this.docBase = leafDocBase;
    this.reader = reader;
    this.leaves = isTopLevel ? Collections.singletonList(this) : null;
  }
  
  LeafReaderContext(LeafReader leafReader) {
    this(null, leafReader, 0, 0, 0, 0);
  }
  
  @Override
  public List<LeafReaderContext> leaves() {
    if (!isTopLevel) {
      throw new UnsupportedOperationException("This is not a top-level context.");
    }
    assert leaves != null;
    return leaves;
  }
  
  @Override
  public List<IndexReaderContext> children() {
    return null;
  }
  
  @Override
  public LeafReader reader() {
    return reader;
  }

  @Override
  public String toString() {
    return "LeafReaderContext(" + reader + " docBase=" + docBase + " ord=" + ord + ")";
  }
}
