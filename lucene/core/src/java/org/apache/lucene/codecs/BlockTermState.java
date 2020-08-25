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
package org.apache.lucene.codecs;

import org.apache.lucene.index.OrdTermState;
import org.apache.lucene.index.TermState;

/**
 * Holds all state required for {@link PostingsReaderBase}
 * to produce a {@link org.apache.lucene.index.PostingsEnum} without re-seeking the
 * terms dict.
 *
 * @lucene.internal
 * 记录某次写入结果
 */
public class BlockTermState extends OrdTermState {
  /** how many docs have this term */
  // 该term 关联了多少doc
  public int docFreq;
  /** total number of occurrences of this term */
  // 该term 在所有的doc中出现了多少次  就是将出现在每个doc中的次数累加起来
  public long totalTermFreq;

  /** the term's ord in the current block */
  // 当前term在 该block的位置
  public int termBlockOrd;
  /** fp into the terms dict primary file (_X.tim) that holds this term */
  // TODO: update BTR to nuke this
  public long blockFilePointer;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected BlockTermState() {
  }

  @Override
  public void copyFrom(TermState _other) {
    assert _other instanceof BlockTermState : "can not copy from " + _other.getClass().getName();
    BlockTermState other = (BlockTermState) _other;
    super.copyFrom(_other);
    docFreq = other.docFreq;
    totalTermFreq = other.totalTermFreq;
    termBlockOrd = other.termBlockOrd;
    blockFilePointer = other.blockFilePointer;
  }

  @Override
  public String toString() {
    return "docFreq=" + docFreq + " totalTermFreq=" + totalTermFreq + " termBlockOrd=" + termBlockOrd + " blockFP=" + blockFilePointer;
  }
}
