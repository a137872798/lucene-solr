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
package org.apache.lucene.search;


/** Holds one hit in {@link TopDocs}. */
// 记录 IndexSearcher 的搜索结果  score代表结果文档的得分  doc是文档的id

public class ScoreDoc {

  /** The score of this document for the query. */
  public float score;

  /** A hit document's number.
   * @see IndexSearcher#doc(int) */
  public int doc;

  /** Only set by {@link TopDocs#merge}*/
  public int shardIndex;

  /** Constructs a ScoreDoc. */
  public ScoreDoc(int doc, float score) {
    this(doc, score, -1);
  }

  /** Constructs a ScoreDoc. */
  public ScoreDoc(int doc, float score, int shardIndex) {
    this.doc = doc;
    this.score = score;
    this.shardIndex = shardIndex;
  }
  
  // A convenience method for debugging.
  @Override
  public String toString() {
    return "doc=" + doc + " score=" + score + " shardIndex=" + shardIndex;
  }
}
