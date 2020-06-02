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


/**
 * Controls how much information is stored in the postings lists.
 * @lucene.experimental
 * 代表索引中应该存储什么信息
 */

public enum IndexOptions { 
  // NOTE: order is important here; FieldInfo uses this
  // order to merge two conflicting IndexOptions (always
  // "downgrades" by picking the lowest).
  /** Not indexed */
  NONE,
  /** 
   * Only documents are indexed: term frequencies and positions are omitted.
   * Phrase and other positional queries on the field will throw an exception, and scoring
   * will behave as if any term in the document appears only once.
   * 索引中只存储文档信息
   */
  DOCS,
  /** 
   * Only documents and term frequencies are indexed: positions are omitted. 
   * This enables normal scoring, except Phrase and other positional queries
   * will throw an exception.
   * 文档和词的频率都将被索引
   */  
  DOCS_AND_FREQS,
  /** 
   * Indexes documents, frequencies and positions.
   * This is a typical default for full-text search: full scoring is enabled
   * and positional queries are supported.
   * 文档/频率/pos都将被存储   这是一个典型的全文本查询
   */
  DOCS_AND_FREQS_AND_POSITIONS,
  /** 
   * Indexes documents, frequencies, positions and offsets.
   * Character offsets are encoded alongside the positions. 
   */
  DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
}
  
