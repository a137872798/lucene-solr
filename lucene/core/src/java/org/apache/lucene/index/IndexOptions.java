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
 * 域的哪些信息会存储到索引中
 */

public enum IndexOptions { 
  // NOTE: order is important here; FieldInfo uses this
  // order to merge two conflicting IndexOptions (always
  // "downgrades" by picking the lowest).
  /** Not indexed */  // 不会将域的任何信息存储到索引中  这样就不能通过域来检索到目标文档
  NONE,
  /** 
   * Only documents are indexed: term frequencies and positions are omitted.
   * Phrase and other positional queries on the field will throw an exception, and scoring
   * will behave as if any term in the document appears only once.
   * 域所属的文档号会被写入到索引中
   */
  DOCS,
  /** 
   * Only documents and term frequencies are indexed: positions are omitted. 
   * This enables normal scoring, except Phrase and other positional queries
   * will throw an exception.
   * 将域所属的文档号 以及词频写入到 索引中   但是没有写入位置信息     当使用有关位置的query对象进行搜索时 会抛出异常
   */  
  DOCS_AND_FREQS,
  /** 
   * Indexes documents, frequencies and positions.
   * This is a typical default for full-text search: full scoring is enabled
   * and positional queries are supported.
   * 文档/频率/pos都将被存储  这是支持全文搜索的默认配置
   */
  DOCS_AND_FREQS_AND_POSITIONS,
  /** 
   * Indexes documents, frequencies, positions and offsets.
   * Character offsets are encoded alongside the positions. 
   */
  DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
}
  
