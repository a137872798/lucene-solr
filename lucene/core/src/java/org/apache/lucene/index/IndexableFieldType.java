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


import java.util.Map;

import org.apache.lucene.analysis.Analyzer; // javadocs

/** 
 * Describes the properties of a field.
 * @lucene.experimental
 * 代表索引中某个字段的类型
 */
public interface IndexableFieldType {

  /** True if the field's value should be stored */ // 该字段是否应该被排序
  public boolean stored();
  
  /** 
   * True if this field's value should be analyzed by the
   * {@link Analyzer}.
   * <p>
   * This has no effect if {@link #indexOptions()} returns
   * IndexOptions.NONE.
   */
  // TODO: shouldn't we remove this?  Whether/how a field is
  // tokenized is an impl detail under Field?   该字段是否需要被分析
  public boolean tokenized();

  /** 
   * True if this field's indexed form should be also stored 
   * into term vectors.
   * <p>
   * This builds a miniature inverted-index for this field which
   * can be accessed in a document-oriented way from 
   * {@link IndexReader#getTermVector(int,String)}.
   * <p>
   * This option is illegal if {@link #indexOptions()} returns
   * IndexOptions.NONE.
   * 是否应该存储向量
   */
  public boolean storeTermVectors();

  /** 
   * True if this field's token character offsets should also
   * be stored into term vectors.
   * <p>
   * This option is illegal if term vectors are not enabled for the field
   * ({@link #storeTermVectors()} is false)
   * 代表偏移量是否应该被保存
   */
  public boolean storeTermVectorOffsets();

  /** 
   * True if this field's token positions should also be stored
   * into the term vectors.
   * <p>
   * This option is illegal if term vectors are not enabled for the field
   * ({@link #storeTermVectors()} is false).
   * 指针是否应该被保存  指针和偏移量的区别是什么???
   */
  public boolean storeTermVectorPositions();
  
  /** 
   * True if this field's token payloads should also be stored
   * into the term vectors.
   * <p>
   * This option is illegal if term vector positions are not enabled 
   * for the field ({@link #storeTermVectors()} is false).
   * token负载的东西是否应该被保存
   */
  public boolean storeTermVectorPayloads();

  /**
   * True if normalization values should be omitted for the field.
   * <p>
   * This saves memory, but at the expense of scoring quality (length normalization
   * will be disabled), and if you omit norms, you cannot use index-time boosts.
   * 是否省略规范化
   */
  public boolean omitNorms();

  /** {@link IndexOptions}, describing what should be
   *  recorded into the inverted index */   // 代表对应的索引需要存储哪些信息
  public IndexOptions indexOptions();

  /** 
   * DocValues {@link DocValuesType}: how the field's value will be indexed
   * into docValues.
   * 代表文档的数据类型
   */
  public DocValuesType docValuesType();

  /**
   * If this is positive (representing the number of point dimensions), the field is indexed as a point.
   */
  public int pointDimensionCount();

  /**
   * The number of dimensions used for the index key
   * 索引键的维度
   */
  public int pointIndexDimensionCount();

  /**
   * The number of bytes in each dimension's values.
   * 维度的字节数
   */
  public int pointNumBytes();

  /**
   * Attributes for the field type.
   *
   * Attributes are not thread-safe, user must not add attributes while other threads are indexing documents
   * with this field type.
   *
   * @return Map
   */
  public Map<String, String> getAttributes();
}
