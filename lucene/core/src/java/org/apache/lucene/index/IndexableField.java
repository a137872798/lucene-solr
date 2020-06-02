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


import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.BytesRef;

// TODO: how to handle versioning here...?

/** Represents a single field for indexing.  IndexWriter
 *  consumes Iterable&lt;IndexableField&gt; as a document.
 *
 *  @lucene.experimental */
// 代表索引中的某个字段    IndexWriter 每次都是以 Field为单位
public interface IndexableField {

  /** Field name */ // 返回该字符的名称
  public String name();

  /** {@link IndexableFieldType} describing the properties
   * of this field. */ // 该字符的类型
  public IndexableFieldType fieldType();

  /**
   * Creates the TokenStream used for indexing this field.  If appropriate,
   * implementations should use the given Analyzer to create the TokenStreams.
   *
   * @param analyzer Analyzer that should be used to create the TokenStreams from
   * @param reuse TokenStream for a previous instance of this field <b>name</b>. This allows
   *              custom field types (like StringField and NumericField) that do not use
   *              the analyzer to still have good performance. Note: the passed-in type
   *              may be inappropriate, for example if you mix up different types of Fields
   *              for the same field name. So it's the responsibility of the implementation to
   *              check.
   * @return TokenStream value for indexing the document.  Should always return
   *         a non-null value if the field is to be indexed
   *         将该字段转换成一个 token流
   */
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse);

  /** Non-null if this field has a binary value */
  // 以二进制数据流的方式读取索引字段
  public BytesRef binaryValue();

  /** Non-null if this field has a string value */
  // 以字符串的方式读取索引字段
  public String stringValue();

  /**
   * Non-null if this field has a string value
   */
  default CharSequence getCharSequenceValue() {
    return stringValue();
  }

  /** Non-null if this field has a Reader value */
  // 将数据以输入流的形式返回
  public Reader readerValue();

  /** Non-null if this field has a numeric value */
  // 将数据以数字形式返回
  public Number numericValue();
}
