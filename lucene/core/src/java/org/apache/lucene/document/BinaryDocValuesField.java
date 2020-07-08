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
package org.apache.lucene.document;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;

/**
 * Field that stores a per-document {@link BytesRef} value.  
 * <p>
 * The values are stored directly with no sharing, which is a good fit when
 * the fields don't share (many) values, such as a title field.  If values 
 * may be shared and sorted it's better to use {@link SortedDocValuesField}.  
 * Here's an example usage:
 * 
 * <pre class="prettyprint">
 *   document.add(new BinaryDocValuesField(name, new BytesRef("hello")));
 * </pre>
 * 
 * <p>
 * If you also need to store the value, you should add a
 * separate {@link StoredField} instance.
 * 
 * @see BinaryDocValues
 * 代表该 field 关联的docValue 为二进制类型
 * */
public class BinaryDocValuesField extends Field {
  
  /**
   * Type for straight bytes DocValues.
   */
  public static final FieldType TYPE = new FieldType();
  static {
    // 标明该字段类型为 二进制
    TYPE.setDocValuesType(DocValuesType.BINARY);
    // 冻结该属性 无法再被修改
    TYPE.freeze();
  }
  
  /**
   * Create a new binary DocValues field.
   * @param name field name
   * @param value binary content
   * @throws IllegalArgumentException if the field name is null
   */
  public BinaryDocValuesField(String name, BytesRef value) {
    super(name, TYPE);
    fieldsData = value;
  }
}
