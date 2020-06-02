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

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFieldVisitor;

/** A {@link StoredFieldVisitor} that creates a {@link
 *  Document} from stored fields.
 *  <p>
 *  This visitor supports loading all stored fields, or only specific
 *  requested fields provided from a {@link Set}.
 *  <p>
 *  This is used by {@link IndexReader#document(int)} to load a
 *  document.
 *
 * @lucene.experimental */
// storedFieldVisitor 代表将field 存到doc上

public class DocumentStoredFieldVisitor extends StoredFieldVisitor {
  /**
   * 存储传入的 field
   */
  private final Document doc = new Document();
  private final Set<String> fieldsToAdd;

  /** 
   * Load only fields named in the provided <code>Set&lt;String&gt;</code>. 
   * @param fieldsToAdd Set of fields to load, or <code>null</code> (all fields).
   */
  public DocumentStoredFieldVisitor(Set<String> fieldsToAdd) {
    this.fieldsToAdd = fieldsToAdd;
  }

  /** Load only fields named in the provided fields. */
  public DocumentStoredFieldVisitor(String... fields) {
    fieldsToAdd = new HashSet<>(fields.length);
    for(String field : fields) {
      fieldsToAdd.add(field);
    }
  }

  /** Load all stored fields. */
  public DocumentStoredFieldVisitor() {
    this.fieldsToAdd = null;
  }

  /**
   * 添加一个二进制形式的field
   * @param fieldInfo
   * @param value newly allocated byte array with the binary contents.
   * @throws IOException
   */
  @Override
  public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
    doc.add(new StoredField(fieldInfo.name, value));
  }

  /**
   * 为 doc 增加一个 string 类型的 field
   * @param fieldInfo
   * @param value
   * @throws IOException
   */
  @Override
  public void stringField(FieldInfo fieldInfo, String value) throws IOException {
    // 从fieldInfo 中剥离某些属性 转移到 fieldType上
    final FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(fieldInfo.hasVectors());
    ft.setOmitNorms(fieldInfo.omitsNorms());
    ft.setIndexOptions(fieldInfo.getIndexOptions());
    doc.add(new StoredField(fieldInfo.name, Objects.requireNonNull(value, "String value should not be null"), ft));
  }

  @Override
  public void intField(FieldInfo fieldInfo, int value) {
    doc.add(new StoredField(fieldInfo.name, value));
  }

  @Override
  public void longField(FieldInfo fieldInfo, long value) {
    doc.add(new StoredField(fieldInfo.name, value));
  }

  @Override
  public void floatField(FieldInfo fieldInfo, float value) {
    doc.add(new StoredField(fieldInfo.name, value));
  }

  @Override
  public void doubleField(FieldInfo fieldInfo, double value) {
    doc.add(new StoredField(fieldInfo.name, value));
  }

  @Override
  public Status needsField(FieldInfo fieldInfo) throws IOException {
    return fieldsToAdd == null || fieldsToAdd.contains(fieldInfo.name) ? Status.YES : Status.NO;
  }

  /**
   * Retrieve the visited document.
   * @return {@link Document} populated with stored fields. Note that only
   *         the stored information in the field instances is valid,
   *         data such as indexing options, term vector options,
   *         etc is not set.
   */
  public Document getDocument() {
    return doc;
  }
}
