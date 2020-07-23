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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

/**
 * LeafReader implemented by codec APIs.
 * 该对象用于统筹基于各种格式读取数据的 reader 并维护一个 fieldInfo 列表 只要查询的field能够在fieldInfo中找到 配合指定的reader  实现功能
 */
public abstract class CodecReader extends LeafReader implements Accountable {
  
  /** Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.) */
  protected CodecReader() {}

  // 根据需要查询的数据  返回对应格式的reader
  /** 
   * Expert: retrieve thread-private StoredFieldsReader
   * @lucene.internal 
   */
  public abstract StoredFieldsReader getFieldsReader();
  
  /** 
   * Expert: retrieve thread-private TermVectorsReader
   * @lucene.internal 
   */
  public abstract TermVectorsReader getTermVectorsReader();
  
  /** 
   * Expert: retrieve underlying NormsProducer
   * @lucene.internal 
   */
  public abstract NormsProducer getNormsReader();
  
  /** 
   * Expert: retrieve underlying DocValuesProducer
   * @lucene.internal 
   */
  public abstract DocValuesProducer getDocValuesReader();
  
  /**
   * Expert: retrieve underlying FieldsProducer
   * @lucene.internal
   */
  public abstract FieldsProducer getPostingsReader();

  /**
   * Expert: retrieve underlying PointsReader
   * @lucene.internal
   */
  public abstract PointsReader getPointsReader();

  /**
   * 先通过 id 定位到reader对象 之后使用 visitor 读取doc内部的数据
   * @param docID
   * @param visitor
   * @throws IOException
   */
  @Override
  public final void document(int docID, StoredFieldVisitor visitor) throws IOException {
    checkBounds(docID);
    getFieldsReader().visitDocument(docID, visitor);
  }

  /**
   * 套路是类似的  都是先获取满足api功能的对应reader 之后进行转发
   * @param docID
   * @return
   * @throws IOException
   */
  @Override
  public final Fields getTermVectors(int docID) throws IOException {
    TermVectorsReader termVectorsReader = getTermVectorsReader();
    if (termVectorsReader == null) {
      return null;
    }
    checkBounds(docID);
    return termVectorsReader.get(docID);
  }

  /**
   * 确保传入的 docId 不能超过最大的 docId
   * @param docID
   */
  private void checkBounds(int docID) {
    Objects.checkIndex(docID, maxDoc());
  }

  /**
   * 通过传入 field的描述信息 返回一组词
   * @param field
   * @return
   * @throws IOException
   */
  @Override
  public final Terms terms(String field) throws IOException {
    //ensureOpen(); no; getPostingsReader calls this
    // We could check the FieldInfo IndexOptions but there's no point since
    //   PostingsReader will simply return null for fields that don't exist or that have no terms index.
    // 基于指针reader读取
    return getPostingsReader().terms(field);
  }

  // returns the FieldInfo that corresponds to the given field and type, or
  // null if the field does not exist, or not indexed as the requested
  // DovDocValuesType.
  private FieldInfo getDVField(String field, DocValuesType type) {
    // 找到该字段的详细信息
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    // 判断数值类型是否匹配  匹配的情况下才返回 fieldInfo
    if (fi.getDocValuesType() == DocValuesType.NONE) {
      // Field was not indexed with doc values
      return null;
    }
    if (fi.getDocValuesType() != type) {
      // Field DocValues are different than requested type
      return null;
    }

    return fi;
  }

  /**
   * 读取某个 field的数值
   * @param field
   * @return
   * @throws IOException
   */
  @Override
  public final NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getDVField(field, DocValuesType.NUMERIC);
    if (fi == null) {
      return null;
    }
    // 通过对应的reader 对象 获取目标field的值
    return getDocValuesReader().getNumeric(fi);
  }

  @Override
  public final BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    // 从 fieldInfos 中 根据fieldName 找到 fieldInfo
    FieldInfo fi = getDVField(field, DocValuesType.BINARY);
    if (fi == null) {
      return null;
    }
    // 读取之前写入到 docValue 索引文件中的数据
    return getDocValuesReader().getBinary(fi);
  }

  @Override
  public final SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    // 检测该 field 对应的 docValueType 是否是 SORTED
    FieldInfo fi = getDVField(field, DocValuesType.SORTED);
    if (fi == null) {
      return null;
    }
    // 获取排序结果
    return getDocValuesReader().getSorted(fi);
  }
  
  @Override
  public final SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();

    FieldInfo fi = getDVField(field, DocValuesType.SORTED_NUMERIC);
    if (fi == null) {
      return null;
    }
    return getDocValuesReader().getSortedNumeric(fi);
  }

  @Override
  public final SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getDVField(field, DocValuesType.SORTED_SET);
    if (fi == null) {
      return null;
    }
    return getDocValuesReader().getSortedSet(fi);
  }
  
  @Override
  public final NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null || fi.hasNorms() == false) {
      // Field does not exist or does not index norms
      return null;
    }

    return getNormsReader().getNorms(fi);
  }

  @Override
  public final PointValues getPointValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null || fi.getPointDimensionCount() == 0) {
      // Field does not exist or does not index points
      return null;
    }

    return getPointsReader().getValues(field);
  }

  @Override
  protected void doClose() throws IOException {
  }
  
  @Override
  public long ramBytesUsed() {
    ensureOpen();
    
    // terms/postings
    long ramBytesUsed = getPostingsReader().ramBytesUsed();
    
    // norms
    if (getNormsReader() != null) {
      ramBytesUsed += getNormsReader().ramBytesUsed();
    }
    
    // docvalues
    if (getDocValuesReader() != null) {
      ramBytesUsed += getDocValuesReader().ramBytesUsed();
    }
    
    // stored fields
    if (getFieldsReader() != null) {
      ramBytesUsed += getFieldsReader().ramBytesUsed();
    }
    
    // term vectors
    if (getTermVectorsReader() != null) {
      ramBytesUsed += getTermVectorsReader().ramBytesUsed();
    }

    // points
    if (getPointsReader() != null) {
      ramBytesUsed += getPointsReader().ramBytesUsed();
    }
    
    return ramBytesUsed;
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    ensureOpen();
    final List<Accountable> resources = new ArrayList<>(6);
    
    // terms/postings
    resources.add(Accountables.namedAccountable("postings", getPostingsReader()));
    
    // norms
    if (getNormsReader() != null) {
      resources.add(Accountables.namedAccountable("norms", getNormsReader()));
    }
    
    // docvalues
    if (getDocValuesReader() != null) {
      resources.add(Accountables.namedAccountable("docvalues", getDocValuesReader()));
    }
    
    // stored fields
    if (getFieldsReader() != null) {
      resources.add(Accountables.namedAccountable("stored fields", getFieldsReader()));
    }

    // term vectors
    if (getTermVectorsReader() != null) {
      resources.add(Accountables.namedAccountable("term vectors", getTermVectorsReader()));
    }

    // points
    if (getPointsReader() != null) {
      resources.add(Accountables.namedAccountable("points", getPointsReader()));
    }
    
    return Collections.unmodifiableList(resources);
  }

  @Override
  public void checkIntegrity() throws IOException {
    ensureOpen();
    
    // terms/postings
    getPostingsReader().checkIntegrity();
    
    // norms
    if (getNormsReader() != null) {
      getNormsReader().checkIntegrity();
    }
    
    // docvalues
    if (getDocValuesReader() != null) {
      getDocValuesReader().checkIntegrity();
    }

    // stored fields
    if (getFieldsReader() != null) {
      getFieldsReader().checkIntegrity();
    }
    
    // term vectors
    if (getTermVectorsReader() != null) {
      getTermVectorsReader().checkIntegrity();
    }

    // points
    if (getPointsReader() != null) {
      getPointsReader().checkIntegrity();
    }
  }
}
