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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
class NumericDocValuesWriter extends DocValuesWriter {

  private PackedLongValues.Builder pending;
  private PackedLongValues finalValues;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private DocsWithFieldSet docsWithField;
  private final FieldInfo fieldInfo;
  private int lastDocID = -1;

  /**
   *
   * @param fieldInfo docValueType 都是设置在某个field上的
   * @param iwBytesUsed  该对象一般都是不断传递进来 用于估量当前使用的总内存的  可以先不看
   */
  public NumericDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    // 生成一个增量存储的 packed 对象  packed本身的特性是按位存储数据
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    bytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    iwBytesUsed.addAndGet(bytesUsed);
  }

  /**
   * 存储 某个doc下该field 存储的值
   * 在处理多个doc时  field可能会出现重复的情况 然后每个 field对象对应一个 DocValueWriter对象  在处理doc时 按照docId 触发该方法
   * @param docID
   * @param value
   */
  public void addValue(int docID, long value) {
    // 要求id单调递增
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }

    // 写入到差值存储的容器中
    pending.add(value);
    // 追加关联关系  内部使用了 位图
    docsWithField.add(docID);

    updateBytesUsed();

    lastDocID = docID;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void finish(int maxDoc) {
  }

  /**
   * 根据传入的  sortedField 对象获取对应的排序对象
   * @param maxDoc
   * @param sortField
   * @return
   * @throws IOException
   */
  @Override
  Sorter.DocComparator getDocComparator(int maxDoc, SortField sortField) throws IOException {
    assert finalValues == null;
    finalValues = pending.build();
    final BufferedNumericDocValues docValues =
        new BufferedNumericDocValues(finalValues, docsWithField.iterator());
    return Sorter.getDocComparator(maxDoc, sortField, () -> null, () -> docValues);
  }

  @Override
  DocIdSetIterator getDocIdSet() {
    return docsWithField.iterator();
  }

  /**
   *
   * @param maxDoc 当前最大的docId
   * @param sortMap   docId 根据对应的值已经排序过了  该对象可以通过docId的自然顺序找到排序后的位置
   * @param oldDocValues  该对象可以遍历 docId 并获取绑定的value
   * @return
   * @throws IOException
   */
  static SortingLeafReader.CachedNumericDVs sortDocValues(int maxDoc, Sorter.DocMap sortMap, NumericDocValues oldDocValues) throws IOException {
    FixedBitSet docsWithField = new FixedBitSet(maxDoc);
    // 该数组存放 docId 上读取出来的值
    long[] values = new long[maxDoc];
    while (true) {
      int docID = oldDocValues.nextDoc();
      if (docID == NO_MORE_DOCS) {
        break;
      }
      // 找到排序后的位置
      int newDocID = sortMap.oldToNew(docID);
      docsWithField.set(newDocID);
      // 在对应的位置设置 绑定的值
      values[newDocID] = oldDocValues.longValue();
    }
    return new SortingLeafReader.CachedNumericDVs(values, docsWithField);
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final PackedLongValues values;
    if (finalValues == null) {
      values = pending.build();
    } else {
      values = finalValues;
    }

    final SortingLeafReader.CachedNumericDVs sorted;
    if (sortMap != null) {
      NumericDocValues oldValues = new BufferedNumericDocValues(values, docsWithField.iterator());
      sorted = sortDocValues(state.segmentInfo.maxDoc(), sortMap, oldValues);
    } else {
      sorted = null;
    }

    dvConsumer.addNumericField(fieldInfo,
                               new EmptyDocValuesProducer() {
                                 @Override
                                 public NumericDocValues getNumeric(FieldInfo fieldInfo) {
                                   if (fieldInfo != NumericDocValuesWriter.this.fieldInfo) {
                                     throw new IllegalArgumentException("wrong fieldInfo");
                                   }
                                   if (sorted == null) {
                                     return new BufferedNumericDocValues(values, docsWithField.iterator());
                                   } else {
                                     return new SortingLeafReader.SortingNumericDocValues(sorted);
                                   }
                                 }
                               });
  }

  // iterates over the values we have in ram
  // 该对象就是之前写入的 docValue的读取器
  private static class BufferedNumericDocValues extends NumericDocValues {
    final PackedLongValues.Iterator iter;
    final DocIdSetIterator docsWithField;
    private long value;

    BufferedNumericDocValues(PackedLongValues values, DocIdSetIterator docsWithFields) {
      this.iter = values.iterator();
      this.docsWithField = docsWithFields;
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithField.nextDoc();
      if (docID != NO_MORE_DOCS) {
        value = iter.next();
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }

    @Override
    public long longValue() {
      return value;
    }
  }
}
