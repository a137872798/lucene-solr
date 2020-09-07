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
import java.util.Arrays;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 *  Buffers up pending long[] per doc, sorts, then flushes when segment flushes.
 *  基于field.value（数字类型） 为doc进行重排序
 * */
class SortedNumericDocValuesWriter extends DocValuesWriter {

  /**
   * 存储了所有 field的值
   */
  private PackedLongValues.Builder pending; // stream of all values
  /**
   * 总计写入了多少field.value
   */
  private PackedLongValues.Builder pendingCounts; // count of values per doc
  private DocsWithFieldSet docsWithField;
  private final Counter iwBytesUsed;
  private long bytesUsed; // this only tracks differences in 'pending' and 'pendingCounts'
  private final FieldInfo fieldInfo;
  private int currentDoc = -1;
  /**
   * 记录当前写入的值 (未排序)
   */
  private long currentValues[] = new long[8];
  /**
   * 记录当前doc已经写入了多少数据
   */
  private int currentUpto = 0;

  private PackedLongValues finalValues;
  private PackedLongValues finalValuesCount;

  public SortedNumericDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    pendingCounts = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    bytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed() + docsWithField.ramBytesUsed() + RamUsageEstimator.sizeOf(currentValues);
    iwBytesUsed.addAndGet(bytesUsed);
  }

  /**
   * 因为 数字类型已经完成排序了 所以确保每次写入的值都是递增的 可以采用差值写入  该对象本身比较特殊 是支持往一个doc中同时写入多个相同的field的
   * 这时在doc之间的顺序是由 同一个doc下的这些field的最大值 或者最小值 决定的 (通过selector来决定)
   * @param docID
   * @param value
   */
  public void addValue(int docID, long value) {
    assert docID >= currentDoc;
    // 以doc为单位 每次切换doc前 将之前写入的数据排序后写入pending中
    if (docID != currentDoc) {
      finishCurrentDoc();
      currentDoc = docID;
    }

    addOneValue(value);
    updateBytesUsed();
  }
  
  // finalize currentDoc: this sorts the values in the current doc
  private void finishCurrentDoc() {
    // 初始状态忽略该动作
    if (currentDoc == -1) {
      return;
    }
    // 每次在真正写入 pending前 先将现在存储的数据进行排序
    Arrays.sort(currentValues, 0, currentUpto);
    for (int i = 0; i < currentUpto; i++) {
      pending.add(currentValues[i]);
    }
    // record the number of values for this doc
    pendingCounts.add(currentUpto);
    currentUpto = 0;

    docsWithField.add(currentDoc);
  }

  @Override
  public void finish(int maxDoc) {
    finishCurrentDoc();
  }

  /**
   *
   * @param value
   */
  private void addOneValue(long value) {
    if (currentUpto == currentValues.length) {
      currentValues = ArrayUtil.grow(currentValues, currentValues.length+1);
    }
    
    currentValues[currentUpto] = value;
    currentUpto++;
  }
  
  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed() + docsWithField.ramBytesUsed() + RamUsageEstimator.sizeOf(currentValues);
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  /**
   * 为内部数据进行排序
   * @param maxDoc
   * @param sortField
   * @return
   * @throws IOException
   */
  @Override
  Sorter.DocComparator getDocComparator(int maxDoc, SortField sortField) throws IOException {
    assert sortField instanceof SortedNumericSortField;
    assert finalValues == null && finalValuesCount == null;
    finalValues = pending.build();
    finalValuesCount = pendingCounts.build();
    final SortedNumericDocValues docValues =
        new BufferedSortedNumericDocValues(finalValues, finalValuesCount, docsWithField.iterator());
    SortedNumericSortField sf = (SortedNumericSortField) sortField;

    return Sorter.getDocComparator(maxDoc, sf, () -> null,
        // 这里需要额外做一层包装 屏蔽同一个doc下多个相同field的影响
        () -> SortedNumericSelector.wrap(docValues, sf.getSelector(), sf.getNumericType()));
  }

  /**
   * 重排序
   * @param maxDoc
   * @param sortMap
   * @param oldValues
   * @return
   * @throws IOException
   */
  private long[][] sortDocValues(int maxDoc, Sorter.DocMap sortMap, SortedNumericDocValues oldValues) throws IOException {
    long[][] values = new long[maxDoc][];
    int docID;
    while ((docID = oldValues.nextDoc()) != NO_MORE_DOCS) {
      // 将docId 上值对应的 ord 作为新的docId
      int newDocID = sortMap.oldToNew(docID);
      // 在该doc下  该field总计出现了多少次
      long[] docValues = new long[oldValues.docValueCount()];
      // 将值原封不动的转移过来
      for (int i = 0; i < docValues.length; i++) {
        docValues[i] = oldValues.nextValue();
      }
      values[newDocID] = docValues;
    }
    return values;
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final PackedLongValues values;
    final PackedLongValues valueCounts;
    if (finalValues == null) {
      values = pending.build();
      valueCounts = pendingCounts.build();
    } else {
      values = finalValues;
      valueCounts = finalValuesCount;
    }

    // 第一维 代表排序后的docId  第二维代表此时docId下存储的多次field的值
    final long[][] sorted;
    if (sortMap != null) {
      // 使用sortMap 对之前写入的doc顺序做重排序
      sorted = sortDocValues(state.segmentInfo.maxDoc(), sortMap,
          new BufferedSortedNumericDocValues(values, valueCounts, docsWithField.iterator()));
    } else {
      sorted = null;
    }

    dvConsumer.addSortedNumericField(fieldInfo,
                                     new EmptyDocValuesProducer() {
                                       @Override
                                       public SortedNumericDocValues getSortedNumeric(FieldInfo fieldInfoIn) {
                                         if (fieldInfoIn != fieldInfo) {
                                           throw new IllegalArgumentException("wrong fieldInfo");
                                         }
                                         final SortedNumericDocValues buf =
                                             new BufferedSortedNumericDocValues(values, valueCounts, docsWithField.iterator());
                                         if (sorted == null) {
                                           return buf;
                                         } else {
                                           return new SortingLeafReader.SortingSortedNumericDocValues(buf, sorted);
                                         }
                                       }
                                     });
  }

  private static class BufferedSortedNumericDocValues extends SortedNumericDocValues {

    /**
     * 用于迭代 field.value
     */
    final PackedLongValues.Iterator valuesIter;
    /**
     * 迭代该field在某个doc下写入了多少次
     */
    final PackedLongValues.Iterator valueCountsIter;
    final DocIdSetIterator docsWithField;
    private int valueCount;
    private int valueUpto;

    public BufferedSortedNumericDocValues(PackedLongValues values, PackedLongValues valueCounts, DocIdSetIterator docsWithField) {
      valuesIter = values.iterator();
      valueCountsIter = valueCounts.iterator();
      this.docsWithField = docsWithField;
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      // 确保上个doc下该field的所有数据都已经读取完
      for (int i = valueUpto; i < valueCount; ++i) {
        valuesIter.next();
      }

      int docID = docsWithField.nextDoc();
      if (docID != NO_MORE_DOCS) {
        // 获取该field在该doc下写入多少数据
        valueCount = Math.toIntExact(valueCountsIter.next());
        valueUpto = 0;
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
    public int docValueCount() {
      return valueCount;
    }

    @Override
    public long nextValue() {
      if (valueUpto == valueCount) {
        throw new IllegalStateException();
      }
      valueUpto++;
      return valuesIter.next();
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }
  }

  @Override
  DocIdSetIterator getDocIdSet() {
    return docsWithField.iterator();
  }
}
