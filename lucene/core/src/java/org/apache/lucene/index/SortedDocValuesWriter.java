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
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

/** Buffers up pending byte[] per doc, deref and sorting via
 *  int ord, then flushes when segment flushes.
 *  这里是将 field.value 全部写入不需要经过分词器处理
 *
 *  在doc中写入了这种 SortedXXX的值 也不一定会起作用 必须要 IndexConfig.Sort中包含这些field
 */
class SortedDocValuesWriter extends DocValuesWriter {
  final BytesRefHash hash;
  private PackedLongValues.Builder pending;

  /**
   * doc位图
   */
  private DocsWithFieldSet docsWithField;
  private final Counter iwBytesUsed;
  private long bytesUsed; // this currently only tracks differences in 'pending'
  private final FieldInfo fieldInfo;
  private int lastDocID = -1;

  private PackedLongValues finalOrds;
  private int[] finalSortedValues;
  /**
   * termId 作为下标 value 对应该term的顺序
   */
  private int[] finalOrdMap;

  public SortedDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    hash = new BytesRefHash(
        new ByteBlockPool(
                // 该对象在每次分配和回收内存时 会修改计数器对应的值
            new ByteBlockPool.DirectTrackingAllocator(iwBytesUsed)),
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    bytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  /**
   * 记录当前field 所在的doc  以及field.value
   * @param docID
   * @param value
   */
  public void addValue(int docID, BytesRef value) {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException("field \"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > (BYTE_BLOCK_SIZE - 2)) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + (BYTE_BLOCK_SIZE - 2));
    }

    addOneValue(value);
    // 记录docId
    docsWithField.add(docID);

    lastDocID = docID;
  }

  @Override
  public void finish(int maxDoc) {
    updateBytesUsed();
  }

  /**
   * 该value 本身已经做过排序处理了
   * @param value
   */
  private void addOneValue(BytesRef value) {
    // 将数据存储到hash结构下的 BytePool中
    int termID = hash.add(value);
    // 代表 value之前已经写入过
    if (termID < 0) {
      termID = -termID-1;
    } else {
      // reserve additional space for each unique value:
      // 1. when indexing, when hash is 50% full, rehash() suddenly needs 2*size ints.
      //    TODO: can this same OOM happen in THPF?
      // 2. when flushing, we need 1 int per value (slot in the ordMap).
      iwBytesUsed.addAndGet(2 * Integer.BYTES);
    }

    // 这里只存储 返回的id
    pending.add(termID);
    updateBytesUsed();
  }
  
  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  /**
   * 为本次结果排序
   * @param maxDoc
   * @param sortField
   * @return
   * @throws IOException
   */
  @Override
  Sorter.DocComparator getDocComparator(int maxDoc, SortField sortField) throws IOException {
    assert sortField.getType().equals(SortField.Type.STRING);
    assert finalSortedValues == null && finalOrdMap == null &&finalOrds == null;
    int valueCount = hash.size();
    // 生成一个 按照term数值进行排序的 termId[] 对象
    finalSortedValues = hash.sort();
    finalOrds = pending.build();
    finalOrdMap = new int[valueCount];
    for (int ord = 0; ord < valueCount; ord++) {
      // 这里就生成了一个 按照大小关系排序后的 termId 数组
      finalOrdMap[finalSortedValues[ord]] = ord;
    }
    final SortedDocValues docValues =
        new BufferedSortedDocValues(hash, valueCount, finalOrds, finalSortedValues, finalOrdMap,
            docsWithField.iterator());
    return Sorter.getDocComparator(maxDoc, sortField, () -> docValues, () -> null);
  }

  private int[] sortDocValues(int maxDoc, Sorter.DocMap sortMap, SortedDocValues oldValues) throws IOException {
    int[] ords = new int[maxDoc];
    Arrays.fill(ords, -1);
    int docID;
    while ((docID = oldValues.nextDoc()) != NO_MORE_DOCS) {
      // 将旧的docId 转化成ord 并以该ord作为新的docId   在生成sortMap的过程中 当所有comparator比较后都相等时 会按照旧的docId进行排序 所以这里能确保newDocId不会出现重复
      int newDocID = sortMap.oldToNew(docID);
      // 这里的 ordValue 是该field在该doc下的term的ord 与 newDocId无关
      ords[newDocID] = oldValues.ordValue();
    }
    return ords;
  }

  /**
   * 如果sortMap 不为空 按照该顺序为doc重排序
   * @param state
   * @param sortMap
   * @param dvConsumer
   * @throws IOException
   */
  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final int valueCount = hash.size();
    final PackedLongValues ords;
    final int[] sortedValues;
    final int[] ordMap;
    // 代表没有调用过 getComparator 这里初始化相关信息
    if (finalOrds == null) {
      sortedValues = hash.sort();
      ords = pending.build();
      ordMap = new int[valueCount];
      for (int ord = 0; ord < valueCount; ord++) {
        ordMap[sortedValues[ord]] = ord;
      }
    } else {
      sortedValues = finalSortedValues;
      ords = finalOrds;
      ordMap = finalOrdMap;
    }

    // sorted 以新docId 作为下标 存储的是在新的doc下 term的ord (ord可以转换成 termId)
    final int[] sorted;
    if (sortMap != null) {
      // 使用sortMap 进行重排序
      sorted = sortDocValues(state.segmentInfo.maxDoc(), sortMap,
          new BufferedSortedDocValues(hash, valueCount, ords, sortedValues, ordMap, docsWithField.iterator()));
    } else {
      sorted = null;
    }
    dvConsumer.addSortedField(fieldInfo,
                              new EmptyDocValuesProducer() {
                                @Override
                                public SortedDocValues getSorted(FieldInfo fieldInfoIn) {
                                  if (fieldInfoIn != fieldInfo) {
                                    throw new IllegalArgumentException("wrong fieldInfo");
                                  }
                                  final SortedDocValues buf =
                                      new BufferedSortedDocValues(hash, valueCount, ords, sortedValues, ordMap, docsWithField.iterator());
                                  if (sorted == null) {
                                   return buf;
                                  }
                                  return new SortingLeafReader.SortingSortedDocValues(buf, sorted);
                                }
                              });
  }

  private static class BufferedSortedDocValues extends SortedDocValues {

    /**
     * 存储term信息的hash桶
     */
    final BytesRefHash hash;
    final BytesRef scratch = new BytesRef();

    /**
     * 按照term大小 为termId进行排序 (下标是ord value是 termId )
     */
    final int[] sortedValues;
    final int[] ordMap;
    /**
     * 总计写入了多少值
     */
    final int valueCount;
    private int ord;

    /**
     * 遍历field在每个doc下的 value 对应的termId 可以重复出现
     */
    final PackedLongValues.Iterator iter;

    /**
     * 用于迭代该field出现过的所有doc
     */
    final DocIdSetIterator docsWithField;

    public BufferedSortedDocValues(BytesRefHash hash, int valueCount, PackedLongValues docToOrd, int[] sortedValues, int[] ordMap, DocIdSetIterator docsWithField) {
      this.hash = hash;
      this.valueCount = valueCount;
      this.sortedValues = sortedValues;
      this.iter = docToOrd.iterator();
      this.ordMap = ordMap;
      this.docsWithField = docsWithField;
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithField.nextDoc();
      if (docID != NO_MORE_DOCS) {
        // 这里是获取field在该doc下写入的term的id
        ord = Math.toIntExact(iter.next());
        // 通过termId 转换成序号
        ord = ordMap[ord];
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
    public int ordValue() {
      return ord;
    }

    /**
     * 传入 ord 反向获取term信息 实际上就是依赖于 termHash
     * @param ord ordinal to lookup (must be &gt;= 0 and &lt; {@link #getValueCount()})
     * @return
     */
    @Override
    public BytesRef lookupOrd(int ord) {
      assert ord >= 0 && ord < sortedValues.length;
      assert sortedValues[ord] >= 0 && sortedValues[ord] < sortedValues.length;
      // 反向获取termId
      hash.get(sortedValues[ord], scratch);
      return scratch;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }
  }

  @Override
  DocIdSetIterator getDocIdSet() {
    return docsWithField.iterator();
  }
}
