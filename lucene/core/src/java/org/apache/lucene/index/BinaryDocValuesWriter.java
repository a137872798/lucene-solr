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
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Buffers up pending byte[] per doc, then flushes when
 *  segment flushes. */
// 存储的是二进制数据
class BinaryDocValuesWriter extends DocValuesWriter {

  /** Maximum length for a binary field. */
  private static final int MAX_LENGTH = ArrayUtil.MAX_ARRAY_LENGTH;

  // 32 KB block sizes for PagedBytes storage:
  private final static int BLOCK_BITS = 15;

  /**
   * 该对象内部的block是byte[] (用于存储二进制数据)
   */
  private final PagedBytes bytes;
  private final DataOutput bytesOut;

  private final Counter iwBytesUsed;
  /**
   * 该对象专门存储long型的数据  这里用于保存二进制数据的长度
   */
  private final PackedLongValues.Builder lengths;
  private DocsWithFieldSet docsWithField;
  private final FieldInfo fieldInfo;
  private long bytesUsed;
  private int lastDocID = -1;
  private int maxLength = 0;

  public BinaryDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.bytes = new PagedBytes(BLOCK_BITS);
    this.bytesOut = bytes.getDataOutput();
    this.lengths = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    this.iwBytesUsed = iwBytesUsed;
    this.docsWithField = new DocsWithFieldSet();
    this.bytesUsed = lengths.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  /**
   * 代表此时写入了某个 doc 关联的值
   * @param docID
   * @param value
   */
  public void addValue(int docID, BytesRef value) {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > MAX_LENGTH) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + MAX_LENGTH);
    }

    maxLength = Math.max(value.length, maxLength);
    lengths.add(value.length);
    try {
      bytesOut.writeBytes(value.bytes, value.offset, value.length);
    } catch (IOException ioe) {
      // Should never happen!
      throw new RuntimeException(ioe);
    }
    // 使用位图来存储 docId
    docsWithField.add(docID);
    updateBytesUsed();

    lastDocID = docID;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = lengths.ramBytesUsed() + bytes.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void finish(int maxDoc) {
  }

  /**
   * 该对象代表内部的数据已经完成排序
   * @param maxDoc
   * @param sortMap
   * @param oldValues
   * @return
   * @throws IOException
   */
  private SortingLeafReader.CachedBinaryDVs sortDocValues(int maxDoc, Sorter.DocMap sortMap, BinaryDocValues oldValues) throws IOException {
    FixedBitSet docsWithField = new FixedBitSet(maxDoc);
    BytesRef[] values = new BytesRef[maxDoc];
    while (true) {
      // 遍历 原来的docId
      int docID = oldValues.nextDoc();
      if (docID == NO_MORE_DOCS) {
        break;
      }
      int newDocID = sortMap.oldToNew(docID);
      docsWithField.set(newDocID);
      values[newDocID] = BytesRef.deepCopyOf(oldValues.binaryValue());
    }
    return new SortingLeafReader.CachedBinaryDVs(values, docsWithField);
  }

  @Override
  Sorter.DocComparator getDocComparator(int numDoc, SortField sortField) throws IOException {
    throw new IllegalArgumentException("It is forbidden to sort on a binary field");
  }

  /**
   * 将 docValue 写入到文件中
   * @param state
   * @param sortMap  代表doc 已经重新排序过了
   * @param dvConsumer  该对象就是 负责将数据写入到 索引文件的对象
   * @throws IOException
   */
  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    // 冻结bytes 对象 无法再写入数据
    bytes.freeze(false);
    // 该对象专门用于存储 每个docValue的长度
    final PackedLongValues lengths = this.lengths.build();
    // 跟 norm 一个套路 如果携带了 sortMap 就将docValue 排序
    final SortingLeafReader.CachedBinaryDVs sorted;
    if (sortMap != null) {
      sorted = sortDocValues(state.segmentInfo.maxDoc(), sortMap,
          new BufferedBinaryDocValues(lengths, maxLength, bytes.getDataInput(), docsWithField.iterator()));
    } else {
      sorted = null;
    }
    dvConsumer.addBinaryField(fieldInfo,
                              new EmptyDocValuesProducer() {
                                @Override
                                public BinaryDocValues getBinary(FieldInfo fieldInfoIn) {
                                  if (fieldInfoIn != fieldInfo) {
                                    throw new IllegalArgumentException("wrong fieldInfo");
                                  }
                                  if (sorted == null) {
                                    return new BufferedBinaryDocValues(lengths, maxLength, bytes.getDataInput(), docsWithField.iterator());
                                  } else {
                                    return new SortingLeafReader.SortingBinaryDocValues(sorted);
                                  }
                                }
                              });
  }

  // iterates over the values we have in ram
  private static class BufferedBinaryDocValues extends BinaryDocValues {
    final BytesRefBuilder value;
    final PackedLongValues.Iterator lengthsIterator;
    /**
     * 这个位图是用来存储 docId的  （该迭代器是位图生成的）
     */
    final DocIdSetIterator docsWithField;
    final DataInput bytesIterator;
    
    BufferedBinaryDocValues(PackedLongValues lengths, int maxLength, DataInput bytesIterator, DocIdSetIterator docsWithFields) {
      this.value = new BytesRefBuilder();
      this.value.grow(maxLength);
      this.lengthsIterator = lengths.iterator();
      this.bytesIterator = bytesIterator;
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
        int length = Math.toIntExact(lengthsIterator.next());
        // 根据长度 从value中读取数据
        value.setLength(length);
        bytesIterator.readBytes(value.bytes(), 0, length);
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
    public BytesRef binaryValue() {
      return value.get();
    }
  }

  @Override
  DocIdSetIterator getDocIdSet() {
    return docsWithField.iterator();
  }
}
