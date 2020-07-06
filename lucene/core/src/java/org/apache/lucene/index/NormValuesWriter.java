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

import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/** Buffers up pending long per doc, then flushes when
 *  segment flushes. */
// 该对象负责将 标准因子写入到 索引文件中   跟 普通的docValueWriter类似
class NormValuesWriter {

  private DocsWithFieldSet docsWithField;
  private PackedLongValues.Builder pending;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private final FieldInfo fieldInfo;
  private int lastDocID = -1;

  /**
   * @param fieldInfo   是否忽略标准因子的信息是携带在 field 上的  当 omitNorm 为false时 才会创建对应的对象
   * @param iwBytesUsed   多个对象共用这个计数器 用来统计内存开销
   */
  public NormValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    docsWithField = new DocsWithFieldSet();
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    bytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, long value) {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("Norm for \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }

    pending.add(value);
    docsWithField.add(docID);

    updateBytesUsed();

    lastDocID = docID;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  public void finish(int maxDoc) {
  }

  /**
   * 将标准因子的信息写入到 索引文件中
   * @param state
   * @param sortMap
   * @param normsConsumer
   * @throws IOException
   */
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, NormsConsumer normsConsumer) throws IOException {
    final PackedLongValues values = pending.build();
    final SortingLeafReader.CachedNumericDVs sorted;
    // 代表根据 docValue 做了排序
    if (sortMap != null) {
      sorted = NumericDocValuesWriter.sortDocValues(state.segmentInfo.maxDoc(), sortMap,
          new BufferedNorms(values, docsWithField.iterator()));
    } else {
      sorted = null;
    }
    // 设置某个field 有关的标准因子信息
    normsConsumer.addNormsField(fieldInfo,   // 存储了field的详细信息
                                new NormsProducer() {
                                  @Override
                                  public NumericDocValues getNorms(FieldInfo fieldInfo2) {
                                   if (fieldInfo != NormValuesWriter.this.fieldInfo) {
                                     throw new IllegalArgumentException("wrong fieldInfo");
                                   }
                                   if (sorted == null) {
                                     return new BufferedNorms(values, docsWithField.iterator());
                                   } else {
                                     // 该对象也是 NumericDocValues 的子类 只是读取数据的时候是按照 docValue的顺序所对应的docId的顺序 而不是自然顺序
                                     return new SortingLeafReader.SortingNumericDocValues(sorted);
                                   }
                                  }

                                  @Override
                                  public void checkIntegrity() {
                                  }

                                  @Override
                                  public void close() {
                                  }
                                  
                                  @Override
                                  public long ramBytesUsed() {
                                    return 0;
                                  }
                               });
  }

  // TODO: norms should only visit docs that had a field indexed!!
  
  // iterates over the values we have in ram
  private static class BufferedNorms extends NumericDocValues {
    final PackedLongValues.Iterator iter;
    final DocIdSetIterator docsWithField;
    private long value;

    BufferedNorms(PackedLongValues values, DocIdSetIterator docsWithFields) {
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

