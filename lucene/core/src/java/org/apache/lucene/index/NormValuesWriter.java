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
 *  segment flushes.
 *  标准因子 实际上就是得分
 */
class NormValuesWriter {

  private DocsWithFieldSet docsWithField;
  private PackedLongValues.Builder pending;
  private final Counter iwBytesUsed;
  private long bytesUsed;
  private final FieldInfo fieldInfo;
  private int lastDocID = -1;

  /**
   * @param fieldInfo
   * @param iwBytesUsed   多个对象共用这个计数器 用来统计内存开销
   */
  public NormValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    // 存储该field 关联的所有 docId
    docsWithField = new DocsWithFieldSet();
    // 该对象就是基于位存储数据
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    bytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    iwBytesUsed.addAndGet(bytesUsed);
  }

  /**
   * 存储 field在该doc下的得分信息
   * @param docID
   * @param value
   */
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
   * @param state  描述本次刷盘的信息
   * @param sortMap  如果中途field被重新排序过了 通过该对象映射顺序
   * @param normsConsumer   实际上就是写入索引文件的对象
   * @throws IOException
   */
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, NormsConsumer normsConsumer) throws IOException {
    // 将之前存储的所有field信息 以压缩格式存储
    final PackedLongValues values = pending.build();
    // TODO 先忽略排序   这个leaf指代的是通过跳跃表检索到的某个fst索引吗???
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
            // 这里自定义了一个 通过传入field获取标准因子的对象 实际上就是读取 values内的数据
                                new NormsProducer() {
                                  @Override
                                  public NumericDocValues getNorms(FieldInfo fieldInfo) {
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
  /**
   * 开放了遍历内部Num类型数据的api
   */
  private static class BufferedNorms extends NumericDocValues {
    final PackedLongValues.Iterator iter;
    final DocIdSetIterator docsWithField;
    private long value;

    BufferedNorms(PackedLongValues values, DocIdSetIterator docsWithFields) {
      this.iter = values.iterator();
      this.docsWithField = docsWithFields;
    }

    /**
     * 返回当前位图指向的docId
     * @return
     */
    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithField.nextDoc();
      if (docID != NO_MORE_DOCS) {
        // 切换doc的同时 读取下一个field的标准因子
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

