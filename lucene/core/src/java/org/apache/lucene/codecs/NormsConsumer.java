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
package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;

/** 
 * Abstract API that consumes normalization values.  
 * Concrete implementations of this
 * actually do "something" with the norms (write it into
 * the index in a specific format).
 * <p>
 * The lifecycle is:
 * <ol>
 *   <li>NormsConsumer is created by 
 *       {@link NormsFormat#normsConsumer(SegmentWriteState)}.
 *   <li>{@link #addNormsField} is called for each field with
 *       normalization values. The API is a "pull" rather
 *       than "push", and the implementation is free to iterate over the 
 *       values multiple times ({@link Iterable#iterator()}).
 *   <li>After all fields are added, the consumer is {@link #close}d.
 * </ol>
 *
 * @lucene.experimental
 * 该对象 负责将标准因子 写入到索引文件中
 */
public abstract class NormsConsumer implements Closeable {
  
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected NormsConsumer() {}
  
  /**
   * Writes normalization values for a field.
   * @param field field information
   * @param normsProducer NormsProducer of the numeric norm values
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addNormsField(FieldInfo field, NormsProducer normsProducer) throws IOException;

  /** Merges in the fields from the readers in 
   *  <code>mergeState</code>. The default implementation 
   *  calls {@link #mergeNormsField} for each field,
   *  filling segments with missing norms for the field with zeros. 
   *  Implementations can override this method 
   *  for more sophisticated merging (bulk-byte copying, etc). */
  // 将参与merge的多个 segment相关的标准因子信息合并后写入到 新的segment对应的标准因子文件
  public void merge(MergeState mergeState) throws IOException {
    for(NormsProducer normsProducer : mergeState.normsProducers) {
      // 如果某个 segment对应的 field下没有标准因子信息 那么返回的 producer为null
      if (normsProducer != null) {
        normsProducer.checkIntegrity();
      }
    }
    for (FieldInfo mergeFieldInfo : mergeState.mergeFieldInfos) {
      if (mergeFieldInfo.hasNorms()) {
        mergeNormsField(mergeFieldInfo, mergeState);
      }
    }
  }
  
  /**
   * Tracks state of one numeric sub-reader that we are merging
   * 在merge 过程中 每个field的标准因子是通过从所有段中找到该field的标准因子信息 并整合后形成的 每个sub对象就对应一个segment下的field
   */
  private static class NumericDocValuesSub extends DocIDMerger.Sub {

    /**
     * 标准因子 就是一个 NumDocValue
     */
    private final NumericDocValues values;
    
    public NumericDocValuesSub(MergeState.DocMap docMap, NumericDocValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /**
   * Merges the norms from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addNormsField}, passing
   * an Iterable that merges and filters deleted documents on the fly.
   * @param mergeFieldInfo  将多个segment的field合并后 的某个field
   * @param mergeState 包含了merge所需要的全部信息
   */
  public void mergeNormsField(final FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {

    // TODO: try to share code with default merge of DVConsumer by passing MatchAllBits ?
    addNormsField(mergeFieldInfo,
                  // 该对象根据 field 返回对应的标准因子数据
                  new NormsProducer() {
                    @Override
                    public NumericDocValues getNorms(FieldInfo fieldInfo) throws IOException {
                      if (fieldInfo != mergeFieldInfo) {
                        throw new IllegalArgumentException("wrong fieldInfo");
                      }

                        List<NumericDocValuesSub> subs = new ArrayList<>();
                        assert mergeState.docMaps.length == mergeState.docValuesProducers.length;
                        // 这里是所有参与merge的段   从每个段下找到 同一fieldInfo 对应的标准因子
                        for (int i=0;i<mergeState.docValuesProducers.length;i++) {
                          NumericDocValues norms = null;
                          // 获取段对应的 标准因子读取对象
                          NormsProducer normsProducer = mergeState.normsProducers[i];
                          if (normsProducer != null) {
                            // 从某个段下从获取查找对应的field    如果在该段下的这个field 有标准因子 就生成reader对象
                            FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                            if (readerFieldInfo != null && readerFieldInfo.hasNorms()) {
                              norms = normsProducer.getNorms(readerFieldInfo);
                            }
                          }

                          // 将多个 reader对应的标准因子合并成一个对象
                          if (norms != null) {
                            subs.add(new NumericDocValuesSub(mergeState.docMaps[i], norms));
                          }
                        }

                        // 将这些sub对象合并
                        final DocIDMerger<NumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);

                        // 该对象是将多个段 整合后  按照globalDocId 顺序 获取对应的标准因子  (标准因子是以field为维度创建 doc为单位存储的)
                        return new NumericDocValues() {
                          private int docID = -1;
                          private NumericDocValuesSub current;

                          @Override
                          public int docID() {
                            return docID;
                          }

                          @Override
                          public int nextDoc() throws IOException {
                            current = docIDMerger.next();
                            if (current == null) {
                              docID = NO_MORE_DOCS;
                            } else {
                              // 这个值是 merge后的 globalDocId
                              docID = current.mappedDocID;
                            }
                            return docID;
                          }

                          @Override
                          public int advance(int target) throws IOException {
                            throw new UnsupportedOperationException();
                          }

                          @Override
                          public boolean advanceExact(int target) throws IOException {
                            throw new UnsupportedOperationException();
                          }

                          @Override
                          public long cost() {
                            return 0;
                          }

                          @Override
                          public long longValue() throws IOException {
                            // 返回对应段的标准因子
                            return current.values.longValue();
                          }
                        };
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
}
