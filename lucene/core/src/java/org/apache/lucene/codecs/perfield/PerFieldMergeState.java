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
package org.apache.lucene.codecs.perfield;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Terms;

/**
 * Utility class to update the {@link MergeState} instance to be restricted to a set of fields.
 * <p>
 * Warning: the input {@linkplain MergeState} instance will be updated when calling {@link #apply(Collection)}.
 * <p>
 * It should be called within a {@code try &#123;...&#125; finally &#123;...&#125;} block to make sure that the mergeState instance is
 * restored to its original state:
 * <pre>
 * PerFieldMergeState pfMergeState = new PerFieldMergeState(mergeState);
 * try {
 *   doSomething(pfMergeState.apply(fields));
 *   ...
 * } finally {
 *   pfMergeState.reset();
 * }
 * </pre>
 */
final class PerFieldMergeState {
  private final MergeState in;

  /**
   * merge后的新段中所有 field 信息
   */
  private final FieldInfos orgMergeFieldInfos;
  /**
   * 合并前每个 segment关联的field信息
   */
  private final FieldInfos[] orgFieldInfos;
  /**
   * 对应的是FieldsReader
   */
  private final FieldsProducer[] orgFieldsProducers;

  /**
   * 通过一个包含了各种merge所需属性的对象进行初始化
   * @param in
   */
  PerFieldMergeState(MergeState in) {
    this.in = in;
    this.orgMergeFieldInfos = in.mergeFieldInfos;
    this.orgFieldInfos = new FieldInfos[in.fieldInfos.length];
    this.orgFieldsProducers = new FieldsProducer[in.fieldsProducers.length];

    System.arraycopy(in.fieldInfos, 0, this.orgFieldInfos, 0, this.orgFieldInfos.length);
    System.arraycopy(in.fieldsProducers, 0, this.orgFieldsProducers, 0, this.orgFieldsProducers.length);
  }

  /**
   * Update the input {@link MergeState} instance to restrict the fields to the given ones.
   *
   * @param fields The fields to keep in the updated instance.
   * @return The updated instance.
   */
  MergeState apply(Collection<String> fields) {
    // 因为外层使用者 已经将field 根据写入的格式进行划分了 所以此时使用一层包装确保接下来的使用不会处理到标明要以其他格式写入的field
    in.mergeFieldInfos = new FilterFieldInfos(orgMergeFieldInfos, fields);
    // 针对 merge前的field信息也要做处理  确保在处理过程中 仅能访问当目标fields
    for (int i = 0; i < orgFieldInfos.length; i++) {
      in.fieldInfos[i] = new FilterFieldInfos(orgFieldInfos[i], fields);
    }
    for (int i = 0; i < orgFieldsProducers.length; i++) {
      in.fieldsProducers[i] = new FilterFieldsProducer(orgFieldsProducers[i], fields);
    }
    return in;
  }

  /**
   * Resets the input {@link MergeState} instance to its original state.
   *
   * @return The reset instance.
   */
  MergeState reset() {
    in.mergeFieldInfos = orgMergeFieldInfos;
    System.arraycopy(orgFieldInfos, 0, in.fieldInfos, 0, in.fieldInfos.length);
    System.arraycopy(orgFieldsProducers, 0, in.fieldsProducers, 0, in.fieldsProducers.length);
    return in;
  }

  /**
   * fieldInfo的包装对象
   */
  private static class FilterFieldInfos extends FieldInfos {

    /**
     * 哪些fieldName会被拦截
     */
    private final Set<String> filteredNames;
    /**
     * 记录原始 fieldInfo 中有多少命中
     */
    private final List<FieldInfo> filtered;

    // Copy of the private fields from FieldInfos
    // Renamed so as to be less confusing about which fields we're referring to
    private final boolean filteredHasVectors;
    private final boolean filteredHasProx;
    private final boolean filteredHasPayloads;
    private final boolean filteredHasOffsets;
    private final boolean filteredHasFreq;
    private final boolean filteredHasNorms;
    private final boolean filteredHasDocValues;
    private final boolean filteredHasPointValues;

    FilterFieldInfos(FieldInfos src, Collection<String> filterFields) {
      // Copy all the input FieldInfo objects since the field numbering must be kept consistent
      // 生成一个完全相同的副本
      super(toArray(src));

      boolean hasVectors = false;
      boolean hasProx = false;
      boolean hasPayloads = false;
      boolean hasOffsets = false;
      boolean hasFreq = false;
      boolean hasNorms = false;
      boolean hasDocValues = false;
      boolean hasPointValues = false;

      this.filteredNames = new HashSet<>(filterFields);
      this.filtered = new ArrayList<>(filterFields.size());
      // 注意到这里 只有命中过滤容器的field的属性 才会被参考
      for (FieldInfo fi : src) {
        if (this.filteredNames.contains(fi.name)) {
          this.filtered.add(fi);
          hasVectors |= fi.hasVectors();
          hasProx |= fi.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
          hasFreq |= fi.getIndexOptions() != IndexOptions.DOCS;
          hasOffsets |= fi.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
          hasNorms |= fi.hasNorms();
          hasDocValues |= fi.getDocValuesType() != DocValuesType.NONE;
          hasPayloads |= fi.hasPayloads();
          hasPointValues |= (fi.getPointDimensionCount() != 0);
        }
      }

      this.filteredHasVectors = hasVectors;
      this.filteredHasProx = hasProx;
      this.filteredHasPayloads = hasPayloads;
      this.filteredHasOffsets = hasOffsets;
      this.filteredHasFreq = hasFreq;
      this.filteredHasNorms = hasNorms;
      this.filteredHasDocValues = hasDocValues;
      this.filteredHasPointValues = hasPointValues;
    }

    private static FieldInfo[] toArray(FieldInfos src) {
      FieldInfo[] res = new FieldInfo[src.size()];
      int i = 0;
      for (FieldInfo fi : src) {
        res[i++] = fi;
      }
      return res;
    }

    @Override
    public Iterator<FieldInfo> iterator() {
      return filtered.iterator();
    }

    @Override
    public boolean hasFreq() {
      return filteredHasFreq;
    }

    @Override
    public boolean hasProx() {
      return filteredHasProx;
    }

    @Override
    public boolean hasPayloads() {
      return filteredHasPayloads;
    }

    @Override
    public boolean hasOffsets() {
      return filteredHasOffsets;
    }

    @Override
    public boolean hasVectors() {
      return filteredHasVectors;
    }

    @Override
    public boolean hasNorms() {
      return filteredHasNorms;
    }

    @Override
    public boolean hasDocValues() {
      return filteredHasDocValues;
    }

    @Override
    public boolean hasPointValues() {
      return filteredHasPointValues;
    }

    @Override
    public int size() {
      return filtered.size();
    }

    @Override
    public FieldInfo fieldInfo(String fieldName) {
      if (!filteredNames.contains(fieldName)) {
        // Throw IAE to be consistent with fieldInfo(int) which throws it as well on invalid numbers
        throw new IllegalArgumentException("The field named '" + fieldName + "' is not accessible in the current " +
            "merge context, available ones are: " + filteredNames);
      }
      return super.fieldInfo(fieldName);
    }

    @Override
    public FieldInfo fieldInfo(int fieldNumber) {
      FieldInfo res = super.fieldInfo(fieldNumber);
      if (!filteredNames.contains(res.name)) {
        throw new IllegalArgumentException("The field named '" + res.name + "' numbered '" + fieldNumber + "' is not " +
            "accessible in the current merge context, available ones are: " + filteredNames);
      }
      return res;
    }
  }

  /**
   * 对field 读取对象做处理  应该就是指定未命中的 field时 不返回数据吧
   */
  private static class FilterFieldsProducer extends FieldsProducer {
    private final FieldsProducer in;
    /**
     * 只能处理下面的field
     */
    private final List<String> filtered;

    FilterFieldsProducer(FieldsProducer in, Collection<String> filterFields) {
      this.in = in;
      this.filtered = new ArrayList<>(filterFields);
    }

    @Override
    public long ramBytesUsed() {
      return in.ramBytesUsed();
    }

    @Override
    public Iterator<String> iterator() {
      return filtered.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      if (!filtered.contains(field)) {
        throw new IllegalArgumentException("The field named '" + field + "' is not accessible in the current " +
            "merge context, available ones are: " + filtered);
      }
      return in.terms(field);
    }

    @Override
    public int size() {
      return filtered.size();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public void checkIntegrity() throws IOException {
      in.checkIntegrity();
    }
  }
}
