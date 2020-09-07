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
import java.util.List;
import java.util.Locale;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.index.IndexWriter.isCongruentSort;

/** Holds common state used during segment merging.
 * 在融合过程中 维护一些公共状态
 *
 * @lucene.experimental */
public class MergeState {

  /** Maps document IDs from old segments to document IDs in the new segment */
  // 每个 segment doc 与 合并后新的 segmentdoc 的映射关系
  public final DocMap[] docMaps;

  // Only used by IW when it must remap deletes that arrived against the merging segments while a merge was running:
  // 该对象在未处理的情况下  docId -> docId 也就是不做处理
  final DocMap[] leafDocMaps;

  /** {@link SegmentInfo} of the newly merged segment. */
  // 描述某个段的信息
  public final SegmentInfo segmentInfo;

  /** {@link FieldInfos} of the newly merged segment. */
  // 在将所有参与merge的fieldInfo合并后 会将结果设置到该字段上
  public FieldInfos mergeFieldInfos;

  /** Stored field producers being merged */
  public final StoredFieldsReader[] storedFieldsReaders;

  /** Term vector producers being merged */
  public final TermVectorsReader[] termVectorsReaders;

  /** Norms producers being merged */
  public final NormsProducer[] normsProducers;

  /** DocValues producers being merged */
  public final DocValuesProducer[] docValuesProducers;

  /** FieldInfos being merged */
  public final FieldInfos[] fieldInfos;

  /** Live docs for each reader */
  public final Bits[] liveDocs;

  /** Postings to merge */
  public final FieldsProducer[] fieldsProducers;

  /** Point readers to merge */
  public final PointsReader[] pointsReaders;

  /** Max docs per reader */
  // 存储每个segment 对应的 maxDoc
  public final int[] maxDocs;

  /** InfoStream for debugging messages. */
  public final InfoStream infoStream;

  /**
   * Indicates if the index needs to be sorted
   * 代表内部doc 依照参与排序的field.value做了排序
   */
  public boolean needsIndexSort;

  /**
   *
   * @param originalReaders  参与merge的所有segment对应的 reader
   * @param segmentInfo   写入的目标segment
   * @param infoStream
   * @throws IOException
   */
  MergeState(List<CodecReader> originalReaders, SegmentInfo segmentInfo, InfoStream infoStream) throws IOException {

    this.infoStream = infoStream;

    // 获取目标段的排序规则
    final Sort indexSort = segmentInfo.getIndexSort();
    int numReaders = originalReaders.size();
    leafDocMaps = new DocMap[numReaders];
    // 做校验 避免之前的reader排序规则和此时的排序规则不一致
    List<CodecReader> readers = maybeSortReaders(originalReaders, segmentInfo);

    // 生成存储各个segment关键信息的数组对象
    maxDocs = new int[numReaders];
    fieldsProducers = new FieldsProducer[numReaders];
    normsProducers = new NormsProducer[numReaders];
    storedFieldsReaders = new StoredFieldsReader[numReaders];
    termVectorsReaders = new TermVectorsReader[numReaders];
    docValuesProducers = new DocValuesProducer[numReaders];
    pointsReaders = new PointsReader[numReaders];
    fieldInfos = new FieldInfos[numReaders];
    liveDocs = new Bits[numReaders];

    int numDocs = 0;
    for(int i=0;i<numReaders;i++) {
      final CodecReader reader = readers.get(i);

      maxDocs[i] = reader.maxDoc();
      liveDocs[i] = reader.getLiveDocs();
      fieldInfos[i] = reader.getFieldInfos();

      // 如果reader对象存在 返回一个merge专用的实例对象
      normsProducers[i] = reader.getNormsReader();
      if (normsProducers[i] != null) {
        normsProducers[i] = normsProducers[i].getMergeInstance();
      }
      
      docValuesProducers[i] = reader.getDocValuesReader();
      if (docValuesProducers[i] != null) {
        docValuesProducers[i] = docValuesProducers[i].getMergeInstance();
      }
      
      storedFieldsReaders[i] = reader.getFieldsReader();
      if (storedFieldsReaders[i] != null) {
        storedFieldsReaders[i] = storedFieldsReaders[i].getMergeInstance();
      }
      
      termVectorsReaders[i] = reader.getTermVectorsReader();
      if (termVectorsReaders[i] != null) {
        termVectorsReaders[i] = termVectorsReaders[i].getMergeInstance();
      }
      
      fieldsProducers[i] = reader.getPostingsReader().getMergeInstance();
      pointsReaders[i] = reader.getPointsReader();
      if (pointsReaders[i] != null) {
        pointsReaders[i] = pointsReaders[i].getMergeInstance();
      }
      // 在merge前已经确保此时读取到的就是当前最新的 doc信息
      numDocs += reader.numDocs();
    }

    segmentInfo.setMaxDoc(numDocs);

    this.segmentInfo = segmentInfo;

    // 为每个段创建 将旧doc 映射到合并后全局doc的映射对象
    this.docMaps = buildDocMaps(readers, indexSort);
  }

  // Remap docIDs around deletions
  // 如果不存在 sort 对象  那么只针对deletion进行整理
  private DocMap[] buildDeletionDocMaps(List<CodecReader> readers) {

    int totalDocs = 0;
    int numReaders = readers.size();
    DocMap[] docMaps = new DocMap[numReaders];

    for (int i = 0; i < numReaders; i++) {
      LeafReader reader = readers.get(i);
      // 获取此时最新的 live位图
      Bits liveDocs = reader.getLiveDocs();

      final PackedLongValues delDocMap;
      if (liveDocs != null) {
        // 将doc编号前移  避免已经被删除的doc占用无效的位置
        delDocMap = removeDeletes(reader.maxDoc(), liveDocs);
      } else {
        // 因为所有doc都存活 所以不需要处理
        delDocMap = null;
      }

      // 段与段之间的 docId 被连接了起来
      final int docBase = totalDocs;
      docMaps[i] = new DocMap() {
        @Override
        public int get(int docID) {
          // liveDocs 为null 代表所有doc都存活 直接采用累加的方式计算docId就好
          if (liveDocs == null) {
            // docBase 是上一个段的 最后一个docId
            return docBase + docID;
          } else if (liveDocs.get(docID)) {
            return docBase + (int) delDocMap.get(docID);
          } else {
            return -1;
          }
        }
      };
      totalDocs += reader.numDocs();
    }

    return docMaps;
  }

  /**
   * 将每个reader内部的数据 按照sort 进行排序
   * @param readers
   * @param indexSort   一个sort记录了所有影响排序的 field信息 这些field 被称为 sortField
   * @return
   * @throws IOException
   */
  private DocMap[] buildDocMaps(List<CodecReader> readers, Sort indexSort) throws IOException {

    if (indexSort == null) {
      // no index sort ... we only must map around deletions, and rebase to the merged segment's docID space
      // 避免在合并时写入了已经被删除的doc  这里针对被删除的doc映射到-1
      return buildDeletionDocMaps(readers);
    // 利用二叉堆实现多个reader对象的重排序
    } else {
      // do a merge sort of the incoming leaves:
      long t0 = System.nanoTime();
      // 构建了一个能将多个reader的doc 映射成全局doc的对象
      DocMap[] result = MultiSorter.sort(indexSort, readers);
      if (result == null) {
        // 这里代表reader内的数据一开始就是有序的 就不会处理del了 所以需要单独做一次排除操作
        // already sorted so we can switch back to map around deletions
        return buildDeletionDocMaps(readers);
      } else {
        needsIndexSort = true;
      }
      long t1 = System.nanoTime();
      if (infoStream.isEnabled("SM")) {
        infoStream.message("SM", String.format(Locale.ROOT, "%.2f msec to build merge sorted DocMaps", (t1-t0)/1000000.0));
      }
      return result;
    }
  }

  /**
   * @param originalReaders
   * @param segmentInfo
   * @return
   * @throws IOException
   */
  private List<CodecReader> maybeSortReaders(List<CodecReader> originalReaders, SegmentInfo segmentInfo) throws IOException {

    // Default to identity:
    for(int i=0;i<originalReaders.size();i++) {
      // 默认情况下保持原顺序返回
      leafDocMaps[i] = new DocMap() {
          @Override
          public int get(int docID) {
            return docID;
          }
        };
    }

    // 如果写入的目标 segment没有排序要求 那么就按照这个顺序进行merge就好
    Sort indexSort = segmentInfo.getIndexSort();
    if (indexSort == null) {
      return originalReaders;
    }

    List<CodecReader> readers = new ArrayList<>(originalReaders.size());

    // 做一个校验 确保之前reader是基于此时使用的排序规则排序的
    for (CodecReader leaf : originalReaders) {
      Sort segmentSort = leaf.getMetaData().getSort();
      if (segmentSort == null || isCongruentSort(indexSort, segmentSort) == false) {
        throw new IllegalArgumentException("index sort mismatch: merged segment has sort=" + indexSort +
            " but to-be-merged segment has sort=" + (segmentSort == null ? "null" : segmentSort));
      }
      readers.add(leaf);
    }

    return readers;
  }

  /** A map of doc IDs. */
  // 通过原有的顺序 可以映射到排序后的doc
  public static abstract class DocMap {
    /** Sole constructor */
    public DocMap() {
    }

    /** Return the mapped docID or -1 if the given doc is not mapped. */
    public abstract int get(int docID);
  }

  /**
   * 将还存活的 doc 写入到 压缩结构中
   * @param maxDoc
   * @param liveDocs
   * @return
   */
  static PackedLongValues removeDeletes(final int maxDoc, final Bits liveDocs) {
    // 该结构支持每次写入一个long值 并在build时 检测写入的所有 long值 最多占用多少位 之后按位存储数据
    final PackedLongValues.Builder docMapBuilder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    int del = 0;
    for (int i = 0; i < maxDoc; ++i) {
      docMapBuilder.add(i - del);
      // 这里是在往前移吧 比如 2，3，4，5 此时3被删除了 4就变成了新的3  5变成了新的4
      if (liveDocs.get(i) == false) {
        ++del;
      }
    }
    return docMapBuilder.build();
  }
}
