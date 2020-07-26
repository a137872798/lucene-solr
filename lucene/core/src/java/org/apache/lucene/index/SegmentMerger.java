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
import java.util.List;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;

/**
 * The SegmentMerger class combines two or more Segments, represented by an
 * IndexReader, into a single Segment.  Call the merge method to combine the
 * segments.
 *
 * @see #merge
 * 该对象负责将多个 segment 融合到一个中
 */
final class SegmentMerger {
  /**
   * 这些segment 归属于哪个directory
   */
  private final Directory directory;

  /**
   * 编解码器
   */
  private final Codec codec;

  /**
   * 记录一些上下文信息
   */
  private final IOContext context;

  /**
   * 记录了一些merge需要的信息
   */
  final MergeState mergeState;
  private final FieldInfos.Builder fieldInfosBuilder;

  /**
   * note, just like in codec apis Directory 'dir' is NOT the same as segmentInfo.dir!!
   * @param readers  本次参与merge的segment对应的reader
   * @param segmentInfo   merge后将会写入到哪个段中
   * @param infoStream
   * @param dir  包装后的目录对象 追加了限流能力   就是在 writerXXX方法时 会阻塞
   * @param fieldNumbers   维护全局的映射对象
   * @param context
   * @throws IOException
   */
  SegmentMerger(List<CodecReader> readers, SegmentInfo segmentInfo, InfoStream infoStream, Directory dir,
                FieldInfos.FieldNumbers fieldNumbers, IOContext context) throws IOException {
    if (context.context != IOContext.Context.MERGE) {
      throw new IllegalArgumentException("IOContext.context should be MERGE; got: " + context.context);
    }

    // 根据reader内部的信息 生成state对象
    mergeState = new MergeState(readers, segmentInfo, infoStream);
    directory = dir;
    this.codec = segmentInfo.getCodec();
    this.context = context;
    // 这样通过该对象创建的 field 会在 全局 Num对象中分配数字
    this.fieldInfosBuilder = new FieldInfos.Builder(fieldNumbers);
    Version minVersion = Version.LATEST;

    // 考虑兼容性 返回了 他们之中最小的版本号
    for (CodecReader reader : readers) {
      Version leafMinVersion = reader.getMetaData().getMinVersion();
      if (leafMinVersion == null) {
        minVersion = null;
        break;
      }
      if (minVersion.onOrAfter(leafMinVersion)) {
        minVersion = leafMinVersion;
      }

    }
    assert segmentInfo.minVersion == null : "The min version should be set by SegmentMerger for merged segments";
    segmentInfo.minVersion = minVersion;
    if (mergeState.infoStream.isEnabled("SM")) {
      if (segmentInfo.getIndexSort() != null) {
        mergeState.infoStream.message("SM", "index sort during merge: " + segmentInfo.getIndexSort());
      }
    }
  }
  
  /**
   * True if any merging should happen
   * 是否有必要进行merge 在生成 state对象时 已经计算并设置过一次 maxDoc了
   */
  boolean shouldMerge() {
    return mergeState.segmentInfo.maxDoc() > 0;
  }

  /**
   * Merges the readers into the directory passed to the constructor
   * @return The number of documents that were merged
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * 将所有相关数据merge后写入到文件
   */
  MergeState merge() throws IOException {
    if (!shouldMerge()) {
      throw new IllegalStateException("Merge would result in 0 document segment");
    }
    mergeFieldInfos();
    long t0 = 0;
    if (mergeState.infoStream.isEnabled("SM")) {
      t0 = System.nanoTime();
    }
    // 将每个 segment的doc数据 写入到新的段中   写入到doc内的数据都是已经排好序的吗  在哪里处理的 以及为什么要这么做???
    int numMerged = mergeFields();
    if (mergeState.infoStream.isEnabled("SM")) {
      long t1 = System.nanoTime();
      mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge stored fields [" + numMerged + " docs]");
    }
    assert numMerged == mergeState.segmentInfo.maxDoc(): "numMerged=" + numMerged + " vs mergeState.segmentInfo.maxDoc()=" + mergeState.segmentInfo.maxDoc();

    // 这里创建2个记录状态信息的对象
    final SegmentWriteState segmentWriteState = new SegmentWriteState(mergeState.infoStream, directory, mergeState.segmentInfo,
                                                                      mergeState.mergeFieldInfos, null, context);
    final SegmentReadState segmentReadState = new SegmentReadState(directory, mergeState.segmentInfo, mergeState.mergeFieldInfos,
        IOContext.READ, segmentWriteState.segmentSuffix);

    // fieldInfos 由参与merge的所有segment携带的 fieldInfo 合并成  只要有一个fieldInfo 携带标准因子 该标识就为true
    if (mergeState.mergeFieldInfos.hasNorms()) {
      if (mergeState.infoStream.isEnabled("SM")) {
        t0 = System.nanoTime();
      }
      // 这里完成了对标准因子的合并
      mergeNorms(segmentWriteState);
      if (mergeState.infoStream.isEnabled("SM")) {
        long t1 = System.nanoTime();
        mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge norms [" + numMerged + " docs]");
      }
    }

    if (mergeState.infoStream.isEnabled("SM")) {
      t0 = System.nanoTime();
    }
    // 这里找到合并后段的 标准因子
    try (NormsProducer norms = mergeState.mergeFieldInfos.hasNorms()
        ? codec.normsFormat().normsProducer(segmentReadState)
        : null) {
      NormsProducer normsMergeInstance = null;
      if (norms != null) {
        // Use the merge instance in order to reuse the same IndexInput for all terms
        normsMergeInstance = norms.getMergeInstance();
      }
      // term和doc是什么关系  以及 为什么要针对标准因子 单独merge一次
      mergeTerms(segmentWriteState, normsMergeInstance);
    }
    if (mergeState.infoStream.isEnabled("SM")) {
      long t1 = System.nanoTime();
      mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge postings [" + numMerged + " docs]");
    }

    if (mergeState.infoStream.isEnabled("SM")) {
      t0 = System.nanoTime();
    }
    if (mergeState.mergeFieldInfos.hasDocValues()) {
      mergeDocValues(segmentWriteState);
    }
    if (mergeState.infoStream.isEnabled("SM")) {
      long t1 = System.nanoTime();
      mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge doc values [" + numMerged + " docs]");
    }

    if (mergeState.infoStream.isEnabled("SM")) {
      t0 = System.nanoTime();
    }
    if (mergeState.mergeFieldInfos.hasPointValues()) {
      mergePoints(segmentWriteState);
    }
    if (mergeState.infoStream.isEnabled("SM")) {
      long t1 = System.nanoTime();
      mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge points [" + numMerged + " docs]");
    }

    if (mergeState.mergeFieldInfos.hasVectors()) {
      if (mergeState.infoStream.isEnabled("SM")) {
        t0 = System.nanoTime();
      }
      numMerged = mergeVectors();
      if (mergeState.infoStream.isEnabled("SM")) {
        long t1 = System.nanoTime();
        mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to merge vectors [" + numMerged + " docs]");
      }
      assert numMerged == mergeState.segmentInfo.maxDoc();
    }
    
    // write the merged infos
    if (mergeState.infoStream.isEnabled("SM")) {
      t0 = System.nanoTime();
    }
    codec.fieldInfosFormat().write(directory, mergeState.segmentInfo, "", mergeState.mergeFieldInfos, context);
    if (mergeState.infoStream.isEnabled("SM")) {
      long t1 = System.nanoTime();
      mergeState.infoStream.message("SM", ((t1-t0)/1000000) + " msec to write field infos [" + numMerged + " docs]");
    }

    return mergeState;
  }

  private void mergeDocValues(SegmentWriteState segmentWriteState) throws IOException {
    try (DocValuesConsumer consumer = codec.docValuesFormat().fieldsConsumer(segmentWriteState)) {
      consumer.merge(mergeState);
    }
  }

  private void mergePoints(SegmentWriteState segmentWriteState) throws IOException {
    try (PointsWriter writer = codec.pointsFormat().fieldsWriter(segmentWriteState)) {
      writer.merge(mergeState);
    }
  }

  /**
   * 合并标准因子信息
   * @param segmentWriteState
   * @throws IOException
   */
  private void mergeNorms(SegmentWriteState segmentWriteState) throws IOException {
    try (NormsConsumer consumer = codec.normsFormat().normsConsumer(segmentWriteState)) {
      consumer.merge(mergeState);
    }
  }

  /**
   * 将 fieldInfo 信息merge   这里会对fieldInfo 去重  (这里认为相同指的是 fieldInfo.name 一致 与num无关)
   */
  public void mergeFieldInfos() {
    for (FieldInfos readerFieldInfos : mergeState.fieldInfos) {
      for (FieldInfo fi : readerFieldInfos) {
        fieldInfosBuilder.add(fi);
      }
    }
    // 在这里为 merge对象设置 合并后相关的fieldInfo信息
    mergeState.mergeFieldInfos = fieldInfosBuilder.finish();
  }

  /**
   * Merge stored fields from each of the segments into the new one.
   * @return The number of documents in all of the readers
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  private int mergeFields() throws IOException {
    // 打开输出流 将merge数据写入到文件中
    try (StoredFieldsWriter fieldsWriter = codec.storedFieldsFormat().fieldsWriter(directory, mergeState.segmentInfo, context)) {
      return fieldsWriter.merge(mergeState);
    }
  }

  /**
   * Merge the TermVectors from each of the segments into the new one.
   * @throws IOException if there is a low-level IO error
   */
  private int mergeVectors() throws IOException {
    try (TermVectorsWriter termVectorsWriter = codec.termVectorsFormat().vectorsWriter(directory, mergeState.segmentInfo, context)) {
      return termVectorsWriter.merge(mergeState);
    }
  }

  private void mergeTerms(SegmentWriteState segmentWriteState, NormsProducer norms) throws IOException {
    try (FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(segmentWriteState)) {
      consumer.merge(mergeState, norms);
    }
  }
}
