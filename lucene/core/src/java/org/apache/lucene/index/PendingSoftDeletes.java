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

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;

final class PendingSoftDeletes extends PendingDeletes {

  /**
   * 被标记成软删除的字段
   */
  private final String field;
  private long dvGeneration = -2;
  /**
   * 软删除的信息会记录在父类的liveDoc位图中  所以这里需要一个只记录硬删除的位图做区分
   */
  private final PendingDeletes hardDeletes;

  PendingSoftDeletes(String field, SegmentCommitInfo info)  {
    super(info, null, info.getDelCount(true) == 0);
    this.field = field;
    hardDeletes = new PendingDeletes(info);
  }

  PendingSoftDeletes(String field, SegmentReader reader, SegmentCommitInfo info) {
    super(reader, info);
    this.field = field;
    hardDeletes = new PendingDeletes(reader, info);
  }

  @Override
  boolean delete(int docID) throws IOException {
    FixedBitSet mutableBits = getMutableBits(); // we need to fetch this first it might be a shared instance with hardDeletes
    // 父类的 liveDoc位图存储的是包含硬删除和软删除的doc  如果使用父类的位图 会无法区分该doc是被硬删除还是软删除
    if (hardDeletes.delete(docID)) {
      // 本次首次删除 需要同步到父对象变量的 liveDoc上
      if (mutableBits.get(docID)) { // delete it here too!
        mutableBits.clear(docID);
        assert hardDeletes.delete(docID) == false;
      } else {
        // if it was deleted subtract the delCount
        // pendingDeleteCount 表示的是待处理的软删除doc
        // 但是此时该doc刚好被硬删除了  所以相当于不需要处理该doc  pending数 减小了
        pendingDeleteCount--;
        assert assertPendingDeletes();
      }
      return true;
    }
    return false;
  }

  /**
   * 总删除数量 就是用于标记软删除数量的 pendingDeleteCount + 加上硬删除数量
   * @return
   */
  @Override
  protected int numPendingDeletes() {
    return super.numPendingDeletes() + hardDeletes.numPendingDeletes();
  }

  /**
   * 当初始化对应的reader对象时 先读取 liveDoc 并将存活的doc进行同步
   * @param reader
   * @param info
   * @throws IOException
   */
  @Override
  void onNewReader(CodecReader reader, SegmentCommitInfo info) throws IOException {
    super.onNewReader(reader, info);
    // 该对象内部维护一个 hardDeletes
    hardDeletes.onNewReader(reader, info);

    // 该值初始状态为-2 首次调用必然比 info.getDocValuesGen()小
    if (dvGeneration < info.getDocValuesGen()) { // only re-calculate this if we haven't seen this generation
      final DocIdSetIterator iterator = DocValuesFieldExistsQuery.getDocValuesDocIdSetIterator(field, reader);
      int newDelCount;
      if (iterator != null) { // nothing is deleted we don't have a soft deletes field in this segment
        assert info.info.maxDoc() > 0 : "maxDoc is 0";
        // 将被标记成软删除的doc 输入到父类的liveDoc位图上
        newDelCount = applySoftDeletes(iterator, getMutableBits());
        assert newDelCount >= 0 : " illegal pending delete count: " + newDelCount;
      } else {
        // 如果field类型为 NONE是无法存储 docValue信息的 也就无法遍历到doc 所以不会删除任何数据
        newDelCount = 0;
      }
      assert info.getSoftDelCount() == newDelCount : "softDeleteCount doesn't match " + info.getSoftDelCount() + " != " + newDelCount;
      dvGeneration = info.getDocValuesGen();
    }
    assert getDelCount() <= info.info.maxDoc() : getDelCount() + " > " + info.info.maxDoc();
  }

  @Override
  boolean writeLiveDocs(Directory dir) throws IOException {
    // we need to set this here to make sure our stats in SCI are up-to-date otherwise we might hit an assertion
    // when the hard deletes are set since we need to account for docs that used to be only soft-delete but now hard-deleted
    // 更新此时的软删除数量  但是软删除的doc此时还没有写入到 liveDoc中 仅仅是在segmentInfo中记录一个数量
    this.info.setSoftDelCount(this.info.getSoftDelCount() + pendingDeleteCount);
    super.dropChanges();
    // delegate the write to the hard deletes - it will only write if somebody used it.
    if (hardDeletes.writeLiveDocs(dir)) {
      return true;
    }
    return false;
  }

  @Override
  void dropChanges() {
    // don't reset anything here - this is called after a merge (successful or not) to prevent
    // rewriting the deleted docs to disk. we only pass it on and reset the number of pending deletes
    hardDeletes.dropChanges();
  }

  /**
   * Clears all bits in the given bitset that are set and are also in the given DocIdSetIterator.
   *
   * @param iterator the doc ID set iterator for apply   包含所有软删除的field 命中的doc
   * @param bits the bit set to apply the deletes to
   * @return the number of bits changed by this function
   *
   */
  static int applySoftDeletes(DocIdSetIterator iterator, FixedBitSet bits) throws IOException {
    assert iterator != null;
    int newDeletes = 0;
    int docID;

    // 该迭代器的意思是 field可能追加到了新的doc上 那么该doc就需要标记成删除
    DocValuesFieldUpdates.Iterator hasValue = iterator instanceof DocValuesFieldUpdates.Iterator
        ? (DocValuesFieldUpdates.Iterator) iterator : null;
    while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      // hasValue == null 时 就代表是简单的情况 就是将迭代器命中的doc 标记成删除
      // hasValue.hasValue() 代表本次是基于 docValue的更新触发的并且可能原本某个doc不存在值 现在标记成了存在值 需要标记成软删除
      if (hasValue == null || hasValue.hasValue()) {
        if (bits.get(docID)) { // doc is live - clear it
          bits.clear(docID);
          newDeletes++;
          // now that we know we deleted it and we fully control the hard deletes we can do correct accounting
          // below.
        }
      } else {
        // 因为之前field在该doc下的值被移除掉了 反而认为该doc不需要被删除
        if (bits.get(docID) == false) {
          bits.set(docID);
          newDeletes--;
        }
      }
    }
    return newDeletes;
  }

  /**
   * 当 field.value发生变化时  它可能此时追加到了新的doc  那么就要将该doc标记成软删除
   * @param info the field info of the field that's updated
   * @param iterator the values to apply
   * @throws IOException
   */
  @Override
  void onDocValuesUpdate(FieldInfo info, DocValuesFieldUpdates.Iterator iterator) throws IOException {
    if (this.field.equals(info.name)) {
      // 更新此时软删除的数量
      pendingDeleteCount += applySoftDeletes(iterator, getMutableBits());
      assert assertPendingDeletes();
      this.info.setSoftDelCount(this.info.getSoftDelCount() + pendingDeleteCount);
      super.dropChanges();
    }
    assert dvGeneration < info.getDocValuesGen() : "we have seen this generation update already: " + dvGeneration + " vs. " + info.getDocValuesGen();
    assert dvGeneration != -2 : "docValues generation is still uninitialized";
    dvGeneration = info.getDocValuesGen();
  }

  private boolean assertPendingDeletes() {
    assert pendingDeleteCount + info.getSoftDelCount() >= 0 : " illegal pending delete count: " + pendingDeleteCount + info.getSoftDelCount();
    assert info.info.maxDoc() >= getDelCount();
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PendingSoftDeletes(seg=").append(info);
    sb.append(" numPendingDeletes=").append(pendingDeleteCount);
    sb.append(" field=").append(field);
    sb.append(" dvGeneration=").append(dvGeneration);
    sb.append(" hardDeletes=").append(hardDeletes);
    return sb.toString();
  }

  @Override
  int numDeletesToMerge(MergePolicy policy, IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    ensureInitialized(readerIOSupplier); // initialize to ensure we have accurate counts
    return super.numDeletesToMerge(policy, readerIOSupplier);
  }

  private void ensureInitialized(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    if (dvGeneration == -2) {
      // 当此时还没有设置过field的信息时 主动获取 并根据fieldDocValue 获取本标记成软删除的doc
      FieldInfos fieldInfos = readFieldInfos();
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      // we try to only open a reader if it's really necessary ie. indices that are mainly append only might have
      // big segments that don't even have any docs in the soft deletes field. In such a case it's simply
      // enough to look at the FieldInfo for the field and check if the field has DocValues
      if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE) {
        // in order to get accurate numbers we need to have a least one reader see here.
        onNewReader(readerIOSupplier.get(), info);
      } else {
        // we are safe here since we don't have any doc values for the soft-delete field on disk
        // no need to open a new reader
        dvGeneration = fieldInfo == null ? -1 : fieldInfo.getDocValuesGen();
      }
    }
  }

  @Override
  boolean isFullyDeleted(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    ensureInitialized(readerIOSupplier); // initialize to ensure we have accurate counts - only needed in the soft-delete case
    return super.isFullyDeleted(readerIOSupplier);
  }

  /**
   * 读取此时该segment下最新的fieldInfo信息
   * @return
   * @throws IOException
   */
  private FieldInfos readFieldInfos() throws IOException {
    SegmentInfo segInfo = info.info;
    Directory dir = segInfo.dir;
    if (info.hasFieldUpdates() == false) {
      // updates always outside of CFS
      Closeable toClose;
      if (segInfo.getUseCompoundFile()) {
        toClose = dir = segInfo.getCodec().compoundFormat().getCompoundReader(segInfo.dir, segInfo, IOContext.READONCE);
      } else {
        toClose = null;
        dir = segInfo.dir;
      }
      try {
        return segInfo.getCodec().fieldInfosFormat().read(dir, segInfo, "", IOContext.READONCE);
      } finally {
        IOUtils.close(toClose);
      }
    } else {
      FieldInfosFormat fisFormat = segInfo.getCodec().fieldInfosFormat();
      final String segmentSuffix = Long.toString(info.getFieldInfosGen(), Character.MAX_RADIX);
      return fisFormat.read(dir, segInfo, segmentSuffix, IOContext.READONCE);
    }
  }

  @Override
  Bits getHardLiveDocs() {
    return hardDeletes.getLiveDocs();
  }

  @Override
  boolean mustInitOnDelete() {
    return liveDocsInitialized == false;
  }

  /**
   * 计算软删除的数量总数
   * @param softDeletedDocs
   * @param hardDeletes  此时的liveDoc 位图已经经过一轮删除了 （通过termNode 将term命中的doc标记为删除状态）
   * @return
   * @throws IOException
   */
  static int countSoftDeletes(DocIdSetIterator softDeletedDocs, Bits hardDeletes) throws IOException {
    int count = 0;
    if (softDeletedDocs != null) {
      int doc;
      while ((doc = softDeletedDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (hardDeletes == null || hardDeletes.get(doc)) {
          count++;
        }
      }
    }
    return count;
  }
}
