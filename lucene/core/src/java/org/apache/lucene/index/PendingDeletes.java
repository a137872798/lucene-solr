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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;

/**
 * This class handles accounting and applying pending deletes for live segment readers
 * 该对象维护此时待删除的 doc 以及还存活的doc
 */
class PendingDeletes {


  /**
   * 记录的是针对哪个段的信息
   */
  protected final SegmentCommitInfo info;
  // Read-only live docs, null until live docs are initialized or if all docs are alive
  // 描述此时还存在的 docId 位图   该对象未被初始化时默认所有doc都存活 (节省空间的一种方法)
  private Bits liveDocs;
  // Writeable live docs, null if this instance is not ready to accept writes, in which
  // case getMutableBits needs to be called
  // 当要删除某个doc之前 先操作该位图  之后应该有一个同步操作 将liveDoc与该位图同步
  private FixedBitSet writeableLiveDocs;

  /**
   * 当前有多少待删除的doc   每次调用delete只会增加该值 而不会立即写入到索引文件
   */
  protected int pendingDeleteCount;

  /**
   * liveDocs 是否被初始化
   */
  boolean liveDocsInitialized;

  PendingDeletes(SegmentReader reader, SegmentCommitInfo info) {
    this(info, reader.getLiveDocs(), true);
    // 也就是删除的文档最先反映在 reader对象上  当真正删除后 修改info的delCount
    pendingDeleteCount = reader.numDeletedDocs() - info.getDelCount();
  }

  PendingDeletes(SegmentCommitInfo info) {
    this(info, null, info.hasDeletions() == false);
    // if we don't have deletions we can mark it as initialized since we might receive deletes on a segment
    // without having a reader opened on it ie. after a merge when we apply the deletes that IW received while merging.
    // For segments that were published we enforce a reader in the BufferedUpdatesStream.SegmentState ctor
  }

  PendingDeletes(SegmentCommitInfo info, Bits liveDocs, boolean liveDocsInitialized) {
    this.info = info;
    this.liveDocs = liveDocs;
    pendingDeleteCount = 0;
    this.liveDocsInitialized = liveDocsInitialized;
  }

  /**
   * 返回可变位图 数据根据 liveDocs 初始化
   * @return
   */
  protected FixedBitSet getMutableBits() {
    // if we pull mutable bits but we haven't been initialized something is completely off.
    // this means we receive deletes without having the bitset that is on-disk ready to be cloned
    assert liveDocsInitialized : "can't delete if liveDocs are not initialized";
    if (writeableLiveDocs == null) {
      // Copy on write: this means we've cloned a
      // SegmentReader sharing the current liveDocs
      // instance; must now make a private clone so we can
      // change it:
      if (liveDocs != null) {
        writeableLiveDocs = FixedBitSet.copyOf(liveDocs);
      } else {
        // 在liveDocs 还没有初始化的时候 认为所有doc都存活
        writeableLiveDocs = new FixedBitSet(info.info.maxDoc());
        writeableLiveDocs.set(0, info.info.maxDoc());
      }
      liveDocs = writeableLiveDocs.asReadOnlyBits();
    }
    return writeableLiveDocs;
  }


  /**
   * Marks a document as deleted in this segment and return true if a document got actually deleted or
   * if the document was already deleted.
   * 追加某个要删除的 doc
   */
  boolean delete(int docID) throws IOException {
    assert info.info.maxDoc() > 0;
    FixedBitSet mutableBits = getMutableBits();
    assert mutableBits != null;
    assert docID >= 0 && docID < mutableBits.length() : "out of bounds: docid=" + docID + " liveDocsLength=" + mutableBits.length() + " seg=" + info.info.name + " maxDoc=" + info.info.maxDoc();
    final boolean didDelete = mutableBits.get(docID);
    // 如果位图上的标识被清除代表增加了要删除的doc
    if (didDelete) {
      // 每次要修改的都是 writableBitSet
      // 之后才会同步到liveDocs，那个时候应该就是写入到索引文件的时候
      mutableBits.clear(docID);
      pendingDeleteCount++;
    }
    return didDelete;
  }

  /**
   * Returns a snapshot of the current live docs.
   */
  Bits getLiveDocs() {
    // Prevent modifications to the returned live docs
    // 这个方法应该有特定的调用时机 否则针对writeableLiveDocs 的改动都丢失了
    writeableLiveDocs = null;
    return liveDocs;
  }

  /**
   * Returns a snapshot of the hard live docs.
   */
  Bits getHardLiveDocs() {
    return getLiveDocs();
  }

  /**
   * Returns the number of pending deletes that are not written to disk.
   */
  protected int numPendingDeletes() {
    return pendingDeleteCount;
  }

  /**
   * Called once a new reader is opened for this segment ie. when deletes or updates are applied.
   * 某个reader 初始化完成后 读取了 docLive文件的数据  这时就可以用来初始化内部的 liveDocs数据了
   */
  void onNewReader(CodecReader reader, SegmentCommitInfo info) throws IOException {
    if (liveDocsInitialized == false) {
      assert writeableLiveDocs == null;
      if (reader.hasDeletions()) {
        // we only initialize this once either in the ctor or here
        // if we use the live docs from a reader it has to be in a situation where we don't
        // have any existing live docs
        assert pendingDeleteCount == 0 : "pendingDeleteCount: " + pendingDeleteCount;
        // 设置 live位图
        liveDocs = reader.getLiveDocs();
        assert liveDocs == null || assertCheckLiveDocs(liveDocs, info.info.maxDoc(), info.getDelCount());
      }
      // 设置位图初始化完成的标识
      liveDocsInitialized = true;
    }
  }

  private boolean assertCheckLiveDocs(Bits bits, int expectedLength, int expectedDeleteCount) {
    assert bits.length() == expectedLength;
    int deletedCount = 0;
    for (int i = 0; i < bits.length(); i++) {
      if (bits.get(i) == false) {
        deletedCount++;
      }
    }
    assert deletedCount == expectedDeleteCount : "deleted: " + deletedCount + " != expected: " + expectedDeleteCount;
    return true;
  }

  /**
   * Resets the pending docs
   */
  void dropChanges() {
    pendingDeleteCount = 0;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PendingDeletes(seg=").append(info);
    sb.append(" numPendingDeletes=").append(pendingDeleteCount);
    sb.append(" writeable=").append(writeableLiveDocs != null);
    return sb.toString();
  }

  /**
   * Writes the live docs to disk and returns <code>true</code> if any new docs were written.
   * 将标记此时还有哪些 doc存活的信息写入到索引文件
   */
  boolean writeLiveDocs(Directory dir) throws IOException {
    // 只有此时存在未删除的doc时 才允许写入索引文件 否则没有意义
    if (pendingDeleteCount == 0) {
      return false;
    }

    Bits liveDocs = this.liveDocs;
    assert liveDocs != null;
    // We have new deletes
    assert liveDocs.length() == info.info.maxDoc();

    // Do this so we can delete any created files on
    // exception; this saves all codecs from having to do
    // it:
    // 包装成会记录本次操作涉及到的所有文件名的对象
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);

    // We can write directly to the actual name (vs to a
    // .tmp & renaming it) because the file is not live
    // until segments file is written:
    boolean success = false;
    try {
      Codec codec = info.info.getCodec();
      // TODO 执行该方法时 liveDocs 应该已经同步过一次最新数据了 (删除动作已经生效)
      codec.liveDocsFormat().writeLiveDocs(liveDocs, trackingDir, info, pendingDeleteCount, IOContext.DEFAULT);
      success = true;
    } finally {
      if (!success) {
        // Advance only the nextWriteDelGen so that a 2nd
        // attempt to write will write to a new file
        // 更新 gen 之后的重试就会写入到新的文件
        info.advanceNextWriteDelGen();

        // Delete any partially created file(s):
        // 删除刚才写入失败的文件
        for (String fileName : trackingDir.getCreatedFiles()) {
          IOUtils.deleteFilesIgnoringExceptions(dir, fileName);
        }
      }
    }

    // If we hit an exc in the line above (eg disk full)
    // then info's delGen remains pointing to the previous
    // (successfully written) del docs:
    info.advanceDelGen();
    // 更新 info的删除数量
    info.setDelCount(info.getDelCount() + pendingDeleteCount);
    // 重置 pendingDeleteCount
    dropChanges();
    return true;
  }

  /**
   * Returns <code>true</code> iff the segment represented by this {@link PendingDeletes} is fully deleted
   * fully delete 就代表此时所有的doc 都已经被删除了
   */
  boolean isFullyDeleted(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    return getDelCount() == info.info.maxDoc();
  }

  /**
   * Called for every field update for the given field at flush time
   * @param info the field info of the field that's updated
   * @param iterator the values to apply
   */
  void onDocValuesUpdate(FieldInfo info, DocValuesFieldUpdates.Iterator iterator) throws IOException {
  }

  int numDeletesToMerge(MergePolicy policy, IOSupplier<CodecReader> readerIOSupplier) throws IOException {
    return policy.numDeletesToMerge(info, getDelCount(), readerIOSupplier);
  }

  /**
   * Returns true if the given reader needs to be refreshed in order to see the latest deletes
   */
  final boolean needsRefresh(CodecReader reader) {
    return reader.getLiveDocs() != getLiveDocs() || reader.numDeletedDocs() != getDelCount();
  }

  /**
   * Returns the number of deleted docs in the segment.
   */
  final int getDelCount() {
    int delCount = info.getDelCount() + info.getSoftDelCount() + numPendingDeletes();
    return delCount;
  }

  /**
   * Returns the number of live documents in this segment
   */
  final int numDocs() {
    return info.info.maxDoc() - getDelCount();
  }

  // Call only from assert!
  boolean verifyDocCounts(CodecReader reader) {
    int count = 0;
    Bits liveDocs = getLiveDocs();
    if (liveDocs != null) {
      for(int docID = 0; docID < info.info.maxDoc(); docID++) {
        if (liveDocs.get(docID)) {
          count++;
        }
      }
    } else {
      count = info.info.maxDoc();
    }
    assert numDocs() == count: "info.maxDoc=" + info.info.maxDoc() + " info.getDelCount()=" + info.getDelCount() +
        " info.getSoftDelCount()=" + info.getSoftDelCount() +
        " pendingDeletes=" + toString() + " count=" + count + " numDocs: " + numDocs();
    assert reader.numDocs() == numDocs() : "reader.numDocs() = " + reader.numDocs() + " numDocs() " + numDocs();
    assert reader.numDeletedDocs() <= info.info.maxDoc(): "delCount=" + reader.numDeletedDocs() + " info.maxDoc=" +
        info.info.maxDoc() + " rld.pendingDeleteCount=" + numPendingDeletes() +
        " info.getDelCount()=" + info.getDelCount();
    return true;
  }

  /**
   * Returns {@code true} if we have to initialize this PendingDeletes before {@link #delete(int)};
   * otherwise this PendingDeletes is ready to accept deletes. A PendingDeletes can be initialized
   * by providing it a reader via {@link #onNewReader(CodecReader, SegmentCommitInfo)}.
   */
  boolean mustInitOnDelete() {
    return false;
  }
}
