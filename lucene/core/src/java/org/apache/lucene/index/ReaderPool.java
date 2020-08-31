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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

/** Holds shared SegmentReader instances. IndexWriter uses
 *  SegmentReaders for 1) applying deletes/DV updates, 2) doing
 *  merges, 3) handing out a real-time reader.  This pool
 *  reuses instances of the SegmentReaders in all these
 *  places if it is in "near real-time mode" (getReader()
 *  has been called on this instance). */
// 该对象是一个reader池   因为reader对象的创建需要获取句柄 相对比较耗时   并且lucene 经常读取索引文件中的数据 所以就需要这个池对象
// 并且该对象可以统一管理下面所有的reader
final class ReaderPool implements Closeable {

  /**
   * 以段为单位 存储该段下读取各个维度数据的 reader
   */
  private final Map<SegmentCommitInfo,ReadersAndUpdates> readerMap = new HashMap<>();

  /**
   * 读取的目录 以及 包装对象
   */
  private final Directory directory;
  private final Directory originalDirectory;
  /**
   * 该对象维护了全局范围的  fieldName 与 fieldNum的映射关系
   */
  private final FieldInfos.FieldNumbers fieldNumbers;
  /**
   * 获取此时已经完成的 update/delete 的年代信息   对应 BufferedUpdatesStream::getCompletedDelGen
   */
  private final LongSupplier completedDelGenSupplier;
  private final InfoStream infoStream;
  /**
   * 本次 IndexWriter 涉及到的所有段信息
   */
  private final SegmentInfos segmentInfos;

  /**
   * 被标识为 软删除的 field
   */
  private final String softDeletesField;
  // This is a "write once" variable (like the organic dye
  // on a DVD-R that may or may not be heated by a laser and
  // then cooled to permanently record the event): it's
  // false, by default until {@link #enableReaderPooling()}
  // is called for the first time,
  // at which point it's switched to true and never changes
  // back to false.  Once this is true, we hold open and
  // reuse SegmentReader instances internally for applying
  // deletes, doing merges, and reopening near real-time
  // readers.
  // in practice this should be called once the readers are likely
  // to be needed and reused ie if IndexWriter#getReader is called.
  // 是否将reader池化处理   该变量只允许被修改一次 且之后无法修改
  private volatile boolean poolReaders;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   *
   * @param directory   读取的目标目录  已经被包装过 (比如读取前确保此时持有文件锁)
   * @param originalDirectory    原始目录 未包装
   * @param segmentInfos   本次涉及到所有的段信息  如果indexWriter 选择新建一个段 那么该对象内部为空
   * @param fieldNumbers  这是一个以 IndexWriter为单位的全局映射对象
   * @param completedDelGenSupplier   用于获取已经完成del的gen信息
   * @param infoStream
   * @param softDeletesField  被标明使用软删除的字段
   * @param reader  该属性可以为null
   * @throws IOException
   */
  ReaderPool(Directory directory, Directory originalDirectory, SegmentInfos segmentInfos,
             FieldInfos.FieldNumbers fieldNumbers, LongSupplier completedDelGenSupplier, InfoStream infoStream,
             String softDeletesField, StandardDirectoryReader reader) throws IOException {
    this.directory = directory;
    this.originalDirectory = originalDirectory;
    this.segmentInfos = segmentInfos;
    this.fieldNumbers = fieldNumbers;
    this.completedDelGenSupplier = completedDelGenSupplier;
    this.infoStream = infoStream;
    this.softDeletesField = softDeletesField;

    // TODO 先假设 reader 为null的情况
    if (reader != null) {
      // Pre-enroll all segment readers into the reader pool; this is necessary so
      // any in-memory NRT live docs are correctly carried over, and so NRT readers
      // pulled from this IW share the same segment reader:
      List<LeafReaderContext> leaves = reader.leaves();
      assert segmentInfos.size() == leaves.size();
      for (int i=0;i<leaves.size();i++) {
        LeafReaderContext leaf = leaves.get(i);
        SegmentReader segReader = (SegmentReader) leaf.reader();
        SegmentReader newReader = new SegmentReader(segmentInfos.info(i), segReader, segReader.getLiveDocs(),
            segReader.getHardLiveDocs(), segReader.numDocs(), true);
        readerMap.put(newReader.getOriginalSegmentInfo(), new ReadersAndUpdates(segmentInfos.getIndexCreatedVersionMajor(),
            newReader, newPendingDeletes(newReader, newReader.getOriginalSegmentInfo())));
      }
    }
  }

  /** Asserts this info still exists in IW's segment infos */
  synchronized boolean assertInfoIsLive(SegmentCommitInfo info) {
    int idx = segmentInfos.indexOf(info);
    assert idx != -1: "info=" + info + " isn't live";
    assert segmentInfos.info(idx) == info: "info=" + info + " doesn't match live info in segmentInfos";
    return true;
  }

  /**
   * Drops reader for the given {@link SegmentCommitInfo} if it's pooled
   * @return <code>true</code> if a reader is pooled
   * 代表不再需要读取某个 segment的信息了  不再维护对应的reader 对象
   */
  synchronized boolean drop(SegmentCommitInfo info) throws IOException {
    final ReadersAndUpdates rld = readerMap.get(info);
    if (rld != null) {
      assert info == rld.info;
      readerMap.remove(info);
      rld.dropReaders();
      return true;
    }
    return false;
  }

  /**
   * Returns the sum of the ram used by all the buffered readers and updates in MB
   */
  synchronized long ramBytesUsed() {
    long bytes = 0;
    for (ReadersAndUpdates rld : readerMap.values()) {
      bytes += rld.ramBytesUsed.get();
    }
    return bytes;
  }

  /**
   * Returns <code>true</code> if any of the buffered readers and updates has at least one pending delete
   * 是否发生过任何删除动作  (可以是已经删除的数量 或者是pendingDelete的数量)
   */
  synchronized boolean anyDeletions() {
    for(ReadersAndUpdates rld : readerMap.values()) {
      if (rld.getDelCount() > 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Enables reader pooling for this pool. This should be called once the readers in this pool are shared with an
   * outside resource like an NRT reader. Once reader pooling is enabled a {@link ReadersAndUpdates} will be kept around
   * in the reader pool on calling {@link #release(ReadersAndUpdates, boolean)} until the segment get dropped via calls
   * to {@link #drop(SegmentCommitInfo)} or {@link #dropAll()} or {@link #close()}.
   * Reader pooling is disabled upon construction but can't be disabled again once it's enabled.
   * 设置成 允许池化 reader
   */
  void enableReaderPooling() {
    poolReaders = true;
  }

  boolean isReaderPoolingEnabled() {
    return poolReaders;
  }

  /**
   * Releases the {@link ReadersAndUpdates}. This should only be called if the {@link #get(SegmentCommitInfo, boolean)}
   * is called with the create paramter set to true.
   * @return <code>true</code> if any files were written by this release call.
   * 释放某个 rld对象的引用计数
   */
  synchronized boolean release(ReadersAndUpdates rld, boolean assertInfoLive) throws IOException {
    boolean changed = false;
    // Matches incRef in get:
    // 正常情况下该对象被创建时 默认引用计数就是1 又会额外增加1 那么在release时 正常情况是回到 1
    rld.decRef();

    if (rld.refCount() == 0) {
      // This happens if the segment was just merged away,
      // while a buffered deletes packet was still applying deletes/updates to it.
      assert readerMap.containsKey(rld.info) == false: "seg=" + rld.info
          + " has refCount 0 but still unexpectedly exists in the reader pool";
    } else {

      // Pool still holds a ref:
      assert rld.refCount() > 0: "refCount=" + rld.refCount() + " reader=" + rld.info;

      // ReadersAndUpdates 对象被创建时引用计数就是1 同时 如果create为true 引用计数再增加就是2   实际上正常操作每次引用计数刚好会回归1
      if (poolReaders == false && rld.refCount() == 1 && readerMap.containsKey(rld.info)) {
        // This is the last ref to this RLD, and we're not
        // pooling, so remove it:
        // 如果此时存在pendingDelete数据 那么生成最新的 liveDoc索引文件
        if (rld.writeLiveDocs(directory)) {
          // Make sure we only write del docs for a live segment:
          assert assertInfoLive == false || assertInfoIsLive(rld.info);
          // Must checkpoint because we just
          // created new _X_N.del and field updates files;
          // don't call IW.checkpoint because that also
          // increments SIS.version, which we do not want to
          // do here: it was done previously (after we
          // invoked BDS.applyDeletes), whereas here all we
          // did was move the state to disk:
          changed = true;
        }
        if (rld.writeFieldUpdates(directory, fieldNumbers, completedDelGenSupplier.getAsLong(), infoStream)) {
          changed = true;
        }
        if (rld.getNumDVUpdates() == 0) {
          rld.dropReaders();
          readerMap.remove(rld.info);
        } else {
          // We are forced to pool this segment until its deletes fully apply (no delGen gaps)
        }
      }
    }
    // 开启池化后不做任何处理 仅减少引用计数
    return changed;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      dropAll();
    }
  }

  /**
   * Writes all doc values updates to disk if there are any.
   * @return <code>true</code> iff any files where written
   */
  boolean writeAllDocValuesUpdates() throws IOException {
    Collection<ReadersAndUpdates> copy;
    synchronized (this) {
      // this needs to be protected by the reader pool lock otherwise we hit ConcurrentModificationException
      copy = new HashSet<>(readerMap.values());
    }
    boolean any = false;
    for (ReadersAndUpdates rld : copy) {
      any |= rld.writeFieldUpdates(directory, fieldNumbers, completedDelGenSupplier.getAsLong(), infoStream);
    }
    return any;
  }

  /**
   * Writes all doc values updates to disk if there are any.
   * @return <code>true</code> iff any files where written
   * 将之前处理 update对象时 抽取出来的更新信息与之前的数据合并成新的 DV FieldInfo 索引文件
   */
  boolean writeDocValuesUpdatesForMerge(List<SegmentCommitInfo> infos) throws IOException {
    boolean any = false;
    for (SegmentCommitInfo info : infos) {
      ReadersAndUpdates rld = get(info, false);
      if (rld != null) {
        any |= rld.writeFieldUpdates(directory, fieldNumbers, completedDelGenSupplier.getAsLong(), infoStream);
        rld.setIsMerging();
      }
    }
    return any;
  }

  /**
   * Returns a list of all currently maintained ReadersAndUpdates sorted by it's ram consumption largest to smallest.
   * This list can also contain readers that don't consume any ram at this point ie. don't have any updates buffered.
   * 按照内存占用倒序 返回所有reader对象
   */
  synchronized List<ReadersAndUpdates> getReadersByRam() {
    class RamRecordingHolder {
      final ReadersAndUpdates updates;
      final long ramBytesUsed;
      RamRecordingHolder(ReadersAndUpdates updates) {
        this.updates = updates;
        this.ramBytesUsed = updates.ramBytesUsed.get();
      }
    }
    final ArrayList<RamRecordingHolder> readersByRam;
    synchronized (this) {
      if (readerMap.isEmpty()) {
        return Collections.emptyList();
      }
      readersByRam = new ArrayList<>(readerMap.size());
      for (ReadersAndUpdates rld : readerMap.values()) {
        // we have to record the ram usage once and then sort
        // since the ram usage can change concurrently and that will confuse the sort or hit an assertion
        // the we can acquire here is not enough we would need to lock all ReadersAndUpdates to make sure it doesn't
        // change
        readersByRam.add(new RamRecordingHolder(rld));
      }
    }
    // Sort this outside of the lock by largest ramBytesUsed:
    CollectionUtil.introSort(readersByRam, (a, b) -> Long.compare(b.ramBytesUsed, a.ramBytesUsed));
    return Collections.unmodifiableList(readersByRam.stream().map(h -> h.updates).collect(Collectors.toList()));
  }


  /** Remove all our references to readers, and commits
   *  any pending changes. */
  synchronized void dropAll() throws IOException {
    Throwable priorE = null;
    final Iterator<Map.Entry<SegmentCommitInfo,ReadersAndUpdates>> it = readerMap.entrySet().iterator();
    while(it.hasNext()) {
      final ReadersAndUpdates rld = it.next().getValue();

      // Important to remove as-we-go, not with .clear()
      // in the end, in case we hit an exception;
      // otherwise we could over-decref if close() is
      // called again:
      it.remove();

      // NOTE: it is allowed that these decRefs do not
      // actually close the SRs; this happens when a
      // near real-time reader is kept open after the
      // IndexWriter instance is closed:
      try {
        rld.dropReaders();
      } catch (Throwable t) {
        priorE = IOUtils.useOrSuppress(priorE, t);
      }
    }
    assert readerMap.size() == 0;
    if (priorE != null) {
      throw IOUtils.rethrowAlways(priorE);
    }
  }

  /**
   * Commit live docs changes for the segment readers for
   * the provided infos.
   *
   * @throws IOException If there is a low-level I/O error
   * 将此时最新的 liveDoc信息持久化到磁盘
   */
  synchronized boolean commit(SegmentInfos infos) throws IOException {
    boolean atLeastOneChange = false;
    for (SegmentCommitInfo info : infos) {
      final ReadersAndUpdates rld = readerMap.get(info);
      if (rld != null) {
        assert rld.info == info;
        // 将最新的liveDoc信息写入到磁盘    虽然最新的doc已经更新 但是它跟 docValue并没有做同步  docValue此时获取到的也是更新后的最新值 但是已经被删除的doc 可能还是会被读取到  包括这些影响都没有同步到最核心的 term相关的索引文件
        // term还是按照之前的 value解析出来的 以及遍历时还会出现不存在的doc
        boolean changed = rld.writeLiveDocs(directory);
        changed |= rld.writeFieldUpdates(directory, fieldNumbers, completedDelGenSupplier.getAsLong(), infoStream);

        if (changed) {
          // Make sure we only write del docs for a live segment:
          assert assertInfoIsLive(info);

          // Must checkpoint because we just
          // created new _X_N.del and field updates files;
          // don't call IW.checkpoint because that also
          // increments SIS.version, which we do not want to
          // do here: it was done previously (after we
          // invoked BDS.applyDeletes), whereas here all we
          // did was move the state to disk:
          atLeastOneChange = true;
        }
      }
    }
    return atLeastOneChange;
  }

  /**
   * Returns <code>true</code> iff there are any buffered doc values updates. Otherwise <code>false</code>.
   */
  synchronized boolean anyDocValuesChanges() {
    for (ReadersAndUpdates rld : readerMap.values()) {
      // NOTE: we don't check for pending deletes because deletes carry over in RAM to NRT readers
      if (rld.getNumDVUpdates() != 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Obtain a ReadersAndLiveDocs instance from the
   * readerPool.  If create is true, you must later call
   * {@link #release(ReadersAndUpdates, boolean)}.
   * 获取用于读取某个段下各种索引文件的 输入流
   */
  synchronized ReadersAndUpdates get(SegmentCommitInfo info, boolean create) {
    assert info.info.dir ==  originalDirectory: "info.dir=" + info.info.dir + " vs " + originalDirectory;
    if (closed.get()) {
      assert readerMap.isEmpty() : "Reader map is not empty: " + readerMap;
      throw new AlreadyClosedException("ReaderPool is already closed");
    }

    // 尝试从缓存容器中获取
    ReadersAndUpdates rld = readerMap.get(info);
    if (rld == null) {
      if (create == false) {
        return null;
      }

      // 选择新建对象   这里会为 segment创建一个 维护此时liveDoc信息的 pendingDeletes
      rld = new ReadersAndUpdates(segmentInfos.getIndexCreatedVersionMajor(), info, newPendingDeletes(info));
      // Steal initial reference:
      readerMap.put(info, rld);
    } else {
      assert rld.info == info: "rld.info=" + rld.info + " info=" + info + " isLive?=" + assertInfoIsLive(rld.info)
          + " vs " + assertInfoIsLive(info);
    }

    // 增加引用计数
    if (create) {
      // Return ref to caller:
      rld.incRef();
    }

    assert noDups();

    return rld;
  }

  /**
   * 创建一个描述某个段 此时 aliveDoc位图的对象
   * @param info
   * @return
   */
  private PendingDeletes newPendingDeletes(SegmentCommitInfo info) {
    // TODO 先忽略软删除
    return softDeletesField == null ? new PendingDeletes(info) : new PendingSoftDeletes(softDeletesField, info);
  }

  private PendingDeletes newPendingDeletes(SegmentReader reader, SegmentCommitInfo info) {
    return softDeletesField == null ? new PendingDeletes(reader, info) :
        new PendingSoftDeletes(softDeletesField, reader, info);
  }

  // Make sure that every segment appears only once in the
  // pool:
  private boolean noDups() {
    Set<String> seen = new HashSet<>();
    for(SegmentCommitInfo info : readerMap.keySet()) {
      assert !seen.contains(info.info.name);
      seen.add(info.info.name);
    }
    return true;
  }
}