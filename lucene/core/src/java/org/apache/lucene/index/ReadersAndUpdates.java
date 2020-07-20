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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

// Used by IndexWriter to hold open SegmentReaders (for
// searching or merging), plus pending deletes and updates,
// for a given segment
// 统一管理   reader对象  以及 docValueUpdate 对象
final class ReadersAndUpdates {
  // Not final because we replace (clone) when we need to
  // change it and it's been shared:
  final SegmentCommitInfo info;

  // Tracks how many consumers are using this instance:
  // 记录该对象的引用计数
  private final AtomicInteger refCount = new AtomicInteger(1);

  // Set once (null, and then maybe set, and never set again):
  // 内部整合了各种reader 对象
  private SegmentReader reader;

  // How many further deletions we've done against
  // liveDocs vs when we loaded it or last wrote it:
  // 记录该段有关删除doc的信息
  private final PendingDeletes pendingDeletes;

  // the major version this index was created with
  private final int indexCreatedVersionMajor;

  // Indicates whether this segment is currently being merged. While a segment
  // is merging, all field updates are also registered in the
  // mergingNumericUpdates map. Also, calls to writeFieldUpdates merge the 
  // updates with mergingNumericUpdates.
  // That way, when the segment is done merging, IndexWriter can apply the
  // updates on the merged segment too.
  // 代表此时正在merge中
  private boolean isMerging = false;

  // Holds resolved (to docIDs) doc values updates that have not yet been
  // written to the index
  // 记录了某个域下更新了哪些docValue的容器
  private final Map<String,List<DocValuesFieldUpdates>> pendingDVUpdates = new HashMap<>();

  // Holds resolved (to docIDs) doc values updates that were resolved while
  // this segment was being merged; at the end of the merge we carry over
  // these updates (remapping their docIDs) to the newly merged segment
  private final Map<String,List<DocValuesFieldUpdates>> mergingDVUpdates = new HashMap<>();

  // Only set if there are doc values updates against this segment, and the index is sorted:
  Sorter.DocMap sortMap;

  final AtomicLong ramBytesUsed = new AtomicLong();

  /**
   *
   * @param indexCreatedVersionMajor  本次 segmentInfo 对应的主版本号
   * @param info 对应某个段的信息
   * @param pendingDeletes  描述待删除的任务
   */
  ReadersAndUpdates(int indexCreatedVersionMajor, SegmentCommitInfo info, PendingDeletes pendingDeletes) {
    this.info = info;
    this.pendingDeletes = pendingDeletes;
    this.indexCreatedVersionMajor = indexCreatedVersionMajor;
  }

  /** Init from a previously opened SegmentReader.
   *
   * <p>NOTE: steals incoming ref from reader. */
  ReadersAndUpdates(int indexCreatedVersionMajor, SegmentReader reader, PendingDeletes pendingDeletes) throws IOException {
    this(indexCreatedVersionMajor, reader.getOriginalSegmentInfo(), pendingDeletes);
    this.reader = reader;
    pendingDeletes.onNewReader(reader, info);
  }

  public void incRef() {
    final int rc = refCount.incrementAndGet();
    assert rc > 1: "seg=" + info;
  }

  public void decRef() {
    final int rc = refCount.decrementAndGet();
    assert rc >= 0: "seg=" + info;
  }

  public int refCount() {
    final int rc = refCount.get();
    assert rc >= 0;
    return rc;
  }

  /**
   * 总计删除了多少doc  包含待删除的
   * @return
   */
  public synchronized int getDelCount() {
    return pendingDeletes.getDelCount();
  }
  private synchronized boolean assertNoDupGen(List<DocValuesFieldUpdates> fieldUpdates, DocValuesFieldUpdates update) {
    for (int i=0;i<fieldUpdates.size();i++) {
      DocValuesFieldUpdates oldUpdate = fieldUpdates.get(i);
      if (oldUpdate.delGen == update.delGen) {
        throw new AssertionError("duplicate delGen=" + update.delGen + " for seg=" + info);
      }
    }
    return true;
  }

  /** Adds a new resolved (meaning it maps docIDs to new values) doc values packet.  We buffer these in RAM and write to disk when too much
   *  RAM is used or when a merge needs to kick off, or a commit/refresh. */
  // 存储一个 记录了哪些doc会更新的 update 对象
  public synchronized void addDVUpdate(DocValuesFieldUpdates update) throws IOException {
    // 更新已经完成就不需要添加到该对象中了
    if (update.getFinished() == false) {
      throw new IllegalArgumentException("call finish first");
    }
    List<DocValuesFieldUpdates> fieldUpdates = pendingDVUpdates.computeIfAbsent(update.field, key -> new ArrayList<>());
    assert assertNoDupGen(fieldUpdates, update);

    ramBytesUsed.addAndGet(update.ramBytesUsed());

    fieldUpdates.add(update);

    // TODO merge 相关的先忽略
    if (isMerging) {
      fieldUpdates = mergingDVUpdates.get(update.field);
      if (fieldUpdates == null) {
        fieldUpdates = new ArrayList<>();
        mergingDVUpdates.put(update.field, fieldUpdates);
      }
      fieldUpdates.add(update);
    }
  }

  public synchronized long getNumDVUpdates() {
    long count = 0;
    for (List<DocValuesFieldUpdates> updates : pendingDVUpdates.values()) {
      count += updates.size();
    }
    return count;
  }
  

  /** Returns a {@link SegmentReader}. */
  // 获取内部真正用于读取索引文件的对象
  public synchronized SegmentReader getReader(IOContext context) throws IOException {
    if (reader == null) {
      // We steal returned ref:
      // 这个reader 整合了读取各种field 数据的逻辑
      reader = new SegmentReader(info, indexCreatedVersionMajor, context);
      // 数据读取完后 立即将当前还存活的 doc位图回填到 pendingDelete 对象中
      pendingDeletes.onNewReader(reader, info);
    }

    // Ref for caller
    reader.incRef();
    return reader;
  }

  public synchronized void release(SegmentReader sr) throws IOException {
    assert info == sr.getOriginalSegmentInfo();
    // 对应 getReader 中的 incRef 同时引用计数归0时 释放句柄
    sr.decRef();
  }

  /**
   * 将删除信息同步到  pendingDelete中
   * @param docID
   * @return
   * @throws IOException
   */
  public synchronized boolean delete(int docID) throws IOException {
    if (reader == null && pendingDeletes.mustInitOnDelete()) {
      getReader(IOContext.READ).decRef(); // pass a reader to initialize the pending deletes
    }
    return pendingDeletes.delete(docID);
  }

  // NOTE: removes callers ref    释放引用计数 尝试关闭内部所有reader 对象
  public synchronized void dropReaders() throws IOException {
    // TODO: can we somehow use IOUtils here...?  problem is
    // we are calling .decRef not .close)...
    if (reader != null) {
      try {
        reader.decRef();
      } finally {
        reader = null;
      }
    }

    decRef();
  }

  /**
   * Returns a ref to a clone. NOTE: you should decRef() the reader when you're
   * done (ie do not call close()).
   * TODO 这里有体现只读吗???
   */
  public synchronized SegmentReader getReadOnlyClone(IOContext context) throws IOException {
    if (reader == null) {
      getReader(context).decRef();
      assert reader != null;
    }
    // force new liveDocs
    Bits liveDocs = pendingDeletes.getLiveDocs();
    if (liveDocs != null) {
      return new SegmentReader(info, reader, liveDocs, pendingDeletes.getHardLiveDocs(), pendingDeletes.numDocs(), true);
    } else {
      // liveDocs == null and reader != null. That can only be if there are no deletes
      // 这里就是正常的创建reader
      assert reader.getLiveDocs() == null;
      reader.incRef();
      return reader;
    }
  }

  synchronized int numDeletesToMerge(MergePolicy policy) throws IOException {
    return pendingDeletes.numDeletesToMerge(policy, this::getLatestReader);
  }

  private synchronized CodecReader getLatestReader() throws IOException {
    if (this.reader == null) {
      // get a reader and dec the ref right away we just make sure we have a reader
      getReader(IOContext.READ).decRef();
    }
    if (pendingDeletes.needsRefresh(reader)) {
      // we have a reader but its live-docs are out of sync. let's create a temporary one that we never share
      swapNewReaderWithLatestLiveDocs();
    }
    return reader;
  }

  /**
   * Returns a snapshot of the live docs.
   */
  public synchronized Bits getLiveDocs() {
    return pendingDeletes.getLiveDocs();
  }

  /**
   * Returns the live-docs bits excluding documents that are not live due to soft-deletes
   */
  public synchronized Bits getHardLiveDocs() {
    return pendingDeletes.getHardLiveDocs();
  }

  public synchronized void dropChanges() {
    // Discard (don't save) changes when we are dropping
    // the reader; this is used only on the sub-readers
    // after a successful merge.  If deletes had
    // accumulated on those sub-readers while the merge
    // is running, by now we have carried forward those
    // deletes onto the newly merged segment, so we can
    // discard them on the sub-readers:
    pendingDeletes.dropChanges();
    dropMergingUpdates();
  }

  // Commit live docs (writes new _X_N.del files) and field updates (writes new
  // _X_N updates files) to the directory; returns true if it wrote any file
  // and false if there were no new deletes or updates to write:
  public synchronized boolean writeLiveDocs(Directory dir) throws IOException {
    return pendingDeletes.writeLiveDocs(dir);
  }

  /**
   * 处理 docValue 对象
   * @param infos   此时对应某个段下 最新的fieldInfo (也就是已经按照docValue 更新过数值的docType)
   * @param dir
   * @param dvFormat
   * @param reader
   * @param fieldFiles
   * @param maxDelGen
   * @param infoStream
   * @throws IOException
   */
  private synchronized void handleDVUpdates(FieldInfos infos,
                                            Directory dir, DocValuesFormat dvFormat, final SegmentReader reader,
                                            Map<Integer,Set<String>> fieldFiles, long maxDelGen, InfoStream infoStream) throws IOException {

    // key 对应fieldName
    for (Entry<String,List<DocValuesFieldUpdates>> ent : pendingDVUpdates.entrySet()) {
      final String field = ent.getKey();
      final List<DocValuesFieldUpdates> updates = ent.getValue();
      // 看来默认所有update对象的 docValue type 都是一样的
      DocValuesType type = updates.get(0).type;
      assert type == DocValuesType.NUMERIC || type == DocValuesType.BINARY : "unsupported type: " + type;
      final List<DocValuesFieldUpdates> updatesToApply = new ArrayList<>();
      long bytes = 0;
      for(DocValuesFieldUpdates update : updates) {
        // 只有年代小于 maxDelGen的才会被考虑需要处理   TODO maxDelGen 难道记录的不是已经处理好的年代吗  不是应该忽略该年代之前的请求吗
        if (update.delGen <= maxDelGen) {
          // safe to apply this one
          bytes += update.ramBytesUsed();
          updatesToApply.add(update);
        }
      }
      if (updatesToApply.isEmpty()) {
        // nothing to apply yet
        continue;
      }
      if (infoStream.isEnabled("BD")) {
        infoStream.message("BD", String.format(Locale.ROOT,
                                               "now write %d pending numeric DV updates for field=%s, seg=%s, bytes=%.3f MB",
                                               updatesToApply.size(),
                                               field,
                                               info,
                                               bytes/1024./1024.));
      }
      // 获取下一个有关 docValue 的年代
      final long nextDocValuesGen = info.getNextDocValuesGen();
      // 生成后缀文件名 并准备开启一个新的索引文件
      final String segmentSuffix = Long.toString(nextDocValuesGen, Character.MAX_RADIX);
      final IOContext updatesContext = new IOContext(new FlushInfo(info.info.maxDoc(), bytes));

      // 找到fieldInfo
      final FieldInfo fieldInfo = infos.fieldInfo(field);
      assert fieldInfo != null;
      // 更新当前该field 的 docValue 所在索引文件的年代
      fieldInfo.setDocValuesGen(nextDocValuesGen);
      // 只针对这个field 创建了一个新的 fieldInfos
      final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { fieldInfo });
      // separately also track which files were created for this gen
      final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
      final SegmentWriteState state = new SegmentWriteState(null, trackingDir, info.info, fieldInfos, null, updatesContext, segmentSuffix);
      try (final DocValuesConsumer fieldsConsumer = dvFormat.fieldsConsumer(state)) {
        Function<FieldInfo, DocValuesFieldUpdates.Iterator> updateSupplier = (info) -> {
          if (info != fieldInfo) {
            throw new IllegalArgumentException("expected field info for field: " + fieldInfo.name + " but got: " + info.name);
          }
          // 更新本次涉及到的所有 update 对象 生成docValue迭代器 并且将他们合并   合并后的迭代器 内部元素排序按照docId的正序
          DocValuesFieldUpdates.Iterator[] subs = new DocValuesFieldUpdates.Iterator[updatesToApply.size()];
          for(int i=0;i<subs.length;i++) {
            subs[i] = updatesToApply.get(i).iterator();
          }
          return  DocValuesFieldUpdates.mergedIterator(subs);
        };
        // 在处理前的钩子
        pendingDeletes.onDocValuesUpdate(fieldInfo, updateSupplier.apply(fieldInfo));

        // 如果docValue 是 二进制类型
        if (type == DocValuesType.BINARY) {
          fieldsConsumer.addBinaryField(fieldInfo, new EmptyDocValuesProducer() {

            // 这里重写了根据 field 获取二进制值的方法
            @Override
            public BinaryDocValues getBinary(FieldInfo fieldInfoIn) throws IOException {
              // 获取 有关该field下所有 更新信息涉及到的 doc 迭代器
              DocValuesFieldUpdates.Iterator iterator = updateSupplier.apply(fieldInfo);


              final MergedDocValues<BinaryDocValues> mergedDocValues = new MergedDocValues<>(
                  reader.getBinaryDocValues(field),
                  DocValuesFieldUpdates.Iterator.asBinaryDocValues(iterator), iterator);
              // Merge sort of the original doc values with updated doc values:
              return new BinaryDocValues() {
                @Override
                public BytesRef binaryValue() throws IOException {
                  return mergedDocValues.currentValuesSupplier.binaryValue();
                }
                @Override
                public boolean advanceExact(int target) {
                  return mergedDocValues.advanceExact(target);
                }
                @Override
                public int docID() {
                  return mergedDocValues.docID();
                }
                @Override
                public int nextDoc() throws IOException {
                  return mergedDocValues.nextDoc();
                }
                @Override
                public int advance(int target) {
                  return mergedDocValues.advance(target);
                }
                @Override
                public long cost() {
                  return mergedDocValues.cost();
                }
              };
            }
          });
        } else {
          // write the numeric updates to a new gen'd docvalues file
          fieldsConsumer.addNumericField(fieldInfo, new EmptyDocValuesProducer() {
            @Override
            public NumericDocValues getNumeric(FieldInfo fieldInfoIn) throws IOException {
              DocValuesFieldUpdates.Iterator iterator = updateSupplier.apply(fieldInfo);
              final MergedDocValues<NumericDocValues> mergedDocValues = new MergedDocValues<>(
                  reader.getNumericDocValues(field),
                  DocValuesFieldUpdates.Iterator.asNumericDocValues(iterator), iterator);
              // Merge sort of the original doc values with updated doc values:
              return new NumericDocValues() {
                @Override
                public long longValue() throws IOException {
                  return mergedDocValues.currentValuesSupplier.longValue();
                }
                @Override
                public boolean advanceExact(int target) {
                  return mergedDocValues.advanceExact(target);
                }
                @Override
                public int docID() {
                  return mergedDocValues.docID();
                }
                @Override
                public int nextDoc() throws IOException {
                  return mergedDocValues.nextDoc();
                }
                @Override
                public int advance(int target) {
                  return mergedDocValues.advance(target);
                }
                @Override
                public long cost() {
                  return mergedDocValues.cost();
                }
              };
            }
          });
        }
      }
      info.advanceDocValuesGen();
      assert !fieldFiles.containsKey(fieldInfo.number);
      fieldFiles.put(fieldInfo.number, trackingDir.getCreatedFiles());
    }
  }

  /**
   * This class merges the current on-disk DV with an incoming update DV instance and merges the two instances
   * giving the incoming update precedence in terms of values, in other words the values of the update always
   * wins over the on-disk version.
   */
  static final class MergedDocValues<DocValuesInstance extends DocValuesIterator> extends DocValuesIterator {
    private final DocValuesFieldUpdates.Iterator updateIterator;
    // merged docID
    private int docIDOut = -1;
    // docID from our original doc values
    private int docIDOnDisk = -1;
    // docID from our updates
    private int updateDocID = -1;

    private final DocValuesInstance onDiskDocValues;
    private final DocValuesInstance updateDocValues;
    DocValuesInstance currentValuesSupplier;

    protected MergedDocValues(DocValuesInstance onDiskDocValues, DocValuesInstance updateDocValues, DocValuesFieldUpdates.Iterator updateIterator) {
      this.onDiskDocValues = onDiskDocValues;
      this.updateDocValues = updateDocValues;
      this.updateIterator = updateIterator;
    }

    @Override
    public int docID() {
      return docIDOut;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return onDiskDocValues.cost();
    }

    @Override
    public int nextDoc() throws IOException {
      boolean hasValue = false;
      do {
        if (docIDOnDisk == docIDOut) {
          if (onDiskDocValues == null) {
            docIDOnDisk = NO_MORE_DOCS;
          } else {
            docIDOnDisk = onDiskDocValues.nextDoc();
          }
        }
        if (updateDocID == docIDOut) {
          updateDocID = updateDocValues.nextDoc();
        }
        if (docIDOnDisk < updateDocID) {
          // no update to this doc - we use the on-disk values
          docIDOut = docIDOnDisk;
          currentValuesSupplier = onDiskDocValues;
          hasValue = true;
        } else {
          docIDOut = updateDocID;
          if (docIDOut != NO_MORE_DOCS) {
            currentValuesSupplier = updateDocValues;
            hasValue = updateIterator.hasValue();
          } else {
            hasValue = true;
          }
        }
      } while (hasValue == false);
      return docIDOut;
    }
  };

  private synchronized Set<String> writeFieldInfosGen(FieldInfos fieldInfos, Directory dir,
                                                      FieldInfosFormat infosFormat) throws IOException {
    final long nextFieldInfosGen = info.getNextFieldInfosGen();
    final String segmentSuffix = Long.toString(nextFieldInfosGen, Character.MAX_RADIX);
    // we write approximately that many bytes (based on Lucene46DVF):
    // HEADER + FOOTER: 40
    // 90 bytes per-field (over estimating long name and attributes map)
    final long estInfosSize = 40 + 90 * fieldInfos.size();
    final IOContext infosContext = new IOContext(new FlushInfo(info.info.maxDoc(), estInfosSize));
    // separately also track which files were created for this gen
    final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
    infosFormat.write(trackingDir, info.info, segmentSuffix, fieldInfos, infosContext);
    info.advanceFieldInfosGen();
    return trackingDir.getCreatedFiles();
  }

  /**
   *
   * @param dir   本次写入的目标目录
   * @param fieldNumbers    fieldNum 映射容器
   * @param maxDelGen   此时已经完成的update对象 对应的delGen
   * @param infoStream
   * @return
   * @throws IOException
   */
  public synchronized boolean writeFieldUpdates(Directory dir, FieldInfos.FieldNumbers fieldNumbers, long maxDelGen, InfoStream infoStream) throws IOException {
    long startTimeNS = System.nanoTime();
    final Map<Integer,Set<String>> newDVFiles = new HashMap<>();
    Set<String> fieldInfosFiles = null;
    FieldInfos fieldInfos = null;
    boolean any = false;
    for (List<DocValuesFieldUpdates> updates : pendingDVUpdates.values()) {
      // Sort by increasing delGen:
      // 按 delGen 正序排序
      Collections.sort(updates, Comparator.comparingLong(a -> a.delGen));
      for (DocValuesFieldUpdates update : updates) {
        if (update.delGen <= maxDelGen && update.any()) {
          any = true;
          break;
        }
      }
    }

    if (any == false) {
      // no updates
      return false;
    }

    // Do this so we can delete any created files on
    // exception; this saves all codecs from having to do it:

    // 上面确定了有需要更新的数据
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
    
    boolean success = false;
    try {
      final Codec codec = info.info.getCodec();

      // reader could be null e.g. for a just merged segment (from
      // IndexWriter.commitMergedDeletes).
      final SegmentReader reader;
      if (this.reader == null) {
        reader = new SegmentReader(info, indexCreatedVersionMajor, IOContext.READONCE);
        pendingDeletes.onNewReader(reader, info);
      } else {
        reader = this.reader;
      }
      
      try {
        // clone FieldInfos so that we can update their dvGen separately from
        // the reader's infos and write them to a new fieldInfos_gen file.
        // 记录当前处理的所有 fieldInfo 最大的号码
        int maxFieldNumber = -1;
        // 每个reader对象对应一组 fieldInfo
        Map<String, FieldInfo> byName = new HashMap<>();
        for (FieldInfo fi : reader.getFieldInfos()) {
          // cannot use builder.add(fi) because it does not preserve
          // the local field number. Field numbers can be different from
          // the global ones if the segment was created externally (and added to
          // this index with IndexWriter#addIndexes(Directory)).
          byName.put(fi.name, cloneFieldInfo(fi, fi.number));
          maxFieldNumber = Math.max(fi.number, maxFieldNumber);
        }

        // create new fields with the right DV type
        FieldInfos.Builder builder = new FieldInfos.Builder(fieldNumbers);
        for (List<DocValuesFieldUpdates> updates : pendingDVUpdates.values()) {
          DocValuesFieldUpdates update = updates.get(0);

          if (byName.containsKey(update.field)) {
            // the field already exists in this segment
            FieldInfo fi = byName.get(update.field);
            // 更新关于这个 field的值类型
            fi.setDocValuesType(update.type);
          } else {
            // the field is not present in this segment so we clone the global field
            // (which is guaranteed to exist) and remaps its field number locally.
            // 如果本次 update 中出现了 之前初始化时不存在的field信息  那么进行初始化 并将信息追加到全局的 field映射对象中
            assert fieldNumbers.contains(update.field, update.type);
            FieldInfo fi = cloneFieldInfo(builder.getOrAdd(update.field), ++maxFieldNumber);
            fi.setDocValuesType(update.type);
            byName.put(fi.name, fi);
          }
        }

        // 这时 byName 已经存放了针对该segment 下最新的field信息了
        fieldInfos = new FieldInfos(byName.values().toArray(new FieldInfo[0]));
        final DocValuesFormat docValuesFormat = codec.docValuesFormat();
        
        handleDVUpdates(fieldInfos, trackingDir, docValuesFormat, reader, newDVFiles, maxDelGen, infoStream);

        fieldInfosFiles = writeFieldInfosGen(fieldInfos, trackingDir, codec.fieldInfosFormat());
      } finally {
        if (reader != this.reader) {
          reader.close();
        }
      }
    
      success = true;
    } finally {
      if (success == false) {
        // Advance only the nextWriteFieldInfosGen and nextWriteDocValuesGen, so
        // that a 2nd attempt to write will write to a new file
        info.advanceNextWriteFieldInfosGen();
        info.advanceNextWriteDocValuesGen();
        
        // Delete any partially created file(s):
        for (String fileName : trackingDir.getCreatedFiles()) {
          IOUtils.deleteFilesIgnoringExceptions(dir, fileName);
        }
      }
    }

    // Prune the now-written DV updates:
    long bytesFreed = 0;
    Iterator<Map.Entry<String,List<DocValuesFieldUpdates>>> it = pendingDVUpdates.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String,List<DocValuesFieldUpdates>> ent = it.next();
      int upto = 0;
      List<DocValuesFieldUpdates> updates = ent.getValue();
      for (DocValuesFieldUpdates update : updates) {
        if (update.delGen > maxDelGen) {
          // not yet applied
          updates.set(upto, update);
          upto++;
        } else {
          bytesFreed += update.ramBytesUsed();
        }
      }
      if (upto == 0) {
        it.remove();
      } else {
        updates.subList(upto, updates.size()).clear();
      }
    }

    long bytes = ramBytesUsed.addAndGet(-bytesFreed);
    assert bytes >= 0;

    // if there is a reader open, reopen it to reflect the updates
    if (reader != null) {
      swapNewReaderWithLatestLiveDocs();
    }

    // writing field updates succeeded
    assert fieldInfosFiles != null;
    info.setFieldInfosFiles(fieldInfosFiles);
    
    // update the doc-values updates files. the files map each field to its set
    // of files, hence we copy from the existing map all fields w/ updates that
    // were not updated in this session, and add new mappings for fields that
    // were updated now.
    assert newDVFiles.isEmpty() == false;
    for (Entry<Integer,Set<String>> e : info.getDocValuesUpdatesFiles().entrySet()) {
      if (newDVFiles.containsKey(e.getKey()) == false) {
        newDVFiles.put(e.getKey(), e.getValue());
      }
    }
    info.setDocValuesUpdatesFiles(newDVFiles);

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD", String.format(Locale.ROOT, "done write field updates for seg=%s; took %.3fs; new files: %s",
                                             info, (System.nanoTime() - startTimeNS)/1000000000.0, newDVFiles));
    }
    return true;
  }

  /**
   * 创建一个 fieldInfo的副本
   * @param fi
   * @param fieldNumber
   * @return
   */
  private FieldInfo cloneFieldInfo(FieldInfo fi, int fieldNumber) {
    return new FieldInfo(fi.name, fieldNumber, fi.hasVectors(), fi.omitsNorms(), fi.hasPayloads(),
        fi.getIndexOptions(), fi.getDocValuesType(), fi.getDocValuesGen(), new HashMap<>(fi.attributes()),
        fi.getPointDimensionCount(), fi.getPointIndexDimensionCount(), fi.getPointNumBytes(), fi.isSoftDeletesField());
  }

  private SegmentReader createNewReaderWithLatestLiveDocs(SegmentReader reader) throws IOException {
    assert reader != null;
    assert Thread.holdsLock(this) : Thread.currentThread().getName();
    SegmentReader newReader = new SegmentReader(info, reader, pendingDeletes.getLiveDocs(),
        pendingDeletes.getHardLiveDocs(), pendingDeletes.numDocs(), true);
    boolean success2 = false;
    try {
      pendingDeletes.onNewReader(newReader, info);
      reader.decRef();
      success2 = true;
    } finally {
      if (success2 == false) {
        newReader.decRef();
      }
    }
    return newReader;
  }

  private void swapNewReaderWithLatestLiveDocs() throws IOException {
    reader = createNewReaderWithLatestLiveDocs(reader);
  }

  synchronized void setIsMerging() {
    // This ensures any newly resolved doc value updates while we are merging are
    // saved for re-applying after this segment is done merging:
    if (isMerging == false) {
      isMerging = true;
      assert mergingDVUpdates.isEmpty();
    }
  }

  synchronized boolean isMerging() {
    return isMerging;
  }

  final static class MergeReader {
    final SegmentReader reader;
    final Bits hardLiveDocs;

    MergeReader(SegmentReader reader, Bits hardLiveDocs) {
      this.reader = reader;
      this.hardLiveDocs = hardLiveDocs;
    }
  }

  /** Returns a reader for merge, with the latest doc values updates and deletions. */
  synchronized MergeReader getReaderForMerge(IOContext context) throws IOException {

    // We must carry over any still-pending DV updates because they were not
    // successfully written, e.g. because there was a hole in the delGens,
    // or they arrived after we wrote all DVs for merge but before we set
    // isMerging here:
    for (Map.Entry<String, List<DocValuesFieldUpdates>> ent : pendingDVUpdates.entrySet()) {
      List<DocValuesFieldUpdates> mergingUpdates = mergingDVUpdates.get(ent.getKey());
      if (mergingUpdates == null) {
        mergingUpdates = new ArrayList<>();
        mergingDVUpdates.put(ent.getKey(), mergingUpdates);
      }
      mergingUpdates.addAll(ent.getValue());
    }
    
    SegmentReader reader = getReader(context);
    if (pendingDeletes.needsRefresh(reader)) {
      // beware of zombies:
      assert pendingDeletes.getLiveDocs() != null;
      reader = createNewReaderWithLatestLiveDocs(reader);
    }
    assert pendingDeletes.verifyDocCounts(reader);
    return new MergeReader(reader, pendingDeletes.getHardLiveDocs());
  }
  
  /**
   * Drops all merging updates. Called from IndexWriter after this segment
   * finished merging (whether successfully or not).
   */
  public synchronized void dropMergingUpdates() {
    mergingDVUpdates.clear();
    isMerging = false;
  }

  public synchronized Map<String,List<DocValuesFieldUpdates>> getMergingDVUpdates() {
    // We must atomically (in single sync'd block) clear isMerging when we return the DV updates otherwise we can lose updates:
    isMerging = false;
    return mergingDVUpdates;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReadersAndLiveDocs(seg=").append(info);
    sb.append(" pendingDeletes=").append(pendingDeletes);
    return sb.toString();
  }

  public synchronized boolean isFullyDeleted() throws IOException {
    return pendingDeletes.isFullyDeleted(this::getLatestReader);
  }

  boolean keepFullyDeletedSegment(MergePolicy mergePolicy) throws IOException {
    return mergePolicy.keepFullyDeletedSegment(this::getLatestReader);
  }
}
