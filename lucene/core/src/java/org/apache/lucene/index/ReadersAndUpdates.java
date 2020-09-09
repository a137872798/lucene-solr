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
// 统一管理某个segment下   reader对象  以及 docValueUpdate 对象
final class ReadersAndUpdates {
    // Not final because we replace (clone) when we need to
    // change it and it's been shared:
    final SegmentCommitInfo info;

    // Tracks how many consumers are using this instance:
    // 记录该对象的引用计数  默认 是1
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
    private final Map<String, List<DocValuesFieldUpdates>> pendingDVUpdates = new HashMap<>();

    // Holds resolved (to docIDs) doc values updates that were resolved while
    // this segment was being merged; at the end of the merge we carry over
    // these updates (remapping their docIDs) to the newly merged segment
    // 在执行merge时 会将pendingDVUpdates 的任务拷贝一份到该容器中
    private final Map<String, List<DocValuesFieldUpdates>> mergingDVUpdates = new HashMap<>();

    // Only set if there are doc values updates against this segment, and the index is sorted:
    Sorter.DocMap sortMap;

    final AtomicLong ramBytesUsed = new AtomicLong();

    /**
     * @param indexCreatedVersionMajor 本次 segmentInfo 对应的主版本号
     * @param info                     对应某个段的信息
     * @param pendingDeletes           维护段的 liveDoc信息
     */
    ReadersAndUpdates(int indexCreatedVersionMajor, SegmentCommitInfo info, PendingDeletes pendingDeletes) {
        this.info = info;
        this.pendingDeletes = pendingDeletes;
        this.indexCreatedVersionMajor = indexCreatedVersionMajor;
    }

    /**
     * Init from a previously opened SegmentReader.
     *
     * <p>NOTE: steals incoming ref from reader.
     */
    ReadersAndUpdates(int indexCreatedVersionMajor, SegmentReader reader, PendingDeletes pendingDeletes) throws IOException {
        this(indexCreatedVersionMajor, reader.getOriginalSegmentInfo(), pendingDeletes);
        this.reader = reader;
        pendingDeletes.onNewReader(reader, info);
    }

    public void incRef() {
        final int rc = refCount.incrementAndGet();
        assert rc > 1 : "seg=" + info;
    }

    public void decRef() {
        final int rc = refCount.decrementAndGet();
        assert rc >= 0 : "seg=" + info;
    }

    public int refCount() {
        final int rc = refCount.get();
        assert rc >= 0;
        return rc;
    }

    /**
     * 总计删除了多少doc  包含待删除的
     *
     * @return
     */
    public synchronized int getDelCount() {
        return pendingDeletes.getDelCount();
    }

    /**
     * ignore
     *
     * @param fieldUpdates
     * @param update
     * @return
     */
    private synchronized boolean assertNoDupGen(List<DocValuesFieldUpdates> fieldUpdates, DocValuesFieldUpdates update) {
        for (int i = 0; i < fieldUpdates.size(); i++) {
            DocValuesFieldUpdates oldUpdate = fieldUpdates.get(i);
            if (oldUpdate.delGen == update.delGen) {
                throw new AssertionError("duplicate delGen=" + update.delGen + " for seg=" + info);
            }
        }
        return true;
    }

    /**
     * Adds a new resolved (meaning it maps docIDs to new values) doc values packet.  We buffer these in RAM and write to disk when too much
     * RAM is used or when a merge needs to kick off, or a commit/refresh.
     * @param update  记录了某个field 下哪些doc 的数据将会更新
     */
    public synchronized void addDVUpdate(DocValuesFieldUpdates update) throws IOException {
        if (update.getFinished() == false) {
            throw new IllegalArgumentException("call finish first");
        }

        // 存储到以field 为key 的容器中
        List<DocValuesFieldUpdates> fieldUpdates = pendingDVUpdates.computeIfAbsent(update.field, key -> new ArrayList<>());
        assert assertNoDupGen(fieldUpdates, update);

        ramBytesUsed.addAndGet(update.ramBytesUsed());

        fieldUpdates.add(update);

        // 记录在merge过程中新增的 update信息  并在merge完成后 将这些update信息 作用到新的segment上
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


    /**
     * Returns a {@link SegmentReader}.
     * 真正维护各种 reader对象的实际是 SegmentReader
     */
    public synchronized SegmentReader getReader(IOContext context) throws IOException {
        if (reader == null) {
            // We steal returned ref:
            // 这个reader 整合了读取各种索引文件的逻辑
            reader = new SegmentReader(info, indexCreatedVersionMajor, context);
            // 数据读取完后 立即将liveDoc位图回填到 pendingDelete 对象中
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
     *
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

    // NOTE: removes callers ref    释放引用计数 关闭reader对象
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

        // 本对象自身也要被丢弃
        decRef();
    }

    /**
     * Returns a ref to a clone. NOTE: you should decRef() the reader when you're
     * done (ie do not call close()).
     */
    public synchronized SegmentReader getReadOnlyClone(IOContext context) throws IOException {
        // 首先reader对象初始化时 引用计数就自带1 在getReader 中回调用 IncRef() 变成2 这里又变回1
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
            assert reader.getLiveDocs() == null;
            // 如果reader对象不是以副本形式返回的话 就需要增加引用计数    1代表reader对象处于可用状态 并且最低值就是1 低于该值该对象会被销毁
            reader.incRef();
            return reader;
        }
    }

    /**
     * 计算该 rld对象对应的 segment下需要删除多少数据
     *
     * @param policy
     * @return
     * @throws IOException
     */
    synchronized int numDeletesToMerge(MergePolicy policy) throws IOException {
        return pendingDeletes.numDeletesToMerge(policy, this::getLatestReader);
    }

    /**
     * 获取最新的reader对象
     * @return
     * @throws IOException
     */
    private synchronized CodecReader getLatestReader() throws IOException {
        // 确保reader对象已经被初始化
        if (this.reader == null) {
            // get a reader and dec the ref right away we just make sure we have a reader
            getReader(IOContext.READ).decRef();
        }
        // 代表此时 reader对象读取的 liveDoc 信息已经过时了 需要重新生成reader 对象
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

    /**
     * 放弃之前未保存的 delete update 数据
     */
    public synchronized void dropChanges() {
        // Discard (don't save) changes when we are dropping
        // the reader; this is used only on the sub-readers
        // after a successful merge.  If deletes had
        // accumulated on those sub-readers while the merge
        // is running, by now we have carried forward those
        // deletes onto the newly merged segment, so we can
        // discard them on the sub-readers:
        pendingDeletes.dropChanges();
        // 丢弃在 merging时插入的更新对象
        dropMergingUpdates();
    }

    // Commit live docs (writes new _X_N.del files) and field updates (writes new
    // _X_N updates files) to the directory; returns true if it wrote any file
    // and false if there were no new deletes or updates to write:
    // 将此时 liveDoc 信息写入到索引文件中
    public synchronized boolean writeLiveDocs(Directory dir) throws IOException {
        return pendingDeletes.writeLiveDocs(dir);
    }

    /**
     *
     * 将此时更新后 最新的docValueType 信息写入到索引文件中
     * @param infos      此时segment下最新的fieldInfos
     * @param dir        目标目录
     * @param dvFormat   对应的文件格式
     * @param reader     用于读取当前segment下所有数据的 reader
     * @param fieldFiles 用于回填数据的容器   key 对应本次处理的fieldNum   value 对应新生成的存储docValue的索引文件
     * @param maxDelGen  此时已经处理完的 update gen 也就是 bufferedUpdateStream.completeGen
     * @param infoStream
     * @throws IOException
     */
    private synchronized void handleDVUpdates(FieldInfos infos,
                                              Directory dir, DocValuesFormat dvFormat, final SegmentReader reader,
                                              Map<Integer, Set<String>> fieldFiles, long maxDelGen, InfoStream infoStream) throws IOException {

        // key对应fieldName
        for (Entry<String, List<DocValuesFieldUpdates>> ent : pendingDVUpdates.entrySet()) {
            final String field = ent.getKey();
            final List<DocValuesFieldUpdates> updates = ent.getValue();
            // 以field为单位 所有docUpdate 对象的 type都是一致的
            DocValuesType type = updates.get(0).type;
            assert type == DocValuesType.NUMERIC || type == DocValuesType.BINARY : "unsupported type: " + type;
            final List<DocValuesFieldUpdates> updatesToApply = new ArrayList<>();
            long bytes = 0;
            for (DocValuesFieldUpdates update : updates) {
                // 只有年代小于 maxDelGen的才会被考虑需要处理
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
                        bytes / 1024. / 1024.));
            }
            // 每当更新某个field下的 docValueType 就更新一次gen
            final long nextDocValuesGen = info.getNextDocValuesGen();
            // 根据年代生成后缀名
            final String segmentSuffix = Long.toString(nextDocValuesGen, Character.MAX_RADIX);
            final IOContext updatesContext = new IOContext(new FlushInfo(info.info.maxDoc(), bytes));

            // 找到fieldInfo
            // ！！！ 从下面的逻辑可以看出  即使将某个field下所有的值都更新成 hasValue == false 但是field本身并不会从这个segment中移除 也就是在merge时 该field还是要写入
            final FieldInfo fieldInfo = infos.fieldInfo(field);
            assert fieldInfo != null;
            // 更新当前该field 的 docValue 所在索引文件的年代
            fieldInfo.setDocValuesGen(nextDocValuesGen);
            // 只针对这个field 创建了一个新的 fieldInfos
            final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[]{fieldInfo});
            // separately also track which files were created for this gen
            final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
            // 生成的  state 对象仅包含 一个fieldInfo
            final SegmentWriteState state = new SegmentWriteState(null, trackingDir, info.info, fieldInfos, null, updatesContext, segmentSuffix);

            // format 对象以field 为单位存储数据
            try (final DocValuesConsumer fieldsConsumer = dvFormat.fieldsConsumer(state)) {
                Function<FieldInfo, DocValuesFieldUpdates.Iterator> updateSupplier = (info) -> {
                    if (info != fieldInfo) {
                        throw new IllegalArgumentException("expected field info for field: " + fieldInfo.name + " but got: " + info.name);
                    }
                    // 每个迭代器 对应一个 符合条件 并会作用在该field上的 update对象
                    DocValuesFieldUpdates.Iterator[] subs = new DocValuesFieldUpdates.Iterator[updatesToApply.size()];
                    for (int i = 0; i < subs.length; i++) {
                        subs[i] = updatesToApply.get(i).iterator();
                    }
                    // 将这些迭代器合并  实际上就是通过二叉堆 将这些update对象按照doc顺序排序   如果同一个doc 同时被多个update更新 只使用gen最大的进行处理   同时同一gen针对同一doc更新多次 也是取最后次的
                    // 这里如果多个更新 对应到同一个doc时 应该只按照最新的处理
                    return DocValuesFieldUpdates.mergedIterator(subs);
                };
                // 在硬删除下该方法是NOOP 在软删除场景下 某个原本不存在软删除field的doc 可能会增加该field 反而要变成被软删除的doc
                pendingDeletes.onDocValuesUpdate(fieldInfo, updateSupplier.apply(fieldInfo));

                // 如果docValue 是 二进制类型
                if (type == DocValuesType.BINARY) {

                    // 这里将docValue的信息写入到索引文件中
                    fieldsConsumer.addBinaryField(fieldInfo, new EmptyDocValuesProducer() {

                        // 这里重写了根据 field 获取二进制值的方法
                        @Override
                        public BinaryDocValues getBinary(FieldInfo fieldInfoIn) throws IOException {
                            // 获取 有关该field下所有 更新信息涉及到的 doc 迭代器
                            DocValuesFieldUpdates.Iterator iterator = updateSupplier.apply(fieldInfo);

                            // 这个对象还是按照docId的顺序 返回 此时还在内存的 docValue 或者已经读取到磁盘的docValue
                            final MergedDocValues<BinaryDocValues> mergedDocValues = new MergedDocValues<>(
                                    // 读取之前写入到索引文件的数据    docValue的写入是这样的  在解析doc时是以field为单位 这时将field.value 写入索引文件 这个就叫 docValue
                                    // 只有当之前写入时 docValueType 相同才会去读取 否则返回null
                                    reader.getBinaryDocValues(field),
                                    // 当前 iterator是一个更宽泛的接口 有更多无关的api 通过包装后确保只暴露有关 binary相关的api
                                    DocValuesFieldUpdates.Iterator.asBinaryDocValues(iterator), iterator);
                            // Merge sort of the original doc values with updated doc values:

                            // 这里对应 getBinary 返回值  这些数据会写入到索引文件中
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
                    // 跟上面的套路一致 不过写入类型是 numeric
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
            // 在处理完后更新了 segment下 docValue 的gen
            info.advanceDocValuesGen();
            assert !fieldFiles.containsKey(fieldInfo.number);
            fieldFiles.put(fieldInfo.number, trackingDir.getCreatedFiles());
        }
    }

    /**
     * This class merges the current on-disk DV with an incoming update DV instance and merges the two instances
     * giving the incoming update precedence in terms of values, in other words the values of the update always
     * wins over the on-disk version.
     * 应该是想将之前同类型的 数据 与本次update的 数据结合 起来 如果docValueType 本身发生了改变就丢弃之前的数据
     * 因为本次更新只覆盖到部分doc 未覆盖的部分还是使用之前的value
     */
    static final class MergedDocValues<DocValuesInstance extends DocValuesIterator> extends DocValuesIterator {

        /**
         * 该对象内部包装的也是 updateDocValues 所以调用updateDocValues 时 该值也会自动往后 就不需要手动调用nextDoc了
         */
        private final DocValuesFieldUpdates.Iterator updateIterator;
        // merged docID
        private int docIDOut = -1;
        // docID from our original doc values
        private int docIDOnDisk = -1;
        // docID from our updates
        private int updateDocID = -1;

        private final DocValuesInstance onDiskDocValues;
        private final DocValuesInstance updateDocValues;

        /**
         * 代表此时读取到的doc 对应的值(比如docValue ) 从这个对象中获取
         */
        DocValuesInstance currentValuesSupplier;

        /**
         * @param onDiskDocValues 这是 field此时写入到索引文件中的数据  如果之前的docValueType 与本次更新的docValueType的类型不同 则返回null
         * @param updateDocValues 本次有关field下 哪些相关的doc被更新  以及更新的值
         * @param updateIterator  同updateDocValues 不过updateDocValues 是被装饰器屏蔽了部分api
         */
        protected MergedDocValues(DocValuesInstance onDiskDocValues, DocValuesInstance updateDocValues, DocValuesFieldUpdates.Iterator updateIterator) {
            this.onDiskDocValues = onDiskDocValues;
            this.updateDocValues = updateDocValues;
            this.updateIterator = updateIterator;
        }

        @Override
        public int docID() {
            return docIDOut;
        }

        // 这个merge对象不允许直接指定某个docId
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

        /**
         * 迭代到下一个doc
         *
         * @return
         * @throws IOException
         */
        @Override
        public int nextDoc() throws IOException {
            boolean hasValue = false;
            do {
                // 初始状态2个值都是-1
                if (docIDOnDisk == docIDOut) {
                    // 读取磁盘中的数据
                    if (onDiskDocValues == null) {
                        docIDOnDisk = NO_MORE_DOCS;
                    } else {
                        docIDOnDisk = onDiskDocValues.nextDoc();
                    }
                }
                // 从当前存储了  update数据的迭代器中 获取下一个docId
                if (updateDocID == docIDOut) {
                    updateDocID = updateDocValues.nextDoc();
                }
                // 此时磁盘中的数据需要优先写入
                if (docIDOnDisk < updateDocID) {
                    // no update to this doc - we use the on-disk values
                    docIDOut = docIDOnDisk;
                    currentValuesSupplier = onDiskDocValues;
                    hasValue = true;
                } else {
                    // 当update 迭代器doc <= 磁盘doc时都使用update的数据 间接说明doc相同时 会做覆盖操作
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


    /**
     * 将fieldInfo 信息持久化
     * @param fieldInfos  当前field的信息
     * @param dir  目标目录
     * @param infosFormat  标明 fieldInfo 会以什么格式存储
     * @return 返回的是本次存储最新fieldInfo 信息的索引文件
     * @throws IOException
     */
    private synchronized Set<String> writeFieldInfosGen(FieldInfos fieldInfos, Directory dir,
                                                        FieldInfosFormat infosFormat) throws IOException {
        // 获取更新对应的gen 作为索引文件名的后缀
        final long nextFieldInfosGen = info.getNextFieldInfosGen();
        // 生成后缀名
        final String segmentSuffix = Long.toString(nextFieldInfosGen, Character.MAX_RADIX);
        // we write approximately that many bytes (based on Lucene46DVF):
        // HEADER + FOOTER: 40
        // 90 bytes per-field (over estimating long name and attributes map)
        // 这里预估会占用多少大小
        final long estInfosSize = 40 + 90 * fieldInfos.size();
        final IOContext infosContext = new IOContext(new FlushInfo(info.info.maxDoc(), estInfosSize));
        // separately also track which files were created for this gen
        final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
        // 写入 fieldInfo  （就是将fieldInfo 挨个属性写入到索引文件中）
        infosFormat.write(trackingDir, info.info, segmentSuffix, fieldInfos, infosContext);
        info.advanceFieldInfosGen();
        return trackingDir.getCreatedFiles();
    }

    /**
     * 将当前该对象内部维护的  针对某个field的doc更新信息写入到索引文件
     * @param dir          本次写入的目标目录
     * @param fieldNumbers  通过fieldNum 可以找到对应的field信息  该对象是以 indexWriter为单位共享的
     * @param maxDelGen    此时已经完成的update对象 对应的delGen
     * @param infoStream
     * @return
     * @throws IOException
     */
    public synchronized boolean writeFieldUpdates(Directory dir, FieldInfos.FieldNumbers fieldNumbers, long maxDelGen, InfoStream infoStream) throws IOException {
        long startTimeNS = System.nanoTime();
        final Map<Integer, Set<String>> newDVFiles = new HashMap<>();
        // 本次由于fieldInfo 信息发生了变化而 产生的新索引文件
        Set<String> fieldInfosFiles = null;
        FieldInfos fieldInfos = null;
        boolean any = false;
        // 因为这些数据 并没有强制处理 所以可能会囤积很多不同gen的 update的数据
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

        // 检测本次是否有需要处理的数据
        if (any == false) {
            // no updates
            return false;
        }

        // Do this so we can delete any created files on
        // exception; this saves all codecs from having to do it:

        // 上面确定了有需要更新的数据  使用该包装类 主要是当写入新的索引文件失败时 能够删除文件
        TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);

        boolean success = false;
        try {
            final Codec codec = info.info.getCodec();

            // reader could be null e.g. for a just merged segment (from
            // IndexWriter.commitMergedDeletes).   啥 merge 可能会导致 reader为null
            final SegmentReader reader;
            if (this.reader == null) {
                // 这里只创建读取一次的对象
                reader = new SegmentReader(info, indexCreatedVersionMajor, IOContext.READONCE);
                pendingDeletes.onNewReader(reader, info);
            } else {
                reader = this.reader;
            }

            try {
                // clone FieldInfos so that we can update their dvGen separately from
                // the reader's infos and write them to a new fieldInfos_gen file.
                // 记录本次处理的field中最大的号码
                int maxFieldNumber = -1;
                // 以 fieldName 为key 创建一个映射对象
                Map<String, FieldInfo> byName = new HashMap<>();
                for (FieldInfo fi : reader.getFieldInfos()) {
                    // cannot use builder.add(fi) because it does not preserve
                    // the local field number. Field numbers can be different from
                    // the global ones if the segment was created externally (and added to
                    // this index with IndexWriter#addIndexes(Directory)).
                    // 创建副本对象 并存储到byName中  注意这里使用的是之前的num
                    byName.put(fi.name, cloneFieldInfo(fi, fi.number));
                    maxFieldNumber = Math.max(fi.number, maxFieldNumber);
                }

                // create new fields with the right DV type
                FieldInfos.Builder builder = new FieldInfos.Builder(fieldNumbers);
                // 开始处理之前存入的 update对象
                for (List<DocValuesFieldUpdates> updates : pendingDVUpdates.values()) {
                    DocValuesFieldUpdates update = updates.get(0);

                    // 修改 field 的DV 类型
                    if (byName.containsKey(update.field)) {
                        // the field already exists in this segment
                        FieldInfo fi = byName.get(update.field);
                        fi.setDocValuesType(update.type);
                    } else {
                        // the field is not present in this segment so we clone the global field
                        // (which is guaranteed to exist) and remaps its field number locally.
                        // 因为  update对象是全局共用的  所以内部可能包含了全局范围内各种field  而该segment 就不一定有该field
                        // 但是从插入  pendingDVUpdates 的逻辑来看 如果该segment下无法找到 某个field的数据 那么是无法生成对应的DocValuesFieldUpdates   以及插入到 pendingDVUpdates 的
                        // TODO 所以可以先忽略这里的逻辑
                        assert fieldNumbers.contains(update.field, update.type);
                        // 这个时候选择生成一个该field的副本 并使用新的 num
                        FieldInfo fi = cloneFieldInfo(builder.getOrAdd(update.field), ++maxFieldNumber);
                        // 指定 docValue的类型 并添加到映射容器中
                        fi.setDocValuesType(update.type);
                        byName.put(fi.name, fi);
                    }
                }

                // 将该 segment下面的field 组成一个新的 fieldInfos 包含新增的fieldInfo
                fieldInfos = new FieldInfos(byName.values().toArray(new FieldInfo[0]));
                final DocValuesFormat docValuesFormat = codec.docValuesFormat();

                // 将最新的 docValueType信息 写入到索引文件中  同时将本次相关的所有fieldNum 和新写入的所有索引文件存储到 newDVFiles 中
                handleDVUpdates(fieldInfos, trackingDir, docValuesFormat, reader, newDVFiles, maxDelGen, infoStream);

                // 由于docValueType的更新 可能会导致2个结果  需要将结果持久化
                // 1. 该segment下增加了新的fieldInfo
                // 2. 该fieldInfo的 docValueType 发生了变化
                fieldInfosFiles = writeFieldInfosGen(fieldInfos, trackingDir, codec.fieldInfosFormat());
            } finally {
                // 代表临时生成的reader 使用完后就要关闭
                if (reader != this.reader) {
                    reader.close();
                }
            }

            success = true;
        } finally {
            if (success == false) {
                // Advance only the nextWriteFieldInfosGen and nextWriteDocValuesGen, so
                // that a 2nd attempt to write will write to a new file
                // 当写入失败时 可能此时出现了不正确的 索引文件 所以就要更新 nextGen
                info.advanceNextWriteFieldInfosGen();
                info.advanceNextWriteDocValuesGen();

                // Delete any partially created file(s):
                // 尝试删除不完整的文件
                for (String fileName : trackingDir.getCreatedFiles()) {
                    IOUtils.deleteFilesIgnoringExceptions(dir, fileName);
                }
            }
        }

        // 至此 docValue 和 fieldInfo 的最新数据已经刷盘成功
        // Prune the now-written DV updates:
        long bytesFreed = 0;
        Iterator<Map.Entry<String, List<DocValuesFieldUpdates>>> it = pendingDVUpdates.entrySet().iterator();

        // 将已经处理过的update 移除掉
        while (it.hasNext()) {
            Map.Entry<String, List<DocValuesFieldUpdates>> ent = it.next();
            int upto = 0;
            List<DocValuesFieldUpdates> updates = ent.getValue();
            // 这里就是做清理工作 将 delGen前的 update移除
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
            // 重新读取数据 确保能读取到最新的改动
            swapNewReaderWithLatestLiveDocs();
        }

        // writing field updates succeeded
        assert fieldInfosFiles != null;
        // 更新segment 下读取fieldInfo 信息的索引文件
        info.setFieldInfosFiles(fieldInfosFiles);

        // update the doc-values updates files. the files map each field to its set
        // of files, hence we copy from the existing map all fields w/ updates that
        // were not updated in this session, and add new mappings for fields that
        // were updated now.
        assert newDVFiles.isEmpty() == false;
        // 某些未命中的fieldInfo 信息不会保存在newDVFiles 中 这样就要从之前的容器中取出信息
        for (Entry<Integer, Set<String>> e : info.getDocValuesUpdatesFiles().entrySet()) {
            if (newDVFiles.containsKey(e.getKey()) == false) {
                newDVFiles.put(e.getKey(), e.getValue());
            }
        }
        // 之后将所有fieldInfo DV对应的索引文件写入到 segmentInfo中
        info.setDocValuesUpdatesFiles(newDVFiles);

        if (infoStream.isEnabled("BD")) {
            infoStream.message("BD", String.format(Locale.ROOT, "done write field updates for seg=%s; took %.3fs; new files: %s",
                    info, (System.nanoTime() - startTimeNS) / 1000000000.0, newDVFiles));
        }
        return true;
    }

    /**
     * 创建一个 fieldInfo的副本
     *
     * @param fi
     * @param fieldNumber
     * @return
     */
    private FieldInfo cloneFieldInfo(FieldInfo fi, int fieldNumber) {
        return new FieldInfo(fi.name, fieldNumber, fi.hasVectors(), fi.omitsNorms(), fi.hasPayloads(),
                fi.getIndexOptions(), fi.getDocValuesType(), fi.getDocValuesGen(), new HashMap<>(fi.attributes()),
                fi.getPointDimensionCount(), fi.getPointIndexDimensionCount(), fi.getPointNumBytes(), fi.isSoftDeletesField());
    }

    /**
     * 更新之前reader读取的 索引文件
     *
     * @param reader
     * @return
     * @throws IOException
     */
    private SegmentReader createNewReaderWithLatestLiveDocs(SegmentReader reader) throws IOException {
        assert reader != null;
        assert Thread.holdsLock(this) : Thread.currentThread().getName();
        SegmentReader newReader = new SegmentReader(info, reader, pendingDeletes.getLiveDocs(),
                pendingDeletes.getHardLiveDocs(), pendingDeletes.numDocs(), true);
        boolean success2 = false;
        try {
            // 将新的 liveDoc 设置到 pendingDelete中
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

    /**
     * 将当前对象标记成正在merging中   这样之后的更新信息就会在merge容器中存储一份 这样merge后的segment就不会丢失这部分更新数据
     */
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

    /**
     * Returns a reader for merge, with the latest doc values updates and deletions.
     * 获取一个用于merge的reader对象  它包含了此时最新的 DV 信息 fieldInfo 信息  和 liveDoc信息
     */
    synchronized MergeReader getReaderForMerge(IOContext context) throws IOException {

        // We must carry over any still-pending DV updates because they were not
        // successfully written, e.g. because there was a hole in the delGens,
        // or they arrived after we wrote all DVs for merge but before we set
        // isMerging here:
        // 将待执行的 update 对象  在mergingDVUpdates 中也存储了一份
        for (Map.Entry<String, List<DocValuesFieldUpdates>> ent : pendingDVUpdates.entrySet()) {
            List<DocValuesFieldUpdates> mergingUpdates = mergingDVUpdates.get(ent.getKey());
            if (mergingUpdates == null) {
                mergingUpdates = new ArrayList<>();
                mergingDVUpdates.put(ent.getKey(), mergingUpdates);
            }
            mergingUpdates.addAll(ent.getValue());
        }

        SegmentReader reader = getReader(context);
        // 代表 liveDoc 信息发生了变化  将最新的liveDoc信息同步到 segmentReader中  因为此时马上就要merge了 所以就没必要将最新的liveDoc信息持久化了
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
     * 丢弃在 merging过程中插入的新的 update 对象
     */
    public synchronized void dropMergingUpdates() {
        mergingDVUpdates.clear();
        isMerging = false;
    }

    /**
     * 解除merge状态 并返回在merge过程中接收到的update信息
     * @return
     */
    public synchronized Map<String, List<DocValuesFieldUpdates>> getMergingDVUpdates() {
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
