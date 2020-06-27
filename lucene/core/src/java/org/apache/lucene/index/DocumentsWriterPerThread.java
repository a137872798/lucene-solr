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
import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocumentsWriterDeleteQueue.DeleteSlice;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ByteBlockPool.Allocator;
import org.apache.lucene.util.ByteBlockPool.DirectTrackingAllocator;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

/**
 * 针对docWriter 的操作可能是通过多个线程   每个线程对应一个该对象
 */
final class DocumentsWriterPerThread {

    /**
     * The IndexingChain must define the {@link #getChain(DocumentsWriterPerThread)} method
     * which returns the DocConsumer that the DocumentsWriter calls to process the
     * documents.
     * 生成一个处理文档的链式结构 每个环节都会抽取某些属性 便于生成对应的索引文件
     */
    abstract static class IndexingChain {
        abstract DocConsumer getChain(DocumentsWriterPerThread documentsWriterPerThread) throws IOException;
    }

    /**
     * 该线程在执行过程中遇到的异常
     */
    private Throwable abortingException;

    final void onAbortingException(Throwable throwable) {
        assert abortingException == null : "aborting exception has already been set";
        abortingException = throwable;
    }

    final boolean hasHitAbortingException() {
        return abortingException != null;
    }

    final boolean isAborted() {
        return aborted;
    }


    static final IndexingChain defaultIndexingChain = new IndexingChain() {

        @Override
        DocConsumer getChain(DocumentsWriterPerThread documentsWriterPerThread) {
            return new DefaultIndexingChain(documentsWriterPerThread);
        }
    };

    /**
     * 携带某个文档的相关参数
     */
    static class DocState {
        /**
         * 当前负责写入文档数据的 线程
         */
        final DocumentsWriterPerThread docWriter;
        /**
         * 该对象负责对写入的数据进行分词
         */
        final Analyzer analyzer;
        InfoStream infoStream;
        /**
         * 该对象负责打分
         */
        Similarity similarity;
        /**
         * 更新此时 doc 对应的文档号
         */
        int docID;
        /**
         * 用于遍历当前文档内部的域信息
         */
        Iterable<? extends IndexableField> doc;

        DocState(DocumentsWriterPerThread docWriter, Analyzer analyzer, InfoStream infoStream) {
            this.docWriter = docWriter;
            this.infoStream = infoStream;
            this.analyzer = analyzer;
        }

        public void clear() {
            // don't hold onto doc nor analyzer, in case it is
            // largish:
            doc = null;
        }
    }

    /**
     * 用于描述已经完成刷盘的 段对象
     */
    static final class FlushedSegment {
        /**
         * 描述段信息
         */
        final SegmentCommitInfo segmentInfo;
        /**
         * 描述 段下所有的doc 下所有的域信息
         */
        final FieldInfos fieldInfos;
        final FrozenBufferedUpdates segmentUpdates;
        /**
         * 位图对象
         */
        final FixedBitSet liveDocs;
        final Sorter.DocMap sortMap;
        final int delCount;

        private FlushedSegment(InfoStream infoStream, SegmentCommitInfo segmentInfo, FieldInfos fieldInfos,
                               BufferedUpdates segmentUpdates, FixedBitSet liveDocs, int delCount, Sorter.DocMap sortMap) {
            this.segmentInfo = segmentInfo;
            this.fieldInfos = fieldInfos;
            this.segmentUpdates = segmentUpdates != null && segmentUpdates.any() ? new FrozenBufferedUpdates(infoStream, segmentUpdates, segmentInfo) : null;
            this.liveDocs = liveDocs;
            this.delCount = delCount;
            this.sortMap = sortMap;
        }
    }

    /**
     * Called if we hit an exception at a bad time (when
     * updating the index files) and must discard all
     * currently buffered docs.  This resets our state,
     * discarding any docs added since last flush.
     */
    // 标记当前对象不可用    该方法是在处理过程中遇到了异常时调用的   这时会选择丢弃之前记录的所有更新信息
    void abort() throws IOException {
        aborted = true;
        // pendingNumDocs 本身由多个线程进行填充 这里是将本线程的份 从pendingNumDocs 中去除
        pendingNumDocs.addAndGet(-numDocsInRAM);
        try {
            if (infoStream.isEnabled("DWPT")) {
                infoStream.message("DWPT", "now abort");
            }
            try {
                // 终止处理器
                consumer.abort();
            } finally {
                // 清理由本线程收集的 更新动作
                pendingUpdates.clear();
            }
        } finally {
            if (infoStream.isEnabled("DWPT")) {
                infoStream.message("DWPT", "done abort");
            }
        }
    }

    private final static boolean INFO_VERBOSE = false;
    /**
     * 这里指定了 各种编码方式  而索引文件就是按照这些文件格式写入的  当前使用的编码器是  Lucene84
     */
    final Codec codec;
    /**
     * 代表数据会写入到哪个目录 该包装对象在原有的基础上 会记录每次操作创建的文件名
     */
    final TrackingDirectoryWrapper directory;
    /**
     * 描述当前文档的信息
     */
    final DocState docState;
    /**
     * 该对象处理传入的文档
     */
    private final DocConsumer consumer;
    final Counter bytesUsed;

    // Updates for our still-in-RAM (to be flushed next) segment
    // 该对象实际上接收到的是 globalSlice所有的更新信息
    private final BufferedUpdates pendingUpdates;

    private final SegmentInfo segmentInfo;     // Current segment we are working on
    /**
     * 当前对象是否被禁用
     */
    private boolean aborted = false;   // True if we aborted
    /**
     * 当前正在刷盘中
     */
    private SetOnce<Boolean> flushPending = new SetOnce<>();
    /**
     * 上次提交了多少 bytes
     */
    private volatile long lastCommittedBytesUsed;
    /**
     * 代表 本次刷盘已经完成
     */
    private SetOnce<Boolean> hasFlushed = new SetOnce<>();

    /**
     * 该对象负责抽取 doc的域信息 并生成 FieldInfos
     */
    private final FieldInfos.Builder fieldInfos;
    private final InfoStream infoStream;
    /**
     * 实际上按照写入的顺序生成了文档号
     */
    private int numDocsInRAM;
    final DocumentsWriterDeleteQueue deleteQueue;
    private final DeleteSlice deleteSlice;
    private final NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);
    final Allocator byteBlockAllocator;
    final IntBlockPool.Allocator intBlockAllocator;
    private final AtomicLong pendingNumDocs;
    private final LiveIndexWriterConfig indexWriterConfig;
    /**
     * 是否在相关地方打印日志
     */
    private final boolean enableTestPoints;
    /**
     * 代表创建该索引的版本
     */
    private final int indexVersionCreated;
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * 槽中存放的是 待删除的 docId
     */
    private int[] deleteDocIDs = new int[0];
    private int numDeletedDocIds = 0;


    /**
     * @param indexVersionCreated
     * @param segmentName
     * @param directoryOrig
     * @param directory
     * @param indexWriterConfig
     * @param infoStream
     * @param deleteQueue
     * @param fieldInfos
     * @param pendingNumDocs      归属于某个 pool的 thread 共享同一个 pendingNumDocs
     * @param enableTestPoints
     * @throws IOException
     */
    DocumentsWriterPerThread(int indexVersionCreated, String segmentName, Directory directoryOrig, Directory directory, LiveIndexWriterConfig indexWriterConfig, InfoStream infoStream, DocumentsWriterDeleteQueue deleteQueue,
                             FieldInfos.Builder fieldInfos, AtomicLong pendingNumDocs, boolean enableTestPoints) throws IOException {
        this.directory = new TrackingDirectoryWrapper(directory);
        this.fieldInfos = fieldInfos;
        this.indexWriterConfig = indexWriterConfig;
        this.infoStream = infoStream;
        this.codec = indexWriterConfig.getCodec();
        // 这里通过 thread 和 analyzer 初始化 docState
        this.docState = new DocState(this, indexWriterConfig.getAnalyzer(), infoStream);
        // 获取打分器
        this.docState.similarity = indexWriterConfig.getSimilarity();
        // 由同一个 pool 创建的线程会共享一个  numDoc 对象
        this.pendingNumDocs = pendingNumDocs;
        bytesUsed = Counter.newCounter();
        // 当使用该分配器 创建 BB 对象时 还会额外记录使用了多少内存
        byteBlockAllocator = new DirectTrackingAllocator(bytesUsed);
        // 创建记录该段 某些field 更新状态的对象
        pendingUpdates = new BufferedUpdates(segmentName);
        intBlockAllocator = new IntBlockAllocator(bytesUsed);
        this.deleteQueue = Objects.requireNonNull(deleteQueue);
        assert numDocsInRAM == 0 : "num docs " + numDocsInRAM;
        // 每个线程会持有自己的分片对象  不是共享的    这个deleteQueue 应该对应了某个pool
        deleteSlice = deleteQueue.newSlice();
        // 在创建段对象的时候 声明版本信息 用于判断索引能够兼容
        segmentInfo = new SegmentInfo(directoryOrig, Version.LATEST, Version.LATEST, segmentName, -1, false, codec, Collections.emptyMap(), StringHelper.randomId(), Collections.emptyMap(), indexWriterConfig.getIndexSort());
        assert numDocsInRAM == 0;
        if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
            infoStream.message("DWPT", Thread.currentThread().getName() + " init seg=" + segmentName + " delQueue=" + deleteQueue);
        }
        // 打印日志相关的
        this.enableTestPoints = enableTestPoints;
        // 代表索引的版本
        this.indexVersionCreated = indexVersionCreated;
        // this should be the last call in the ctor
        // it really sucks that we need to pull this within the ctor and pass this ref to the chain!
        consumer = indexWriterConfig.getIndexingChain().getChain(this);
    }

    FieldInfos.Builder getFieldInfosBuilder() {
        return fieldInfos;
    }

    int getIndexCreatedVersionMajor() {
        return indexVersionCreated;
    }

    final void testPoint(String message) {
        if (enableTestPoints) {
            assert infoStream.isEnabled("TP"); // don't enable unless you need them.
            infoStream.message("TP", message);
        }
    }

    /**
     * Anything that will add N docs to the index should reserve first to
     * make sure it's allowed.
     */
    // 每当写入一个 doc 时调用该方法
    private void reserveOneDoc() {
        // 增加 pendingNumDocs   如果超过了 max 则抛出异常
        if (pendingNumDocs.incrementAndGet() > IndexWriter.getActualMaxDocs()) {
            // Reserve failed: put the one doc back and throw exc:
            pendingNumDocs.decrementAndGet();
            throw new IllegalArgumentException("number of documents in the index cannot exceed " + IndexWriter.getActualMaxDocs());
        }
    }

    /**
     * @param docs               2层迭代器  外层代表多个doc  内层代表doc下的多个 field
     * @param deleteNode
     * @param flushNotifications 通过监听器触发响应钩子
     * @return
     * @throws IOException
     */
    long updateDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs, DocumentsWriterDeleteQueue.Node<?> deleteNode, DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
        try {
            testPoint("DocumentsWriterPerThread addDocuments start");
            assert hasHitAbortingException() == false : "DWPT has hit aborting exception but is still indexing";
            if (INFO_VERBOSE && infoStream.isEnabled("DWPT")) {
                infoStream.message("DWPT", Thread.currentThread().getName() + " update delTerm=" + deleteNode + " docID=" + docState.docID + " seg=" + segmentInfo.name);
            }
            // 返回当前 docId
            final int docsInRamBefore = numDocsInRAM;
            boolean allDocsIndexed = false;
            try {
                for (Iterable<? extends IndexableField> doc : docs) {
                    // Even on exception, the document is still added (but marked
                    // deleted), so we don't need to un-reserve at that point.
                    // Aborting exceptions will actually "lose" more than one
                    // document, so the counter will be "wrong" in that case, but
                    // it's very hard to fix (we can't easily distinguish aborting
                    // vs non-aborting exceptions):
                    // 每个更新的文档 都会增加 pendingNumDoc
                    reserveOneDoc();
                    docState.doc = doc;
                    docState.docID = numDocsInRAM;
                    try {
                        // 每个读取到的doc 都会通过consumer 进行处理
                        consumer.processDocument();
                    } finally {
                        numDocsInRAM++; // we count the doc anyway even in the case of an exception
                    }
                }
                // 代表已经为所有插入的doc 生成了索引
                allDocsIndexed = true;
                return finishDocuments(deleteNode, docsInRamBefore);
            } finally {
                // 代表出现了预期外的情况
                if (!allDocsIndexed && !aborted) {
                    // the iterator threw an exception that is not aborting
                    // go and mark all docs from this block as deleted
                    // 将本次添加的部分doc 全部删除
                    deleteLastDocs(numDocsInRAM - docsInRamBefore);
                }
                // 清空当前的  doc
                docState.clear();
            }
        } finally {
            maybeAbort("updateDocuments", flushNotifications);
        }
    }

    /**
     * 处理 node
     *
     * @param deleteNode
     * @param docIdUpTo
     * @return
     */
    private long finishDocuments(DocumentsWriterDeleteQueue.Node<?> deleteNode, int docIdUpTo) {
        /*
         * here we actually finish the document in two steps 1. push the delete into
         * the queue and update our slice. 2. increment the DWPT private document
         * id.
         *
         * the updated slice we get from 1. holds all the deletes that have occurred
         * since we updated the slice the last time.
         */
        // Apply delTerm only after all indexing has
        // succeeded, but apply it only to docs prior to when
        // this batch started:
        long seqNo;
        // 如果本次携带了 需要处理的 doc更新
        if (deleteNode != null) {
            // 这里使用该线程自己创建的 slice 对象   将 node添加到slice内部的链表结构
            seqNo = deleteQueue.add(deleteNode, deleteSlice);
            assert deleteSlice.isTail(deleteNode) : "expected the delete term as the tail item";
            // 将node的变化 记录到 pendingUpdates 上
            deleteSlice.apply(pendingUpdates, docIdUpTo);
            return seqNo;
        } else {
            // 无论是否携带了 deleteNode 顺便将 分片与 deleteQueue 内部的节点做同步
            seqNo = deleteQueue.updateSlice(deleteSlice);
            // 代表分片发生了更新  做同步操作
            if (seqNo < 0) {
                seqNo = -seqNo;
                deleteSlice.apply(pendingUpdates, docIdUpTo);
            } else {
                //  sliceHead = sliceTail;
                deleteSlice.reset();
            }
        }

        return seqNo;
    }

    // This method marks the last N docs as deleted. This is used
    // in the case of a non-aborting exception. There are several cases
    // where we fail a document ie. due to an exception during analysis
    // that causes the doc to be rejected but won't cause the DWPT to be
    // stale nor the entire IW to abort and shutdown. In such a case
    // we only mark these docs as deleted and turn it into a livedocs
    // during flush
    // 删除最后的几个 doc  一般是在生成索引  部分成功部分失败时  选择将成功的部分删除
    // 这里没有直接执行 IO 操作 看来IO 操作会集中在某一时机触发  这也是为了提高效率  将松散的IO 操作尽可能的整合
    private void deleteLastDocs(int docCount) {
        int from = numDocsInRAM - docCount;
        int to = numDocsInRAM;
        int size = deleteDocIDs.length;
        deleteDocIDs = ArrayUtil.grow(deleteDocIDs, numDeletedDocIds + (to - from));
        for (int docId = from; docId < to; docId++) {
            deleteDocIDs[numDeletedDocIds++] = docId;
        }
        bytesUsed.addAndGet((deleteDocIDs.length - size) * Integer.SIZE);
        // NOTE: we do not trigger flush here.  This is
        // potentially a RAM leak, if you have an app that tries
        // to add docs but every single doc always hits a
        // non-aborting exception.  Allowing a flush here gets
        // very messy because we are only invoked when handling
        // exceptions so to do this properly, while handling an
        // exception we'd have to go off and flush new deletes
        // which is risky (likely would hit some other
        // confounding exception).
    }

    /**
     * Returns the number of RAM resident documents in this {@link DocumentsWriterPerThread}
     */
    public int getNumDocsInRAM() {
        // public for FlushPolicy
        return numDocsInRAM;
    }

    /**
     * Prepares this DWPT for flushing. This method will freeze and return the
     * {@link DocumentsWriterDeleteQueue}s global buffer and apply all pending
     * deletes to this DWPT.
     * 通过之前保存的  更新数据 生成一个最终的更新对象  这样可以避免小而多的更新操作
     */
    FrozenBufferedUpdates prepareFlush() {
        assert numDocsInRAM > 0;
        // 实际上使用内部的  globalSlice 创建对象
        final FrozenBufferedUpdates globalUpdates = deleteQueue.freezeGlobalBuffer(deleteSlice);
        /* deleteSlice can possibly be null if we have hit non-aborting exceptions during indexing and never succeeded
        adding a document. */
        // 将变化同步到 pendingUpdates 中
        if (deleteSlice != null) {
            // apply all deletes before we flush and release the delete slice
            deleteSlice.apply(pendingUpdates, numDocsInRAM);
            assert deleteSlice.isEmpty();
            deleteSlice.reset();
        }
        return globalUpdates;
    }

    /**
     * Flush all pending docs to a new segment
     * 将所有待刷盘的 对象进行刷盘
     */
    FlushedSegment flush(DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
        assert flushPending.get() == Boolean.TRUE;
        assert numDocsInRAM > 0;
        assert deleteSlice.isEmpty() : "all deletes must be applied in prepareFlush";
        segmentInfo.setMaxDoc(numDocsInRAM);
        // 可以看作一个简单的 bean 对象
        final SegmentWriteState flushState = new SegmentWriteState(infoStream, directory, segmentInfo, fieldInfos.finish(),
                pendingUpdates, new IOContext(new FlushInfo(numDocsInRAM, bytesUsed())));
        final double startMBUsed = bytesUsed() / 1024. / 1024.;

        // Apply delete-by-docID now (delete-byDocID only
        // happens when an exception is hit processing that
        // doc, eg if analyzer has some problem w/ the text):
        // 代表有部分 doc 需要被删除
        if (numDeletedDocIds > 0) {
            flushState.liveDocs = new FixedBitSet(numDocsInRAM);
            flushState.liveDocs.set(0, numDocsInRAM);
            for (int i = 0; i < numDeletedDocIds; i++) {
                flushState.liveDocs.clear(deleteDocIDs[i]);
            }
            flushState.delCountOnFlush = numDeletedDocIds;
            bytesUsed.addAndGet(-(deleteDocIDs.length * Integer.SIZE));
            deleteDocIDs = null;
        }

        // 如果当前对象已经被禁用  无法正确执行刷盘操作 返回 null
        if (aborted) {
            if (infoStream.isEnabled("DWPT")) {
                infoStream.message("DWPT", "flush: skip because aborting is set");
            }
            return null;
        }

        long t0 = System.nanoTime();

        if (infoStream.isEnabled("DWPT")) {
            infoStream.message("DWPT", "flush postings as segment " + flushState.segmentInfo.name + " numDocs=" + numDocsInRAM);
        }
        final Sorter.DocMap sortMap;
        try {
            // 对应匹配软删除域的所有doc
            DocIdSetIterator softDeletedDocs;
            // 如果某个字段打算采用软删除
            if (indexWriterConfig.getSoftDeletesField() != null) {
                softDeletedDocs = consumer.getHasDocValues(indexWriterConfig.getSoftDeletesField());
            } else {
                softDeletedDocs = null;
            }
            sortMap = consumer.flush(flushState);
            if (softDeletedDocs == null) {
                flushState.softDelCountOnFlush = 0;
            } else {
                flushState.softDelCountOnFlush = PendingSoftDeletes.countSoftDeletes(softDeletedDocs, flushState.liveDocs);
                assert flushState.segmentInfo.maxDoc() >= flushState.softDelCountOnFlush + flushState.delCountOnFlush;
            }
            // We clear this here because we already resolved them (private to this segment) when writing postings:
            // 当所有更新动作都执行完毕时 清理之前暂存在内存中的数据
            pendingUpdates.clearDeleteTerms();
            // 存储本次操作所创建的所有文件
            segmentInfo.setFiles(new HashSet<>(directory.getCreatedFiles()));

            final SegmentCommitInfo segmentInfoPerCommit = new SegmentCommitInfo(segmentInfo, 0, flushState.softDelCountOnFlush, -1L, -1L, -1L, StringHelper.randomId());
            if (infoStream.isEnabled("DWPT")) {
                infoStream.message("DWPT", "new segment has " + (flushState.liveDocs == null ? 0 : flushState.delCountOnFlush) + " deleted docs");
                infoStream.message("DWPT", "new segment has " + flushState.softDelCountOnFlush + " soft-deleted docs");
                infoStream.message("DWPT", "new segment has " +
                        (flushState.fieldInfos.hasVectors() ? "vectors" : "no vectors") + "; " +
                        (flushState.fieldInfos.hasNorms() ? "norms" : "no norms") + "; " +
                        (flushState.fieldInfos.hasDocValues() ? "docValues" : "no docValues") + "; " +
                        (flushState.fieldInfos.hasProx() ? "prox" : "no prox") + "; " +
                        (flushState.fieldInfos.hasFreq() ? "freqs" : "no freqs"));
                infoStream.message("DWPT", "flushedFiles=" + segmentInfoPerCommit.files());
                infoStream.message("DWPT", "flushed codec=" + codec);
            }

            final BufferedUpdates segmentDeletes;
            if (pendingUpdates.deleteQueries.isEmpty() && pendingUpdates.numFieldUpdates.get() == 0) {
                pendingUpdates.clear();
                segmentDeletes = null;
            } else {
                segmentDeletes = pendingUpdates;
            }

            if (infoStream.isEnabled("DWPT")) {
                final double newSegmentSize = segmentInfoPerCommit.sizeInBytes() / 1024. / 1024.;
                infoStream.message("DWPT", "flushed: segment=" + segmentInfo.name +
                        " ramUsed=" + nf.format(startMBUsed) + " MB" +
                        " newFlushedSize=" + nf.format(newSegmentSize) + " MB" +
                        " docs/MB=" + nf.format(flushState.segmentInfo.maxDoc() / newSegmentSize));
            }

            assert segmentInfo != null;

            FlushedSegment fs = new FlushedSegment(infoStream, segmentInfoPerCommit, flushState.fieldInfos,
                    segmentDeletes, flushState.liveDocs, flushState.delCountOnFlush, sortMap);
            sealFlushedSegment(fs, sortMap, flushNotifications);
            if (infoStream.isEnabled("DWPT")) {
                infoStream.message("DWPT", "flush time " + ((System.nanoTime() - t0) / 1000000.0) + " msec");
            }
            return fs;
        } catch (Throwable t) {
            onAbortingException(t);
            throw t;
        } finally {
            maybeAbort("flush", flushNotifications);
            hasFlushed.set(Boolean.TRUE);
        }
    }

    /**
     * 当发生意外情况时 可能需要终止该对象
     *
     * @param location
     * @param flushNotifications
     * @throws IOException
     */
    private void maybeAbort(String location, DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
        // 当截获异常  并设置了 abortingException    终止本对象
        if (hasHitAbortingException() && aborted == false) {
            // if we are already aborted don't do anything here
            try {
                abort();
            } finally {
                // whatever we do here we have to fire this tragic event up.
                // 触发监听器
                flushNotifications.onTragicEvent(abortingException, location);
            }
        }
    }

    private final Set<String> filesToDelete = new HashSet<>();

    Set<String> pendingFilesToDelete() {
        return filesToDelete;
    }

    private FixedBitSet sortLiveDocs(Bits liveDocs, Sorter.DocMap sortMap) {
        assert liveDocs != null && sortMap != null;
        FixedBitSet sortedLiveDocs = new FixedBitSet(liveDocs.length());
        sortedLiveDocs.set(0, liveDocs.length());
        for (int i = 0; i < liveDocs.length(); i++) {
            if (liveDocs.get(i) == false) {
                sortedLiveDocs.clear(sortMap.oldToNew(i));
            }
        }
        return sortedLiveDocs;
    }

    /**
     * Seals the {@link SegmentInfo} for the new flushed segment and persists
     * the deleted documents {@link FixedBitSet}.
     */
    void sealFlushedSegment(FlushedSegment flushedSegment, Sorter.DocMap sortMap, DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
        assert flushedSegment != null;
        SegmentCommitInfo newSegment = flushedSegment.segmentInfo;

        IndexWriter.setDiagnostics(newSegment.info, IndexWriter.SOURCE_FLUSH);

        IOContext context = new IOContext(new FlushInfo(newSegment.info.maxDoc(), newSegment.sizeInBytes()));

        boolean success = false;
        try {

            if (indexWriterConfig.getUseCompoundFile()) {
                Set<String> originalFiles = newSegment.info.files();
                // TODO: like addIndexes, we are relying on createCompoundFile to successfully cleanup...
                IndexWriter.createCompoundFile(infoStream, new TrackingDirectoryWrapper(directory), newSegment.info, context, flushNotifications::deleteUnusedFiles);
                filesToDelete.addAll(originalFiles);
                newSegment.info.setUseCompoundFile(true);
            }

            // Have codec write SegmentInfo.  Must do this after
            // creating CFS so that 1) .si isn't slurped into CFS,
            // and 2) .si reflects useCompoundFile=true change
            // above:
            codec.segmentInfoFormat().write(directory, newSegment.info, context);

            // TODO: ideally we would freeze newSegment here!!
            // because any changes after writing the .si will be
            // lost...

            // Must write deleted docs after the CFS so we don't
            // slurp the del file into CFS:
            if (flushedSegment.liveDocs != null) {
                final int delCount = flushedSegment.delCount;
                assert delCount > 0;
                if (infoStream.isEnabled("DWPT")) {
                    infoStream.message("DWPT", "flush: write " + delCount + " deletes gen=" + flushedSegment.segmentInfo.getDelGen());
                }

                // TODO: we should prune the segment if it's 100%
                // deleted... but merge will also catch it.

                // TODO: in the NRT case it'd be better to hand
                // this del vector over to the
                // shortly-to-be-opened SegmentReader and let it
                // carry the changes; there's no reason to use
                // filesystem as intermediary here.

                SegmentCommitInfo info = flushedSegment.segmentInfo;
                Codec codec = info.info.getCodec();
                final FixedBitSet bits;
                if (sortMap == null) {
                    bits = flushedSegment.liveDocs;
                } else {
                    bits = sortLiveDocs(flushedSegment.liveDocs, sortMap);
                }
                codec.liveDocsFormat().writeLiveDocs(bits, directory, info, delCount, context);
                newSegment.setDelCount(delCount);
                newSegment.advanceDelGen();
            }

            success = true;
        } finally {
            if (!success) {
                if (infoStream.isEnabled("DWPT")) {
                    infoStream.message("DWPT",
                            "hit exception creating compound file for newly flushed segment " + newSegment.info.name);
                }
            }
        }
    }

    /**
     * Get current segment info we are writing.
     */
    SegmentInfo getSegmentInfo() {
        return segmentInfo;
    }

    long bytesUsed() {
        return bytesUsed.get() + pendingUpdates.ramBytesUsed();
    }

    /* Initial chunks size of the shared byte[] blocks used to
       store postings data */
    final static int BYTE_BLOCK_NOT_MASK = ~BYTE_BLOCK_MASK;

    /* if you increase this, you must fix field cache impl for
     * getTerms/getTermsIndex requires <= 32768 */
    final static int MAX_TERM_LENGTH_UTF8 = BYTE_BLOCK_SIZE - 2;


    /**
     * 使用该对象分配 内存时 同时会在计数器中做记录
     */
    private static class IntBlockAllocator extends IntBlockPool.Allocator {
        private final Counter bytesUsed;

        public IntBlockAllocator(Counter bytesUsed) {
            super(IntBlockPool.INT_BLOCK_SIZE);
            this.bytesUsed = bytesUsed;
        }

        /* Allocate another int[] from the shared pool */
        @Override
        public int[] getIntBlock() {
            int[] b = new int[IntBlockPool.INT_BLOCK_SIZE];
            bytesUsed.addAndGet(IntBlockPool.INT_BLOCK_SIZE * Integer.BYTES);
            return b;
        }

        @Override
        public void recycleIntBlocks(int[][] blocks, int offset, int length) {
            bytesUsed.addAndGet(-(length * (IntBlockPool.INT_BLOCK_SIZE * Integer.BYTES)));
        }

    }

    @Override
    public String toString() {
        return "DocumentsWriterPerThread [pendingDeletes=" + pendingUpdates
                + ", segment=" + (segmentInfo != null ? segmentInfo.name : "null") + ", aborted=" + aborted + ", numDocsInRAM="
                + numDocsInRAM + ", deleteQueue=" + deleteQueue + ", " + numDeletedDocIds + " deleted docIds" + "]";
    }


    /**
     * Returns true iff this DWPT is marked as flush pending
     */
    boolean isFlushPending() {
        return flushPending.get() == Boolean.TRUE;
    }

    /**
     * Sets this DWPT as flush pending. This can only be set once.
     */
    void setFlushPending() {
        flushPending.set(Boolean.TRUE);
    }


    /**
     * Returns the last committed bytes for this DWPT. This method can be called
     * without acquiring the DWPTs lock.
     */
    long getLastCommittedBytesUsed() {
        return lastCommittedBytesUsed;
    }

    /**
     * Commits the current {@link #bytesUsed()} and stores it's value for later reuse.
     * The last committed bytes used can be retrieved via {@link #getLastCommittedBytesUsed()}
     *
     * @return the delta between the current {@link #bytesUsed()} and the current {@link #getLastCommittedBytesUsed()}
     */
    long commitLastBytesUsed() {
        assert isHeldByCurrentThread();
        long delta = bytesUsed() - lastCommittedBytesUsed;
        lastCommittedBytesUsed += delta;
        return delta;
    }

    /**
     * Locks this DWPT for exclusive access.
     *
     * @see ReentrantLock#lock()
     */
    void lock() {
        lock.lock();
    }

    /**
     * Acquires the DWPT's lock only if it is not held by another thread at the time
     * of invocation.
     *
     * @return true if the lock was acquired.
     * @see ReentrantLock#tryLock()
     */
    boolean tryLock() {
        return lock.tryLock();
    }

    /**
     * Returns true if the DWPT's lock is held by the current thread
     *
     * @see ReentrantLock#isHeldByCurrentThread()
     */
    boolean isHeldByCurrentThread() {
        return lock.isHeldByCurrentThread();
    }

    /**
     * Unlocks the DWPT's lock
     *
     * @see ReentrantLock#unlock()
     */
    void unlock() {
        lock.unlock();
    }

    /**
     * Returns <code>true</code> iff this DWPT has been flushed
     */
    boolean hasFlushed() {
        return hasFlushed.get() == Boolean.TRUE;
    }
}
