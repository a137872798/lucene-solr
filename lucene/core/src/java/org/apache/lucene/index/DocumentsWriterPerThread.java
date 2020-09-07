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
 * 具体的写入逻辑都在这个类里面
 * PerThread的含义是该对象一次只能被一条线程持有 (每次执行相关方法前都要确保获取该对象的锁)
 */
final class DocumentsWriterPerThread {

    /**
     * The IndexingChain must define the {@link #getChain(DocumentsWriterPerThread)} method
     * which returns the DocConsumer that the DocumentsWriter calls to process the
     * documents.
     * 该对象定义了整个处理流程 从解析token流 到将相关attr存储到索引文件
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


    /**
     * 每个线程会维护一个处理器 负责通过分词器解析doc 并抽取相关信息存储到索引文件中
     */
    static final IndexingChain defaultIndexingChain = new IndexingChain() {

        @Override
        DocConsumer getChain(DocumentsWriterPerThread documentsWriterPerThread) {
            return new DefaultIndexingChain(documentsWriterPerThread);
        }
    };

    /**
     * 简单的bean对象
     */
    static class DocState {
        /**
         * 该描述信息绑定在哪个thread上
         */
        final DocumentsWriterPerThread docWriter;
        /**
         * thread 使用哪个分词器解析doc
         */
        final Analyzer analyzer;
        InfoStream infoStream;
        /**
         * 该对象负责对doc打分
         */
        Similarity similarity;
        /**
         * 此时处理到的最新的doc id
         */
        int docID;
        /**
         * 此时正在处理的doc  可以通过它遍历内部的一组field  (field 有自己的name 和value 以及 记录哪些信息会存储到索引文件的 indexableType)
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
         * 该段对应的所有doc下存在的field
         */
        final FieldInfos fieldInfos;
        /**
         * 使用记录更新/删除信息的数据包装后生成的对象
         */
        final FrozenBufferedUpdates segmentUpdates;
        /**
         * 描述此时还存在的所有doc  在flush时 已经处理过一轮针对term的删除了   不过这些doc信息还是写入到索引文件中了
         */
        final FixedBitSet liveDocs;
        /**
         * 代表刷盘时 doc已经发生过重排序了
         */
        final Sorter.DocMap sortMap;
        /**
         * 记录在刷盘时删除的doc数量  包括解析失败的 和 命中  termNode 导致被删除的
         */
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
     * 代表由该对象解析的数据 将被放弃
     */
    void abort() throws IOException {
        aborted = true;
        pendingNumDocs.addAndGet(-numDocsInRAM);
        try {
            if (infoStream.isEnabled("DWPT")) {
                infoStream.message("DWPT", "now abort");
            }
            try {
                // 终止处理器  实际上会关闭内部所有的索引文件
                consumer.abort();
            } finally {
                // 将本线程之前从 globalSlice上采集的所有 delete信息删除
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
     * 该对象处理传入的文档  就是chain 对象
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
     * 距离上次刷盘到现在 在内存中写入了多少数据
     */
    private volatile long lastCommittedBytesUsed;
    /**
     * 代表 本次刷盘已经完成  该对象代表着整个解析/刷盘过程都是单线程完成的
     */
    private SetOnce<Boolean> hasFlushed = new SetOnce<>();

    /**
     * 该对象负责抽取 doc的域信息 并生成 FieldInfos
     */
    private final FieldInfos.Builder fieldInfos;
    private final InfoStream infoStream;

    /**
     * 此时内存中有多少 doc
     */
    private int numDocsInRAM;
    final DocumentsWriterDeleteQueue deleteQueue;
    private final DeleteSlice deleteSlice;
    private final NumberFormat nf = NumberFormat.getInstance(Locale.ROOT);

    // 该对象还指定了 内存分配器
    final Allocator byteBlockAllocator;
    final IntBlockPool.Allocator intBlockAllocator;
    /**
     * 代表此时正在解析的doc (解析指的是通过分词器抽取数据 以及生成索引结构的过程)
     */
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
     * 存储因生成索引失败 要删除的doc
     */
    private int[] deleteDocIDs = new int[0];
    private int numDeletedDocIds = 0;


    /**
     * @param indexVersionCreated  描述创建的索引版本
     * @param segmentName   注意 每个 thread对象维护一个段名 而thread对象本身是可复用的 也就是往IndexWriter并发写入doc
     *                      每个线程会对应一个 DocWriterThread 并有一个对应的segmentName
     * @param directoryOrig 存储索引文件的目录
     * @param directory  通过装饰器包装后的目录
     * @param indexWriterConfig  配置信息
     * @param infoStream   日志对象
     * @param deleteQueue
     * @param fieldInfos   通过该对象创建的field 会存储到一个以 IndexWriter为单位的全局容器中
     * @param pendingNumDocs      归属于某个 DocWriter的 thread 共享同一个 pendingNumDocs
     * @param enableTestPoints
     * @throws IOException
     */
    DocumentsWriterPerThread(int indexVersionCreated, String segmentName, Directory directoryOrig, Directory directory, LiveIndexWriterConfig indexWriterConfig, InfoStream infoStream, DocumentsWriterDeleteQueue deleteQueue,
                             FieldInfos.Builder fieldInfos, AtomicLong pendingNumDocs, boolean enableTestPoints) throws IOException {
        // directory 原本在 IndexWriter上增加了对文件锁的判断 这里又进行了一层包装 通过该对象创建的文件都会被记录下来 以便在遇到异常时删除文件
        this.directory = new TrackingDirectoryWrapper(directory);
        this.fieldInfos = fieldInfos;
        this.indexWriterConfig = indexWriterConfig;
        this.infoStream = infoStream;
        this.codec = indexWriterConfig.getCodec();
        // 只是一个简单的bean对象 内部描述了 该thread 通过哪个analyzer 对写入的doc进行解析 以及当前正在处理的doc信息
        this.docState = new DocState(this, indexWriterConfig.getAnalyzer(), infoStream);
        // 为状态对象设置 打分器
        this.docState.similarity = indexWriterConfig.getSimilarity();
        // 由同一个 docWriter 创建的线程会共享一个  numDoc 对象 记录此时待刷盘的doc
        this.pendingNumDocs = pendingNumDocs;
        bytesUsed = Counter.newCounter();
        // 当使用该分配器 创建 BB 对象时 还会额外记录使用了多少内存
        byteBlockAllocator = new DirectTrackingAllocator(bytesUsed);
        // 注意 每个thread 对应一个存储doc更新的容器  同时也对应一个段
        pendingUpdates = new BufferedUpdates(segmentName);
        intBlockAllocator = new IntBlockAllocator(bytesUsed);
        // 同一个DocWriter下的所有thread共用一个删除队列  queue负责采集更新/删除信息
        this.deleteQueue = Objects.requireNonNull(deleteQueue);
        assert numDocsInRAM == 0 : "num docs " + numDocsInRAM;
        // 每个线程对应的doc解析/刷盘对象会持有自己的分片对象
        deleteSlice = deleteQueue.newSlice();
        // 根据段的版本号  创建段的描述信息   当该对象开始刷盘时 会将信息写入到segmentInfo中   一旦PerThread对象完成刷盘后 就可以被丢弃了
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
        // 初始化处理器对象 该对象包含了解析doc 存储数据到索引文件的核心逻辑
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
    // 增加此时正在处理的doc
    private void reserveOneDoc() {
        // 增加 pendingNumDocs   如果超过了 max 则抛出异常
        if (pendingNumDocs.incrementAndGet() > IndexWriter.getActualMaxDocs()) {
            // Reserve failed: put the one doc back and throw exc:
            pendingNumDocs.decrementAndGet();
            throw new IllegalArgumentException("number of documents in the index cannot exceed " + IndexWriter.getActualMaxDocs());
        }
    }

    /**
     * 开始处理本次待写入的doc
     * @param docs               2层迭代器  外层代表多个doc  内层代表doc下的多个 field
     * @param deleteNode
     * @param flushNotifications 通过监听器触发响应钩子 就是IndexWriter
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
            // 在处理前 内存中已经维护了多少doc
            final int docsInRamBefore = numDocsInRAM;
            // 检测是否所有doc 都已经生成了索引结构数据
            boolean allDocsIndexed = false;
            try {
                for (Iterable<? extends IndexableField> doc : docs) {
                    // Even on exception, the document is still added (but marked
                    // deleted), so we don't need to un-reserve at that point.
                    // Aborting exceptions will actually "lose" more than one
                    // document, so the counter will be "wrong" in that case, but
                    // it's very hard to fix (we can't easily distinguish aborting
                    // vs non-aborting exceptions):
                    // 当此时正在处理某个doc 时 会增加全局计数器 pendingNumDocs
                    reserveOneDoc();
                    // 通过状态对象记录此时doc的信息
                    docState.doc = doc;
                    // 从这里可以推断 每次存储在内存中的docs id以0为起点   一旦刷盘后 该perThread被丢弃
                    docState.docID = numDocsInRAM;
                    try {
                        // 开始处理当前doc 主要流程就是遍历doc下所有field  将field.value通过分词器拆解后 将term的position/offset/payload/freq等信息写入到writer中
                        // 还有 field.value/norm 等信息  不一定刷盘 要看此时是否在内存中写入了很多term 或者已经处理了很多doc
                        consumer.processDocument();
                    } finally {
                        numDocsInRAM++; // we count the doc anyway even in the case of an exception
                    }
                }
                // 代表成功为这些doc 生成了索引
                allDocsIndexed = true;
                return finishDocuments(deleteNode, docsInRamBefore);
            } finally {
                // 代表出现了预期外的情况
                if (!allDocsIndexed && !aborted) {
                    // the iterator threw an exception that is not aborting
                    // go and mark all docs from this block as deleted
                    // 当生成索引失败时 将本次处理的所有doc标记成待删除
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
     * 这里是找到符合条件的doc 并尝试删除
     * @param deleteNode    代表一个删除条件   稍后满足该条件的doc 都会被删除
     * @param docIdUpTo   处理本批 待生成索引的doc前  内存中已经包含了多少doc
     * @return  这里返回的是 DeleteQueue的序列号
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
        if (deleteNode != null) {
            // 自己的分片会记录每次删除的节点 并尝试将全局分片的 node转移到 BufferedUpdates中
            seqNo = deleteQueue.add(deleteNode, deleteSlice);
            assert deleteSlice.isTail(deleteNode) : "expected the delete term as the tail item";
            // 每个分片都可以看到这段时间内 其他线程 写入到 queue中的node  这里是将上次node 到本次node之前所有数据写入到自己的 bufferedUpdates中
            deleteSlice.apply(pendingUpdates, docIdUpTo);
            return seqNo;
        } else {
            // 代表本次更新doc 操作 不涉及到删除
            // 同样将自己的分片与 deleteQueue的节点做同步
            seqNo = deleteQueue.updateSlice(deleteSlice);
            // 代表此时该线程记录的 节点与全局队列不一致 将那些node信息也记录下来
            if (seqNo < 0) {
                seqNo = -seqNo;
                deleteSlice.apply(pendingUpdates, docIdUpTo);
            } else {
                //  sliceHead = sliceTail;
                // 代表这段时间内没有新的node写入 实际上此时 head就应该是tail
                deleteSlice.reset();
            }
        }

        return seqNo;
    }

    /**
     * This method marks the last N docs as deleted. This is used
     * in the case of a non-aborting exception. There are several cases
     * where we fail a document ie. due to an exception during analysis
     * that causes the doc to be rejected but won't cause the DWPT to be
     * stale nor the entire IW to abort and shutdown. In such a case
     * we only mark these docs as deleted and turn it into a livedocs
     * during flush
     * @param docCount 从后往前要删除多少doc
     */
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
     * 将全局容器中采集到的所有 更新/删除操作冻结并返回  结果会设置到 perThread申请刷盘时生成的  ticket中
     */
    FrozenBufferedUpdates prepareFlush() {
        assert numDocsInRAM > 0;
        // 实际上使用内部的  globalSlice 创建对象
        final FrozenBufferedUpdates globalUpdates = deleteQueue.freezeGlobalBuffer(deleteSlice);
        /* deleteSlice can possibly be null if we have hit non-aborting exceptions during indexing and never succeeded
        adding a document. */
        // 将变化同步到 pendingUpdates 中  刷盘时按照termNode删除 doc 都是依赖于该 perThread对象对应的分片
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
     * 将此时内存中维护的doc索引数据写入到磁盘中
     * @param flushNotifications   IndexWriter 会监听刷盘状态
     * @return  刷盘后生成的段对象
     */
    FlushedSegment flush(DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
        assert flushPending.get() == Boolean.TRUE;
        assert numDocsInRAM > 0;
        assert deleteSlice.isEmpty() : "all deletes must be applied in prepareFlush";
        // 设置此时段内维护的doc总数   numDocsInRAM 包含了本次失败的doc
        segmentInfo.setMaxDoc(numDocsInRAM);
        // 生成描述本次刷盘信息的对象
        final SegmentWriteState flushState = new SegmentWriteState(infoStream, directory, segmentInfo, fieldInfos.finish(),
                pendingUpdates, new IOContext(new FlushInfo(numDocsInRAM, bytesUsed())));

        // 计算此时占用了多少内存
        final double startMBUsed = bytesUsed() / 1024. / 1024.;

        // Apply delete-by-docID now (delete-byDocID only
        // happens when an exception is hit processing that
        // doc, eg if analyzer has some problem w/ the text):
        // 代表某些doc生成索引失败  这些doc不会被写入到索引文件中
        if (numDeletedDocIds > 0) {
            // 先根据最大值创建位图对象 然后将写入失败的doc 从位图中去除
            flushState.liveDocs = new FixedBitSet(numDocsInRAM);
            // 初始状态 将所有位设置成1
            flushState.liveDocs.set(0, numDocsInRAM);
            for (int i = 0; i < numDeletedDocIds; i++) {
                flushState.liveDocs.clear(deleteDocIDs[i]);
            }
            // 记录本次生成索引失败的 doc数量
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

        // 记录刷盘的起始时间
        long t0 = System.nanoTime();

        if (infoStream.isEnabled("DWPT")) {
            infoStream.message("DWPT", "flush postings as segment " + flushState.segmentInfo.name + " numDocs=" + numDocsInRAM);
        }
        // 针对存在 SortedField的doc 会按照 SortedField 进行重排序
        final Sorter.DocMap sortMap;
        try {
            // 对应匹配软删除域的所有doc
            DocIdSetIterator softDeletedDocs;
            // TODO 先忽略软删除
            if (indexWriterConfig.getSoftDeletesField() != null) {
                softDeletedDocs = consumer.getHasDocValues(indexWriterConfig.getSoftDeletesField());
            } else {
                softDeletedDocs = null;
            }
            // 将之前解析doc 后生成的各种数据写入到索引文件中  返回一个排序相关的map
            sortMap = consumer.flush(flushState);
            // 忽略软删除
            if (softDeletedDocs == null) {
                flushState.softDelCountOnFlush = 0;
            } else {
                flushState.softDelCountOnFlush = PendingSoftDeletes.countSoftDeletes(softDeletedDocs, flushState.liveDocs);
                assert flushState.segmentInfo.maxDoc() >= flushState.softDelCountOnFlush + flushState.delCountOnFlush;
            }
            // We clear this here because we already resolved them (private to this segment) when writing postings:
            // 因为刷盘过程中  pendingUpdates中有关term要删除的doc的部分 已经处理过了 所以不需要继续维护这部分数据了
            pendingUpdates.clearDeleteTerms();
            // 存储本次操作所创建的所有文件
            segmentInfo.setFiles(new HashSet<>(directory.getCreatedFiles()));

            // 当segment的数据刷盘完成时  会创建一个 commitInfo对象   初始化的这个对象很多属性都是默认值
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

            // 对应本 preThread 记录的所有需要删除/更新的doc
            final BufferedUpdates segmentDeletes;
            // 检查此时是否还有待更新/删除的数据
            if (pendingUpdates.deleteQueries.isEmpty() && pendingUpdates.numFieldUpdates.get() == 0) {
                pendingUpdates.clear();
                segmentDeletes = null;
            } else {
                // 还有就要继续使用这个对象
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

            // 这里生成一个叫做  已经完成刷盘的 segment对象   代表着之前解析doc的数据已经写入到磁盘中了
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

    /**
     * 记录待删除的文件
     */
    private final Set<String> filesToDelete = new HashSet<>();

    Set<String> pendingFilesToDelete() {
        return filesToDelete;
    }

    /**
     * 使用重排序后的doc 填充liveDoc
     * @param liveDocs
     * @param sortMap
     * @return
     */
    private FixedBitSet sortLiveDocs(Bits liveDocs, Sorter.DocMap sortMap) {
        assert liveDocs != null && sortMap != null;
        // 看来大多数场景还是在已知 会使用多少空间的情况下创建位图对象
        FixedBitSet sortedLiveDocs = new FixedBitSet(liveDocs.length());
        // 初始所有位为1
        sortedLiveDocs.set(0, liveDocs.length());
        for (int i = 0; i < liveDocs.length(); i++) {
            if (liveDocs.get(i) == false) {
                // 通过原来的下标 换算成新的下标
                sortedLiveDocs.clear(sortMap.oldToNew(i));
            }
        }
        return sortedLiveDocs;
    }

    /**
     * Seals the {@link SegmentInfo} for the new flushed segment and persists
     * the deleted documents {@link FixedBitSet}.
     * 将描述segment的信息持久化到索引文件中
     */
    void sealFlushedSegment(FlushedSegment flushedSegment, Sorter.DocMap sortMap, DocumentsWriter.FlushNotifications flushNotifications) throws IOException {
        assert flushedSegment != null;
        // 本次提交动作相关的段信息
        SegmentCommitInfo newSegment = flushedSegment.segmentInfo;

        // 将一些便于诊断的信息插入到 segmentInfo 中
        IndexWriter.setDiagnostics(newSegment.info, IndexWriter.SOURCE_FLUSH);

        // 创建一个刷盘上下文
        IOContext context = new IOContext(new FlushInfo(newSegment.info.maxDoc(), newSegment.sizeInBytes()));

        boolean success = false;
        try {

            // TODO 忽略合成文件
            if (indexWriterConfig.getUseCompoundFile()) {
                // 这些文件是本次操作过程中生成的
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
            // 将描述段的相关信息写入 到索引文件
            codec.segmentInfoFormat().write(directory, newSegment.info, context);

            // TODO: ideally we would freeze newSegment here!!
            // because any changes after writing the .si will be
            // lost...

            // Must write deleted docs after the CFS so we don't
            // slurp the del file into CFS:
            // 代表有某些doc被删除了  如果所有doc都存在 那么该值为null  在flush中 会将通过 TermNode删除的doc标记在位图中
            if (flushedSegment.liveDocs != null) {
                // 这里同时包含了写入失败的doc  以及 被termNode命中的doc
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

                // 获取该段的描述信息
                SegmentCommitInfo info = flushedSegment.segmentInfo;
                Codec codec = info.info.getCodec();
                final FixedBitSet bits;
                if (sortMap == null) {
                    bits = flushedSegment.liveDocs;
                } else {
                    // 首先要确保liveDoc中部分doc已经被删除  这样才会重排序 并将排序后的结果写入liveDoc中
                    bits = sortLiveDocs(flushedSegment.liveDocs, sortMap);
                }
                // 这里将 当前还留存的 doc 信息写入到索引文件中
                codec.liveDocsFormat().writeLiveDocs(bits, directory, info, delCount, context);
                newSegment.setDelCount(delCount);
                // 这里还会重置segment的id
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
     * 一个 segment_N 对应多个段 每个段对应一个 segmentInfo 对应一个perThread
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
     * 标记当前处在待刷盘状态
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
