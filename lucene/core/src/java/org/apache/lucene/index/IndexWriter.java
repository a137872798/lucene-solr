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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.index.FieldInfos.FieldNumbers;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.LockValidatingDirectoryWrapper;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.Version;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * An <code>IndexWriter</code> creates and maintains an index.
 *
 * <p>The {@link OpenMode} option on
 * {@link IndexWriterConfig#setOpenMode(OpenMode)} determines
 * whether a new index is created, or whether an existing index is
 * opened. Note that you can open an index with {@link OpenMode#CREATE}
 * even while readers are using the index. The old readers will
 * continue to search the "point in time" snapshot they had opened,
 * and won't see the newly created index until they re-open. If
 * {@link OpenMode#CREATE_OR_APPEND} is used IndexWriter will create a
 * new index if there is not already an index at the provided path
 * and otherwise open the existing index.</p>
 *
 * <p>In either case, documents are added with {@link #addDocument(Iterable)
 * addDocument} and removed with {@link #deleteDocuments(Term...)} or {@link
 * #deleteDocuments(Query...)}. A document can be updated with {@link
 * #updateDocument(Term, Iterable) updateDocument} (which just deletes
 * and then adds the entire document). When finished adding, deleting
 * and updating documents, {@link #close() close} should be called.</p>
 *
 * <a id="sequence_numbers"></a>
 * <p>Each method that changes the index returns a {@code long} sequence number, which
 * expresses the effective order in which each change was applied.
 * {@link #commit} also returns a sequence number, describing which
 * changes are in the commit point and which are not.  Sequence numbers
 * are transient (not saved into the index in any way) and only valid
 * within a single {@code IndexWriter} instance.</p>
 *
 * <a id="flush"></a>
 * <p>These changes are buffered in memory and periodically
 * flushed to the {@link Directory} (during the above method
 * calls). A flush is triggered when there are enough added documents
 * since the last flush. Flushing is triggered either by RAM usage of the
 * documents (see {@link IndexWriterConfig#setRAMBufferSizeMB}) or the
 * number of added documents (see {@link IndexWriterConfig#setMaxBufferedDocs(int)}).
 * The default is to flush when RAM usage hits
 * {@link IndexWriterConfig#DEFAULT_RAM_BUFFER_SIZE_MB} MB. For
 * best indexing speed you should flush by RAM usage with a
 * large RAM buffer.
 * In contrast to the other flush options {@link IndexWriterConfig#setRAMBufferSizeMB} and
 * {@link IndexWriterConfig#setMaxBufferedDocs(int)}, deleted terms
 * won't trigger a segment flush. Note that flushing just moves the
 * internal buffered state in IndexWriter into the index, but
 * these changes are not visible to IndexReader until either
 * {@link #commit()} or {@link #close} is called.  A flush may
 * also trigger one or more segment merges which by default
 * run with a background thread so as not to block the
 * addDocument calls (see <a href="#mergePolicy">below</a>
 * for changing the {@link MergeScheduler}).</p>
 *
 * <p>Opening an <code>IndexWriter</code> creates a lock file for the directory in use. Trying to open
 * another <code>IndexWriter</code> on the same directory will lead to a
 * {@link LockObtainFailedException}.</p>
 *
 * <a id="deletionPolicy"></a>
 * <p>Expert: <code>IndexWriter</code> allows an optional
 * {@link IndexDeletionPolicy} implementation to be specified.  You
 * can use this to control when prior commits are deleted from
 * the index.  The default policy is {@link KeepOnlyLastCommitDeletionPolicy}
 * which removes all prior commits as soon as a new commit is
 * done.  Creating your own policy can allow you to explicitly
 * keep previous "point in time" commits alive in the index for
 * some time, either because this is useful for your application,
 * or to give readers enough time to refresh to the new commit
 * without having the old commit deleted out from under them.
 * The latter is necessary when multiple computers take turns opening
 * their own {@code IndexWriter} and {@code IndexReader}s
 * against a single shared index mounted via remote filesystems
 * like NFS which do not support "delete on last close" semantics.
 * A single computer accessing an index via NFS is fine with the
 * default deletion policy since NFS clients emulate "delete on
 * last close" locally.  That said, accessing an index via NFS
 * will likely result in poor performance compared to a local IO
 * device. </p>
 *
 * <a id="mergePolicy"></a> <p>Expert:
 * <code>IndexWriter</code> allows you to separately change
 * the {@link MergePolicy} and the {@link MergeScheduler}.
 * The {@link MergePolicy} is invoked whenever there are
 * changes to the segments in the index.  Its role is to
 * select which merges to do, if any, and return a {@link
 * MergePolicy.MergeSpecification} describing the merges.
 * The default is {@link LogByteSizeMergePolicy}.  Then, the {@link
 * MergeScheduler} is invoked with the requested merges and
 * it decides when and how to run the merges.  The default is
 * {@link ConcurrentMergeScheduler}. </p>
 *
 * <a id="OOME"></a><p><b>NOTE</b>: if you hit a
 * VirtualMachineError, or disaster strikes during a checkpoint
 * then IndexWriter will close itself.  This is a
 * defensive measure in case any internal state (buffered
 * documents, deletions, reference counts) were corrupted.
 * Any subsequent calls will throw an AlreadyClosedException.</p>
 *
 * <a id="thread-safety"></a><p><b>NOTE</b>: {@link
 * IndexWriter} instances are completely thread
 * safe, meaning multiple threads can call any of its
 * methods, concurrently.  If your application requires
 * external synchronization, you should <b>not</b>
 * synchronize on the <code>IndexWriter</code> instance as
 * this may cause deadlock; use your own (non-Lucene) objects
 * instead. </p>
 *
 * <p><b>NOTE</b>: If you call
 * <code>Thread.interrupt()</code> on a thread that's within
 * IndexWriter, IndexWriter will try to catch this (eg, if
 * it's in a wait() or Thread.sleep()), and will then throw
 * the unchecked exception {@link ThreadInterruptedException}
 * and <b>clear</b> the interrupt status on the thread.</p>
 */

/*
 * Clarification: Check Points (and commits)
 * IndexWriter writes new index files to the directory without writing a new segments_N
 * file which references these new files. It also means that the state of
 * the in memory SegmentInfos object is different than the most recent
 * segments_N file written to the directory.
 *
 * Each time the SegmentInfos is changed, and matches the (possibly
 * modified) directory files, we have a new "check point".
 * If the modified/new SegmentInfos is written to disk - as a new
 * (generation of) segments_N file - this check point is also an
 * IndexCommit.
 *
 * A new checkpoint always replaces the previous checkpoint and
 * becomes the new "front" of the index. This allows the IndexFileDeleter
 * to delete files that are referenced only by stale checkpoints.
 * (files that were created since the last commit, but are no longer
 * referenced by the "front" of the index). For this, IndexFileDeleter
 * keeps track of the last non commit checkpoint.
 * 该对象包含了 解析文本 存储索引文件  合并索引文件 等等功能
 */
public class IndexWriter implements Closeable, TwoPhaseCommit, Accountable,
        MergePolicy.MergeContext {

    /**
     * Hard limit on maximum number of documents that may be added to the
     * index.  If you try to add more than this you'll hit {@code IllegalArgumentException}.
     */
    // We defensively subtract 128 to be well below the lowest
    // ArrayUtil.MAX_ARRAY_LENGTH on "typical" JVMs.  We don't just use
    // ArrayUtil.MAX_ARRAY_LENGTH here because this can vary across JVMs:
    // 由同一个IndexWriter并发写入的 doc 在同一时间 只允许解析这么多doc  解析代表通过分词器处理 以及生成索引结构
    public static final int MAX_DOCS = Integer.MAX_VALUE - 128;

    /**
     * Maximum value of the token position in an indexed field.
     */
    public static final int MAX_POSITION = Integer.MAX_VALUE - 128;

    // Use package-private instance var to enforce the limit so testing
    // can use less electricity:
    // 该属性可以通过api 进行修改  默认情况就是 MAX_DOCS
    private static int actualMaxDocs = MAX_DOCS;

    /**
     * Used only for testing.
     */
    static void setMaxDocs(int maxDocs) {
        if (maxDocs > MAX_DOCS) {
            // Cannot go higher than the hard max:
            throw new IllegalArgumentException("maxDocs must be <= IndexWriter.MAX_DOCS=" + MAX_DOCS + "; got: " + maxDocs);
        }
        IndexWriter.actualMaxDocs = maxDocs;
    }

    static int getActualMaxDocs() {
        return IndexWriter.actualMaxDocs;
    }

    /**
     * Used only for testing.
     */
    private final boolean enableTestPoints;

    private static final int UNBOUNDED_MAX_MERGE_SEGMENTS = -1;

    // 一些常量字符串
    /**
     * Name of the write lock in the index.
     */
    public static final String WRITE_LOCK_NAME = "write.lock";
    /**
     * Key for the source of a segment in the {@link SegmentInfo#getDiagnostics() diagnostics}.
     */
    public static final String SOURCE = "source";
    /**
     * Source of a segment which results from a merge of other segments.
     */
    public static final String SOURCE_MERGE = "merge";
    /**
     * Source of a segment which results from a flush.
     */
    public static final String SOURCE_FLUSH = "flush";
    /**
     * Source of a segment which results from a call to {@link #addIndexes(CodecReader...)}.
     */
    public static final String SOURCE_ADDINDEXES_READERS = "addIndexes(CodecReader...)";

    /**
     * Absolute hard maximum length for a term, in bytes once
     * encoded as UTF8.  If a term arrives from the analyzer
     * longer than this length, an
     * <code>IllegalArgumentException</code>  is thrown
     * and a message is printed to infoStream, if set (see {@link
     * IndexWriterConfig#setInfoStream(InfoStream)}).
     */
    public final static int MAX_TERM_LENGTH = DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8;

    /**
     * Maximum length string for a stored field.
     * 当直接存储某个field.value 时 允许的数据最大长度
     */
    public final static int MAX_STORED_STRING_LENGTH = ArrayUtil.MAX_ARRAY_LENGTH / UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR;

    // when unrecoverable disaster strikes, we populate this with the reason that we had to close IndexWriter
    // 记录写入过程中出现的异常
    private final AtomicReference<Throwable> tragedy = new AtomicReference<>(null);

    /**
     * 代表本次要写入的原始目录
     */
    private final Directory directoryOrig;       // original user directory
    /**
     * orig的包装对象
     */
    private final Directory directory;           // wrapped with additional checks

    /**
     * 记录发生了多少次变化
     */
    private final AtomicLong changeCount = new AtomicLong(); // increments every time a change is completed
    /**
     * 上次触发 commit 时写入多少数据
     */
    private volatile long lastCommitChangeCount; // last changeCount that was committed

    /**
     * 在初始化IndexWriter时 会加载 segment_N 文件  读取此时的 segmentInfos信息 这个数据在rollback时使用  rollback会舍弃当前所有的segmentInfo 并还原到初始化该IndexWriter时的segment信息
     */
    private List<SegmentCommitInfo> rollbackSegments;      // list of segmentInfo we will fallback to if the commit fails

    /**
     * 内部包含一组segmentCommitInfo
     * 当调用prepareCommit后 暂存到该队列 之后如果调用了commit 将会真正存储到directory中
     */
    private volatile SegmentInfos pendingCommit;            // set when a commit is pending (after prepareCommit() & before commit())
    private volatile long pendingSeqNo;

    /**
     * 记录待提交的变化数   是某个时刻changeCount的值
     */
    private volatile long pendingCommitChangeCount;

    /**
     * 代表需要提交的文件
     */
    private Collection<String> filesToCommit;

    /**
     * 代表本次初始化时 从segment_N 文件中读取到的所有段信息
     */
    private final SegmentInfos segmentInfos;
    /**
     * 在全局范围内存储 fieldNum与fieldName 等映射关系的容器
     */
    final FieldNumbers globalFieldNumberMap;

    /**
     * 该对象负责向 doc写入数据
     */
    final DocumentsWriter docWriter;
    /**
     * 事件队列对象 内部的event需要传入 writer
     */
    private final EventQueue eventQueue = new EventQueue(this);
    /**
     * 本对象还会作为一个 提供merge对象的数据源  相关方法都会转发给该对象
     */
    private final MergeScheduler.MergeSource mergeSource = new IndexWriterMergeSource(this);

    /**
     * 往doc写入数据时 需要通过锁做同步控制
     */
    private final ReentrantLock writeDocValuesLock = new ReentrantLock();

    /**
     * 暂存任务的队列  多生产者多消费者队列 每个执行flush的线程都可以往内部添加任务 并且每个执行完 刷盘后的线程最终都会消费队列中的任务
     */
    static final class EventQueue implements Closeable {

        /**
         * 代表 IndexWriter 已经被关闭
         */
        private volatile boolean closed;
        // we use a semaphore here instead of simply synced methods to allow
        // events to be processed concurrently by multiple threads such that all events
        // for a certain thread are processed once the thread returns from IW  这里使用信号量的意义是?  允许一定程度的并发 但是避免高并发 ???  有用吗 外部捕获到对应异常时如何处理呢
        private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);
        /**
         * 缓冲区  或者说缓冲队列
         */
        private final Queue<Event> queue = new ConcurrentLinkedQueue<>();

        /**
         * 实际方法被委托给该对象
         */
        private final IndexWriter writer;

        EventQueue(IndexWriter writer) {
            this.writer = writer;
        }

        private void acquire() {
            if (permits.tryAcquire() == false) {
                throw new AlreadyClosedException("queue is closed");
            }
            if (closed) {
                permits.release();
                throw new AlreadyClosedException("queue is closed");
            }
        }

        boolean add(Event event) {
            // 允许多线程并发添加事件到队列
            acquire();
            try {
                return queue.add(event);
            } finally {
                permits.release();
            }
        }

        void processEvents() throws IOException {
            acquire();
            try {
                processEventsInternal();
            } finally {
                permits.release();
            }
        }

        /**
         * 在当前线程中处理所有的事件  该线程也就是IO线程
         *
         * @throws IOException
         */
        private void processEventsInternal() throws IOException {
            assert Integer.MAX_VALUE - permits.availablePermits() > 0 : "must acquire a permit before processing events";
            Event event;
            while ((event = queue.poll()) != null) {
                event.process(writer);
            }
        }

        /**
         * 关闭任务队列 并且执行剩余任务  类似线程池的优雅关闭
         *
         * @throws IOException
         */
        @Override
        public synchronized void close() throws IOException { // synced to prevent double closing
            assert closed == false : "we should never close this twice";
            closed = true;
            // it's possible that we close this queue while we are in a processEvents call
            // writer此时存在某种异常无法正常工作所以放弃之前的任务
            if (writer.getTragicException() != null) {
                // we are already handling a tragic exception let's drop it all on the floor and return
                queue.clear();
            } else {
                // now we acquire all the permits to ensure we are the only one processing the queue
                try {
                    // 确保其他线程无法再调用 acquire
                    permits.acquire(Integer.MAX_VALUE);
                } catch (InterruptedException e) {
                    throw new ThreadInterruptedException(e);
                }
                try {
                    // 当前线程(IO线程)会处理所有事件
                    processEventsInternal();
                } finally {
                    permits.release(Integer.MAX_VALUE);
                }
            }
        }
    }

    /**
     * 该对象负责删除索引数据
     */
    private final IndexFileDeleter deleter;

    // used by forceMerge to note those needing merging
    // merge时挑选的segment 必须确保在该容器内
    private final Map<SegmentCommitInfo, Boolean> segmentsToMerge = new HashMap<>();
    /**
     * 每次最多允许merge 多少segment
     */
    private int mergeMaxNumSegments;

    /**
     * 进程锁 使得同一时间只有一个进程在操作目录 避免索引文件被并发修改
     */
    private Lock writeLock;

    private volatile boolean closed;
    private volatile boolean closing;

    /**
     * 代表认为当前 segmentInfos 需要触发一次merge
     */
    private final AtomicBoolean maybeMerge = new AtomicBoolean();

    private Iterable<Map.Entry<String, String>> commitUserData;

    // Holds all SegmentInfo instances currently involved in
    // 此时正在merge的 segment
    private final HashSet<SegmentCommitInfo> mergingSegments = new HashSet<>();
    /**
     * 该对象就是标记了 每隔多久触发一次merge  同时从 mergeSource中获取数据 并进行合并
     */
    private final MergeScheduler mergeScheduler;
    private final Set<SegmentMerger> runningAddIndexesMerges = new HashSet<>();
    /**
     * 存储等待被merge的对象  每个OneMerge对象 代表一次完整的merge任务 内部包含多个segment的信息
     * 该容器与mergingSegments  应该就像2个 flush容器一样  一个标明这个对象正在merge 不允许加入新的 oneMerge中 另一个容器专门负责给 执行merge的线程提供任务
     */
    private final LinkedList<MergePolicy.OneMerge> pendingMerges = new LinkedList<>();
    /**
     * 正在merge中的对象  pendingMerges的元素被取出后会移动到这里
     */
    private final Set<MergePolicy.OneMerge> runningMerges = new HashSet<>();
    private final List<MergePolicy.OneMerge> mergeExceptions = new ArrayList<>();
    private long mergeGen;

    private boolean stopMerges; // TODO make sure this is only changed once and never set back to false
    private boolean didMessageState;

    /**
     * 记录该对象总计刷盘了多少次
     */
    private final AtomicInteger flushCount = new AtomicInteger();

    /**
     * 记录总计处理了多少次 FrozenBufferedUpdates
     */
    private final AtomicInteger flushDeletesCount = new AtomicInteger();
    private final ReaderPool readerPool;

    /**
     * 每个 flushTicket 内部包含一个 FrozenBufferedUpdates  记录着本次的数据变化  而当刷盘任务完成时 会将其"发布"  这时会将FrozenBufferedUpdates 添加到该对象内部
     */
    private final BufferedUpdatesStream bufferedUpdatesStream;

    /**
     * Counts how many merges have completed; this is used by {@link #forceApply(FrozenBufferedUpdates)}
     * to handle concurrently apply deletes/updates with merges completing.
     */
    // 记录总计完成了多少 merge 任务
    private final AtomicLong mergeFinishedGen = new AtomicLong();

    // The instance that was passed to the constructor. It is saved only in order
    // to allow users to query an IndexWriter settings.
    private final LiveIndexWriterConfig config;

    /**
     * System.nanoTime() when commit started; used to write
     * an infoStream message about how long commit took.
     */
    private long startCommitTime;

    /**
     * How many documents are in the index, or are in the process of being
     * added (reserved).  E.g., operations like addIndexes will first reserve
     * the right to add N docs, before they actually change the index,
     * much like how hotels place an "authorization hold" on your credit
     * card to make sure they can later charge you when you check out.
     * 当前 Index维护的doc总数
     */
    private final AtomicLong pendingNumDocs = new AtomicLong();
    private final boolean softDeletesEnabled;

    /**
     * IndexWriter 携带一个内置的监听器对象 监听 docWriter工作状况 并根据不同情况触发不同钩子
     */
    private final DocumentsWriter.FlushNotifications flushNotifications = new DocumentsWriter.FlushNotifications() {

        /**
         *
         * @param files
         */
        @Override
        public void deleteUnusedFiles(Collection<String> files) {
            // 删除不再访问的文件 委托给IndexFileDeleter对象
            eventQueue.add(w -> w.deleteNewFiles(files));
        }

        /**
         * 代表某个段信息刷盘失败了   实际上会变成deleteNewFiles  也就是删除刷盘失败的文件
         * @param info
         */
        @Override
        public void flushFailed(SegmentInfo info) {
            eventQueue.add(w -> w.flushFailed(info));
        }

        /**
         * 当某个segment 刷盘完成时  发布该segment  此时通过用户线程触发该方法
         * @throws IOException
         */
        @Override
        public void afterSegmentsFlushed() throws IOException {
            publishFlushedSegments(false);
        }

        /**
         * 当检测到异常时 设置异常对象
         * @param event
         * @param message
         */
        @Override
        public void onTragicEvent(Throwable event, String message) {
            IndexWriter.this.onTragicEvent(event, message);
        }

        /**
         * 当将 BufferedUpdate 对象包装成 ticket 并插入到 ticketQueue中后 会触发该方法 为任务队列添加一个任务， 注意 此时还没有线程去执行任务， 业务线程会继续走流程
         * 此时也是触发publishFlushedSegments 不过主要是处理 包装 BufferedUpdate的ticket
         */
        @Override
        public void onDeletesApplied() {
            eventQueue.add(w -> {
                        try {
                            w.publishFlushedSegments(true);
                        } finally {
                            flushCount.incrementAndGet();
                        }
                    }
            );
        }

        /**
         * 代表此时待刷盘的perThread对象很多   并且调用flush的业务线程暂停继续从pool中拉取可刷盘的 perThread
         */
        @Override
        public void onTicketBacklog() {
            eventQueue.add(w -> w.publishFlushedSegments(true));
        }
    };

    /**
     * 生成基于索引查询数据的 reader 对象
     * @return
     * @throws IOException
     */
    DirectoryReader getReader() throws IOException {
        return getReader(true, false);
    }

    /**
     * Expert: returns a readonly reader, covering all
     * committed as well as un-committed changes to the index.
     * This provides "near real-time" searching, in that
     * changes made during an IndexWriter session can be
     * quickly made available for searching without closing
     * the writer nor calling {@link #commit}.
     *
     * <p>Note that this is functionally equivalent to calling
     * {#flush} and then opening a new reader.  But the turnaround time of this
     * method should be faster since it avoids the potentially
     * costly {@link #commit}.</p>
     *
     * <p>You must close the {@link IndexReader} returned by
     * this method once you are done using it.</p>
     *
     * <p>It's <i>near</i> real-time because there is no hard
     * guarantee on how quickly you can get a new reader after
     * making changes with IndexWriter.  You'll have to
     * experiment in your situation to determine if it's
     * fast enough.  As this is a new and experimental
     * feature, please report back on your findings so we can
     * learn, improve and iterate.</p>
     *
     * <p>The resulting reader supports {@link
     * DirectoryReader#openIfChanged}, but that call will simply forward
     * back to this method (though this may change in the
     * future).</p>
     *
     * <p>The very first time this method is called, this
     * writer instance will make every effort to pool the
     * readers that it opens for doing merges, applying
     * deletes, etc.  This means additional resources (RAM,
     * file descriptors, CPU time) will be consumed.</p>
     *
     * <p>For lower latency on reopening a reader, you should
     * call {@link IndexWriterConfig#setMergedSegmentWarmer} to
     * pre-warm a newly merged segment before it's committed
     * to the index.  This is important for minimizing
     * index-to-search delay after a large merge.  </p>
     *
     * <p>If an addIndexes* call is running in another thread,
     * then this reader will only search those segments from
     * the foreign index that have been successfully copied
     * over, so far</p>.
     *
     * <p><b>NOTE</b>: Once the writer is closed, any
     * outstanding readers may continue to be used.  However,
     * if you attempt to reopen any of those readers, you'll
     * hit an {@link AlreadyClosedException}.</p>
     *
     * @return IndexReader that covers entire index plus all
     * changes made so far by this IndexWriter instance
     * @throws IOException If there is a low-level I/O error
     *
     * @param applyAllDeletes 是否要等待所有的update/delete处理完
     * @param writeAllDeletes 是否要将 update/delete处理后的数据持久化
     * @lucene.experimental
     * 获取近实时搜索对象   一般情况下必须等待所有数据处理完成 比如commit 后才开始查询数据
     * 而近实时搜索对象意味着不需要等待commit  数据的变更很快就可见
     */
    DirectoryReader getReader(boolean applyAllDeletes, boolean writeAllDeletes) throws IOException {
        ensureOpen();

        if (writeAllDeletes && applyAllDeletes == false) {
            throw new IllegalArgumentException("applyAllDeletes must be true when writeAllDeletes=true");
        }

        final long tStart = System.currentTimeMillis();

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "flush at getReader");
        }
        // Do this up front before flushing so that the readers
        // obtained during this flush are pooled, the first time
        // this method is called:
        // 设置成 允许reader池化
        readerPool.enableReaderPooling();
        DirectoryReader r = null;
        // 执行刷盘前的钩子
        doBeforeFlush();
        boolean anyChanges = false;
        /*
         * for releasing a NRT reader we must ensure that
         * DW doesn't add any segments or deletes until we are
         * done with creating the NRT DirectoryReader.
         * We release the two stage full flush after we are done opening the
         * directory reader!
         */
        boolean success2 = false;
        try {
            boolean success = false;
            synchronized (fullFlushLock) {
                try {
                    // TODO: should we somehow make the seqNo available in the returned NRT reader?
                    // 触发 fullFlush
                    anyChanges = docWriter.flushAllThreads() < 0;
                    if (anyChanges == false) {
                        // prevent double increment since docWriter#doFlush increments the flushcount
                        // if we flushed anything.
                        // 当没有任何数据写入时 也要增加刷盘次数
                        flushCount.incrementAndGet();
                    }
                    publishFlushedSegments(true);
                    processEvents(false);

                    // 这里会阻塞线程 直到所有 update/delete任务完成  因为某些任务可能被其他线程抢占了   注意一般来讲只是到最后一个刷盘的perThread采集到的update/delete数据
                    // 后面的数据很可能还未处理 暂时也不需要处理 只有当deleteQueue囤积的bytes数达到一定值时 才会处理
                    if (applyAllDeletes) {
                        applyAllDeletesAndUpdates();
                    }

                    synchronized (this) {

                        // NOTE: we cannot carry doc values updates in memory yet, so we always must write them through to disk and re-open each
                        // SegmentReader:

                        // TODO: we could instead just clone SIS and pull/incref readers in sync'd block, and then do this w/o IW's lock?
                        // Must do this sync'd on IW to prevent a merge from completing at the last second and failing to write its DV updates:
                        // 确保此时更新都已经写入到磁盘中
                        writeReaderPool(writeAllDeletes);

                        // Prevent segmentInfos from changing while opening the
                        // reader; in theory we could instead do similar retry logic,
                        // just like we do when loading segments_N

                        // 此时打开NRT 读取对象
                        r = StandardDirectoryReader.open(this, segmentInfos, applyAllDeletes, writeAllDeletes);
                        if (infoStream.isEnabled("IW")) {
                            infoStream.message("IW", "return reader version=" + r.getVersion() + " reader=" + r);
                        }
                    }
                    success = true;
                } finally {
                    // Done: finish the full flush!
                    assert Thread.holdsLock(fullFlushLock);
                    docWriter.finishFullFlush(success);
                    // 之前可能因为fullFlush 而被暂时搁置的  AllDelete请求将会在这里被处理
                    if (success) {
                        processEvents(false);
                        doAfterFlush();
                    } else {
                        if (infoStream.isEnabled("IW")) {
                            infoStream.message("IW", "hit exception during NRT reader");
                        }
                    }
                }
            }
            anyChanges |= maybeMerge.getAndSet(false);
            // merge通过 MergePolicy执行 一般都是使用 ConcurrentMergePolicy 也就是并行执行 可以提高效率 而本次是否立即生成mergeSegment 实际上对本次查询是没有影响的
            if (anyChanges) {
                maybeMerge(config.getMergePolicy(), MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
            }
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "getReader took " + (System.currentTimeMillis() - tStart) + " msec");
            }
            success2 = true;
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "getReader");
            throw tragedy;
        } finally {
            if (!success2) {
                try {
                    IOUtils.closeWhileHandlingException(r);
                } finally {
                    maybeCloseOnTragicEvent();
                }
            }
        }
        return r;
    }

    @Override
    public final long ramBytesUsed() {
        ensureOpen();
        return docWriter.ramBytesUsed();
    }

    /**
     * Returns the number of bytes currently being flushed
     */
    public final long getFlushingBytes() {
        ensureOpen();
        return docWriter.getFlushingBytes();
    }

    /**
     *
     * @throws IOException
     */
    final void writeSomeDocValuesUpdates() throws IOException {
        if (writeDocValuesLock.tryLock()) {
            try {
                final double ramBufferSizeMB = config.getRAMBufferSizeMB();
                // If the reader pool is > 50% of our IW buffer, then write the updates:
                // 代表开启了自动刷盘
                if (ramBufferSizeMB != IndexWriterConfig.DISABLE_AUTO_FLUSH) {
                    long startNS = System.nanoTime();

                    long ramBytesUsed = readerPool.ramBytesUsed();
                    // 此时在 pool 中 所有 ReaderAndUpdate对象中已经存储了很多 update数据  但是如果本次的update数据不够多的话不会强制处理
                    if (ramBytesUsed > 0.5 * ramBufferSizeMB * 1024 * 1024) {
                        if (infoStream.isEnabled("BD")) {
                            infoStream.message("BD", String.format(Locale.ROOT, "now write some pending DV updates: %.2f MB used vs IWC Buffer %.2f MB",
                                    ramBytesUsed / 1024. / 1024., ramBufferSizeMB));
                        }

                        // Sort by largest ramBytesUsed:
                        // 按照内存占用大小倒序 返回所有待处理的reader
                        final List<ReadersAndUpdates> list = readerPool.getReadersByRam();
                        int count = 0;
                        for (ReadersAndUpdates rld : list) {

                            // 此时内存占用已经低于阈值了 停止处理
                            if (ramBytesUsed <= 0.5 * ramBufferSizeMB * 1024 * 1024) {
                                break;
                            }
                            // We need to do before/after because not all RAM in this RAU is used by DV updates, and
                            // not all of those bytes can be written here:
                            long bytesUsedBefore = rld.ramBytesUsed.get();
                            if (bytesUsedBefore == 0) {
                                continue; // nothing to do here - lets not acquire the lock
                            }
                            // Only acquire IW lock on each write, since this is a time consuming operation.  This way
                            // other threads get a chance to run in between our writes.
                            synchronized (this) {
                                // It's possible that the segment of a reader returned by readerPool#getReadersByRam
                                // is dropped before being processed here. If it happens, we need to skip that reader.
                                // this is also best effort to free ram, there might be some other thread writing this rld concurrently
                                // which wins and then if readerPooling is off this rld will be dropped.
                                // 尝试获取该segment对应的reader 对象
                                if (readerPool.get(rld.info, false) == null) {
                                    continue;
                                }
                                // 处理update信息 并将新的 DocValueType持久化到索引文件中 以及将最新的fieldInfo信息持久化到索引文件
                                // bufferedUpdatesStream.getCompletedDelGen() 代表此时已经完全处理完的 update 对应的gen
                                if (rld.writeFieldUpdates(directory, globalFieldNumberMap, bufferedUpdatesStream.getCompletedDelGen(), infoStream)) {
                                    // 将新增的索引文件引用计数+1
                                    checkpointNoSIS();
                                }
                            }
                            long bytesUsedAfter = rld.ramBytesUsed.get();
                            ramBytesUsed -= bytesUsedBefore - bytesUsedAfter;
                            count++;
                        }

                        if (infoStream.isEnabled("BD")) {
                            infoStream.message("BD", String.format(Locale.ROOT, "done write some DV updates for %d segments: now %.2f MB used vs IWC Buffer %.2f MB; took %.2f sec",
                                    count, readerPool.ramBytesUsed() / 1024. / 1024., ramBufferSizeMB, ((System.nanoTime() - startNS) / 1000000000.)));
                        }
                    }
                }
            } finally {
                writeDocValuesLock.unlock();
            }
        }
    }

    /**
     * Obtain the number of deleted docs for a pooled reader.
     * If the reader isn't being pooled, the segmentInfo's
     * delCount is returned.
     * 检测当前 segment 中有多少doc 被删除
     */
    @Override
    public int numDeletedDocs(SegmentCommitInfo info) {
        ensureOpen(false);
        validate(info);
        final ReadersAndUpdates rld = getPooledInstance(info, false);
        if (rld != null) {
            // 返回的数量包含 还未刷盘的 delCount
            return rld.getDelCount(); // get the full count from here since SCI might change concurrently
        } else {
            // 当开启软删除的情况下  将软删除对应的field命中的doc数量也加上
            final int delCount = info.getDelCount(softDeletesEnabled);
            assert delCount <= info.info.maxDoc() : "delCount: " + delCount + " maxDoc: " + info.info.maxDoc();
            return delCount;
        }
    }

    /**
     * Used internally to throw an {@link AlreadyClosedException} if this
     * IndexWriter has been closed or is in the process of closing.
     *
     * @param failIfClosing if true, also fail when {@code IndexWriter} is in the process of
     *                      closing ({@code closing=true}) but not yet done closing (
     *                      {@code closed=false})
     * @throws AlreadyClosedException if this IndexWriter is closed or in the process of closing
     */
    protected final void ensureOpen(boolean failIfClosing) throws AlreadyClosedException {
        if (closed || (failIfClosing && closing)) {
            throw new AlreadyClosedException("this IndexWriter is closed", tragedy.get());
        }
    }

    /**
     * Used internally to throw an {@link
     * AlreadyClosedException} if this IndexWriter has been
     * closed ({@code closed=true}) or is in the process of
     * closing ({@code closing=true}).
     * <p>
     * Calls {@link #ensureOpen(boolean) ensureOpen(true)}.
     *
     * @throws AlreadyClosedException if this IndexWriter is closed
     */
    protected final void ensureOpen() throws AlreadyClosedException {
        ensureOpen(true);
    }

    /**
     * Constructs a new IndexWriter per the settings given in <code>conf</code>.
     * If you want to make "live" changes to this writer instance, use
     * {@link #getConfig()}.
     *
     * <p>
     * <b>NOTE:</b> after ths writer is created, the given configuration instance
     * cannot be passed to another writer.
     *
     * @param d    the index directory. The index is either created or appended  指定本次索引文件将要存储的目录
     *             according <code>conf.getOpenMode()</code>.
     * @param conf the configuration settings according to which IndexWriter should
     *             be initialized.
     * @throws IOException if the directory cannot be read/written to, or if it does not
     *                     exist and <code>conf.getOpenMode()</code> is
     *                     <code>OpenMode.APPEND</code> or if there is any other low-level
     *                     IO error
     */
    public IndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
        enableTestPoints = isEnableTestPoints();
        conf.setIndexWriter(this); // prevent reuse by other instances   避免config被其他对象修改
        config = conf;
        infoStream = config.getInfoStream();
        // 代表config中指明了软删除的字段
        softDeletesEnabled = config.getSoftDeletesField() != null;
        // obtain the write.lock. If the user configured a timeout,
        // we wrap with a sleeper and this might take some time.
        // 通过尝试抢占文件的  fileLock 确保一次只有一个进程往该目录写入索引文件
        // 在抢占锁失败时 会直接抛出异常
        writeLock = d.obtainLock(WRITE_LOCK_NAME);

        boolean success = false;
        try {
            directoryOrig = d;
            // 这里为 目录加了一层包装器  这样在每次操作这个目录时
            // 如果 都会先校验当前锁是否还有效 (因为操作系统级别的文件锁是不可控的 java程序不能确保锁始终有效)
            directory = new LockValidatingDirectoryWrapper(d, writeLock);
            // 默认实现是 ConcurrentMergeScheduler
            mergeScheduler = config.getMergeScheduler();
            // 这里根据环境 设置 merge的动态配置
            mergeScheduler.initialize(infoStream, directoryOrig);
            OpenMode mode = config.getOpenMode();
            final boolean indexExists;  // 代表当前目录下是否已经存在段文件
            final boolean create;   // 代表是否需要创建段文件
            // 当模式是Create时 还需要检测segment_N文件是否已经创建
            if (mode == OpenMode.CREATE) {
                indexExists = DirectoryReader.indexExists(directory);
                create = true;
            // 如果是追加模式则要求segment_N 必须存在
            } else if (mode == OpenMode.APPEND) {
                indexExists = true;
                create = false;
            } else {
                // CREATE_OR_APPEND - create only if an index does not exist
                // 在CREATE_OR_APPEND模式下 如果不存在则创建
                indexExists = DirectoryReader.indexExists(directory);
                create = !indexExists;
            }

            // If index is too old, reading the segments will throw
            // IndexFormatTooOldException.

            String[] files = directory.listAll();

            // Set up our initial SegmentInfos:
            // 在append模式下才会设置
            IndexCommit commit = config.getIndexCommit();

            // Set up our initial SegmentInfos:
            StandardDirectoryReader reader;
            if (commit == null) {
                reader = null;
            } else {
                // 默认情况下 ReaderCommit内部的reader是null
                reader = commit.getReader();
            }

            if (create) {

                // 如果是创建模式 无法使用indexCommit
                if (config.getIndexCommit() != null) {
                    // We cannot both open from a commit point and create:
                    if (mode == OpenMode.CREATE) {
                        throw new IllegalArgumentException("cannot use IndexWriterConfig.setIndexCommit() with OpenMode.CREATE");
                    } else {
                        throw new IllegalArgumentException("cannot use IndexWriterConfig.setIndexCommit() when index has no commit");
                    }
                }

                // Try to read first.  This is to allow create
                // against an index that's currently open for
                // searching.  In this case we write the next
                // segments_N file with no segments:
                // 开始创建段信息对象了  这里指定了索引的版本号  主要用于判断兼容性
                final SegmentInfos sis = new SegmentInfos(config.getIndexCreatedVersionMajor());
                // 如果之前的段文件已经存在
                if (indexExists) {
                    // 读取上次使用IndexWriter时最后写入的 segment_N 文件
                    final SegmentInfos previous = SegmentInfos.readLatestCommit(directory);
                    // 更新gen的初始值  这样当发生变动时 以当前gen为基准增加
                    sis.updateGenerationVersionAndCounter(previous);
                }
                // 如果之前没有segment_N 文件 那么此时只有一个空的segmentInfos对象
                segmentInfos = sis;
                // 触发rollback时 使用的segment
                rollbackSegments = segmentInfos.createBackupSegmentInfos();

                // Record that we have a change (zero out all
                // segments) pending:
                // 标记 infos发生了变化
                changed();

                // 在append模式下 如果 commit中携带了 reader对象
            } else if (reader != null) {
                // Init from an existing already opened NRT or non-NRT reader:

                if (reader.directory() != commit.getDirectory()) {
                    throw new IllegalArgumentException("IndexCommit's reader must have the same directory as the IndexCommit");
                }

                if (reader.directory() != directoryOrig) {
                    throw new IllegalArgumentException("IndexCommit's reader must have the same directory passed to IndexWriter");
                }

                if (reader.segmentInfos.getLastGeneration() == 0) {
                    // TODO: maybe we could allow this?  It's tricky...
                    throw new IllegalArgumentException("index must already have an initial commit to open from reader");
                }

                // Must clone because we don't want the incoming NRT reader to "see" any changes this writer now makes:
                segmentInfos = reader.segmentInfos.clone();

                SegmentInfos lastCommit;
                try {
                    lastCommit = SegmentInfos.readCommit(directoryOrig, segmentInfos.getSegmentsFileName());
                } catch (IOException ioe) {
                    throw new IllegalArgumentException("the provided reader is stale: its prior commit file \"" + segmentInfos.getSegmentsFileName() + "\" is missing from index");
                }

                if (reader.writer != null) {

                    // The old writer better be closed (we have the write lock now!):
                    assert reader.writer.closed;

                    // In case the old writer wrote further segments (which we are now dropping),
                    // update SIS metadata so we remain write-once:
                    segmentInfos.updateGenerationVersionAndCounter(reader.writer.segmentInfos);
                    lastCommit.updateGenerationVersionAndCounter(reader.writer.segmentInfos);
                }

                rollbackSegments = lastCommit.createBackupSegmentInfos();

            } else {
                // 代表处于 append模式下 并且在commit中没有携带reader
                // Init from either the latest commit point, or an explicit prior commit point:
                String lastSegmentsFile = SegmentInfos.getLastCommitSegmentsFileName(files);
                if (lastSegmentsFile == null) {
                    throw new IndexNotFoundException("no segments* file found in " + directory + ": files: " + Arrays.toString(files));
                }

                // Do not use SegmentInfos.read(Directory) since the spooky
                // retrying it does is not necessary here (we hold the write lock):
                // 读取最新的 segment_N 的数据
                segmentInfos = SegmentInfos.readCommit(directoryOrig, lastSegmentsFile);

                // 如果传入了commit则使用它的segmentInfos进行初始化
                if (commit != null) {
                    // Swap out all segments, but, keep metadata in
                    // SegmentInfos, like version & generation, to
                    // preserve write-once.  This is important if
                    // readers are open against the future commit
                    // points.
                    if (commit.getDirectory() != directoryOrig) {
                        throw new IllegalArgumentException("IndexCommit's directory doesn't match my directory, expected=" + directoryOrig + ", got=" + commit.getDirectory());
                    }

                    SegmentInfos oldInfos = SegmentInfos.readCommit(directoryOrig, commit.getSegmentsFileName());
                    segmentInfos.replace(oldInfos);
                    changed();

                    if (infoStream.isEnabled("IW")) {
                        infoStream.message("IW", "init: loaded commit \"" + commit.getSegmentsFileName() + "\"");
                    }
                }

                // 拷贝回滚的数据副本
                rollbackSegments = segmentInfos.createBackupSegmentInfos();
            }

            // 上面已经完成了 segmentInfos 的数据读取
            commitUserData = new HashMap<>(segmentInfos.getUserData()).entrySet();

            pendingNumDocs.set(segmentInfos.totalMaxDoc());

            // start with previous field numbers, but new FieldInfos
            // NOTE: this is correct even for an NRT reader because we'll pull FieldInfos even for the un-committed segments:
            // 使用segmentInfos中的field信息 还原globalFieldNumberMap
            globalFieldNumberMap = getFieldNumberMap();

            // 如果在conf中设置了排序对象 那么所有段下都必须设置排序对象 并且排序对象必须一致
            validateIndexSort();

            // 通过配置项来初始化 刷盘策略   默认的刷盘策略类为 FlushByRamOrCountsPolicy
            config.getFlushPolicy().init(config);
            bufferedUpdatesStream = new BufferedUpdatesStream(infoStream);
            // 该对象负责解析 doc 并将结果写入到各个索引文件中
            docWriter = new DocumentsWriter(flushNotifications, segmentInfos.getIndexCreatedVersionMajor(), pendingNumDocs,
                    enableTestPoints, this::newSegmentName,
                    config, directoryOrig, directory, globalFieldNumberMap);
            readerPool = new ReaderPool(directory, directoryOrig, segmentInfos, globalFieldNumberMap,
                    bufferedUpdatesStream::getCompletedDelGen, infoStream, conf.getSoftDeletesField(), reader);
            // 支持池化的情况下 提前开启池化
            if (config.getReaderPooling()) {
                readerPool.enableReaderPooling();
            }
            // Default deleter (for backwards compatibility) is
            // KeepOnlyLastCommitDeleter:

            // Sync'd is silly here, but IFD asserts we sync'd on the IW instance:
            // 创建删除索引文件的对象
            synchronized (this) {
                deleter = new IndexFileDeleter(files, directoryOrig, directory,
                        config.getIndexDeletionPolicy(),
                        segmentInfos, infoStream, this,
                        indexExists, reader != null);

                // We incRef all files when we return an NRT reader from IW, so all files must exist even in the NRT case:
                assert create || filesExist(segmentInfos);
            }

            // 代表启动时指定的segment 已经被设置成deleted了
            if (deleter.startingCommitDeleted) {
                // Deletion policy deleted the "head" commit point.
                // We have to mark ourself as changed so that if we
                // are closed w/o any further changes we write a new
                // segments_N file.
                changed();
            }

            if (reader != null) {
                // We always assume we are carrying over incoming changes when opening from reader:
                segmentInfos.changed();
                changed();
            }

            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "init: create=" + create + " reader=" + reader);
                messageState();
            }

            success = true;

        } finally {
            if (!success) {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "init: hit exception on init; releasing write lock");
                }
                IOUtils.closeWhileHandlingException(writeLock);
                writeLock = null;
            }
        }
    }

    /**
     * Confirms that the incoming index sort (if any) matches the existing index sort (if any).
     */
    private void validateIndexSort() {
        // 如果在conf中指定了排序对象  必须要求所有段都使用该排序对象
        Sort indexSort = config.getIndexSort();
        if (indexSort != null) {
            for (SegmentCommitInfo info : segmentInfos) {
                Sort segmentIndexSort = info.info.getIndexSort();
                if (segmentIndexSort == null || isCongruentSort(indexSort, segmentIndexSort) == false) {
                    throw new IllegalArgumentException("cannot change previous indexSort=" + segmentIndexSort + " (from segment=" + info + ") to new indexSort=" + indexSort);
                }
            }
        }
    }

    /**
     * Returns true if <code>indexSort</code> is a prefix of <code>otherSort</code>.
     **/
    static boolean isCongruentSort(Sort indexSort, Sort otherSort) {
        final SortField[] fields1 = indexSort.getSort();
        final SortField[] fields2 = otherSort.getSort();
        if (fields1.length > fields2.length) {
            return false;
        }
        return Arrays.asList(fields1).equals(Arrays.asList(fields2).subList(0, fields1.length));
    }

    // reads latest field infos for the commit
    // this is used on IW init and addIndexes(Dir) to create/update the global field map.
    // 读取某个 segment 下所有的field 信息
    static FieldInfos readFieldInfos(SegmentCommitInfo si) throws IOException {
        Codec codec = si.info.getCodec();
        FieldInfosFormat reader = codec.fieldInfosFormat();

        // 段文件中会存储 fieldInfs 的 gen 这样才可以定位到索引文件
        if (si.hasFieldUpdates()) {
            // there are updates, we read latest (always outside of CFS)
            final String segmentSuffix = Long.toString(si.getFieldInfosGen(), Character.MAX_RADIX);
            return reader.read(si.info.dir, si.info, segmentSuffix, IOContext.READONCE);
        } else if (si.info.getUseCompoundFile()) {
            // cfs
            try (Directory cfs = codec.compoundFormat().getCompoundReader(si.info.dir, si.info, IOContext.DEFAULT)) {
                return reader.read(cfs, si.info, "", IOContext.READONCE);
            }
        } else {
            // no cfs
            // 没有标明gen信息   直接按照 segmentName.extend  的文件名格式读取数据
            return reader.read(si.info.dir, si.info, "", IOContext.READONCE);
        }
    }

    /**
     * Loads or returns the already loaded the global field number map for this {@link SegmentInfos}.
     * If this {@link SegmentInfos} has no global field number map the returned instance is empty
     */
    private FieldNumbers getFieldNumberMap() throws IOException {
        // 创建一个 维护了 fieldName 与 fieldNum 的映射容器
        final FieldNumbers map = new FieldNumbers(config.softDeletesField);

        // 读取某个 segment 下所有的field 信息  如果本次创建了一个新的infos 那么内部还没有任何 segmentCommitInfo信息 这里会返回一个空对象
        for (SegmentCommitInfo info : segmentInfos) {
            // 找到每个段下的所有 field 信息
            FieldInfos fis = readFieldInfos(info);
            for (FieldInfo fi : fis) {
                // 添加映射关系  该容器同时确保了 同名field 下的各个标识一定是一样的
                map.addOrGet(fi.name, fi.number, fi.getIndexOptions(), fi.getDocValuesType(), fi.getPointDimensionCount(), fi.getPointIndexDimensionCount(), fi.getPointNumBytes(), fi.isSoftDeletesField());
            }
        }

        return map;
    }

    /**
     * Returns a {@link LiveIndexWriterConfig}, which can be used to query the IndexWriter
     * current settings, as well as modify "live" ones.
     */
    public LiveIndexWriterConfig getConfig() {
        ensureOpen(false);
        return config;
    }

    private void messageState() {
        if (infoStream.isEnabled("IW") && didMessageState == false) {
            didMessageState = true;
            infoStream.message("IW", "\ndir=" + directoryOrig + "\n" +
                    "index=" + segString() + "\n" +
                    "version=" + Version.LATEST.toString() + "\n" +
                    config.toString());
            final StringBuilder unmapInfo = new StringBuilder(Boolean.toString(MMapDirectory.UNMAP_SUPPORTED));
            if (!MMapDirectory.UNMAP_SUPPORTED) {
                unmapInfo.append(" (").append(MMapDirectory.UNMAP_NOT_SUPPORTED_REASON).append(")");
            }
            infoStream.message("IW", "MMapDirectory.UNMAP_SUPPORTED=" + unmapInfo);
        }
    }

    /**
     * Gracefully closes (commits, waits for merges), but calls rollback
     * if there's an exc so the IndexWriter is always closed.  This is called
     * from {@link #close} when {@link IndexWriterConfig#commitOnClose} is
     * {@code true}.
     * 调用close时 触发
     */
    private void shutdown() throws IOException {
        if (pendingCommit != null) {
            throw new IllegalStateException("cannot close: prepareCommit was already called with no corresponding call to commit");
        }
        // Ensure that only one thread actually gets to do the
        // closing
        if (shouldClose(true)) {
            try {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "now flush at close");
                }

                // 将解析的所有数据刷盘
                flush(true, true);
                // 等待merge线程处理完数据
                waitForMerges();
                // 此时才进行提交动作 基于最新的 segment 生成 segment_N 文件
                commitInternal(config.getMergePolicy());
                // 这里主要就是为了设置成close 并关闭相关的类 因为数据在前面已经保存了
                rollbackInternal(); // ie close, since we just committed
            } catch (Throwable t) {
                // Be certain to close the index on any exception
                try {
                    rollbackInternal();
                } catch (Throwable t1) {
                    t.addSuppressed(t1);
                }
                throw t;
            }
        }
    }

    /**
     * Closes all open resources and releases the write lock.
     * <p>
     * If {@link IndexWriterConfig#commitOnClose} is <code>true</code>,
     * this will attempt to gracefully shut down by writing any
     * changes, waiting for any running merges, committing, and closing.
     * In this case, note that:
     * <ul>
     * <li>If you called prepareCommit but failed to call commit, this
     * method will throw {@code IllegalStateException} and the {@code IndexWriter}
     * will not be closed.</li>
     * <li>If this method throws any other exception, the {@code IndexWriter}
     * will be closed, but changes may have been lost.</li>
     * </ul>
     *
     * <p>
     * Note that this may be a costly
     * operation, so, try to re-use a single writer instead of
     * closing and opening a new one.  See {@link #commit()} for
     * caveats about write caching done by some IO devices.
     *
     * <p><b>NOTE</b>: You must ensure no other threads are still making
     * changes at the same time that this method is invoked.</p>
     */
    @Override
    public void close() throws IOException {
        // 如果还有 prepareCommit的数据未处理 直接调用shutdown会报错 必须调用rollback 进行处理
        if (config.getCommitOnClose()) {
            shutdown();
        } else {
            rollback();
        }
    }

    // Returns true if this thread should attempt to close, or
    // false if IndexWriter is now closed; else,
    // waits until another thread finishes closing
    synchronized private boolean shouldClose(boolean waitForClose) {
        while (true) {
            if (closed == false) {
                if (closing == false) {
                    // We get to close  代表抢占成功 返回true
                    closing = true;
                    return true;
                } else if (waitForClose == false) {
                    return false;
                } else {
                    // Another thread is presently trying to close;
                    // wait until it finishes one way (closes
                    // successfully) or another (fails to close)
                    // 等待其他线程关闭完成
                    doWait();
                }
            } else {
                // 代表被其他线程抢先关闭了
                return false;
            }
        }
    }

    /**
     * Returns the Directory used by this index.
     */
    public Directory getDirectory() {
        // return the original directory the user supplied, unwrapped.
        return directoryOrig;
    }

    @Override
    public InfoStream getInfoStream() {
        return infoStream;
    }

    /**
     * Returns the analyzer used by this index.
     */
    public Analyzer getAnalyzer() {
        ensureOpen();
        return config.getAnalyzer();
    }

    /**
     * If {@link SegmentInfos#getVersion} is below {@code newVersion} then update it to this value.
     *
     * @lucene.internal
     */
    public synchronized void advanceSegmentInfosVersion(long newVersion) {
        ensureOpen();
        if (segmentInfos.getVersion() < newVersion) {
            segmentInfos.setVersion(newVersion);
        }
        changed();
    }

    /**
     * Returns true if this index has deletions (including
     * buffered deletions).  Note that this will return true
     * if there are buffered Term/Query deletions, even if it
     * turns out those buffered deletions don't match any
     * documents.
     */
    public synchronized boolean hasDeletions() {
        ensureOpen();
        if (bufferedUpdatesStream.any()
                || docWriter.anyDeletions()
                || readerPool.anyDeletions()) {
            return true;
        }
        for (final SegmentCommitInfo info : segmentInfos) {
            if (info.hasDeletions()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds a document to this index.
     *
     * <p> Note that if an Exception is hit (for example disk full)
     * then the index will be consistent, but this document
     * may not have been added.  Furthermore, it's possible
     * the index will have one segment in non-compound format
     * even when using compound files (when a merge has
     * partially succeeded).</p>
     *
     * <p> This method periodically flushes pending documents
     * to the Directory (see <a href="#flush">above</a>), and
     * also periodically triggers segment merges in the index
     * according to the {@link MergePolicy} in use.</p>
     *
     * <p>Merges temporarily consume space in the
     * directory. The amount of space required is up to 1X the
     * size of all segments being merged, when no
     * readers/searchers are open against the index, and up to
     * 2X the size of all segments being merged when
     * readers/searchers are open against the index (see
     * {@link #forceMerge(int)} for details). The sequence of
     * primitive merge operations performed is governed by the
     * merge policy.
     *
     * <p>Note that each term in the document can be no longer
     * than {@link #MAX_TERM_LENGTH} in bytes, otherwise an
     * IllegalArgumentException will be thrown.</p>
     *
     * <p>Note that it's possible to create an invalid Unicode
     * string in java if a UTF16 surrogate pair is malformed.
     * In this case, the invalid characters are silently
     * replaced with the Unicode replacement character
     * U+FFFD.</p>
     *
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     *                               将doc通过 writer写入到 索引文件中  doc就体现为一组 支持被索引的field
     */
    public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
        return updateDocument(null, doc);
    }

    /**
     * Atomically adds a block of documents with sequentially
     * assigned document IDs, such that an external reader
     * will see all or none of the documents.
     *
     * <p><b>WARNING</b>: the index does not currently record
     * which documents were added as a block.  Today this is
     * fine, because merging will preserve a block. The order of
     * documents within a segment will be preserved, even when child
     * documents within a block are deleted. Most search features
     * (like result grouping and block joining) require you to
     * mark documents; when these documents are deleted these
     * search features will not work as expected. Obviously adding
     * documents to an existing block will require you the reindex
     * the entire block.
     *
     * <p>However it's possible that in the future Lucene may
     * merge more aggressively re-order documents (for example,
     * perhaps to obtain better index compression), in which case
     * you may need to fully re-index your documents at that time.
     *
     * <p>See {@link #addDocument(Iterable)} for details on
     * index and IndexWriter state after an Exception, and
     * flushing/merging temporary free space requirements.</p>
     *
     * <p><b>NOTE</b>: tools that do offline splitting of an index
     * (for example, IndexSplitter in contrib) or
     * re-sorting of documents (for example, IndexSorter in
     * contrib) are not aware of these atomically added documents
     * and will likely break them up.  Use such tools at your
     * own risk!
     *
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     * @lucene.experimental
     * 批量添加一组 doc
     */
    public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
        return updateDocuments((DocumentsWriterDeleteQueue.Node<?>) null, docs);
    }

    /**
     * Atomically deletes documents matching the provided
     * delTerm and adds a block of documents with sequentially
     * assigned document IDs, such that an external reader
     * will see all or none of the documents.
     * <p>
     * See {@link #addDocuments(Iterable)}.
     *
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     * @lucene.experimental
     * 在插入doc的同时 写入delNode 这样包含该term的所有doc都会被删除  作用范围为所有segment
     */
    public long updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
        return updateDocuments(delTerm == null ? null : DocumentsWriterDeleteQueue.newNode(delTerm), docs);
    }

    /**
     * 更新文档数据  先按照node中的数据 删除文档 之后新增 docs
     *
     * @param delNode
     * @param docs
     * @return
     * @throws IOException
     */
    private long updateDocuments(final DocumentsWriterDeleteQueue.Node<?> delNode, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
        ensureOpen();
        boolean success = false;
        try {
            // 针对 doc的操作 实际上都是委托给docWriter
            final long seqNo = maybeProcessEvents(docWriter.updateDocuments(docs, delNode));
            success = true;
            return seqNo;
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "updateDocuments");
            throw tragedy;
        } finally {
            if (success == false) {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "hit exception updating document");
                }
                maybeCloseOnTragicEvent();
            }
        }
    }

    /**
     * Expert:
     * Atomically updates documents matching the provided
     * term with the given doc-values fields
     * and adds a block of documents with sequentially
     * assigned document IDs, such that an external reader
     * will see all or none of the documents.
     * <p>
     * One use of this API is to retain older versions of
     * documents instead of replacing them. The existing
     * documents can be updated to reflect they are no
     * longer current while atomically adding new documents
     * at the same time.
     * <p>
     * In contrast to {@link #updateDocuments(Term, Iterable)}
     * this method will not delete documents in the index
     * matching the given term but instead update them with
     * the given doc-values fields which can be used as a
     * soft-delete mechanism.
     * <p>
     * See {@link #addDocuments(Iterable)}
     * and {@link #updateDocuments(Term, Iterable)}.
     *
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     * @lucene.experimental
     */
    public long softUpdateDocuments(Term term, Iterable<? extends Iterable<? extends IndexableField>> docs, Field... softDeletes) throws IOException {
        if (term == null) {
            throw new IllegalArgumentException("term must not be null");
        }
        if (softDeletes == null || softDeletes.length == 0) {
            throw new IllegalArgumentException("at least one soft delete must be present");
        }
        return updateDocuments(DocumentsWriterDeleteQueue.newNode(buildDocValuesUpdate(term, softDeletes)), docs);
    }

    /**
     * Expert: attempts to delete by document ID, as long as
     * the provided reader is a near-real-time reader (from {@link
     * DirectoryReader#open(IndexWriter)}).  If the
     * provided reader is an NRT reader obtained from this
     * writer, and its segment has not been merged away, then
     * the delete succeeds and this method returns a valid (&gt; 0) sequence
     * number; else, it returns -1 and the caller must then
     * separately delete by Term or Query.
     *
     * <b>NOTE</b>: this method can only delete documents
     * visible to the currently open NRT reader.  If you need
     * to delete documents indexed after opening the NRT
     * reader you must use {@link #deleteDocuments(Term...)}).
     * <p>
     *     尝试直接通过docId 去删除doc
     */
    public synchronized long tryDeleteDocument(IndexReader readerIn, int docID) throws IOException {
        // NOTE: DON'T use docID inside the closure
        return tryModifyDocument(readerIn, docID, (leafDocId, rld) -> {
            // 这里的删除逻辑比较简单 直接将 pendingDelete中目标doc 标记成删除
            if (rld.delete(leafDocId)) {
                if (isFullyDeleted(rld)) {
                    dropDeletedSegment(rld.info);
                    checkpoint();
                }

                // Must bump changeCount so if no other changes
                // happened, we still commit this change:
                changed();
            }
        });
    }

    /**
     * Expert: attempts to update doc values by document ID, as long as
     * the provided reader is a near-real-time reader (from {@link
     * DirectoryReader#open(IndexWriter)}).  If the
     * provided reader is an NRT reader obtained from this
     * writer, and its segment has not been merged away, then
     * the update succeeds and this method returns a valid (&gt; 0) sequence
     * number; else, it returns -1 and the caller must then
     * either retry the update and resolve the document again.
     * If a doc values fields data is <code>null</code> the existing
     * value is removed from all documents matching the term. This can be used
     * to un-delete a soft-deleted document since this method will apply the
     * field update even if the document is marked as deleted.
     *
     * <b>NOTE</b>: this method can only updates documents
     * visible to the currently open NRT reader.  If you need
     * to update documents indexed after opening the NRT
     * reader you must use {@link #updateDocValues(Term, Field...)}.
     */
    public synchronized long tryUpdateDocValue(IndexReader readerIn, int docID, Field... fields) throws IOException {
        // NOTE: DON'T use docID inside the closure
        // 将field 信息转换成一组 DVupdate对象    在直接指定了docId的场景实际上 term就是无用的信息了  本身term + field 就是用于定位一组doc的
        final DocValuesUpdate[] dvUpdates = buildDocValuesUpdate(null, fields);
        return tryModifyDocument(readerIn, docID, (leafDocId, rld) -> {
            long nextGen = bufferedUpdatesStream.getNextGen();
            try {
                // 将本次更新数据按照 fieldName 进行划分   DocValuesFieldUpdates 存储了一组 doc 以及对应的field.value
                Map<String, DocValuesFieldUpdates> fieldUpdatesMap = new HashMap<>();
                for (DocValuesUpdate update : dvUpdates) {
                    DocValuesFieldUpdates docValuesFieldUpdates = fieldUpdatesMap.computeIfAbsent(update.field, k -> {
                        switch (update.type) {
                            case NUMERIC:
                                return new NumericDocValuesFieldUpdates(nextGen, k, rld.info.info.maxDoc());
                            case BINARY:
                                return new BinaryDocValuesFieldUpdates(nextGen, k, rld.info.info.maxDoc());
                            default:
                                throw new AssertionError("type: " + update.type + " is not supported");
                        }
                    });
                    if (update.hasValue()) {
                        switch (update.type) {
                            // 实际上 leafDocId 就是 docID  这里只会增加一条针对目标doc的更新记录
                            case NUMERIC:
                                docValuesFieldUpdates.add(leafDocId, ((NumericDocValuesUpdate) update).getValue());
                                break;
                            case BINARY:
                                docValuesFieldUpdates.add(leafDocId, ((BinaryDocValuesUpdate) update).getValue());
                                break;
                            default:
                                throw new AssertionError("type: " + update.type + " is not supported");
                        }
                    } else {
                        // 就是添加一条 某个doc hasValue == false的记录
                        docValuesFieldUpdates.reset(leafDocId);
                    }
                }
                for (DocValuesFieldUpdates updates : fieldUpdatesMap.values()) {
                    // 固化更新数据 并设置到 readersAndUpdates中
                    updates.finish();
                    rld.addDVUpdate(updates);
                }
            } finally {
                bufferedUpdatesStream.finishedSegment(nextGen);
            }
            // Must bump changeCount so if no other changes
            // happened, we still commit this change:
            changed();
        });
    }

    @FunctionalInterface
    private interface DocModifier {
        void run(int docId, ReadersAndUpdates readersAndUpdates) throws IOException;
    }

    /**
     * 通过指定reader对象 和 docId 将目标segment的doc标记成删除
     * @param readerIn
     * @param docID
     * @param toApply  删除的逻辑实现
     * @return
     * @throws IOException
     */
    private synchronized long tryModifyDocument(IndexReader readerIn, int docID, DocModifier toApply) throws IOException {
        final LeafReader reader;
        if (readerIn instanceof LeafReader) {
            // Reader is already atomic: use the incoming docID:
            reader = (LeafReader) readerIn;
        } else {
            // Composite reader: lookup sub-reader and re-base docID:
            // 如果reader组成了一个树结构 通过docId 快速定位到目标reader
            List<LeafReaderContext> leaves = readerIn.leaves();
            int subIndex = ReaderUtil.subIndex(docID, leaves);
            reader = leaves.get(subIndex).reader();
            // 计算相对docId
            docID -= leaves.get(subIndex).docBase;
            assert docID >= 0;
            assert docID < reader.maxDoc();
        }

        if (!(reader instanceof SegmentReader)) {
            throw new IllegalArgumentException("the reader must be a SegmentReader or composite reader containing only SegmentReaders");
        }

        final SegmentCommitInfo info = ((SegmentReader) reader).getOriginalSegmentInfo();

        // TODO: this is a slow linear search, but, number of
        // segments should be contained unless something is
        // seriously wrong w/ the index, so it should be a minor
        // cost:

        // 必须确保该segment 处于被IndexWriter维护的状态
        if (segmentInfos.indexOf(info) != -1) {
            ReadersAndUpdates rld = getPooledInstance(info, false);
            if (rld != null) {
                synchronized (bufferedUpdatesStream) {
                    toApply.run(docID, rld);
                    return docWriter.getNextSequenceNumber();
                }
            }
        }
        return -1;
    }

    /**
     * Drops a segment that has 100% deleted documents.
     * @param info  该段下所有doc都已经被 删除 可以抛弃这个段
     */
    private synchronized void dropDeletedSegment(SegmentCommitInfo info) throws IOException {
        // If a merge has already registered for this
        // segment, we leave it in the readerPool; the
        // merge will skip merging it and will then drop
        // it once it's done:
        // 如果当前 段正在被合并中 那么不处理他  (在合并过程中doc 应该就会被删除)
        if (mergingSegments.contains(info) == false) {
            // it's possible that we invoke this method more than once for the same SCI
            // we must only remove the docs once!
            boolean dropPendingDocs = segmentInfos.remove(info);
            try {
                // this is sneaky - we might hit an exception while dropping a reader but then we have already
                // removed the segment for the segmentInfo and we lost the pendingDocs update due to that.
                // therefore we execute the adjustPendingNumDocs in a finally block to account for that.
                // 释放该段相关的索引文件输入流
                dropPendingDocs |= readerPool.drop(info);
            } finally {
                // 当成功移除掉segment 时  调整当前待处理的doc数量
                if (dropPendingDocs) {
                    adjustPendingNumDocs(-info.info.maxDoc());
                }
            }
        }
    }

    /**
     * Deletes the document(s) containing any of the
     * terms. All given deletes are applied and flushed atomically
     * at the same time.
     *
     * @param terms array of terms to identify the documents
     *              to be deleted
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     *
     */
    public long deleteDocuments(Term... terms) throws IOException {
        ensureOpen();
        try {
            // 就是将所有term 包装成 termNode 并设置到 deleteQueue中  在 flushPolicy中会检测此时 deleteQueue占用的内存是否过高 是的话会生成一个ticket 并暴露到IndexWriter中
            // 在maybeProcessEvents 中就会处理之前设置的 ticket 将term命中的doc在 pendingDelete的位图中标记为false
            return maybeProcessEvents(docWriter.deleteTerms(terms));
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "deleteDocuments(Term..)");
            throw tragedy;
        }
    }

    /**
     * Deletes the document(s) matching any of the provided queries.
     * All given deletes are applied and flushed atomically at the same time.
     *
     * @param queries array of queries to identify the documents
     *                to be deleted
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     *                               删除命中查询条件的doc
     */
    public long deleteDocuments(Query... queries) throws IOException {
        ensureOpen();

        // LUCENE-6379: Specialize MatchAllDocsQuery
        for (Query query : queries) {
            // 只要存在一个 AllDocs 就删除所有的doc
            if (query.getClass() == MatchAllDocsQuery.class) {
                return deleteAll();
            }
        }

        try {
            return maybeProcessEvents(docWriter.deleteQueries(queries));
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "deleteDocuments(Query..)");
            throw tragedy;
        }
    }

    /**
     * Updates a document by first deleting the document(s)
     * containing <code>term</code> and then adding the new
     * document.  The delete and then add are atomic as seen
     * by a reader on the same index (flush may happen only after
     * the add).
     *
     * @param term the term to identify the document(s) to be
     *             deleted   代表包含该term的doc在稍后都会被删除
     * @param doc  the document to be added   本次添加的doc
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     * 包含term的文档将会被删除 同时doc内部的所有 field 会被写入到索引文件
     */
    public long updateDocument(Term term, Iterable<? extends IndexableField> doc) throws IOException {
        // 如果存在 term的情况 将term 包装成一个 termNode 该node被调用时 会将自己存储到一个 BufferedUpdates 结构中
        return updateDocuments(term == null ? null : DocumentsWriterDeleteQueue.newNode(term), List.of(doc));
    }

    /**
     * Expert:
     * Updates a document by first updating the document(s)
     * containing <code>term</code> with the given doc-values fields
     * and then adding the new document.  The doc-values update and
     * then add are atomic as seen by a reader on the same index
     * (flush may happen only after the add).
     * <p>
     * One use of this API is to retain older versions of
     * documents instead of replacing them. The existing
     * documents can be updated to reflect they are no
     * longer current while atomically adding new documents
     * at the same time.
     * <p>
     * In contrast to {@link #updateDocument(Term, Iterable)}
     * this method will not delete documents in the index
     * matching the given term but instead update them with
     * the given doc-values fields which can be used as a
     * soft-delete mechanism.
     * <p>
     * See {@link #addDocuments(Iterable)}
     * and {@link #updateDocuments(Term, Iterable)}.
     *
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     * @lucene.experimental
     */
    public long softUpdateDocument(Term term, Iterable<? extends IndexableField> doc, Field... softDeletes) throws IOException {
        if (term == null) {
            throw new IllegalArgumentException("term must not be null");
        }
        if (softDeletes == null || softDeletes.length == 0) {
            throw new IllegalArgumentException("at least one soft delete must be present");
        }
        return updateDocuments(DocumentsWriterDeleteQueue.newNode(buildDocValuesUpdate(term, softDeletes)), List.of(doc));
    }


    /**
     * Updates a document's {@link NumericDocValues} for <code>field</code> to the
     * given <code>value</code>. You can only update fields that already exist in
     * the index, not add new fields through this method.
     *
     * @param term  the term to identify the document(s) to be updated
     * @param field field name of the {@link NumericDocValues} field
     * @param value new value for the field
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     */
    public long updateNumericDocValue(Term term, String field, long value) throws IOException {
        ensureOpen();
        if (!globalFieldNumberMap.contains(field, DocValuesType.NUMERIC)) {
            throw new IllegalArgumentException("can only update existing numeric-docvalues fields!");
        }
        if (config.getIndexSortFields().contains(field)) {
            throw new IllegalArgumentException("cannot update docvalues field involved in the index sort, field=" + field + ", sort=" + config.getIndexSort());
        }
        try {
            return maybeProcessEvents(docWriter.updateDocValues(new NumericDocValuesUpdate(term, field, value)));
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "updateNumericDocValue");
            throw tragedy;
        }
    }

    /**
     * Updates a document's {@link BinaryDocValues} for <code>field</code> to the
     * given <code>value</code>. You can only update fields that already exist in
     * the index, not add new fields through this method.
     *
     * <p>
     * <b>NOTE:</b> this method currently replaces the existing value of all
     * affected documents with the new value.
     *
     * @param term  the term to identify the document(s) to be updated
     * @param field field name of the {@link BinaryDocValues} field
     * @param value new value for the field
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     */
    public long updateBinaryDocValue(Term term, String field, BytesRef value) throws IOException {
        ensureOpen();
        if (value == null) {
            throw new IllegalArgumentException("cannot update a field to a null value: " + field);
        }
        if (!globalFieldNumberMap.contains(field, DocValuesType.BINARY)) {
            throw new IllegalArgumentException("can only update existing binary-docvalues fields!");
        }
        try {
            return maybeProcessEvents(docWriter.updateDocValues(new BinaryDocValuesUpdate(term, field, value)));
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "updateBinaryDocValue");
            throw tragedy;
        }
    }

    /**
     * Updates documents' DocValues fields to the given values. Each field update
     * is applied to the set of documents that are associated with the
     * {@link Term} to the same value. All updates are atomically applied and
     * flushed together. If a doc values fields data is <code>null</code> the existing
     * value is removed from all documents matching the term.
     *
     * @param updates the updates to apply
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     */
    public long updateDocValues(Term term, Field... updates) throws IOException {
        ensureOpen();
        DocValuesUpdate[] dvUpdates = buildDocValuesUpdate(term, updates);
        try {
            return maybeProcessEvents(docWriter.updateDocValues(dvUpdates));
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "updateDocValues");
            throw tragedy;
        }
    }

    /**
     * 根据field信息 转换成一组 docValueUpdate对象
     * @param term
     * @param updates
     * @return
     */
    private DocValuesUpdate[] buildDocValuesUpdate(Term term, Field[] updates) {
        DocValuesUpdate[] dvUpdates = new DocValuesUpdate[updates.length];
        for (int i = 0; i < updates.length; i++) {
            final Field f = updates[i];
            // 获取该field对应数值类型
            final DocValuesType dvType = f.fieldType().docValuesType();
            if (dvType == null) {
                throw new NullPointerException("DocValuesType must not be null (field: \"" + f.name() + "\")");
            }
            if (dvType == DocValuesType.NONE) {
                throw new IllegalArgumentException("can only update NUMERIC or BINARY fields! field=" + f.name());
            }

            // 本次使用的field 必须 name 和dvType都命中
            if (globalFieldNumberMap.contains(f.name(), dvType) == false) {
                // if this field doesn't exists we try to add it. if it exists and the DV type doesn't match we
                // get a consistent error message as if you try to do that during an indexing operation.
                // 在这个方法中 如果field 原来的 DV type类型 不是 NONE 则会抛出异常  相当于就是一种检测机制
                globalFieldNumberMap.addOrGet(f.name(), -1, IndexOptions.NONE, dvType, 0, 0, 0, f.name().equals(config.softDeletesField));
                assert globalFieldNumberMap.contains(f.name(), dvType);
            }
            // 涉及到排序相关 field 不允许被更新
            if (config.getIndexSortFields().contains(f.name())) {
                throw new IllegalArgumentException("cannot update docvalues field involved in the index sort, field=" + f.name() + ", sort=" + config.getIndexSort());
            }

            switch (dvType) {
                case NUMERIC:
                    Long value = (Long) f.numericValue();
                    dvUpdates[i] = new NumericDocValuesUpdate(term, f.name(), value);
                    break;
                case BINARY:
                    dvUpdates[i] = new BinaryDocValuesUpdate(term, f.name(), f.binaryValue());
                    break;
                default:
                    throw new IllegalArgumentException("can only update NUMERIC or BINARY fields: field=" + f.name() + ", type=" + dvType);
            }
        }
        return dvUpdates;
    }

    // for test purpose
    final synchronized int getSegmentCount() {
        return segmentInfos.size();
    }

    // for test purpose
    final synchronized int getNumBufferedDocuments() {
        return docWriter.getNumDocs();
    }

    // for test purpose
    final synchronized int maxDoc(int i) {
        if (i >= 0 && i < segmentInfos.size()) {
            return segmentInfos.info(i).info.maxDoc();
        } else {
            return -1;
        }
    }

    // for test purpose
    final int getFlushCount() {
        return flushCount.get();
    }

    // for test purpose
    final int getFlushDeletesCount() {
        return flushDeletesCount.get();
    }

    /**
     * 每次生成唯一的 segment名字
     *
     * @return
     */
    private final String newSegmentName() {
        // Cannot synchronize on IndexWriter because that causes
        // deadlock
        synchronized (segmentInfos) {
            // Important to increment changeCount so that the
            // segmentInfos is written on close.  Otherwise we
            // could close, re-open and re-return the same segment
            // name that was previously returned which can cause
            // problems at least with ConcurrentMergeScheduler.
            changeCount.incrementAndGet();
            segmentInfos.changed();
            return "_" + Long.toString(segmentInfos.counter++, Character.MAX_RADIX);
        }
    }

    /**
     * If enabled, information about merges will be printed to this.
     */
    private final InfoStream infoStream;

    /**
     * Forces merge policy to merge segments until there are
     * {@code <= maxNumSegments}.  The actual merges to be
     * executed are determined by the {@link MergePolicy}.
     *
     * <p>This is a horribly costly operation, especially when
     * you pass a small {@code maxNumSegments}; usually you
     * should only call this if the index is static (will no
     * longer be changed).</p>
     *
     * <p>Note that this requires free space that is proportional
     * to the size of the index in your Directory: 2X if you are
     * not using compound file format, and 3X if you are.
     * For example, if your index size is 10 MB then you need
     * an additional 20 MB free for this to complete (30 MB if
     * you're using compound file format). This is also affected
     * by the {@link Codec} that is used to execute the merge,
     * and may result in even a bigger index. Also, it's best
     * to call {@link #commit()} afterwards, to allow IndexWriter
     * to free up disk space.</p>
     *
     * <p>If some but not all readers re-open while merging
     * is underway, this will cause {@code > 2X} temporary
     * space to be consumed as those new readers will then
     * hold open the temporary segments at that time.  It is
     * best not to re-open readers while merging is running.</p>
     *
     * <p>The actual temporary usage could be much less than
     * these figures (it depends on many factors).</p>
     *
     * <p>In general, once this completes, the total size of the
     * index will be less than the size of the starting index.
     * It could be quite a bit smaller (if there were many
     * pending deletes) or just slightly smaller.</p>
     *
     * <p>If an Exception is hit, for example
     * due to disk full, the index will not be corrupted and no
     * documents will be lost.  However, it may have
     * been partially merged (some segments were merged but
     * not all), and it's possible that one of the segments in
     * the index will be in non-compound format even when
     * using compound file format.  This will occur when the
     * Exception is hit during conversion of the segment into
     * compound format.</p>
     *
     * <p>This call will merge those segments present in
     * the index when the call started.  If other threads are
     * still adding documents and flushing segments, those
     * newly created segments will not be merged unless you
     * call forceMerge again.</p>
     *
     * @param maxNumSegments maximum number of segments left
     *                       in the index after merging finishes
     * @throws CorruptIndexException if the index is corrupt
     * @throws IOException           if there is a low-level IO error
     * @see MergePolicy#findMerges
     */
    public void forceMerge(int maxNumSegments) throws IOException {
        forceMerge(maxNumSegments, true);
    }

    /**
     * Just like {@link #forceMerge(int)}, except you can
     * specify whether the call should block until
     * all merging completes.  This is only meaningful with a
     * {@link MergeScheduler} that is able to run merges in
     * background threads.
     * 指定一个segment数量 并将他们尽可能的合并
     */
    public void forceMerge(int maxNumSegments, boolean doWait) throws IOException {
        ensureOpen();

        if (maxNumSegments < 1) {
            throw new IllegalArgumentException("maxNumSegments must be >= 1; got " + maxNumSegments);
        }

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "forceMerge: index now " + segString());
            infoStream.message("IW", "now flush at forceMerge");
        }
        flush(true, true);
        synchronized (this) {
            resetMergeExceptions();
            segmentsToMerge.clear();
            for (SegmentCommitInfo info : segmentInfos) {
                assert info != null;
                segmentsToMerge.put(info, Boolean.TRUE);
            }
            mergeMaxNumSegments = maxNumSegments;

            // Now mark all pending & running merges for forced
            // merge:
            for (final MergePolicy.OneMerge merge : pendingMerges) {
                merge.maxNumSegments = maxNumSegments;
                if (merge.info != null) {
                    // this can be null since we register the merge under lock before we then do the actual merge and
                    // set the merge.info in _mergeInit
                    segmentsToMerge.put(merge.info, Boolean.TRUE);
                }
            }

            for (final MergePolicy.OneMerge merge : runningMerges) {
                merge.maxNumSegments = maxNumSegments;
                if (merge.info != null) {
                    // this can be null since we put the merge on runningMerges before we do the actual merge and
                    // set the merge.info in _mergeInit
                    segmentsToMerge.put(merge.info, Boolean.TRUE);
                }
            }
        }

        maybeMerge(config.getMergePolicy(), MergeTrigger.EXPLICIT, maxNumSegments);

        if (doWait) {
            synchronized (this) {
                while (true) {
                    if (tragedy.get() != null) {
                        throw new IllegalStateException("this writer hit an unrecoverable error; cannot complete forceMerge", tragedy.get());
                    }

                    if (mergeExceptions.size() > 0) {
                        // Forward any exceptions in background merge
                        // threads to the current thread:
                        final int size = mergeExceptions.size();
                        for (int i = 0; i < size; i++) {
                            final MergePolicy.OneMerge merge = mergeExceptions.get(i);
                            if (merge.maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS) {
                                throw new IOException("background merge hit exception: " + merge.segString(), merge.getException());
                            }
                        }
                    }

                    if (maxNumSegmentsMergesPending()) {
                        testPoint("forceMergeBeforeWait");
                        doWait();
                    } else {
                        break;
                    }
                }
            }

            // If close is called while we are still
            // running, throw an exception so the calling
            // thread will know merging did not
            // complete
            ensureOpen();
        }
        // NOTE: in the ConcurrentMergeScheduler case, when
        // doWait is false, we can return immediately while
        // background threads accomplish the merging
    }

    /**
     * Returns true if any merges in pendingMerges or
     * runningMerges are maxNumSegments merges.
     */
    private synchronized boolean maxNumSegmentsMergesPending() {
        for (final MergePolicy.OneMerge merge : pendingMerges) {
            if (merge.maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS)
                return true;
        }

        for (final MergePolicy.OneMerge merge : runningMerges) {
            if (merge.maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS)
                return true;
        }

        return false;
    }

    /**
     * Just like {@link #forceMergeDeletes()}, except you can
     * specify whether the call should block until the
     * operation completes.  This is only meaningful with a
     * {@link MergeScheduler} that is able to run merges in
     * background threads.
     */
    public void forceMergeDeletes(boolean doWait)
            throws IOException {
        ensureOpen();

        flush(true, true);

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "forceMergeDeletes: index now " + segString());
        }

        final MergePolicy mergePolicy = config.getMergePolicy();
        MergePolicy.MergeSpecification spec;
        boolean newMergesFound = false;
        synchronized (this) {
            spec = mergePolicy.findForcedDeletesMerges(segmentInfos, this);
            newMergesFound = spec != null;
            if (newMergesFound) {
                final int numMerges = spec.merges.size();
                for (int i = 0; i < numMerges; i++)
                    registerMerge(spec.merges.get(i));
            }
        }

        mergeScheduler.merge(mergeSource, MergeTrigger.EXPLICIT);

        if (spec != null && doWait) {
            final int numMerges = spec.merges.size();
            synchronized (this) {
                boolean running = true;
                while (running) {

                    if (tragedy.get() != null) {
                        throw new IllegalStateException("this writer hit an unrecoverable error; cannot complete forceMergeDeletes", tragedy.get());
                    }

                    // Check each merge that MergePolicy asked us to
                    // do, to see if any of them are still running and
                    // if any of them have hit an exception.
                    running = false;
                    for (int i = 0; i < numMerges; i++) {
                        final MergePolicy.OneMerge merge = spec.merges.get(i);
                        if (pendingMerges.contains(merge) || runningMerges.contains(merge)) {
                            running = true;
                        }
                        Throwable t = merge.getException();
                        if (t != null) {
                            throw new IOException("background merge hit exception: " + merge.segString(), t);
                        }
                    }

                    // If any of our merges are still running, wait:
                    if (running)
                        doWait();
                }
            }
        }

        // NOTE: in the ConcurrentMergeScheduler case, when
        // doWait is false, we can return immediately while
        // background threads accomplish the merging
    }


    /**
     * Forces merging of all segments that have deleted
     * documents.  The actual merges to be executed are
     * determined by the {@link MergePolicy}.  For example,
     * the default {@link TieredMergePolicy} will only
     * pick a segment if the percentage of
     * deleted docs is over 10%.
     *
     * <p>This is often a horribly costly operation; rarely
     * is it warranted.</p>
     *
     * <p>To see how
     * many deletions you have pending in your index, call
     * {@link IndexReader#numDeletedDocs}.</p>
     *
     * <p><b>NOTE</b>: this method first flushes a new
     * segment (if there are indexed documents), and applies
     * all buffered deletes.
     */
    public void forceMergeDeletes() throws IOException {
        forceMergeDeletes(true);
    }

    /**
     * Expert: asks the mergePolicy whether any merges are
     * necessary now and if so, runs the requested merges and
     * then iterate (test again if merges are needed) until no
     * more merges are returned by the mergePolicy.
     * <p>
     * Explicit calls to maybeMerge() are usually not
     * necessary. The most common case is when merge policy
     * parameters have changed.
     * <p>
     * This method will call the {@link MergePolicy} with
     * {@link MergeTrigger#EXPLICIT}.
     */
    public final void maybeMerge() throws IOException {
        maybeMerge(config.getMergePolicy(), MergeTrigger.EXPLICIT, UNBOUNDED_MAX_MERGE_SEGMENTS);
    }

    /**
     * 检测是否需要对 segment 进行合并
     * @param mergePolicy  使用的merge策略 默认是 TieredMergePolicy
     * @param trigger  触发merge的原因  比如由于手动调用flush merge触发就是 FULL_FLUSH
     * @param maxNumSegments   最多允许多少segment  如果是-1代表没有数量限制
     * @throws IOException
     */
    private final void maybeMerge(MergePolicy mergePolicy, MergeTrigger trigger, int maxNumSegments) throws IOException {
        ensureOpen(false);

        // 此时找到了新的 需要merge的segment
        if (updatePendingMerges(mergePolicy, trigger, maxNumSegments)) {
            // 使用本线程触发merge任务 并阻塞直到merge处理完  实现则是转发到 IndexWriterMergeSource.merge(merge)
            mergeScheduler.merge(mergeSource, trigger);
        }
    }

    /**
     * @param mergePolicy  使用的merge策略 默认是 TieredMergePolicy
     *
     * @param trigger  触发原因
     * @param maxNumSegments   本次merge的数量上限
     * @return
     * @throws IOException
     */
    private synchronized boolean updatePendingMerges(MergePolicy mergePolicy, MergeTrigger trigger, int maxNumSegments)
            throws IOException {

        // In case infoStream was disabled on init, but then enabled at some
        // point, try again to log the config here:
        messageState();

        assert maxNumSegments == UNBOUNDED_MAX_MERGE_SEGMENTS || maxNumSegments > 0;
        assert trigger != null;
        // 代表此时已经停止 merge了
        if (stopMerges) {
            return false;
        }

        // Do not start new merges if disaster struck   如果此时已经记录了某个妨碍writer正常工作的异常对象 返回false
        if (tragedy.get() != null) {
            return false;
        }
        boolean newMergesFound = false;
        final MergePolicy.MergeSpecification spec;
        // 代表用户指定了参与merge的segment数量
        if (maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS) {
            assert trigger == MergeTrigger.EXPLICIT || trigger == MergeTrigger.MERGE_FINISHED :
                    "Expected EXPLICT or MERGE_FINISHED as trigger even with maxNumSegments set but was: " + trigger.name();

            // 基于当前的段信息生成一个 描述本次merge任务的对象
            spec = mergePolicy.findForcedMerges(segmentInfos, maxNumSegments, Collections.unmodifiableMap(segmentsToMerge), this);
            newMergesFound = spec != null;
            if (newMergesFound) {
                final int numMerges = spec.merges.size();
                for (int i = 0; i < numMerges; i++) {
                    final MergePolicy.OneMerge merge = spec.merges.get(i);
                    merge.maxNumSegments = maxNumSegments;
                }
            }
        } else {
            // 根据当前所有候选的segment 大小 经过一系列排序 筛选 寻求最优解后 返回了一组OneMerge对象
            spec = mergePolicy.findMerges(trigger, segmentInfos, this);
        }
        newMergesFound = spec != null;
        // 如果找到了 符合条件的 oneMerge 就将他们设置到容器中
        if (newMergesFound) {
            final int numMerges = spec.merges.size();
            for (int i = 0; i < numMerges; i++) {
                // 将 OneMerge对象追加到 待处理队列中 同时根据segment信息 计算一些属性
                registerMerge(spec.merges.get(i));
            }
        }
        return newMergesFound;
    }

    /**
     * Expert: to be used by a {@link MergePolicy} to avoid
     * selecting merges for segments already being merged.
     * The returned collection is not cloned, and thus is
     * only safe to access if you hold IndexWriter's lock
     * (which you do when IndexWriter invokes the
     * MergePolicy).
     *
     * <p>The Set is unmodifiable.
     */
    public synchronized Set<SegmentCommitInfo> getMergingSegments() {
        return Collections.unmodifiableSet(mergingSegments);
    }

    /**
     * Expert: the {@link MergeScheduler} calls this method to retrieve the next
     * merge requested by the MergePolicy
     *
     * @lucene.experimental 从writer中返回一个将要被merge的对象
     */
    private synchronized MergePolicy.OneMerge getNextMerge() {
        if (pendingMerges.size() == 0) {
            return null;
        } else {
            // Advance the merge from pending to running
            MergePolicy.OneMerge merge = pendingMerges.removeFirst();
            runningMerges.add(merge);
            return merge;
        }
    }

    /**
     * Expert: returns true if there are merges waiting to be scheduled.
     *
     * @lucene.experimental
     */
    public synchronized boolean hasPendingMerges() {
        return pendingMerges.size() != 0;
    }

    /**
     * Close the <code>IndexWriter</code> without committing
     * any changes that have occurred since the last commit
     * (or since it was opened, if commit hasn't been called).
     * This removes any temporary files that had been created,
     * after which the state of the index will be the same as
     * it was when commit() was last called or when this
     * writer was first opened.  This also clears a previous
     * call to {@link #prepareCommit}.
     *
     * @throws IOException if there is a low-level IO error
     */
    @Override
    public void rollback() throws IOException {
        // don't call ensureOpen here: this acts like "close()" in closeable.

        // Ensure that only one thread actually gets to do the
        // closing, and make sure no commit is also in progress:
        // 阻塞直到本对象被标记成 close   返回true 代表当前线程抢占成功  允许执行接下来的操作
        if (shouldClose(true)) {
            rollbackInternal();
        }
    }

    private void rollbackInternal() throws IOException {
        // Make sure no commit is running, else e.g. we can close while another thread is still fsync'ing:
        synchronized (commitLock) {
            rollbackInternalNoCommit();

            assert pendingNumDocs.get() == segmentInfos.totalMaxDoc()
                    : "pendingNumDocs " + pendingNumDocs.get() + " != " + segmentInfos.totalMaxDoc() + " totalMaxDoc";
        }
    }

    /**
     * 回滚之前的操作
     * @throws IOException
     */
    private void rollbackInternalNoCommit() throws IOException {
        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "rollback");
        }

        try {
            // 强制终止正在执行的 merge
            synchronized (this) {
                // must be synced otherwise register merge might throw and exception if stopMerges
                // changes concurrently, abortMerges is synced as well
                stopMerges = true; // this disables merges forever
                abortMerges();
                assert mergingSegments.isEmpty() : "we aborted all merges but still have merging segments: " + mergingSegments;
            }
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "rollback: done finish merges");
            }

            // Must pre-close in case it increments changeCount so that we can then
            // set it to false before calling rollbackInternal
            // 阻塞等待所有 MergeThread 执行完成  (这些线程检测到stopMerges 会提早结束)
            mergeScheduler.close();

            docWriter.close(); // mark it as closed first to prevent subsequent indexing actions/flushes
            assert !Thread.holdsLock(this) : "IndexWriter lock should never be hold when aborting";
            // 终止此时所有的写入动作 并丢弃数据   (包括 deleteQueue采集到的delete/update数据)
            docWriter.abort(); // don't sync on IW here
            // 在 docWriter.abort() 中不是已经调用过该方法了吗???
            docWriter.flushControl.waitForFlush(); // wait for all concurrently running flushes
            publishFlushedSegments(true); // empty the flush ticket queue otherwise we might not have cleaned up all resources
            eventQueue.close();
            synchronized (this) {

                // 代表之前发起过 prepareCommit 那么尝试将相关索引文件删除   因为调用close也会触发 rollback 所以无法确定之前是否有prepareCommit过数据
                // 如果是正常提交的数据就不会被删除
                if (pendingCommit != null) {
                    // 删除之前创建的 pending_segment文件
                    pendingCommit.rollbackCommit(directory);
                    try {
                        // 尝试删除在 prepareCommit期间 fullFlush创建的 索引文件 如果正在使用中 就不会被删除
                        deleter.decRef(pendingCommit);
                    } finally {
                        pendingCommit = null;
                        notifyAll();
                    }
                }
                final int totalMaxDoc = segmentInfos.totalMaxDoc();
                // Keep the same segmentInfos instance but replace all
                // of its SegmentInfo instances so IFD below will remove
                // any segments we flushed since the last commit:
                // 回到上次提交的状态  在初始化该对象时就会加载上次提交的状态
                segmentInfos.rollbackSegmentInfos(rollbackSegments);
                int rollbackMaxDoc = segmentInfos.totalMaxDoc();
                // now we need to adjust this back to the rolled back SI but don't set it to the absolute value
                // otherwise we might hide internal bugsf
                adjustPendingNumDocs(-(totalMaxDoc - rollbackMaxDoc));
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "rollback: infos=" + segString(segmentInfos));
                }

                testPoint("rollback before checkpoint");

                // Ask deleter to locate unreferenced files & remove
                // them ... only when we are not experiencing a tragedy, else
                // these methods throw ACE:
                if (tragedy.get() == null) {
                    deleter.checkpoint(segmentInfos, false);
                    //
                    deleter.refresh();
                    // 减少此时维护的所有索引文件引用计数 可能导致所有文件的删除
                    deleter.close();
                }

                // 更新上次的changCount值
                lastCommitChangeCount = changeCount.get();
                // Don't bother saving any changes in our segmentInfos
                // 将所有 SegmentReader对象关闭
                readerPool.close();
                // Must set closed while inside same sync block where we call deleter.refresh, else concurrent threads may try to sneak a flush in,
                // after we leave this sync block and before we enter the sync block in the finally clause below that sets closed:
                closed = true;

                IOUtils.close(writeLock); // release write lock
                writeLock = null;
                closed = true;
                closing = false;
                // So any "concurrently closing" threads wake up and see that the close has now completed:
                notifyAll();
            }
        } catch (Throwable throwable) {
            try {
                // Must not hold IW's lock while closing
                // mergeScheduler: this can lead to deadlock,
                // e.g. TestIW.testThreadInterruptDeadlock
                IOUtils.closeWhileHandlingException(mergeScheduler);
                synchronized (this) {
                    // we tried to be nice about it: do the minimum
                    // don't leak a segments_N file if there is a pending commit
                    if (pendingCommit != null) {
                        try {
                            pendingCommit.rollbackCommit(directory);
                            deleter.decRef(pendingCommit);
                        } catch (Throwable t) {
                            throwable.addSuppressed(t);
                        }
                        pendingCommit = null;
                    }

                    // close all the closeables we can (but important is readerPool and writeLock to prevent leaks)
                    IOUtils.closeWhileHandlingException(readerPool, deleter, writeLock);
                    writeLock = null;
                    closed = true;
                    closing = false;

                    // So any "concurrently closing" threads wake up and see that the close has now completed:
                    notifyAll();
                }
            } catch (Throwable t) {
                throwable.addSuppressed(t);
            } finally {
                if (throwable instanceof VirtualMachineError) {
                    try {
                        tragicEvent(throwable, "rollbackInternal");
                    } catch (Throwable t1) {
                        throwable.addSuppressed(t1);
                    }
                }
            }
            throw throwable;
        }
    }

    /**
     * Delete all documents in the index.
     *
     * <p>
     * This method will drop all buffered documents and will remove all segments
     * from the index. This change will not be visible until a {@link #commit()}
     * has been called. This method can be rolled back using {@link #rollback()}.
     * </p>
     *
     * <p>
     * NOTE: this method is much faster than using deleteDocuments( new
     * MatchAllDocsQuery() ). Yet, this method also has different semantics
     * compared to {@link #deleteDocuments(Query...)} since internal
     * data-structures are cleared as well as all segment information is
     * forcefully dropped anti-viral semantics like omitting norms are reset or
     * doc value types are cleared. Essentially a call to {@link #deleteAll()} is
     * equivalent to creating a new {@link IndexWriter} with
     * {@link OpenMode#CREATE} which a delete query only marks documents as
     * deleted.
     * </p>
     *
     * <p>
     * NOTE: this method will forcefully abort all merges in progress. If other
     * threads are running {@link #forceMerge}, {@link #addIndexes(CodecReader[])}
     * or {@link #forceMergeDeletes} methods, they may receive
     * {@link MergePolicy.MergeAbortedException}s.
     *
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     */
    @SuppressWarnings("try")
    public long deleteAll() throws IOException {
        ensureOpen();
        // Remove any buffered docs
        boolean success = false;
        /* hold the full flush lock to prevent concurrency commits / NRT reopens to
         * get in our way and do unnecessary work. -- if we don't lock this here we might
         * get in trouble if */
        /*
         * We first abort and trash everything we have in-memory
         * and keep the thread-states locked, the lockAndAbortAll operation
         * also guarantees "point in time semantics" ie. the checkpoint that we need in terms
         * of logical happens-before relationship in the DW. So we do
         * abort all in memory structures
         * We also drop global field numbering before during abort to make
         * sure it's just like a fresh index.
         */
        try {
            synchronized (fullFlushLock) {
                try (Closeable finalizer = docWriter.lockAndAbortAll()) {
                    processEvents(false);
                    synchronized (this) {
                        try {
                            // Abort any running merges
                            abortMerges();
                            adjustPendingNumDocs(-segmentInfos.totalMaxDoc());
                            // Remove all segments
                            segmentInfos.clear();
                            // Ask deleter to locate unreferenced files & remove them:
                            deleter.checkpoint(segmentInfos, false);

                            /* don't refresh the deleter here since there might
                             * be concurrent indexing requests coming in opening
                             * files on the directory after we called DW#abort()
                             * if we do so these indexing requests might hit FNF exceptions.
                             * We will remove the files incrementally as we go...
                             */
                            // Don't bother saving any changes in our segmentInfos
                            readerPool.dropAll();
                            // Mark that the index has changed
                            changeCount.incrementAndGet();
                            segmentInfos.changed();
                            globalFieldNumberMap.clear();
                            success = true;
                            long seqNo = docWriter.getNextSequenceNumber();
                            return seqNo;
                        } finally {
                            if (success == false) {
                                if (infoStream.isEnabled("IW")) {
                                    infoStream.message("IW", "hit exception during deleteAll");
                                }
                            }
                        }
                    }
                }
            }
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "deleteAll");
            throw tragedy;
        }
    }

    /**
     * Aborts running merges.  Be careful when using this
     * method: when you abort a long-running merge, you lose
     * a lot of work that must later be redone.
     */
    private synchronized void abortMerges() {
        // Abort all pending & running merges:
        for (final MergePolicy.OneMerge merge : pendingMerges) {
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "now abort pending merge " + segString(merge.segments));
            }
            merge.setAborted();
            mergeFinish(merge);
        }
        pendingMerges.clear();

        for (final MergePolicy.OneMerge merge : runningMerges) {
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "now abort running merge " + segString(merge.segments));
            }
            merge.setAborted();
        }

        // We wait here to make all merges stop.  It should not
        // take very long because they periodically check if
        // they are aborted.
        while (runningMerges.size() + runningAddIndexesMerges.size() != 0) {

            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "now wait for " + runningMerges.size()
                        + " running merge/s to abort; currently running addIndexes: " + runningAddIndexesMerges.size());
            }

            doWait();
        }

        notifyAll();
        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "all running merges have aborted");
        }
    }

    /**
     * Wait for any currently outstanding merges to finish.
     *
     * <p>It is guaranteed that any merges started prior to calling this method
     * will have completed once this method completes.</p>
     * 阻塞当前线程 直到merge完成
     */
    void waitForMerges() throws IOException {

        // Give merge scheduler last chance to run, in case
        // any pending merges are waiting. We can't hold IW's lock
        // when going into merge because it can lead to deadlock.
        mergeScheduler.merge(mergeSource, MergeTrigger.CLOSING);

        synchronized (this) {
            ensureOpen(false);
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "waitForMerges");
            }

            // 只要还有待执行的merge任务 就阻塞线程
            while (pendingMerges.size() > 0 || runningMerges.size() > 0) {
                doWait();
            }

            // sanity check
            assert 0 == mergingSegments.size();

            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "waitForMerges done");
            }
        }
    }

    /**
     * Called whenever the SegmentInfos has been updated and
     * the index files referenced exist (correctly) in the
     * index directory.
     */
    private synchronized void checkpoint() throws IOException {
        // 标记 SegmentInfos 发生了变化
        changed();
        // 主要意图是为所有索引文件维护一个正确的引用计数
        deleter.checkpoint(segmentInfos, false);
    }

    /**
     * Checkpoints with IndexFileDeleter, so it's aware of
     * new files, and increments changeCount, so on
     * close/commit we will write a new segments file, but
     * does NOT bump segmentInfos.version.
     */
    private synchronized void checkpointNoSIS() throws IOException {
        changeCount.incrementAndGet();
        deleter.checkpoint(segmentInfos, false);
    }

    /**
     * Called internally if any index state has changed.
     */
    private synchronized void changed() {
        changeCount.incrementAndGet();
        segmentInfos.changed();
    }

    /**
     * 先将 FrozenBufferedUpdates 设置到任务队列中  之后在合适的时机执行
     *
     * @param packet
     * @return
     */
    private synchronized long publishFrozenUpdates(FrozenBufferedUpdates packet) {
        assert packet != null && packet.any();
        // 为更新流对象 追加一个 更新对象  返回此时由 BufferedUpdatesStream 分配的 delGen
        long nextGen = bufferedUpdatesStream.push(packet);
        // Do this as an event so it applies higher in the stack when we are not holding DocumentsWriterFlushQueue.purgeLock:
        // 这里也是先往队列中添加任务 而没有执行
        eventQueue.add(w -> {
            try {
                // we call tryApply here since we don't want to block if a refresh or a flush is already applying the
                // packet. The flush will retry this packet anyway to ensure all of them are applied
                tryApply(packet);
            } catch (Throwable t) {
                try {
                    w.onTragicEvent(t, "applyUpdatesPacket");
                } catch (Throwable t1) {
                    t.addSuppressed(t1);
                }
                throw t;
            }
            w.flushDeletesCount.incrementAndGet();
        });
        return nextGen;
    }

    /**
     * Atomically adds the segment private delete packet and publishes the flushed
     * segments SegmentInfo to the index writer.
     * @param newSegment 本次生成的新段
     * @param fieldInfos 本次解析的所有doc中携带的field
     * @param packet  对应perThread 自创建开始到某个预备刷盘时 从 deleteQueue同步过来的删除信息   deleteQueue的节点信息是所有perThread共享的  所以叫做slice
     * @param globalPacket 从globalSlice 中截获的更新/删除信息
     * @param sortMap  本次刷盘时 doc发生了重排序
     * 发布某个已经完成刷盘的段
     * perThread 首先解析doc 将数据结构化后存储在内存 之后通过刷盘将数据持久化 同时会生成一个 flushedSegment 代表一个完成刷盘的段， 之后在处理因为刷盘创建的ticket时
     * 就会触发该方法
     */
    private synchronized void publishFlushedSegment(SegmentCommitInfo newSegment, FieldInfos fieldInfos,
                                                    FrozenBufferedUpdates packet, FrozenBufferedUpdates globalPacket,
                                                    Sorter.DocMap sortMap) throws IOException {
        boolean published = false;
        try {
            // Lock order IW -> BDS
            ensureOpen(false);

            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "publishFlushedSegment " + newSegment);
            }

            // 处理全局 update/delete信息   实际上只是添加到任务队列中  本线程并没有处理数据
            if (globalPacket != null && globalPacket.any()) {
                publishFrozenUpdates(globalPacket);
            }

            // Publishing the segment must be sync'd on IW -> BDS to make the sure
            // that no merge prunes away the seg. private delete packet
            final long nextGen;
            // 在perThread 处理刷盘时 会将 termNode 处理掉  如果此时 perThread私有的packet 还有别的node未处理时   才将其设置到任务队列中
            if (packet != null && packet.any()) {
                nextGen = publishFrozenUpdates(packet);
            } else {
                // Since we don't have a delete packet to apply we can get a new
                // generation right away
                // 如果 perThread对应的 packet 此时已经处理完所有数据 / 或者没有采集到数据  也会为它 分配一个delGen  并直接标记成成功
                nextGen = bufferedUpdatesStream.getNextGen();
                // No deletes/updates here, so marked finished immediately:
                // 先将当前任务标记成已完成
                bufferedUpdatesStream.finishedSegment(nextGen);
            }
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "publish sets newSegment delGen=" + nextGen + " seg=" + segString(newSegment));
            }
            // 当为segment设置了 bufferedDeleteGen 后 只有 segment.bufferedDeleteGen <= update.delGen  的段对象才会被该update影响到
            newSegment.setBufferedDeletesGen(nextGen);
            // 刷盘完成的段 将会插入到 IndexWriter的 segmentInfos中
            segmentInfos.add(newSegment);
            published = true;

            // 为新插入的 segment下 关联的新生成的所有索引文件通过 IndexFileDeleter 维护引用计数
            checkpoint();

            // 如果本次doc发生了重排序 设置sortMap信息
            if (packet != null && packet.any() && sortMap != null) {
                // TODO: not great we do this heavyish op while holding IW's monitor lock,
                // but it only applies if you are using sorted indices and updating doc values:
                ReadersAndUpdates rld = getPooledInstance(newSegment, true);
                rld.sortMap = sortMap;
                // DON't release this ReadersAndUpdates we need to stick with that sortMap
            }
            FieldInfo fieldInfo = fieldInfos.fieldInfo(config.softDeletesField); // will return null if no soft deletes are present
            // this is a corner case where documents delete them-self with soft deletes. This is used to
            // build delete tombstones etc. in this case we haven't seen any updates to the DV in this fresh flushed segment.
            // if we have seen updates the update code checks if the segment is fully deleted.
            boolean hasInitialSoftDeleted = (fieldInfo != null
                    && fieldInfo.getDocValuesGen() == -1
                    && fieldInfo.getDocValuesType() != DocValuesType.NONE);

            // 代表所有doc 都被删除了
            final boolean isFullyHardDeleted = newSegment.getDelCount() == newSegment.info.maxDoc();
            // we either have a fully hard-deleted segment or one or more docs are soft-deleted. In both cases we need
            // to go and check if they are fully deleted. This has the nice side-effect that we now have accurate numbers
            // for the soft delete right after we flushed to disk.
            // 如果包含有效的软删除字段 需要确定此时哪些doc已经被硬删除了
            if (hasInitialSoftDeleted || isFullyHardDeleted) {
                // this operation is only really executed if needed an if soft-deletes are not configured it only be executed
                // if we deleted all docs in this newly flushed segment.
                ReadersAndUpdates rld = getPooledInstance(newSegment, true);
                try {
                    if (isFullyDeleted(rld)) {
                        dropDeletedSegment(newSegment);
                        checkpoint();
                    }
                } finally {
                    release(rld);
                }
            }

        } finally {
            // 可以看到 通过perThread刷盘完成后的段对象会插入到 IndexWriter中的 segmentInfos中  推测只有插入到 segmentInfos 后才被认为是可以被外部访问的  这里代表插入到SegmentInfos 失败
            if (published == false) {
                // 减少待处理的doc数量
                adjustPendingNumDocs(-newSegment.info.maxDoc());
            }
            flushCount.incrementAndGet();
            // 对用户开放的钩子
            doAfterFlush();
        }

    }

    private synchronized void resetMergeExceptions() {
        mergeExceptions.clear();
        mergeGen++;
    }

    private void noDupDirs(Directory... dirs) {
        HashSet<Directory> dups = new HashSet<>();
        for (int i = 0; i < dirs.length; i++) {
            if (dups.contains(dirs[i]))
                throw new IllegalArgumentException("Directory " + dirs[i] + " appears more than once");
            if (dirs[i] == directoryOrig)
                throw new IllegalArgumentException("Cannot add directory to itself");
            dups.add(dirs[i]);
        }
    }

    /**
     * Acquires write locks on all the directories; be sure
     * to match with a call to {@link IOUtils#close} in a
     * finally clause.
     */
    private List<Lock> acquireWriteLocks(Directory... dirs) throws IOException {
        List<Lock> locks = new ArrayList<>(dirs.length);
        for (int i = 0; i < dirs.length; i++) {
            boolean success = false;
            try {
                Lock lock = dirs[i].obtainLock(WRITE_LOCK_NAME);
                locks.add(lock);
                success = true;
            } finally {
                if (success == false) {
                    // Release all previously acquired locks:
                    // TODO: addSuppressed? it could be many...
                    IOUtils.closeWhileHandlingException(locks);
                }
            }
        }
        return locks;
    }

    /**
     * Adds all segments from an array of indexes into this index.
     *
     * <p>This may be used to parallelize batch indexing. A large document
     * collection can be broken into sub-collections. Each sub-collection can be
     * indexed in parallel, on a different thread, process or machine. The
     * complete index can then be created by merging sub-collection indexes
     * with this method.
     *
     * <p>
     * <b>NOTE:</b> this method acquires the write lock in
     * each directory, to ensure that no {@code IndexWriter}
     * is currently open or tries to open while this is
     * running.
     *
     * <p>This method is transactional in how Exceptions are
     * handled: it does not commit a new segments_N file until
     * all indexes are added.  This means if an Exception
     * occurs (for example disk full), then either no indexes
     * will have been added or they all will have been.
     *
     * <p>Note that this requires temporary free space in the
     * {@link Directory} up to 2X the sum of all input indexes
     * (including the starting index). If readers/searchers
     * are open against the starting index, then temporary
     * free space required will be higher by the size of the
     * starting index (see {@link #forceMerge(int)} for details).
     *
     * <p>This requires this index not be among those to be added.
     *
     * <p>All added indexes must have been created by the same
     * Lucene version as this index.
     *
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException    if the index is corrupt
     * @throws IOException              if there is a low-level IO error
     * @throws IllegalArgumentException if addIndexes would cause
     *                                  the index to exceed {@link #MAX_DOCS}, or if the indoming
     *                                  index sort does not match this index's index sort
     */
    public long addIndexes(Directory... dirs) throws IOException {
        ensureOpen();

        noDupDirs(dirs);

        List<Lock> locks = acquireWriteLocks(dirs);

        Sort indexSort = config.getIndexSort();

        boolean successTop = false;

        long seqNo;

        try {
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "flush at addIndexes(Directory...)");
            }

            flush(false, true);

            List<SegmentCommitInfo> infos = new ArrayList<>();

            // long so we can detect int overflow:
            long totalMaxDoc = 0;
            List<SegmentInfos> commits = new ArrayList<>(dirs.length);
            for (Directory dir : dirs) {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "addIndexes: process directory " + dir);
                }
                SegmentInfos sis = SegmentInfos.readLatestCommit(dir); // read infos from dir
                if (segmentInfos.getIndexCreatedVersionMajor() != sis.getIndexCreatedVersionMajor()) {
                    throw new IllegalArgumentException("Cannot use addIndexes(Directory) with indexes that have been created "
                            + "by a different Lucene version. The current index was generated by Lucene "
                            + segmentInfos.getIndexCreatedVersionMajor()
                            + " while one of the directories contains an index that was generated with Lucene "
                            + sis.getIndexCreatedVersionMajor());
                }
                totalMaxDoc += sis.totalMaxDoc();
                commits.add(sis);
            }

            // Best-effort up front check:
            testReserveDocs(totalMaxDoc);

            boolean success = false;
            try {
                for (SegmentInfos sis : commits) {
                    for (SegmentCommitInfo info : sis) {
                        assert !infos.contains(info) : "dup info dir=" + info.info.dir + " name=" + info.info.name;

                        Sort segmentIndexSort = info.info.getIndexSort();

                        if (indexSort != null && (segmentIndexSort == null || isCongruentSort(indexSort, segmentIndexSort) == false)) {
                            throw new IllegalArgumentException("cannot change index sort from " + segmentIndexSort + " to " + indexSort);
                        }

                        String newSegName = newSegmentName();

                        if (infoStream.isEnabled("IW")) {
                            infoStream.message("IW", "addIndexes: process segment origName=" + info.info.name + " newName=" + newSegName + " info=" + info);
                        }

                        IOContext context = new IOContext(new FlushInfo(info.info.maxDoc(), info.sizeInBytes()));

                        FieldInfos fis = readFieldInfos(info);
                        for (FieldInfo fi : fis) {
                            // This will throw exceptions if any of the incoming fields have an illegal schema change:
                            globalFieldNumberMap.addOrGet(fi.name, fi.number, fi.getIndexOptions(), fi.getDocValuesType(), fi.getPointDimensionCount(), fi.getPointIndexDimensionCount(), fi.getPointNumBytes(), fi.isSoftDeletesField());
                        }
                        infos.add(copySegmentAsIs(info, newSegName, context));
                    }
                }
                success = true;
            } finally {
                if (!success) {
                    for (SegmentCommitInfo sipc : infos) {
                        // Safe: these files must exist
                        deleteNewFiles(sipc.files());
                    }
                }
            }

            synchronized (this) {
                success = false;
                try {
                    ensureOpen();

                    // Now reserve the docs, just before we update SIS:
                    reserveDocs(totalMaxDoc);

                    seqNo = docWriter.getNextSequenceNumber();

                    success = true;
                } finally {
                    if (!success) {
                        for (SegmentCommitInfo sipc : infos) {
                            // Safe: these files must exist
                            deleteNewFiles(sipc.files());
                        }
                    }
                }
                segmentInfos.addAll(infos);
                checkpoint();
            }

            successTop = true;

        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "addIndexes(Directory...)");
            throw tragedy;
        } finally {
            if (successTop) {
                IOUtils.close(locks);
            } else {
                IOUtils.closeWhileHandlingException(locks);
            }
        }
        maybeMerge();

        return seqNo;
    }

    private void validateMergeReader(CodecReader leaf) {
        LeafMetaData segmentMeta = leaf.getMetaData();
        if (segmentInfos.getIndexCreatedVersionMajor() != segmentMeta.getCreatedVersionMajor()) {
            throw new IllegalArgumentException("Cannot merge a segment that has been created with major version "
                    + segmentMeta.getCreatedVersionMajor() + " into this index which has been created by major version "
                    + segmentInfos.getIndexCreatedVersionMajor());
        }

        if (segmentInfos.getIndexCreatedVersionMajor() >= 7 && segmentMeta.getMinVersion() == null) {
            throw new IllegalStateException("Indexes created on or after Lucene 7 must record the created version major, but " + leaf + " hides it");
        }

        Sort leafIndexSort = segmentMeta.getSort();
        if (config.getIndexSort() != null &&
                (leafIndexSort == null || isCongruentSort(config.getIndexSort(), leafIndexSort) == false)) {
            throw new IllegalArgumentException("cannot change index sort from " + leafIndexSort + " to " + config.getIndexSort());
        }
    }

    /**
     * Merges the provided indexes into this index.
     *
     * <p>
     * The provided IndexReaders are not closed.
     *
     * <p>
     * See {@link #addIndexes} for details on transactional semantics, temporary
     * free space required in the Directory, and non-CFS segments on an Exception.
     *
     * <p>
     * <b>NOTE:</b> empty segments are dropped by this method and not added to this
     * index.
     *
     * <p>
     * <b>NOTE:</b> this merges all given {@link LeafReader}s in one
     * merge. If you intend to merge a large number of readers, it may be better
     * to call this method multiple times, each time with a small set of readers.
     * In principle, if you use a merge policy with a {@code mergeFactor} or
     * {@code maxMergeAtOnce} parameter, you should pass that many readers in one
     * call.
     *
     * <p>
     * <b>NOTE:</b> this method does not call or make use of the {@link MergeScheduler},
     * so any custom bandwidth throttling is at the moment ignored.
     *
     * @return The <a href="#sequence_number">sequence number</a>
     * for this operation
     * @throws CorruptIndexException    if the index is corrupt
     * @throws IOException              if there is a low-level IO error
     * @throws IllegalArgumentException if addIndexes would cause the index to exceed {@link #MAX_DOCS}
     */
    public long addIndexes(CodecReader... readers) throws IOException {
        ensureOpen();

        // long so we can detect int overflow:
        long numDocs = 0;
        long seqNo;
        try {
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "flush at addIndexes(CodecReader...)");
            }
            flush(false, true);

            String mergedName = newSegmentName();
            int numSoftDeleted = 0;
            for (CodecReader leaf : readers) {
                numDocs += leaf.numDocs();
                validateMergeReader(leaf);
                if (softDeletesEnabled) {
                    Bits liveDocs = leaf.getLiveDocs();
                    numSoftDeleted += PendingSoftDeletes.countSoftDeletes(
                            DocValuesFieldExistsQuery.getDocValuesDocIdSetIterator(config.getSoftDeletesField(), leaf), liveDocs);
                }
            }

            // Best-effort up front check:
            testReserveDocs(numDocs);

            final IOContext context = new IOContext(new MergeInfo(Math.toIntExact(numDocs), -1, false, UNBOUNDED_MAX_MERGE_SEGMENTS));

            // TODO: somehow we should fix this merge so it's
            // abortable so that IW.close(false) is able to stop it
            TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(directory);
            Codec codec = config.getCodec();
            // We set the min version to null for now, it will be set later by SegmentMerger
            SegmentInfo info = new SegmentInfo(directoryOrig, Version.LATEST, null, mergedName, -1,
                    false, codec, Collections.emptyMap(), StringHelper.randomId(), Collections.emptyMap(), config.getIndexSort());

            SegmentMerger merger = new SegmentMerger(Arrays.asList(readers), info, infoStream, trackingDir,
                    globalFieldNumberMap,
                    context);

            if (!merger.shouldMerge()) {
                return docWriter.getNextSequenceNumber();
            }

            synchronized (this) {
                ensureOpen();
                assert stopMerges == false;
                runningAddIndexesMerges.add(merger);
            }
            try {
                merger.merge();  // merge 'em
            } finally {
                synchronized (this) {
                    runningAddIndexesMerges.remove(merger);
                    notifyAll();
                }
            }
            SegmentCommitInfo infoPerCommit = new SegmentCommitInfo(info, 0, numSoftDeleted, -1L, -1L, -1L, StringHelper.randomId());

            info.setFiles(new HashSet<>(trackingDir.getCreatedFiles()));
            trackingDir.clearCreatedFiles();

            setDiagnostics(info, SOURCE_ADDINDEXES_READERS);

            final MergePolicy mergePolicy = config.getMergePolicy();
            boolean useCompoundFile;
            synchronized (this) { // Guard segmentInfos
                if (stopMerges) {
                    // Safe: these files must exist
                    deleteNewFiles(infoPerCommit.files());

                    return docWriter.getNextSequenceNumber();
                }
                ensureOpen();
                useCompoundFile = mergePolicy.useCompoundFile(segmentInfos, infoPerCommit, this);
            }

            // Now create the compound file if needed
            if (useCompoundFile) {
                Collection<String> filesToDelete = infoPerCommit.files();
                TrackingDirectoryWrapper trackingCFSDir = new TrackingDirectoryWrapper(directory);
                // TODO: unlike merge, on exception we arent sniping any trash cfs files here?
                // createCompoundFile tries to cleanup, but it might not always be able to...
                try {
                    createCompoundFile(infoStream, trackingCFSDir, info, context, this::deleteNewFiles);
                } finally {
                    // delete new non cfs files directly: they were never
                    // registered with IFD
                    deleteNewFiles(filesToDelete);
                }
                info.setUseCompoundFile(true);
            }

            // Have codec write SegmentInfo.  Must do this after
            // creating CFS so that 1) .si isn't slurped into CFS,
            // and 2) .si reflects useCompoundFile=true change
            // above:
            codec.segmentInfoFormat().write(trackingDir, info, context);

            info.addFiles(trackingDir.getCreatedFiles());

            // Register the new segment
            synchronized (this) {
                if (stopMerges) {
                    // Safe: these files must exist
                    deleteNewFiles(infoPerCommit.files());

                    return docWriter.getNextSequenceNumber();
                }
                ensureOpen();

                // Now reserve the docs, just before we update SIS:
                reserveDocs(numDocs);

                segmentInfos.add(infoPerCommit);
                seqNo = docWriter.getNextSequenceNumber();
                checkpoint();
            }
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "addIndexes(CodecReader...)");
            throw tragedy;
        }
        maybeMerge();

        return seqNo;
    }

    /**
     * Copies the segment files as-is into the IndexWriter's directory.
     */
    private SegmentCommitInfo copySegmentAsIs(SegmentCommitInfo info, String segName, IOContext context) throws IOException {

        // Same SI as before but we change directory and name
        SegmentInfo newInfo = new SegmentInfo(directoryOrig, info.info.getVersion(), info.info.getMinVersion(), segName, info.info.maxDoc(),
                info.info.getUseCompoundFile(), info.info.getCodec(),
                info.info.getDiagnostics(), info.info.getId(), info.info.getAttributes(), info.info.getIndexSort());
        SegmentCommitInfo newInfoPerCommit = new SegmentCommitInfo(newInfo, info.getDelCount(), info.getSoftDelCount(), info.getDelGen(),
                info.getFieldInfosGen(), info.getDocValuesGen(), info.getId());

        newInfo.setFiles(info.info.files());
        newInfoPerCommit.setFieldInfosFiles(info.getFieldInfosFiles());
        newInfoPerCommit.setDocValuesUpdatesFiles(info.getDocValuesUpdatesFiles());

        boolean success = false;

        Set<String> copiedFiles = new HashSet<>();
        try {
            // Copy the segment's files
            for (String file : info.files()) {
                final String newFileName = newInfo.namedForThisSegment(file);
                directory.copyFrom(info.info.dir, file, newFileName, context);
                copiedFiles.add(newFileName);
            }
            success = true;
        } finally {
            if (!success) {
                // Safe: these files must exist
                deleteNewFiles(copiedFiles);
            }
        }

        assert copiedFiles.equals(newInfoPerCommit.files()) : "copiedFiles=" + copiedFiles + " vs " + newInfoPerCommit.files();

        return newInfoPerCommit;
    }

    /**
     * A hook for extending classes to execute operations after pending added and
     * deleted documents have been flushed to the Directory but before the change
     * is committed (new segments_N file written).
     */
    protected void doAfterFlush() throws IOException {
    }

    /**
     * A hook for extending classes to execute operations before pending added and
     * deleted documents are flushed to the Directory.
     */
    protected void doBeforeFlush() throws IOException {
    }

    /**
     * <p>Expert: prepare for commit.  This does the
     * first phase of 2-phase commit. This method does all
     * steps necessary to commit changes since this writer
     * was opened: flushes pending added and deleted docs,
     * syncs the index files, writes most of next segments_N
     * file.  After calling this you must call either {@link
     * #commit()} to finish the commit, or {@link
     * #rollback()} to revert the commit and undo all changes
     * done since the writer was opened.</p>
     *
     * <p>You can also just call {@link #commit()} directly
     * without prepareCommit first in which case that method
     * will internally call prepareCommit.
     *
     * @return The <a href="#sequence_number">sequence number</a>
     * of the last operation in the commit.  All sequence numbers &lt;= this value
     * will be reflected in the commit, and all others will not.
     */
    @Override
    public final long prepareCommit() throws IOException {
        // 确保此时 IndexWriter 处于打开状态
        ensureOpen();
        pendingSeqNo = prepareCommitInternal();
        // we must do this outside of the commitLock else we can deadlock:
        // 如果需要合并的话 还会将segment数据合并
        // TODO merge成功后之前参与merge的segment 不是都被移除了吗 那之前生成的pending_segment 还有过期数据啊
        if (maybeMerge.getAndSet(false)) {
            maybeMerge(config.getMergePolicy(), MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
        }
        return pendingSeqNo;
    }

    /**
     * <p>Expert: Flushes the next pending writer per thread buffer if available or the largest active
     * non-pending writer per thread buffer in the calling thread.
     * This can be used to flush documents to disk outside of an indexing thread. In contrast to {@link #flush()}
     * this won't mark all currently active indexing buffers as flush-pending.
     * <p>
     * Note: this method is best-effort and might not flush any segments to disk. If there is a full flush happening
     * concurrently multiple segments might have been flushed.
     * Users of this API can access the IndexWriters current memory consumption via {@link #ramBytesUsed()}
     * </p>
     *
     * @return <code>true</code> iff this method flushed at least on segment to disk.
     * @lucene.experimental
     */
    public final boolean flushNextBuffer() throws IOException {
        try {
            if (docWriter.flushOneDWPT()) {
                processEvents(true);
                return true; // we wrote a segment
            }
            return false;
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "flushNextBuffer");
            throw tragedy;
        } finally {
            maybeCloseOnTragicEvent();
        }
    }

    /**
     * 执行预提交
     * @return
     * @throws IOException
     */
    private long prepareCommitInternal() throws IOException {
        startCommitTime = System.nanoTime();
        synchronized (commitLock) {
            ensureOpen(false);
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "prepareCommit: flush");
                infoStream.message("IW", "  index before flush " + segString());
            }

            if (tragedy.get() != null) {
                throw new IllegalStateException("this writer hit an unrecoverable error; cannot commit", tragedy.get());
            }

            // 代表之前已经有别人触发过预提交了 此时应该调用commit/rollback
            if (pendingCommit != null) {
                throw new IllegalStateException("prepareCommit was already called with no corresponding call to commit");
            }

            // 先执行刷盘前的预备操作
            doBeforeFlush();
            testPoint("startDoFlush");
            SegmentInfos toCommit = null;
            boolean anyChanges = false;
            long seqNo;

            // This is copied from doFlush, except it's modified to
            // clone & incRef the flushed SegmentInfos inside the
            // sync block:

            try {

                synchronized (fullFlushLock) {
                    boolean flushSuccess = false;
                    boolean success = false;
                    try {
                        // 在预提交阶段 需要将之前所有数据刷盘
                        seqNo = docWriter.flushAllThreads();
                        if (seqNo < 0) {
                            anyChanges = true;
                            seqNo = -seqNo;
                        }
                        if (anyChanges == false) {
                            // prevent double increment since docWriter#doFlush increments the flushcount
                            // if we flushed anything.
                            flushCount.incrementAndGet();
                        }
                        // 阻塞线程直到所有ticket都被处理
                        publishFlushedSegments(true);
                        // cannot pass triggerMerges=true here else it can lead to deadlock:
                        // 处理此时囤积的所有 update 对象
                        processEvents(false);

                        flushSuccess = true;

                        // 获取此时所有待处理update 并阻塞等待完成  (因为有些update对象可能会被其他线程抢占)
                        applyAllDeletesAndUpdates();
                        synchronized (this) {
                            // 将delete的变化  update的变化 持久化到磁盘上
                            writeReaderPool(true);
                            // 代表在这个过程中 segment确实发生了变化
                            if (changeCount.get() != lastCommitChangeCount) {
                                // There are changes to commit, so we will write a new segments_N in startCommit.
                                // The act of committing is itself an NRT-visible change (an NRT reader that was
                                // just opened before this should see it on reopen) so we increment changeCount
                                // and segments version so a future NRT reopen will see the change:
                                changeCount.incrementAndGet();
                                segmentInfos.changed();
                            }

                            // 该属性是由用户设置的
                            if (commitUserData != null) {
                                Map<String, String> userData = new HashMap<>();
                                for (Map.Entry<String, String> ent : commitUserData) {
                                    userData.put(ent.getKey(), ent.getValue());
                                }
                                // 将用户信息设置到 segmentInfos中
                                segmentInfos.setUserData(userData, false);
                            }

                            // Must clone the segmentInfos while we still
                            // hold fullFlushLock and while sync'd so that
                            // no partial changes (eg a delete w/o
                            // corresponding add from an updateDocument) can
                            // sneak into the commit point:
                            toCommit = segmentInfos.clone();

                            // 每次segment的信息发生变化 就会修改该值
                            pendingCommitChangeCount = changeCount.get();

                            // This protects the segmentInfos we are now going
                            // to commit.  This is important in case, eg, while
                            // we are trying to sync all referenced files, a
                            // merge completes which would otherwise have
                            // removed the files we are now syncing.
                            // 获取此时所有segment下的文件名  但是不包含 segment_N 文件  看来segment_N 文件应该就是描述了某次 IndexWriter下所有活跃段的信息
                            filesToCommit = toCommit.files(false);
                            deleter.incRef(filesToCommit);
                        }
                        success = true;
                    } finally {
                        if (!success) {
                            if (infoStream.isEnabled("IW")) {
                                infoStream.message("IW", "hit exception during prepareCommit");
                            }
                        }
                        assert Thread.holdsLock(fullFlushLock);
                        // Done: finish the full flush!
                        // 在 fullFlush完成后执行的后置函数
                        docWriter.finishFullFlush(flushSuccess);
                        doAfterFlush();
                    }
                }
            } catch (VirtualMachineError tragedy) {
                tragicEvent(tragedy, "prepareCommit");
                throw tragedy;
            } finally {
                maybeCloseOnTragicEvent();
            }

            try {
                // 代表有perThread 对象的数据刷盘
                if (anyChanges) {
                    maybeMerge.set(true);
                }
                startCommit(toCommit);
                if (pendingCommit == null) {
                    return -1;
                } else {
                    return seqNo;
                }
            } catch (Throwable t) {
                synchronized (this) {
                    if (filesToCommit != null) {
                        try {
                            deleter.decRef(filesToCommit);
                        } catch (Throwable t1) {
                            t.addSuppressed(t1);
                        } finally {
                            filesToCommit = null;
                        }
                    }
                }
                throw t;
            }
        }
    }

    /**
     * Ensures that all changes in the reader-pool are written to disk.
     *
     * @param writeDeletes if <code>true</code> if deletes should be written to disk too.
     *                     需要将delete的影响持久化到磁盘
     *
     * 确保所有变化都已经持久化
     */
    private void writeReaderPool(boolean writeDeletes) throws IOException {
        assert Thread.holdsLock(this);
        if (writeDeletes) {
            // 将此时 liveDoc 与 docValue 信息写入到磁盘
            if (readerPool.commit(segmentInfos)) {
                // 这种检查点仅增加 新索引文件的引用计数  不会增加segment的版本信息
                checkpointNoSIS();
            }
        } else { // only write the docValues
            // 仅仅将更新信息持久化
            if (readerPool.writeAllDocValuesUpdates()) {
                // 为新增的索引文件增加引用计数
                checkpoint();
            }
        }
        // now do some best effort to check if a segment is fully deleted
        // 检测某些段下所有的doc 是否都被删除 是就可以删除整个segment文件
        List<SegmentCommitInfo> toDrop = new ArrayList<>(); // don't modify segmentInfos in-place
        for (SegmentCommitInfo info : segmentInfos) {
            ReadersAndUpdates readersAndUpdates = readerPool.get(info, false);
            if (readersAndUpdates != null) {
                if (isFullyDeleted(readersAndUpdates)) {
                    toDrop.add(info);
                }
            }
        }
        for (SegmentCommitInfo info : toDrop) {
            dropDeletedSegment(info);
        }
        // 更新 segmentInfos的版本信息
        if (toDrop.isEmpty() == false) {
            checkpoint();
        }
    }

    /**
     * Sets the iterator to provide the commit user data map at commit time.  Calling this method
     * is considered a committable change and will be {@link #commit() committed} even if
     * there are no other changes this writer. Note that you must call this method
     * before {@link #prepareCommit()}.  Otherwise it won't be included in the
     * follow-on {@link #commit()}.
     * <p>
     * <b>NOTE:</b> the iterator is late-binding: it is only visited once all documents for the
     * commit have been written to their segments, before the next segments_N file is written
     */
    public final synchronized void setLiveCommitData(Iterable<Map.Entry<String, String>> commitUserData) {
        setLiveCommitData(commitUserData, true);
    }

    /**
     * Sets the commit user data iterator, controlling whether to advance the {@link SegmentInfos#getVersion}.
     *
     * @lucene.internal
     * @see #setLiveCommitData(Iterable)
     */
    public final synchronized void setLiveCommitData(Iterable<Map.Entry<String, String>> commitUserData, boolean doIncrementVersion) {
        this.commitUserData = commitUserData;
        if (doIncrementVersion) {
            segmentInfos.changed();
        }
        changeCount.incrementAndGet();
    }

    /**
     * Returns the commit user data iterable previously set with {@link #setLiveCommitData(Iterable)}, or null if nothing has been set yet.
     */
    public final synchronized Iterable<Map.Entry<String, String>> getLiveCommitData() {
        return commitUserData;
    }

    // Used only by commit and prepareCommit, below; lock
    // order is commitLock -> IW
    private final Object commitLock = new Object();

    /**
     * <p>Commits all pending changes (added and deleted
     * documents, segment merges, added
     * indexes, etc.) to the index, and syncs all referenced
     * index files, such that a reader will see the changes
     * and the index updates will survive an OS or machine
     * crash or power loss.  Note that this does not wait for
     * any running background merges to finish.  This may be a
     * costly operation, so you should test the cost in your
     * application and do it only when really necessary.</p>
     *
     * <p> Note that this operation calls Directory.sync on
     * the index files.  That call should not return until the
     * file contents and metadata are on stable storage.  For
     * FSDirectory, this calls the OS's fsync.  But, beware:
     * some hardware devices may in fact cache writes even
     * during fsync, and return before the bits are actually
     * on stable storage, to give the appearance of faster
     * performance.  If you have such a device, and it does
     * not have a battery backup (for example) then on power
     * loss it may still lose data.  Lucene cannot guarantee
     * consistency on such devices.  </p>
     *
     * <p> If nothing was committed, because there were no
     * pending changes, this returns -1.  Otherwise, it returns
     * the sequence number such that all indexing operations
     * prior to this sequence will be included in the commit
     * point, and all other operations will not. </p>
     *
     * @return The <a href="#sequence_number">sequence number</a>
     * of the last operation in the commit.  All sequence numbers &lt;= this value
     * will be reflected in the commit, and all others will not.
     * @see #prepareCommit
     * 在 prepareCommit后调用   也可以直接调用
     */
    @Override
    public final long commit() throws IOException {
        ensureOpen();
        return commitInternal(config.getMergePolicy());
    }

    /**
     * Returns true if there may be changes that have not been
     * committed.  There are cases where this may return true
     * when there are no actual "real" changes to the index,
     * for example if you've deleted by Term or Query but
     * that Term or Query does not match any documents.
     * Also, if a merge kicked off as a result of flushing a
     * new segment during {@link #commit}, or a concurrent
     * merged finished, this method may return true right
     * after you had just called {@link #commit}.
     * 检测此时是否有未提交的数据  每次segmentInfos 发生变化时 changeCount都会增加 代表此时信息与之前不一致
     */
    public final boolean hasUncommittedChanges() {
        return changeCount.get() != lastCommitChangeCount || hasChangesInRam();
    }

    /**
     * Returns true if there are any changes or deletes that are not flushed or applied.
     */
    boolean hasChangesInRam() {
        return docWriter.anyChanges() || bufferedUpdatesStream.any();
    }

    /**
     * @param mergePolicy
     * @return
     * @throws IOException
     */
    private long commitInternal(MergePolicy mergePolicy) throws IOException {

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "commit: start");
        }

        long seqNo;

        synchronized (commitLock) {
            ensureOpen(false);

            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "commit: enter lock");
            }

            if (pendingCommit == null) {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "commit: now prepare");
                }
                // 在这里会进行预提交  主要是进行 fullFlush 以及处理所有 update/delete信息 和生成一个 pending_segment 索引文件
                seqNo = prepareCommitInternal();
            } else {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "commit: already prepared");
                }
                seqNo = pendingSeqNo;
            }

            // 代表本次处理流程完成
            finishCommit();
        }

        // we must do this outside of the commitLock else we can deadlock:
        if (maybeMerge.getAndSet(false)) {
            maybeMerge(mergePolicy, MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
        }

        return seqNo;
    }

    /**
     *
     * @throws IOException
     */
    @SuppressWarnings("try")
    private void finishCommit() throws IOException {

        boolean commitCompleted = false;
        String committedSegmentsFileName = null;

        try {
            synchronized (this) {
                ensureOpen(false);

                if (tragedy.get() != null) {
                    throw new IllegalStateException("this writer hit an unrecoverable error; cannot complete commit", tragedy.get());
                }

                if (pendingCommit != null) {
                    // 找到本次要提交的索引文件
                    final Collection<String> commitFiles = this.filesToCommit;
                    try (Closeable finalizer = () -> deleter.decRef(commitFiles)) {

                        if (infoStream.isEnabled("IW")) {
                            infoStream.message("IW", "commit: pendingCommit != null");
                        }

                        // 将 pending_segment_N 重命名为 segment_N
                        committedSegmentsFileName = pendingCommit.finishCommit(directory);

                        // we committed, if anything goes wrong after this, we are screwed and it's a tragedy:
                        commitCompleted = true;

                        if (infoStream.isEnabled("IW")) {
                            infoStream.message("IW", "commit: done writing segments file \"" + committedSegmentsFileName + "\"");
                        }

                        // NOTE: don't use this.checkpoint() here, because
                        // we do not want to increment changeCount:
                        // 仅保留最新的一次提交点 并将之前的数据清除
                        deleter.checkpoint(pendingCommit, true);

                        // Carry over generation to our master SegmentInfos:
                        segmentInfos.updateGeneration(pendingCommit);

                        lastCommitChangeCount = pendingCommitChangeCount;
                        // 更新回滚时恢复的  segmentInfos
                        rollbackSegments = pendingCommit.createBackupSegmentInfos();

                    } finally {
                        notifyAll();
                        pendingCommit = null;
                        this.filesToCommit = null;
                    }
                } else {
                    assert filesToCommit == null;
                    if (infoStream.isEnabled("IW")) {
                        infoStream.message("IW", "commit: pendingCommit == null; skip");
                    }
                }
            }
        } catch (Throwable t) {
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "hit exception during finishCommit: " + t.getMessage());
            }
            if (commitCompleted) {
                tragicEvent(t, "finishCommit");
            }
            throw t;
        }

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", String.format(Locale.ROOT, "commit: took %.1f msec", (System.nanoTime() - startCommitTime) / 1000000.0));
            infoStream.message("IW", "commit: done");
        }
    }

    // Ensures only one flush() is actually flushing segments
    // at a time:
    private final Object fullFlushLock = new Object();

    /**
     * Moves all in-memory segments to the {@link Directory}, but does not commit
     * (fsync) them (call {@link #commit} for that).
     * 将解析doc后生成的数据 写到磁盘中  实际上本方法是可以并发调用的 意味着什么呢 也就是刷盘过程中添加的各种任务 会被多线程消费
     */
    public final void flush() throws IOException {
        flush(true, true);
    }

    /**
     * Flush all in-memory buffered updates (adds and deletes)
     * to the Directory.
     *
     * @param triggerMerge    if true, we may merge segments (if
     *                        deletes or docs were flushed) if necessary      当刷盘完成时 根据情况尝试将segment 合并
     * @param applyAllDeletes whether pending deletes should also   是否要阻塞等待当前所有update任务都完成
     */
    final void flush(boolean triggerMerge, boolean applyAllDeletes) throws IOException {

        // NOTE: this method cannot be sync'd because
        // maybeMerge() in turn calls mergeScheduler.merge which
        // in turn can take a long time to run and we don't want
        // to hold the lock for that.  In the case of
        // ConcurrentMergeScheduler this can lead to deadlock
        // when it stalls due to too many running merges.

        // We can be called during close, when closing==true, so we must pass false to ensureOpen:
        ensureOpen(false);
        // doFlush 返回true 代表有数据发生了持久化  无论是处理 update 还是将之前解析并暂存在内存中的数据持久化
        if (doFlush(applyAllDeletes) && triggerMerge) {
            // 执行merge操作
            maybeMerge(config.getMergePolicy(), MergeTrigger.FULL_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
        }
    }

    /**
     * Returns true a segment was flushed or deletes were applied.
     * 将此时存储在内存中的 已经解析完成的数据写入到磁盘
     */
    private boolean doFlush(boolean applyAllDeletes) throws IOException {
        if (tragedy.get() != null) {
            throw new IllegalStateException("this writer hit an unrecoverable error; cannot flush", tragedy.get());
        }

        doBeforeFlush();
        testPoint("startDoFlush");
        boolean success = false;
        try {

            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "  start flush: applyAllDeletes=" + applyAllDeletes);
                infoStream.message("IW", "  index before flush " + segString());
            }
            boolean anyChanges;

            synchronized (fullFlushLock) {
                boolean flushSuccess = false;
                try {
                    long seqNo = docWriter.flushAllThreads();
                    // 此时代表有新的数据刷盘
                    if (seqNo < 0) {
                        seqNo = -seqNo;
                        anyChanges = true;
                    } else {
                        // 代表没有需要处理的 perThread 对象  比如没有解析doc （没有生成任何 perThread）
                        anyChanges = false;
                    }
                    if (!anyChanges) {
                        // flushCount is incremented in flushAllThreads
                        // 即使没有发生数据刷盘也要增加刷盘次数   至于成功刷盘的情况 在 publishFlushedSegment中会增加刷盘次数
                        flushCount.incrementAndGet();
                    }
                    // 之前因为多线程抢占 可能导致某些 ticket 还没有发布 这里就要将所有ticket发布
                    // 也就是将刷盘生成的 segment回填到 segmentInfos 中  以及往任务队列插入一个处理update的任务
                    publishFlushedSegments(true);
                    flushSuccess = true;
                } finally {
                    assert Thread.holdsLock(fullFlushLock);
                    docWriter.finishFullFlush(flushSuccess);
                    // 执行之前存储在任务队列中的所有任务   并且不进行merge
                    processEvents(false);
                }
            }

            // 因为多线程会并发处理delete 和 update 这里是等待所有update对象都处理完
            if (applyAllDeletes) {
                applyAllDeletesAndUpdates();
            }

            // 只要有数据被刷盘 又或者 update对象修改了之前的数据 就标记成需要merge
            anyChanges |= maybeMerge.getAndSet(false);

            synchronized (this) {
                // 将最新的 docValue信息持久化   如果applyAllDeletes == true 将最新的liveDoc信息持久化  同时检测如果发现某个segment下所有doc都被删除 则清除reader对象
                writeReaderPool(applyAllDeletes);
                // 执行刷盘完成后的后置钩子
                doAfterFlush();
                success = true;
                return anyChanges;
            }
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "doFlush");
            throw tragedy;
        } finally {
            if (!success) {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "hit exception during flush");
                }
                maybeCloseOnTragicEvent();
            }
        }
    }

    /**
     * 阻塞，并等待所有正在处理update的线程完成
     * @throws IOException
     */
    private void applyAllDeletesAndUpdates() throws IOException {
        assert Thread.holdsLock(this) == false;
        flushDeletesCount.incrementAndGet();
        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "now apply all deletes for all segments buffered updates bytesUsed=" + bufferedUpdatesStream.ramBytesUsed() + " reader pool bytesUsed=" + readerPool.ramBytesUsed());
        }
        bufferedUpdatesStream.waitApplyAll(this);
    }

    // for testing only
    DocumentsWriter getDocsWriter() {
        return docWriter;
    }

    /**
     * Expert:  Return the number of documents currently
     * buffered in RAM.
     */
    public final synchronized int numRamDocs() {
        ensureOpen();
        return docWriter.getNumDocs();
    }

    private synchronized void ensureValidMerge(MergePolicy.OneMerge merge) {
        for (SegmentCommitInfo info : merge.segments) {
            if (!segmentInfos.contains(info)) {
                throw new MergePolicy.MergeException("MergePolicy selected a segment (" + info.info.name + ") that is not in the current index " + segString());
            }
        }
    }

    /**
     * Carefully merges deletes and updates for the segments we just merged. This
     * is tricky because, although merging will clear all deletes (compacts the
     * documents) and compact all the updates, new deletes and updates may have
     * been flushed to the segments since the merge was started. This method
     * "carries over" such new deletes and updates onto the newly merged segment,
     * and saves the resulting deletes and updates files (incrementing the delete
     * and DV generations for merge.info). If no deletes were flushed, no new
     * deletes file is saved.
     * 当merge 正常完成时触发  主要作用就是捕捉在merge过程中发生的 delete 和 update信息 并作用到merge后的segment上
     * 这样确保能接收到最新的 update信息  该方法还会检测在merge期间新增的merge信息 确保不丢失
     *
     * 针对mergeFinishedGen 的并发控制是通过synchronized 的 虽然在该方法内 没有将新的segment发布到 IndexWriter上
     * 但是包裹该方法的 commitMerge 也是在synchronized 修饰下的 只要确保那个方法中完成了segment的发布就可以
     */
    private synchronized ReadersAndUpdates commitMergedDeletesAndUpdates(MergePolicy.OneMerge merge, MergeState mergeState) throws IOException {

        mergeFinishedGen.incrementAndGet();

        testPoint("startCommitMergeDeletes");

        final List<SegmentCommitInfo> sourceSegments = merge.segments;

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "commitMergeDeletes " + segString(merge.segments));
        }

        // Carefully merge deletes that occurred after we
        // started merging:
        long minGen = Long.MAX_VALUE;

        // Lazy init (only when we find a delete or update to carry over):
        // 将merge后的 segmentCommitInfo 设置到 readerPool中
        final ReadersAndUpdates mergedDeletesAndUpdates = getPooledInstance(merge.info, true);

        int numDeletesBefore = mergedDeletesAndUpdates.getDelCount();
        // field -> delGen -> dv field updates
        Map<String, Map<Long, DocValuesFieldUpdates>> mappedDVUpdates = new HashMap<>();

        boolean anyDVUpdates = false;

        assert sourceSegments.size() == mergeState.docMaps.length;

        // 遍历参与merge的每个 segment
        for (int i = 0; i < sourceSegments.size(); i++) {
            SegmentCommitInfo info = sourceSegments.get(i);
            // 计算所有segment 下 最小的 bufferedDeletesGen
            minGen = Math.min(info.getBufferedDeletesGen(), minGen);
            final int maxDoc = info.info.maxDoc();
            final ReadersAndUpdates rld = getPooledInstance(info, false);
            // We hold a ref, from when we opened the readers during mergeInit, so it better still be in the pool:
            assert rld != null : "seg=" + info.info.name;

            MergeState.DocMap segDocMap = mergeState.docMaps[i];
            // leafDocMaps 在未使用 IndexSort时  直接返回原docId 不做任何映射处理
            MergeState.DocMap segLeafDocMap = mergeState.leafDocMaps[i];

            // 找到在merge期间被标记成删除的doc  并将删除信息添加到 合并后的segment中
            carryOverHardDeletes(mergedDeletesAndUpdates, maxDoc, mergeState.liveDocs[i], merge.hardLiveDocs.get(i), rld.getHardLiveDocs(),
                    segDocMap, segLeafDocMap);

            // Now carry over all doc values updates that were resolved while we were merging, remapping the docIDs to the newly merged docIDs.
            // We only carry over packets that finished resolving; if any are still running (concurrently) they will detect that our merge completed
            // and re-resolve against the newly merged segment:
            // 将merge期间新增的merge信息作用到merge后的segment上
            Map<String, List<DocValuesFieldUpdates>> mergingDVUpdates = rld.getMergingDVUpdates();
            for (Map.Entry<String, List<DocValuesFieldUpdates>> ent : mergingDVUpdates.entrySet()) {

                String field = ent.getKey();

                Map<Long, DocValuesFieldUpdates> mappedField = mappedDVUpdates.get(field);
                if (mappedField == null) {
                    mappedField = new HashMap<>();
                    mappedDVUpdates.put(field, mappedField);
                }

                // 将这些update信息 按照 delGen 分组
                for (DocValuesFieldUpdates updates : ent.getValue()) {

                    // 还未处理的 gen 不需要提前作用到 merge后的段上   可以看到  BufferedUpdateStream.finishedSegment 都是在 IndexWriter的synchronized 中执行的 所以这里检测到还未处理就不可能发生 中途突然完成的情况
                    if (bufferedUpdatesStream.stillRunning(updates.delGen)) {
                        continue;
                    }

                    // sanity check:
                    assert field.equals(updates.field);

                    DocValuesFieldUpdates mappedUpdates = mappedField.get(updates.delGen);
                    if (mappedUpdates == null) {
                        switch (updates.type) {
                            case NUMERIC:
                                mappedUpdates = new NumericDocValuesFieldUpdates(updates.delGen, updates.field, merge.info.info.maxDoc());
                                break;
                            case BINARY:
                                mappedUpdates = new BinaryDocValuesFieldUpdates(updates.delGen, updates.field, merge.info.info.maxDoc());
                                break;
                            default:
                                throw new AssertionError();
                        }
                        mappedField.put(updates.delGen, mappedUpdates);
                    }

                    DocValuesFieldUpdates.Iterator it = updates.iterator();
                    int doc;
                    while ((doc = it.nextDoc()) != NO_MORE_DOCS) {
                        // 将针对之前segment的某个field 的更新信息作用到merge后的segment上   这里涉及到 从某个segment的doc 映射到 全局segment的doc的逻辑
                        int mappedDoc = segDocMap.get(segLeafDocMap.get(doc));
                        if (mappedDoc != -1) {
                            if (it.hasValue()) {
                                // not deleted
                                mappedUpdates.add(mappedDoc, it);
                            } else {
                                mappedUpdates.reset(mappedDoc);
                            }
                            anyDVUpdates = true;
                        }
                    }
                }
            }
        }


        // 代表至少有一个 update对象作用到了新的segment上
        if (anyDVUpdates) {
            // Persist the merged DV updates onto the RAU for the merged segment:
            // 这里插入的都是被处理过的update   gen不符合的不会被插入到该容器中
            for (Map<Long, DocValuesFieldUpdates> d : mappedDVUpdates.values()) {
                for (DocValuesFieldUpdates updates : d.values()) {
                    // 固化这些update对象后 设置到mergedDeletesAndUpdates 中
                    updates.finish();
                    mergedDeletesAndUpdates.addDVUpdate(updates);
                }
            }
        }

        if (infoStream.isEnabled("IW")) {
            if (mergedDeletesAndUpdates == null) {
                infoStream.message("IW", "no new deletes or field updates since merge started");
            } else {
                String msg = mergedDeletesAndUpdates.getDelCount() - numDeletesBefore + " new deletes";
                if (anyDVUpdates) {
                    msg += " and " + mergedDeletesAndUpdates.getNumDVUpdates() + " new field updates";
                    msg += " (" + mergedDeletesAndUpdates.ramBytesUsed.get() + ") bytes";
                }
                msg += " since merge started";
                infoStream.message("IW", msg);
            }
        }

        // 这个值其实不是特别重要 只要确保merge期间的update不丢失 以及之后新建的update 能作用到该segment (BufferedDeletesGen <= update.delGen) 就可以了
        merge.info.setBufferedDeletesGen(minGen);

        return mergedDeletesAndUpdates;
    }

    /**
     * This method carries over hard-deleted documents that are applied to the source segment during a merge.
     * @param mergedReadersAndUpdates   merge后的段对应的  readersAndUpdates 对象
     * @param maxDoc 参与merge的segment的最大文档数
     * @param mergeLiveDocs  某个参与merge的segment 在merge时使用的liveDoc (包含软删除中确定要删除的部分)
     * @param prevHardLiveDocs   在merge过程中 硬删除的位图
     * @param currentHardLiveDocs  此时参与merge的segment最新的hardLiveDocs    在merge过程中 可能会产生新的删除  只处理硬删除的部分
     * @param segDocMap 该对象负责将 每个参与merge的段doc 映射到一个全局doc
     * 应该是因为 segment已经合并完成了 就可以丢弃之前的数据
     */
    private static void carryOverHardDeletes(ReadersAndUpdates mergedReadersAndUpdates, int maxDoc,
                                             Bits mergeLiveDocs, // the liveDocs used to build the segDocMaps
                                             Bits prevHardLiveDocs, // the hard deletes when the merge reader was pulled
                                             Bits currentHardLiveDocs, // the current hard deletes
                                             MergeState.DocMap segDocMap, MergeState.DocMap segLeafDocMap) throws IOException {

        assert mergeLiveDocs == null || mergeLiveDocs.length() == maxDoc;
        // if we mix soft and hard deletes we need to make sure that we only carry over deletes
        // that were not deleted before. Otherwise the segDocMap doesn't contain a mapping.
        // yet this is also required if any MergePolicy modifies the liveDocs since this is
        // what the segDocMap is build on.
        final IntPredicate carryOverDelete = mergeLiveDocs == null || mergeLiveDocs == prevHardLiveDocs
                ? docId -> currentHardLiveDocs.get(docId) == false   // 如果之前的doc此时被标记成删除了 需要将该doc更新成删除状态
                : docId -> mergeLiveDocs.get(docId) && currentHardLiveDocs.get(docId) == false;  // 代表在之前的merge中有部分doc是通过软删除的机制删除的 那为了避免重复删除
                                                                                                // 需要确保本次被硬删除的doc 之前没有通过软删除机制删除

       // 代表在merge前 segment 就有部分doc被删除  那么必须要检测此时最新的位图与之前位图不同的部分
        if (prevHardLiveDocs != null) {
            // If we had deletions on starting the merge we must
            // still have deletions now:
            assert currentHardLiveDocs != null;
            assert mergeLiveDocs != null;
            assert prevHardLiveDocs.length() == maxDoc;
            assert currentHardLiveDocs.length() == maxDoc;

            // There were deletes on this segment when the merge
            // started.  The merge has collapsed away those
            // deletes, but, if new deletes were flushed since
            // the merge started, we must now carefully keep any
            // newly flushed deletes but mapping them to the new
            // docIDs.

            // Since we copy-on-write, if any new deletes were
            // applied after merging has started, we can just
            // check if the before/after liveDocs have changed.
            // If so, we must carefully merge the liveDocs one
            // doc at a time:
            // 在这个过程中 又有新的doc 被删除了
            if (currentHardLiveDocs != prevHardLiveDocs) {
                // This means this segment received new deletes
                // since we started the merge, so we
                // must merge them:
                for (int j = 0; j < maxDoc; j++) {
                    if (prevHardLiveDocs.get(j) == false) {
                        // if the document was deleted before, it better still be deleted!
                        assert currentHardLiveDocs.get(j) == false;
                    // 之前存在 但是此时不存在的doc
                    } else if (carryOverDelete.test(j)) {
                        // the document was deleted while we were merging:
                        // 将merge后的对象的 该doc 标记成删除
                        mergedReadersAndUpdates.delete(segDocMap.get(segLeafDocMap.get(j)));
                    }
                }
            }
        // 代表之前所有doc 都存活 此时有部分doc被删除
        } else if (currentHardLiveDocs != null) {
            assert currentHardLiveDocs.length() == maxDoc;
            // This segment had no deletes before but now it
            // does:
            // 遍历找到在merge期间删除的doc 并设置到 mergedReadersAndUpdates中
            for (int j = 0; j < maxDoc; j++) {
                if (carryOverDelete.test(j)) {
                    mergedReadersAndUpdates.delete(segDocMap.get(segLeafDocMap.get(j)));
                }
            }
        }
    }

    /**
     * 当merge工作完成时触发
     * @param merge
     * @param mergeState
     * @return
     * @throws IOException
     */
    @SuppressWarnings("try")
    private synchronized boolean commitMerge(MergePolicy.OneMerge merge, MergeState mergeState) throws IOException {

        testPoint("startCommitMerge");

        if (tragedy.get() != null) {
            throw new IllegalStateException("this writer hit an unrecoverable error; cannot complete merge", tragedy.get());
        }

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "commitMerge: " + segString(merge.segments) + " index=" + segString());
        }

        assert merge.registerDone;

        // If merge was explicitly aborted, or, if rollback() or
        // rollbackTransaction() had been called since our merge
        // started (which results in an unqualified
        // deleter.refresh() call that will remove any index
        // file that current segments does not reference), we
        // abort this merge
        // 如果本次merge 在执行过程中被终止了
        if (merge.isAborted()) {
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "commitMerge: skip: it was aborted");
            }
            // In case we opened and pooled a reader for this
            // segment, drop it now.  This ensures that we close
            // the reader before trying to delete any of its
            // files.  This is not a very big deal, since this
            // reader will never be used by any NRT reader, and
            // another thread is currently running close(false)
            // so it will be dropped shortly anyway, but not
            // doing this  makes  MockDirWrapper angry in
            // TestNRTThreads (LUCENE-5434):
            // 如果该segment 已经设置到 readerPool中 就进行移除 这样方便删除相关文件
            readerPool.drop(merge.info);
            // Safe: these files must exist:
            // 将该段下维护的所有相关文件都删除
            deleteNewFiles(merge.info.files());
            return false;
        }

        // 避免在merge过程中  update/delete 信息丢失
        final ReadersAndUpdates mergedUpdates = merge.info.info.maxDoc() == 0 ? null : commitMergedDeletesAndUpdates(merge, mergeState);

        // If the doc store we are using has been closed and
        // is in now compound format (but wasn't when we
        // started), then we will switch to the compound
        // format as well:

        assert !segmentInfos.contains(merge.info);

        // 最新的segment 是否为空 或者 在处理了最新的delete信息后 变成空
        final boolean allDeleted = merge.segments.size() == 0 ||
                merge.info.info.maxDoc() == 0 ||
                (mergedUpdates != null && isFullyDeleted(mergedUpdates));

        if (infoStream.isEnabled("IW")) {
            if (allDeleted) {
                infoStream.message("IW", "merged segment " + merge.info + " is 100% deleted; skipping insert");
            }
        }

        final boolean dropSegment = allDeleted;

        // If we merged no segments then we better be dropping
        // the new segment:
        assert merge.segments.size() > 0 || dropSegment;

        assert merge.info.info.maxDoc() != 0 || dropSegment;

        if (mergedUpdates != null) {
            boolean success = false;
            try {
                if (dropSegment) {
                    mergedUpdates.dropChanges();
                }
                // Pass false for assertInfoLive because the merged
                // segment is not yet live (only below do we commit it
                // to the segmentInfos):
                // 仅减少引用计数 实际上没有从池中移除
                release(mergedUpdates, false);
                success = true;
            } finally {
                if (!success) {
                    mergedUpdates.dropChanges();
                    readerPool.drop(merge.info);
                }
            }
        }

        // Must do this after readerPool.release, in case an
        // exception is hit e.g. writing the live docs for the
        // merge segment, in which case we need to abort the
        // merge:
        // 将merge后的段发布到IndexWriter中 同时将之前参与merge的段从 indexWriter中移除  这样forceApply 就不会更新参与过merge的segment了
        // (也没必要再维护这些旧的segment了 同时使用者要经常性调用 IndexReader.openIfChanged 确保读取到最新的segment)
        segmentInfos.applyMergeChanges(merge, dropSegment);

        // Now deduct the deleted docs that we just reclaimed from this
        // merge:
        int delDocCount;
        if (dropSegment) {
            // if we drop the segment we have to reduce the pendingNumDocs by merge.totalMaxDocs since we never drop
            // the docs when we apply deletes if the segment is currently merged.
            delDocCount = merge.totalMaxDoc;
        } else {
            delDocCount = merge.totalMaxDoc - merge.info.info.maxDoc();
        }
        assert delDocCount >= 0;
        // 更新此时维护的doc总数
        adjustPendingNumDocs(-delDocCount);

        if (dropSegment) {
            assert !segmentInfos.contains(merge.info);
            readerPool.drop(merge.info);
            // Safe: these files must exist
            deleteNewFiles(merge.info.files());
        }

        // 因为 segmentInfos 中已经释放了之前参与merge的segment 在checkpoint中 这些segment的引用计数就会归0  这样就达到删除索引文件的目的    (同时基于引用计数的删除确保是安全的)
        try (Closeable finalizer = this::checkpoint) {
            // Must close before checkpoint, otherwise IFD won't be
            // able to delete the held-open files from the merge
            // readers:
            // 将参与merge的所有段对应的reader对象关闭 否则无法删除索引文件
            closeMergeReaders(merge, false);
        }

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "after commitMerge: " + segString());
        }

        // TODO 这个是啥时候设置的
        if (merge.maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS && !dropSegment) {
            // cascade the forceMerge:
            if (!segmentsToMerge.containsKey(merge.info)) {
                segmentsToMerge.put(merge.info, Boolean.FALSE);
            }
        }

        return true;
    }

    private void handleMergeException(Throwable t, MergePolicy.OneMerge merge) throws IOException {

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "handleMergeException: merge=" + segString(merge.segments) + " exc=" + t);
        }

        // Set the exception on the merge, so if
        // forceMerge is waiting on us it sees the root
        // cause exception:
        merge.setException(t);
        addMergeException(merge);

        if (t instanceof MergePolicy.MergeAbortedException) {
            // We can ignore this exception (it happens when
            // deleteAll or rollback is called), unless the
            // merge involves segments from external directories,
            // in which case we must throw it so, for example, the
            // rollbackTransaction code in addIndexes* is
            // executed.
            if (merge.isExternal) { // TODO can we simplify this and just throw all the time? this would simplify this a lot
                throw (MergePolicy.MergeAbortedException) t;
            }
        } else {
            assert t != null;
            throw IOUtils.rethrowAlways(t);
        }
    }

    /**
     * Merges the indicated segments, replacing them in the stack with a
     * single segment.
     *
     * @lucene.experimental
     * 执行merge任务
     */
    protected void merge(MergePolicy.OneMerge merge) throws IOException {

        boolean success = false;

        final long t0 = System.currentTimeMillis();

        // 一般就是 TieredMergePolicy
        final MergePolicy mergePolicy = config.getMergePolicy();
        try {
            try {
                try {
                    // 做一些准备工作  主要就是等待之前的update处理完毕 以及初始化 merge后的segment
                    mergeInit(merge);

                    if (infoStream.isEnabled("IW")) {
                        infoStream.message("IW", "now merge\n  merge=" + segString(merge.segments) + "\n  index=" + segString());
                    }

                    // 主要的merge 逻辑
                    mergeMiddle(merge, mergePolicy);
                    // 触发后置钩子
                    mergeSuccess(merge);
                    success = true;
                } catch (Throwable t) {
                    handleMergeException(t, merge);
                }
            } finally {
                synchronized (this) {

                    // 从merge的相关容器中移除merge对象 同时唤醒等待merge的线程
                    mergeFinish(merge);

                    if (success == false) {
                        if (infoStream.isEnabled("IW")) {
                            infoStream.message("IW", "hit exception during merge");
                        }
                        // TODO maxNumSegments 什么时候不是-1
                    } else if (!merge.isAborted() && (merge.maxNumSegments != UNBOUNDED_MAX_MERGE_SEGMENTS || (!closed && !closing))) {
                        // This merge (and, generally, any change to the
                        // segments) may now enable new merges, so we call
                        // merge policy & update pending merges.
                        updatePendingMerges(mergePolicy, MergeTrigger.MERGE_FINISHED, merge.maxNumSegments);
                    }
                }
            }
        } catch (Throwable t) {
            // Important that tragicEvent is called after mergeFinish, else we hang
            // waiting for our merge thread to be removed from runningMerges:
            tragicEvent(t, "merge");
            throw t;
        }

        if (merge.info != null && merge.isAborted() == false) {
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "merge time " + (System.currentTimeMillis() - t0) + " msec for " + merge.info.info.maxDoc() + " docs");
            }
        }
    }

    /**
     * Hook that's called when the specified merge is complete.
     */
    protected void mergeSuccess(MergePolicy.OneMerge merge) {
    }

    /**
     * Checks whether this merge involves any segments
     * already participating in a merge.  If not, this merge
     * is "registered", meaning we record that its segments
     * are now participating in a merge, and true is
     * returned.  Else (the merge conflicts) false is
     * returned.
     * 将某个 OneMerge 对象注册到merge任务中
     */
    private synchronized boolean registerMerge(MergePolicy.OneMerge merge) throws IOException {

        // 已经注册过的对象 忽略
        if (merge.registerDone) {
            return true;
        }
        assert merge.segments.size() > 0;

        // 此时IndexWriter已经不再执行merge任务了 抛出异常
        if (stopMerges) {
            // 终止内部的 mergeThread
            merge.setAborted();
            throw new MergePolicy.MergeAbortedException("merge is aborted: " + segString(merge.segments));
        }

        boolean isExternal = false;
        // 本次参与merge的所有 segment
        for (SegmentCommitInfo info : merge.segments) {
            // 如果当前merge关联的segment 正在merge中 无法注册该merge 任务
            if (mergingSegments.contains(info)) {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "reject merge " + segString(merge.segments) + ": segment " + segString(info) + " is already marked for merge");
                }
                return false;
            }
            // 出现了未被 writer管理的 segment 忽略
            if (!segmentInfos.contains(info)) {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "reject merge " + segString(merge.segments) + ": segment " + segString(info) + " does not exist in live infos");
                }
                return false;
            }
            // 目录不一致 代表本次merge的数据来自于外部
            if (info.info.dir != directoryOrig) {
                isExternal = true;
            }

            // TODO
            if (segmentsToMerge.containsKey(info)) {
                merge.maxNumSegments = mergeMaxNumSegments;
            }
        }

        ensureValidMerge(merge);

        // 加入到待merge的队列中
        pendingMerges.add(merge);

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "add merge to pendingMerges: " + segString(merge.segments) + " [total " + pendingMerges.size() + " pending]");
        }

        merge.mergeGen = mergeGen;
        merge.isExternal = isExternal;

        // OK it does not conflict; now record that this merge
        // is running (while synchronized) to avoid race
        // condition where two conflicting merges from different
        // threads, start
        if (infoStream.isEnabled("IW")) {
            StringBuilder builder = new StringBuilder("registerMerge merging= [");
            for (SegmentCommitInfo info : mergingSegments) {
                builder.append(info.info.name).append(", ");
            }
            builder.append("]");
            // don't call mergingSegments.toString() could lead to ConcurrentModException
            // since merge updates the segments FieldInfos
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", builder.toString());
            }
        }
        // 将关联的 segment 加入到 mergingSegment中
        for (SegmentCommitInfo info : merge.segments) {
            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "registerMerge info=" + segString(info));
            }
            mergingSegments.add(info);
        }

        assert merge.estimatedMergeBytes == 0;
        assert merge.totalMergeBytes == 0;
        for (SegmentCommitInfo info : merge.segments) {
            if (info.info.maxDoc() > 0) {
                // 查询当前段对象中 有多少doc 将会被删除
                final int delCount = numDeletedDocs(info);
                assert delCount <= info.info.maxDoc();
                // 计算该segment的del比率
                final double delRatio = ((double) delCount) / info.info.maxDoc();
                // 累加参与merge的 segment 空间总数
                merge.estimatedMergeBytes += info.sizeInBytes() * (1.0 - delRatio);
                merge.totalMergeBytes += info.sizeInBytes();
            }
        }

        // Merge is now registered
        merge.registerDone = true;

        return true;
    }

    /**
     * Does initial setup for a merge, which is fast but holds
     * the synchronized lock on IndexWriter instance.
     * 为merge  做一些准备工作
     */
    final void mergeInit(MergePolicy.OneMerge merge) throws IOException {
        assert Thread.holdsLock(this) == false;
        // Make sure any deletes that must be resolved before we commit the merge are complete:
        // 在merge前 等待所有段对应的更新/删除任务完成   注意此时 最新的 DV fieldInfo  liveDoc 不一定刷盘了
        bufferedUpdatesStream.waitApplyForMerge(merge.segments, this);

        boolean success = false;
        try {
            // 主要就是等待 update处理完毕 并将最新的DV 信息 和 fieldInfo 写入到索引文件 还将本次涉及到的所有segment 关联的 ReadersAndUpdates 都标记成 merging
            // 同时初始化 merge后的目标segment
            _mergeInit(merge);
            success = true;
        } finally {
            if (!success) {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "hit exception in mergeInit");
                }
                mergeFinish(merge);
            }
        }
    }

    /**
     * 在merge前执行一些初始化工作
     * @param merge
     * @throws IOException
     */
    private synchronized void _mergeInit(MergePolicy.OneMerge merge) throws IOException {

        testPoint("startMergeInit");

        assert merge.registerDone;
        assert merge.maxNumSegments == UNBOUNDED_MAX_MERGE_SEGMENTS || merge.maxNumSegments > 0;

        if (tragedy.get() != null) {
            throw new IllegalStateException("this writer hit an unrecoverable error; cannot merge", tragedy.get());
        }

        // 如果该merge最终产生的目标 segment 已经被设置 那么直接返回
        if (merge.info != null) {
            // mergeInit already done
            return;
        }

        // 为merge 关联的process对象设置工作线程 这样在进行merge时 就可以触发限流功能了  算是merge前必要的准备工作
        merge.mergeInit();

        // 如果merge对象因为某些原因已经被禁用了  不再继续处理
        if (merge.isAborted()) {
            return;
        }

        // TODO: in the non-pool'd case this is somewhat
        // wasteful, because we open these readers, close them,
        // and then open them again for merging.  Maybe  we
        // could pre-pool them somehow in that case...

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "now apply deletes for " + merge.segments.size() + " merging segments");
        }

        // Must move the pending doc values updates to disk now, else the newly merged segment will not see them:
        // TODO: we could fix merging to pull the merged DV iterator so we don't have to move these updates to disk first, i.e. just carry them
        // in memory:
        // 将 docValue信息写入到索引文件中 并将 ReadersAndUpdates 对象标记成 isMerging
        if (readerPool.writeDocValuesUpdatesForMerge(merge.segments)) {
            // 更新segmentInfos的版本信息  同时为最新生成的索引文件增加引用计数
            checkpoint();
        }

        // Bind a new segment name here so even with
        // ConcurrentMergePolicy we keep deterministic segment
        // names.

        // 这里生成 merge后的段名
        final String mergeSegmentName = newSegmentName();
        // We set the min version to null for now, it will be set later by SegmentMerger
        // 生成一个新的 段对象
        SegmentInfo si = new SegmentInfo(directoryOrig, Version.LATEST, null, mergeSegmentName, -1, false, config.getCodec(),
                Collections.emptyMap(), StringHelper.randomId(), Collections.emptyMap(), config.getIndexSort());

        // 设置诊断信息
        Map<String, String> details = new HashMap<>();
        details.put("mergeMaxNumSegments", "" + merge.maxNumSegments);
        details.put("mergeFactor", Integer.toString(merge.segments.size()));
        setDiagnostics(si, SOURCE_MERGE, details);

        // 这里提前将 commitInfo对象创建好
        merge.setMergeInfo(new SegmentCommitInfo(si, 0, 0, -1L, -1L, -1L, StringHelper.randomId()));

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "merge seg=" + merge.info.info.name + " " + segString(merge.segments));
        }
    }

    static void setDiagnostics(SegmentInfo info, String source) {
        setDiagnostics(info, source, null);
    }

    /**
     * 设置一些用于诊断的描述信息
     * @param info
     * @param source
     * @param details
     */
    private static void setDiagnostics(SegmentInfo info, String source, Map<String, String> details) {
        Map<String, String> diagnostics = new HashMap<>();
        diagnostics.put("source", source);
        diagnostics.put("lucene.version", Version.LATEST.toString());
        diagnostics.put("os", Constants.OS_NAME);
        diagnostics.put("os.arch", Constants.OS_ARCH);
        diagnostics.put("os.version", Constants.OS_VERSION);
        diagnostics.put("java.version", Constants.JAVA_VERSION);
        diagnostics.put("java.vendor", Constants.JAVA_VENDOR);
        // On IBM J9 JVM this is better than java.version which is just 1.7.0 (no update level):
        diagnostics.put("java.runtime.version", System.getProperty("java.runtime.version", "undefined"));
        // Hotspot version, e.g. 2.8 for J9:
        diagnostics.put("java.vm.version", System.getProperty("java.vm.version", "undefined"));
        diagnostics.put("timestamp", Long.toString(System.currentTimeMillis()));
        if (details != null) {
            diagnostics.putAll(details);
        }
        info.setDiagnostics(diagnostics);
    }

    /**
     * Does finishing for a merge, which is fast but holds
     * the synchronized lock on IndexWriter instance.
     * 当某个任务完成时 会回调该方法
     */
    private synchronized void mergeFinish(MergePolicy.OneMerge merge) {

        // forceMerge, addIndexes or waitForMerges may be waiting
        // on merges to finish.
        // 与waitForMerge相互作用 外部通过调用 waitForMerges 等待merge完成 而在这里进行唤醒
        notifyAll();

        // It's possible we are called twice, eg if there was an
        // exception inside mergeInit
        if (merge.registerDone) {
            final List<SegmentCommitInfo> sourceSegments = merge.segments;
            for (SegmentCommitInfo info : sourceSegments) {
                mergingSegments.remove(info);
            }
            merge.registerDone = false;
        }

        runningMerges.remove(merge);
    }

    @SuppressWarnings("try")
    private synchronized void closeMergeReaders(MergePolicy.OneMerge merge, boolean suppressExceptions) throws IOException {
        final boolean drop = suppressExceptions == false;
        try (Closeable finalizer = merge::mergeFinished) {
            // 使用 第二个参数处理每个 readers
            IOUtils.applyToAll(merge.readers, sr -> {
                final ReadersAndUpdates rld = getPooledInstance(sr.getOriginalSegmentInfo(), false);
                // We still hold a ref so it should not have been removed:
                assert rld != null;
                if (drop) {
                    rld.dropChanges();
                } else {
                    rld.dropMergingUpdates();
                }
                rld.release(sr);
                release(rld);
                if (drop) {
                    readerPool.drop(rld.info);
                }
            });
        } finally {
            Collections.fill(merge.readers, null);
        }
    }

    /**
     * 统计本次未处理的软删除的数量 用于更新segmentInfo.softDelCount
     * @param reader
     * @param wrappedLiveDocs
     * @param hardLiveDocs
     * @param softDeleteCounter
     * @param hardDeleteCounter
     * @throws IOException
     */
    private void countSoftDeletes(CodecReader reader, Bits wrappedLiveDocs, Bits hardLiveDocs, Counter softDeleteCounter,
                                  Counter hardDeleteCounter) throws IOException {
        int hardDeleteCount = 0;
        int softDeletesCount = 0;
        DocIdSetIterator softDeletedDocs = DocValuesFieldExistsQuery.getDocValuesDocIdSetIterator(config.getSoftDeletesField(), reader);
        if (softDeletedDocs != null) {
            int docId;
            while ((docId = softDeletedDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (wrappedLiveDocs == null || wrappedLiveDocs.get(docId)) {
                    if (hardLiveDocs == null || hardLiveDocs.get(docId)) {
                        softDeletesCount++;
                    } else {
                        hardDeleteCount++;
                    }
                }
            }
        }
        softDeleteCounter.addAndGet(softDeletesCount);
        hardDeleteCounter.addAndGet(hardDeleteCount);
    }

    private boolean assertSoftDeletesCount(CodecReader reader, int expectedCount) throws IOException {
        Counter count = Counter.newCounter(false);
        Counter hardDeletes = Counter.newCounter(false);
        countSoftDeletes(reader, reader.getLiveDocs(), null, count, hardDeletes);
        assert count.get() == expectedCount : "soft-deletes count mismatch expected: "
                + expectedCount + " but actual: " + count.get();
        return true;
    }

    /**
     * Does the actual (time-consuming) work of the merge,
     * but without holding synchronized lock on IndexWriter
     * instance
     *
     * merge的核心逻辑就在这里
     */
    private int mergeMiddle(MergePolicy.OneMerge merge, MergePolicy mergePolicy) throws IOException {
        testPoint("mergeMiddleStart");
        merge.checkAborted();

        // 包装后 使用该目录创建的输出流 具备限流能力  这样就可以调配 不同merge任务间的速度了  通过限制大数据块的merge任务 可以确保小数据块优先并快速的完成 (并发的merge任务越多 对IO的争用越激烈 就会增加越多的磁道寻址)
        Directory mergeDirectory = mergeScheduler.wrapForMerge(merge, directory);
        List<SegmentCommitInfo> sourceSegments = merge.segments;

        // 将一些merge的相关信息提取出来 生成 MergeInfo  并设置到上下文中
        IOContext context = new IOContext(merge.getStoreMergeInfo());

        // 进一步包装 生成会记录创建的文件的 目录对象
        final TrackingDirectoryWrapper dirWrapper = new TrackingDirectoryWrapper(mergeDirectory);

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "merging " + segString(merge.segments));
        }

        merge.readers = new ArrayList<>(sourceSegments.size());
        merge.hardLiveDocs = new ArrayList<>(sourceSegments.size());

        // This is try/finally to make sure merger's readers are
        // closed:
        boolean success = false;
        try {
            int segUpto = 0;
            // 这里填充 merge需要的必备属性
            while (segUpto < sourceSegments.size()) {

                final SegmentCommitInfo info = sourceSegments.get(segUpto);

                // Hold onto the "live" reader; we will use this to
                // commit merged deletes
                // 获取该对象对应的reader 对象
                final ReadersAndUpdates rld = getPooledInstance(info, true);
                rld.setIsMerging();

                // 确保此时 liveDoc 也是最新的   同时将pendingDV 拷贝一份到 mergingDV中
                ReadersAndUpdates.MergeReader mr = rld.getReaderForMerge(context);
                SegmentReader reader = mr.reader;

                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "seg=" + segString(info) + " reader=" + reader);
                }

                // 将最新的数据 回填到merge中
                merge.hardLiveDocs.add(mr.hardLiveDocs);
                merge.readers.add(reader);
                segUpto++;
            }

            // Let the merge wrap readers
            List<CodecReader> mergeReaders = new ArrayList<>();
            // 记录未处理的软删除数量  本次softMergePolicy 没有命中的doc 将会被删除 就不再需要维护它了
            Counter softDeleteCount = Counter.newCounter(false);
            for (int r = 0; r < merge.readers.size(); r++) {
                // 获取上面设置的reader对象
                SegmentReader reader = merge.readers.get(r);
                // 这里使用 merge对象对原本的 reader对象进行加工  默认情况下直接返回reader  相当于对子类开放的钩子
                // 当如果是使用SoftDeletesRetentionMergePolicy时  初始化该对象需要传入一个query 并且该query命中的doc会从软删除状态变成存活状态
                CodecReader wrappedReader = merge.wrapForMerge(reader);
                validateMergeReader(wrappedReader);

                // 当开启软删除时
                if (softDeletesEnabled) {
                    // 如果不使用软删除对应的 mergePolicy 包装reader对象 那么软删除的doc都会被删除
                    if (reader != wrappedReader) { // if we don't have a wrapped reader we won't preserve any soft-deletes
                        Bits hardLiveDocs = merge.hardLiveDocs.get(r);
                        if (hardLiveDocs != null) { // we only need to do this accounting if we have mixed deletes
                            Bits wrappedLiveDocs = wrappedReader.getLiveDocs();
                            Counter hardDeleteCounter = Counter.newCounter(false);
                            countSoftDeletes(wrappedReader, wrappedLiveDocs, hardLiveDocs, softDeleteCount, hardDeleteCounter);
                            int hardDeleteCount = Math.toIntExact(hardDeleteCounter.get());
                            // Wrap the wrapped reader again if we have excluded some hard-deleted docs
                            // 可能在恢复软删除的doc时 将一些之前硬删除的doc也恢复了 这里要重新标记成删除
                            if (hardDeleteCount > 0) {
                                Bits liveDocs = wrappedLiveDocs == null ? hardLiveDocs : new Bits() {
                                    @Override
                                    public boolean get(int index) {
                                        return hardLiveDocs.get(index) && wrappedLiveDocs.get(index);
                                    }

                                    @Override
                                    public int length() {
                                        return hardLiveDocs.length();
                                    }
                                };
                                wrappedReader = FilterCodecReader.wrapLiveDocs(wrappedReader, liveDocs, wrappedReader.numDocs() - hardDeleteCount);
                            }
                        } else {
                            final int carryOverSoftDeletes = reader.getSegmentInfo().getSoftDelCount() - wrappedReader.numDeletedDocs();
                            assert carryOverSoftDeletes >= 0 : "carry-over soft-deletes must be positive";
                            assert assertSoftDeletesCount(wrappedReader, carryOverSoftDeletes);
                            softDeleteCount.addAndGet(carryOverSoftDeletes);
                        }
                    }
                }
                // 保存包装后的结果
                mergeReaders.add(wrappedReader);
            }

            // 将参与merge 的 reader 以及目标segment 和包装后的特殊目录合并成 SegmentMerge对象
            final SegmentMerger merger = new SegmentMerger(mergeReaders,
                    merge.info.info, infoStream, dirWrapper,
                    globalFieldNumberMap,
                    context);

            // 更新未处理的软删除数量
            merge.info.setSoftDelCount(Math.toIntExact(softDeleteCount.get()));

            // 检测merge是否被终止
            merge.checkAborted();

            // 开始记录merge耗时
            merge.mergeStartNS = System.nanoTime();

            // This is where all the work happens:
            // 确保此时合并后的doc总数>0 否则不需要处理 直接删除旧数据就好
            if (merger.shouldMerge()) {
                // 执行真正的merge操作   在这里合并最核心的 term数据时 并没有将未存活的doc相关的数据剔除掉
                merger.merge();
            }

            MergeState mergeState = merger.mergeState;
            assert mergeState.segmentInfo == merge.info.info;
            // 将期间创建的所有索引文件设置到 segmentInfo中
            merge.info.info.setFiles(new HashSet<>(dirWrapper.getCreatedFiles()));
            Codec codec = config.getCodec();
            // ignore
            if (infoStream.isEnabled("IW")) {
                if (merger.shouldMerge()) {
                    String pauseInfo = merge.getMergeProgress().getPauseTimes().entrySet()
                            .stream()
                            .filter((e) -> e.getValue() > 0)
                            .map((e) -> String.format(Locale.ROOT, "%.1f sec %s",
                                    e.getValue() / 1000000000.,
                                    e.getKey().name().toLowerCase(Locale.ROOT)))
                            .collect(Collectors.joining(", "));
                    if (!pauseInfo.isEmpty()) {
                        pauseInfo = " (" + pauseInfo + ")";
                    }

                    long t1 = System.nanoTime();
                    double sec = (t1 - merge.mergeStartNS) / 1000000000.;
                    double segmentMB = (merge.info.sizeInBytes() / 1024. / 1024.);
                    infoStream.message("IW", "merge codec=" + codec + " maxDoc=" + merge.info.info.maxDoc() + "; merged segment has " +
                            (mergeState.mergeFieldInfos.hasVectors() ? "vectors" : "no vectors") + "; " +
                            (mergeState.mergeFieldInfos.hasNorms() ? "norms" : "no norms") + "; " +
                            (mergeState.mergeFieldInfos.hasDocValues() ? "docValues" : "no docValues") + "; " +
                            (mergeState.mergeFieldInfos.hasProx() ? "prox" : "no prox") + "; " +
                            (mergeState.mergeFieldInfos.hasProx() ? "freqs" : "no freqs") + "; " +
                            (mergeState.mergeFieldInfos.hasPointValues() ? "points" : "no points") + "; " +
                            String.format(Locale.ROOT,
                                    "%.1f sec%s to merge segment [%.2f MB, %.2f MB/sec]",
                                    sec,
                                    pauseInfo,
                                    segmentMB,
                                    segmentMB / sec));
                } else {
                    infoStream.message("IW", "skip merging fully deleted segments");
                }
            }

            if (merger.shouldMerge() == false) {
                // Merge would produce a 0-doc segment, so we do nothing except commit the merge to remove all the 0-doc segments that we "merged":
                assert merge.info.info.maxDoc() == 0;
                // 当某次merge工作完成时 触发
                commitMerge(merge, mergeState);
                return 0;
            }

            assert merge.info.info.maxDoc() > 0;

            // Very important to do this before opening the reader
            // because codec must know if prox was written for
            // this segment:
            boolean useCompoundFile;
            synchronized (this) { // Guard segmentInfos
                useCompoundFile = mergePolicy.useCompoundFile(segmentInfos, merge.info, this);
            }

            // TODO 忽略复合文件
            if (useCompoundFile) {
                success = false;

                Collection<String> filesToRemove = merge.info.files();
                TrackingDirectoryWrapper trackingCFSDir = new TrackingDirectoryWrapper(mergeDirectory);
                try {
                    createCompoundFile(infoStream, trackingCFSDir, merge.info.info, context, this::deleteNewFiles);
                    success = true;
                } catch (Throwable t) {
                    synchronized (this) {
                        if (merge.isAborted()) {
                            // This can happen if rollback is called while we were building
                            // our CFS -- fall through to logic below to remove the non-CFS
                            // merged files:
                            if (infoStream.isEnabled("IW")) {
                                infoStream.message("IW", "hit merge abort exception creating compound file during merge");
                            }
                            return 0;
                        } else {
                            handleMergeException(t, merge);
                        }
                    }
                } finally {
                    if (success == false) {
                        if (infoStream.isEnabled("IW")) {
                            infoStream.message("IW", "hit exception creating compound file during merge");
                        }
                        // Safe: these files must exist
                        deleteNewFiles(merge.info.files());
                    }
                }

                // So that, if we hit exc in deleteNewFiles (next)
                // or in commitMerge (later), we close the
                // per-segment readers in the finally clause below:
                success = false;

                synchronized (this) {

                    // delete new non cfs files directly: they were never
                    // registered with IFD
                    deleteNewFiles(filesToRemove);

                    if (merge.isAborted()) {
                        if (infoStream.isEnabled("IW")) {
                            infoStream.message("IW", "abort merge after building CFS");
                        }
                        // Safe: these files must exist
                        deleteNewFiles(merge.info.files());
                        return 0;
                    }
                }

                merge.info.info.setUseCompoundFile(true);
            } else {
                // So that, if we hit exc in commitMerge (later),
                // we close the per-segment readers in the finally
                // clause below:
                success = false;
            }

            // Have codec write SegmentInfo.  Must do this after
            // creating CFS so that 1) .si isn't slurped into CFS,
            // and 2) .si reflects useCompoundFile=true change
            // above:
            boolean success2 = false;
            try {
                // 这里将merge后最新的 segment 信息写入到索引文件
                codec.segmentInfoFormat().write(directory, merge.info.info, context);
                success2 = true;
            } finally {
                if (!success2) {
                    // Safe: these files must exist
                    // 段信息写入失败时 连同创建的所有索引文件一起删除
                    deleteNewFiles(merge.info.files());
                }
            }

            // TODO: ideally we would freeze merge.info here!!
            // because any changes after writing the .si will be
            // lost...

            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", String.format(Locale.ROOT, "merged segment size=%.3f MB vs estimate=%.3f MB", merge.info.sizeInBytes() / 1024. / 1024., merge.estimatedMergeBytes / 1024 / 1024.));
            }

            // 一般情况下 warmer对象为null 可以先忽略
            final IndexReaderWarmer mergedSegmentWarmer = config.getMergedSegmentWarmer();
            if (readerPool.isReaderPoolingEnabled() && mergedSegmentWarmer != null) {
                final ReadersAndUpdates rld = getPooledInstance(merge.info, true);
                final SegmentReader sr = rld.getReader(IOContext.READ);
                try {
                    mergedSegmentWarmer.warm(sr);
                } finally {
                    synchronized (this) {
                        rld.release(sr);
                        release(rld);
                    }
                }
            }

            // 检测merge过程中的 delete/update 并设置到 merge后的segment上 之后将segment发布
            if (!commitMerge(merge, mergeState)) {
                // commitMerge will return false if this merge was
                // aborted
                return 0;
            }

            success = true;

        } finally {
            // Readers are already closed in commitMerge if we didn't hit
            // an exc:
            if (success == false) {
                closeMergeReaders(merge, true);
            }
        }

        // 返回合并后的总doc数
        return merge.info.info.maxDoc();
    }

    private synchronized void addMergeException(MergePolicy.OneMerge merge) {
        assert merge.getException() != null;
        if (!mergeExceptions.contains(merge) && mergeGen == merge.mergeGen) {
            mergeExceptions.add(merge);
        }
    }

    // For test purposes.
    final int getBufferedDeleteTermsSize() {
        return docWriter.getBufferedDeleteTermsSize();
    }

    // For test purposes.
    final int getNumBufferedDeleteTerms() {
        return docWriter.getNumBufferedDeleteTerms();
    }

    // utility routines for tests
    synchronized SegmentCommitInfo newestSegment() {
        return segmentInfos.size() > 0 ? segmentInfos.info(segmentInfos.size() - 1) : null;
    }

    /**
     * Returns a string description of all segments, for
     * debugging.
     *
     * @lucene.internal
     */
    synchronized String segString() {
        return segString(segmentInfos);
    }

    synchronized String segString(Iterable<SegmentCommitInfo> infos) {
        return StreamSupport.stream(infos.spliterator(), false)
                .map(this::segString).collect(Collectors.joining(" "));
    }

    /**
     * Returns a string description of the specified
     * segment, for debugging.
     *
     * @lucene.internal
     */
    private synchronized String segString(SegmentCommitInfo info) {
        return info.toString(numDeletedDocs(info) - info.getDelCount(softDeletesEnabled));
    }

    private synchronized void doWait() {
        // NOTE: the callers of this method should in theory
        // be able to do simply wait(), but, as a defense
        // against thread timing hazards where notifyAll()
        // fails to be called, we wait for at most 1 second
        // and then return so caller can check if wait
        // conditions are satisfied:
        try {
            wait(1000);
        } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
        }
    }

    // called only from assert
    private boolean filesExist(SegmentInfos toSync) throws IOException {

        Collection<String> files = toSync.files(false);
        for (final String fileName : files) {
            // If this trips it means we are missing a call to
            // .checkpoint somewhere, because by the time we
            // are called, deleter should know about every
            // file referenced by the current head
            // segmentInfos:
            assert deleter.exists(fileName) : "IndexFileDeleter doesn't know about file " + fileName;
        }
        return true;
    }

    // For infoStream output
    synchronized SegmentInfos toLiveInfos(SegmentInfos sis) {
        final SegmentInfos newSIS = new SegmentInfos(sis.getIndexCreatedVersionMajor());
        final Map<SegmentCommitInfo, SegmentCommitInfo> liveSIS = new HashMap<>();
        for (SegmentCommitInfo info : segmentInfos) {
            liveSIS.put(info, info);
        }
        for (SegmentCommitInfo info : sis) {
            SegmentCommitInfo liveInfo = liveSIS.get(info);
            if (liveInfo != null) {
                info = liveInfo;
            }
            newSIS.add(info);
        }

        return newSIS;
    }

    /**
     * Walk through all files referenced by the current
     * segmentInfos and ask the Directory to sync each file,
     * if it wasn't already.  If that succeeds, then we
     * prepare a new segments_N file but do not fully commit
     * it.
     *
     * 为生成 segment_N 做准备
     * @param toSync 参与生成segment_N 的数据    实际上就是  IndexWriter.segmentInfos 的副本
     */
    private void startCommit(final SegmentInfos toSync) throws IOException {

        testPoint("startStartCommit");
        assert pendingCommit == null;

        if (tragedy.get() != null) {
            throw new IllegalStateException("this writer hit an unrecoverable error; cannot commit", tragedy.get());
        }

        try {

            if (infoStream.isEnabled("IW")) {
                infoStream.message("IW", "startCommit(): start");
            }

            synchronized (this) {

                // 上次提交时对应的变化次数 不可能比当前变化次数大
                if (lastCommitChangeCount > changeCount.get()) {
                    throw new IllegalStateException("lastCommitChangeCount=" + lastCommitChangeCount + ",changeCount=" + changeCount);
                }

                // 这种情况是可能的  也就是本次没有任何数据变化
                if (pendingCommitChangeCount == lastCommitChangeCount) {
                    if (infoStream.isEnabled("IW")) {
                        infoStream.message("IW", "  skip startCommit(): no changes pending");
                    }
                    try {
                        deleter.decRef(filesToCommit);
                    } finally {
                        filesToCommit = null;
                    }
                    return;
                }

                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "startCommit index=" + segString(toLiveInfos(toSync)) + " changeCount=" + changeCount);
                }

                assert filesExist(toSync);
            }

            testPoint("midStartCommit");

            boolean pendingCommitSet = false;

            try {

                testPoint("midStartCommit2");

                synchronized (this) {

                    assert pendingCommit == null;

                    assert segmentInfos.getGeneration() == toSync.getGeneration();

                    // Exception here means nothing is prepared
                    // (this method unwinds everything it did on
                    // an exception)
                    // 主要就是将 segmentInfos下所有的 segment信息写入到一个 pending_segments 索引文件中
                    toSync.prepareCommit(directory);
                    if (infoStream.isEnabled("IW")) {
                        infoStream.message("IW", "startCommit: wrote pending segments file \"" + IndexFileNames.fileNameFromGeneration(IndexFileNames.PENDING_SEGMENTS, "", toSync.getGeneration()) + "\"");
                    }

                    pendingCommitSet = true;
                    pendingCommit = toSync;
                }

                // This call can take a long time -- 10s of seconds
                // or more.  We do it without syncing on this:
                boolean success = false;
                final Collection<String> filesToSync;
                try {
                    // 获取此时所有segment下的索引文件 不包含 segment_N 文件  segment_N 是上次commit时生成的描述该indexWriter下活跃segment信息的文件
                    filesToSync = toSync.files(false);
                    // 之前的write 受限于操作系统 可能并没有完全将数据写入到磁盘 可能只是写入到页缓存  sync 底层调用了 fileChannel.force 确保数据完全刷入到磁盘
                    directory.sync(filesToSync);
                    success = true;
                } finally {
                    if (!success) {
                        pendingCommitSet = false;
                        pendingCommit = null;
                        // 失败时 进行回滚
                        toSync.rollbackCommit(directory);
                    }
                }

                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "done all syncs: " + filesToSync);
                }

                testPoint("midStartCommitSuccess");
            } catch (Throwable t) {
                synchronized (this) {
                    if (!pendingCommitSet) {
                        if (infoStream.isEnabled("IW")) {
                            infoStream.message("IW", "hit exception committing segments file");
                        }
                        try {
                            // Hit exception
                            deleter.decRef(filesToCommit);
                        } catch (Throwable t1) {
                            t.addSuppressed(t1);
                        } finally {
                            filesToCommit = null;
                        }
                    }
                }
                throw t;
            } finally {
                synchronized (this) {
                    // Have our master segmentInfos record the
                    // generations we just prepared.  We do this
                    // on error or success so we don't
                    // double-write a segments_N file.
                    segmentInfos.updateGeneration(toSync);
                }
            }
        } catch (VirtualMachineError tragedy) {
            tragicEvent(tragedy, "startCommit");
            throw tragedy;
        }
        testPoint("finishStartCommit");
    }

    /**
     * If {@link DirectoryReader#open(IndexWriter)} has
     * been called (ie, this writer is in near real-time
     * mode), then after a merge completes, this class can be
     * invoked to warm the reader on the newly merged
     * segment, before the merge commits.  This is not
     * required for near real-time search, but will reduce
     * search latency on opening a new near real-time reader
     * after a merge completes.
     *
     * @lucene.experimental <p><b>NOTE</b>: {@link #warm(LeafReader)} is called before any
     * deletes have been carried over to the merged segment.
     */
    @FunctionalInterface
    public interface IndexReaderWarmer {
        /**
         * Invoked on the {@link LeafReader} for the newly
         * merged segment, before that segment is made visible
         * to near-real-time readers.
         */
        void warm(LeafReader reader) throws IOException;
    }

    /**
     * This method should be called on a tragic event ie. if a downstream class of the writer
     * hits an unrecoverable exception. This method does not rethrow the tragic event exception.
     * Note: This method will not close the writer but can be called from any location without respecting any lock order
     * 代表检测到异常
     */
    private void onTragicEvent(Throwable tragedy, String location) {
        // This is not supposed to be tragic: IW is supposed to catch this and
        // ignore, because it means we asked the merge to abort:
        assert tragedy instanceof MergePolicy.MergeAbortedException == false;
        // How can it be a tragedy when nothing happened?
        assert tragedy != null;
        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "hit tragic " + tragedy.getClass().getSimpleName() + " inside " + location);
        }
        // 仅设置一次异常对象
        this.tragedy.compareAndSet(null, tragedy); // only set it once
    }

    /**
     * This method set the tragic exception unless it's already set and closes the writer
     * if necessary. Note this method will not rethrow the throwable passed to it.
     */
    private void tragicEvent(Throwable tragedy, String location) throws IOException {
        try {
            onTragicEvent(tragedy, location);
        } finally {
            maybeCloseOnTragicEvent();
        }
    }

    /**
     * 如果此时已经出现了异常 进行回滚
     * @throws IOException
     */
    private void maybeCloseOnTragicEvent() throws IOException {
        // We cannot hold IW's lock here else it can lead to deadlock:
        assert Thread.holdsLock(this) == false;
        assert Thread.holdsLock(fullFlushLock) == false;
        // if we are already closed (e.g. called by rollback), this will be a no-op.
        if (this.tragedy.get() != null && shouldClose(false)) {
            rollbackInternal();
        }
    }

    /**
     * If this {@code IndexWriter} was closed as a side-effect of a tragic exception,
     * e.g. disk full while flushing a new segment, this returns the root cause exception.
     * Otherwise (no tragic exception has occurred) it returns null.
     */
    public Throwable getTragicException() {
        return tragedy.get();
    }

    /**
     * Returns {@code true} if this {@code IndexWriter} is still open.
     */
    public boolean isOpen() {
        return closing == false && closed == false;
    }

    // Used for testing.  Current points:
    //   startDoFlush
    //   startCommitMerge
    //   startStartCommit
    //   midStartCommit
    //   midStartCommit2
    //   midStartCommitSuccess
    //   finishStartCommit
    //   startCommitMergeDeletes
    //   startMergeInit
    //   DocumentsWriterPerThread addDocuments start
    private void testPoint(String message) {
        if (enableTestPoints) {
            assert infoStream.isEnabled("TP"); // don't enable unless you need them.
            infoStream.message("TP", message);
        }
    }

    /**
     * 检测 当前维护的 SegmentInfos 与 传入的参数是否一致
     * @param infos
     * @return
     */
    synchronized boolean nrtIsCurrent(SegmentInfos infos) {
        ensureOpen();
        boolean isCurrent = infos.getVersion() == segmentInfos.getVersion()
                && docWriter.anyChanges() == false
                && bufferedUpdatesStream.any() == false
                && readerPool.anyDocValuesChanges() == false;
        if (infoStream.isEnabled("IW")) {
            if (isCurrent == false) {
                infoStream.message("IW", "nrtIsCurrent: infoVersion matches: " + (infos.getVersion() == segmentInfos.getVersion()) + "; DW changes: " + docWriter.anyChanges() + "; BD changes: " + bufferedUpdatesStream.any());
            }
        }
        return isCurrent;
    }

    synchronized boolean isClosed() {
        return closed;
    }

    boolean isDeleterClosed() {
        return deleter.isClosed();
    }

    /**
     * Expert: remove any index files that are no longer
     * used.
     *
     * <p> IndexWriter normally deletes unused files itself,
     * during indexing.  However, on Windows, which disallows
     * deletion of open files, if there is a reader open on
     * the index then those files cannot be deleted.  This is
     * fine, because IndexWriter will periodically retry
     * the deletion.</p>
     *
     * <p> However, IndexWriter doesn't try that often: only
     * on open, close, flushing a new segment, and finishing
     * a merge.  If you don't do any of these actions with your
     * IndexWriter, you'll see the unused files linger.  If
     * that's a problem, call this method to delete them
     * (once you've closed the open readers that were
     * preventing their deletion).
     *
     * <p> In addition, you can call this method to delete
     * unreferenced index commits. This might be useful if you
     * are using an {@link IndexDeletionPolicy} which holds
     * onto index commits until some criteria are met, but those
     * commits are no longer needed. Otherwise, those commits will
     * be deleted the next time commit() is called.
     */
    public synchronized void deleteUnusedFiles() throws IOException {
        // TODO: should we remove this method now that it's the Directory's job to retry deletions?  Except, for the super expert IDP use case
        // it's still needed?
        ensureOpen(false);
        deleter.revisitPolicy();
    }

    /**
     * NOTE: this method creates a compound file for all files returned by
     * info.files(). While, generally, this may include separate norms and
     * deletion files, this SegmentInfo must not reference such files when this
     * method is called, because they are not allowed within a compound file.
     * 创建  csf 文件
     */
    static void createCompoundFile(InfoStream infoStream, TrackingDirectoryWrapper directory, final SegmentInfo info, IOContext context, IOUtils.IOConsumer<Collection<String>> deleteFiles) throws IOException {

        // maybe this check is not needed, but why take the risk?
        // 在创建 csf 文件的场景 必须确保本次操作中没有创建新的文件
        if (!directory.getCreatedFiles().isEmpty()) {
            throw new IllegalStateException("pass a clean trackingdir for CFS creation");
        }

        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "create compound file");
        }
        // Now merge all added files
        boolean success = false;
        try {
            // codec 定义了各种索引文件的格式 以及写入过程 这里转发给 codec对象 将数据写入 csf 文件
            info.getCodec().compoundFormat().write(directory, info, context);
            success = true;
        } finally {
            if (!success) {
                // Safe: these files must exist
                // 能进入到这里 就代表 本次操作很可能创建了新的文件  那么就需要进行删除
                deleteFiles.accept(directory.getCreatedFiles());
            }
        }

        // Replace all previous files with the CFS/CFE files:
        // 实际上就是 用清理后的文件 去替代原来的文件
        info.setFiles(new HashSet<>(directory.getCreatedFiles()));
    }

    /**
     * Tries to delete the given files if unreferenced
     *
     * @param files the files to delete
     * @throws IOException if an {@link IOException} occurs
     * @see IndexFileDeleter#deleteNewFiles(Collection)
     * 删除一组无用的文件
     */
    private synchronized void deleteNewFiles(Collection<String> files) throws IOException {
        deleter.deleteNewFiles(files);
    }

    /**
     * Cleans up residuals from a segment that could not be entirely flushed due to an error
     * 某个段在刷盘时失败了   选择删除掉这些文件 (因为此时的文件很可能是残缺的)
     */
    private synchronized void flushFailed(SegmentInfo info) throws IOException {
        // TODO: this really should be a tragic
        Collection<String> files;
        try {
            files = info.files();
        } catch (IllegalStateException ise) {
            // OK
            files = null;
        }
        if (files != null) {
            deleter.deleteNewFiles(files);
        }
    }

    /**
     * Publishes the flushed segment, segment-private deletes (if any) and its
     * associated global delete (if present) to IndexWriter.  The actual
     * publishing operation is synced on {@code IW -> BDS} so that the {@link SegmentInfo}'s
     * delete generation is always GlobalPacket_deleteGeneration + 1
     *
     * @param forced if <code>true</code> this call will block on the ticket queue if the lock is held by another thread.
     *               if <code>false</code> the call will try to acquire the queue lock and exits if it's held by another thread.
     *               当此时flushTicket 过多时 也就是囤积的刷盘任务过多时 会强制触发该方法
     *
     *
     *
     */
    private void publishFlushedSegments(boolean forced) throws IOException {

        // 通过该函数处理 之前因为刷盘而创建的 ticket 对象
        docWriter.purgeFlushTickets(forced, ticket -> {

            // 成功刷盘的 perThread 对象会将最后生成的 flushedSegment对象回填到 ticket中
            DocumentsWriterPerThread.FlushedSegment newSegment = ticket.getFlushedSegment();
            // perThread在执行刷盘前 在创建ticket时 会先冻结globalSlice update/delete对象 并生成FrozenBufferedUpdates
            FrozenBufferedUpdates bufferedUpdates = ticket.getFrozenUpdates();
            // 标记成已经发布
            ticket.markPublished();
            /**
             * 此时存在2种情况
             * 1. 本次ticket 是针对 update创建的
             * 2. 本次ticket 是失败的
             */
            if (newSegment == null) { // this is a flushed global deletes package - not a segments
                // 代表该容器内部存在数据   此时该数据就是 globalSlice 采集到的更新信息
                if (bufferedUpdates != null && bufferedUpdates.any()) { // TODO why can this be null?
                    // 这里并没有直接处理 update 对象 而是先插入到 BufferedUpdatesStream对象中  并分配一个delGen
                    publishFrozenUpdates(bufferedUpdates);
                    if (infoStream.isEnabled("IW")) {
                        infoStream.message("IW", "flush: push buffered updates: " + bufferedUpdates);
                    }
                }
                // 失败的情况下静默处理
            } else {
                // 正常逻辑对应这种情况
                assert newSegment.segmentInfo != null;
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "publishFlushedSegment seg-private updates=" + newSegment.segmentUpdates);
                }
                if (newSegment.segmentUpdates != null && infoStream.isEnabled("DW")) {
                    infoStream.message("IW", "flush: push buffered seg private updates: " + newSegment.segmentUpdates);
                }
                // now publish!
                // 将某个刷盘完成的段  插入到 segmentInfos 中 并为相关索引文件增加引用计数
                publishFlushedSegment(newSegment.segmentInfo, newSegment.fieldInfos, newSegment.segmentUpdates,
                        bufferedUpdates, newSegment.sortMap);
            }
        });
    }

    /**
     * Record that the files referenced by this {@link SegmentInfos} are still in use.
     *
     * @lucene.internal
     */
    public synchronized void incRefDeleter(SegmentInfos segmentInfos) throws IOException {
        ensureOpen();
        deleter.incRef(segmentInfos, false);
        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "incRefDeleter for NRT reader version=" + segmentInfos.getVersion() + " segments=" + segString(segmentInfos));
        }
    }

    /**
     * Record that the files referenced by this {@link SegmentInfos} are no longer in use.  Only call this if you are sure you previously
     * called {@link #incRefDeleter}.
     *
     * @lucene.internal
     */
    public synchronized void decRefDeleter(SegmentInfos segmentInfos) throws IOException {
        ensureOpen();
        deleter.decRef(segmentInfos);
        if (infoStream.isEnabled("IW")) {
            infoStream.message("IW", "decRefDeleter for NRT reader version=" + segmentInfos.getVersion() + " segments=" + segString(segmentInfos));
        }
    }

    /**
     * Processes all events and might trigger a merge if the given seqNo is negative
     *
     * @param seqNo if the seqNo is less than 0 this method will process events otherwise it's a no-op.
     * @return the given seqId inverted if negative.
     * 检测是否存在待处理的任务 以及是否需要merge 顺便进行处理  一般由其他线程调用 这样其他线程就可以协助消费任务了
     */
    private long maybeProcessEvents(long seqNo) throws IOException {
        if (seqNo < 0) {
            seqNo = -seqNo;
            processEvents(true);
        }
        return seqNo;
    }

    private void processEvents(boolean triggerMerge) throws IOException {
        if (tragedy.get() == null) {
            eventQueue.processEvents();
        }
        if (triggerMerge) {
            maybeMerge(getConfig().getMergePolicy(), MergeTrigger.SEGMENT_FLUSH, UNBOUNDED_MAX_MERGE_SEGMENTS);
        }
    }

    /**
     * Interface for internal atomic events. See {@link DocumentsWriter} for details. Events are executed concurrently and no order is guaranteed.
     * Each event should only rely on the serializeability within its process method. All actions that must happen before or after a certain action must be
     * encoded inside the {@link #process(IndexWriter)} method.
     */
    @FunctionalInterface
    interface Event {
        /**
         * Processes the event. This method is called by the {@link IndexWriter}
         * passed as the first argument.
         *
         * @param writer the {@link IndexWriter} that executes the event.
         * @throws IOException if an {@link IOException} occurs
         */
        void process(IndexWriter writer) throws IOException;
    }

    /**
     * Anything that will add N docs to the index should reserve first to
     * make sure it's allowed.  This will throw {@code
     * IllegalArgumentException} if it's not allowed.
     */
    private void reserveDocs(long addedNumDocs) {
        assert addedNumDocs >= 0;
        if (adjustPendingNumDocs(addedNumDocs) > actualMaxDocs) {
            // Reserve failed: put the docs back and throw exc:
            adjustPendingNumDocs(-addedNumDocs);
            tooManyDocs(addedNumDocs);
        }
    }

    /**
     * Does a best-effort check, that the current index would accept this many additional docs, but does not actually reserve them.
     *
     * @throws IllegalArgumentException if there would be too many docs
     */
    private void testReserveDocs(long addedNumDocs) {
        assert addedNumDocs >= 0;
        if (pendingNumDocs.get() + addedNumDocs > actualMaxDocs) {
            tooManyDocs(addedNumDocs);
        }
    }

    private void tooManyDocs(long addedNumDocs) {
        assert addedNumDocs >= 0;
        throw new IllegalArgumentException("number of documents in the index cannot exceed " + actualMaxDocs + " (current document count is " + pendingNumDocs.get() + "; added numDocs is " + addedNumDocs + ")");
    }

    /**
     * Returns the highest <a href="#sequence_number">sequence number</a> across
     * all completed operations, or 0 if no operations have finished yet.  Still
     * in-flight operations (in other threads) are not counted until they finish.
     *
     * @lucene.experimental
     */
    public long getMaxCompletedSequenceNumber() {
        ensureOpen();
        return docWriter.getMaxCompletedSequenceNumber();
    }

    private long adjustPendingNumDocs(long numDocs) {
        long count = pendingNumDocs.addAndGet(numDocs);
        assert count >= 0 : "pendingNumDocs is negative: " + count;
        return count;
    }

    final boolean isFullyDeleted(ReadersAndUpdates readersAndUpdates) throws IOException {
        if (readersAndUpdates.isFullyDeleted()) {
            assert Thread.holdsLock(this);
            return readersAndUpdates.keepFullyDeletedSegment(config.getMergePolicy()) == false;
        }
        return false;
    }

    /**
     * Returns the number of deletes a merge would claim back if the given segment is merged.
     *
     * @param info the segment to get the number of deletes for
     * @lucene.experimental
     * @see MergePolicy#numDeletesToMerge(SegmentCommitInfo, int, org.apache.lucene.util.IOSupplier)
     * 该segment在merge时 预计会删除多少doc
     */
    @Override
    public final int numDeletesToMerge(SegmentCommitInfo info) throws IOException {
        ensureOpen(false);
        validate(info);
        MergePolicy mergePolicy = config.getMergePolicy();
        // 返回 rld实例
        final ReadersAndUpdates rld = getPooledInstance(info, false);
        int numDeletesToMerge;
        if (rld != null) {
            numDeletesToMerge = rld.numDeletesToMerge(mergePolicy);
        } else {
            // if we don't have a  pooled instance lets just return the hard deletes, this is safe!
            // 返回此时已经删除的数量  如果某些doc仅仅是被标记成删除 没有做持久化 那么该值是不会增大的
            numDeletesToMerge = info.getDelCount();
        }
        assert numDeletesToMerge <= info.info.maxDoc() :
                "numDeletesToMerge: " + numDeletesToMerge + " > maxDoc: " + info.info.maxDoc();
        return numDeletesToMerge;
    }

    /**
     * 当 某个 ReadersAndUpdates 对象使用完毕后 进行资源释放
     *
     * @param readersAndUpdates
     * @throws IOException
     */
    void release(ReadersAndUpdates readersAndUpdates) throws IOException {
        release(readersAndUpdates, true);
    }

    private void release(ReadersAndUpdates readersAndUpdates, boolean assertLiveInfo) throws IOException {
        assert Thread.holdsLock(this);
        if (readerPool.release(readersAndUpdates, assertLiveInfo)) {
            // if we write anything here we have to hold the lock otherwise IDF will delete files underneath us
            assert Thread.holdsLock(this);
            checkpointNoSIS();
        }
    }

    /**
     * readerPool 对象负责维护所有段的各种索引文件对应的reader
     *
     * @param info
     * @param create 如果不存在则创建
     * @return
     */
    ReadersAndUpdates getPooledInstance(SegmentCommitInfo info, boolean create) {
        ensureOpen(false);
        return readerPool.get(info, create);
    }


    /**
     * Translates a frozen packet of delete term/query, or doc values
     * updates, into their actual docIDs in the index, and applies the change.  This is a heavy
     * operation and is done concurrently by incoming indexing threads.
     * This method will return immediately without blocking if another thread is currently
     * applying the package. In order to ensure the packet has been applied,
     * {@link IndexWriter#forceApply(FrozenBufferedUpdates)} must be called.
     * <p>
     * 开始处理 deleteQueue中采集到的删除信息  这里针对每个perThread会产生2个 updates对象 一个是全局对象 一个是该perThread 对应的deleteSlice 采集到的删除对象
     * 并且 如果针对 perThread产生的分片对象如果只有 termNode信息 那么会在flush时 直接处理掉这部分 也就是在 liveDoc中直接清除掉这部分 这样 该对象就不会被作为任务插入到任务队列了
     */
    @SuppressWarnings("try")
    final boolean tryApply(FrozenBufferedUpdates updates) throws IOException {
        // 因为任务队列中的 Event 会被多线程并发调用 所以只有抢占到updates锁对象的线程 才能够获得该对象的处理权
        if (updates.tryLock()) {
            try {
                forceApply(updates);
                return true;
            } finally {
                updates.unlock();
            }
        }
        return false;
    }

    /**
     * Translates a frozen packet of delete term/query, or doc values
     * updates, into their actual docIDs in the index, and applies the change.  This is a heavy
     * operation and is done concurrently by incoming indexing threads.
     * 由某个线程处理内部所有 待更新/删除的数据
     */
    final void forceApply(FrozenBufferedUpdates updates) throws IOException {
        updates.lock();
        try {
            // 如果这个update 对象已经处理过了 忽略
            if (updates.isApplied()) {
                // already done
                return;
            }
            long startNS = System.nanoTime();

            assert updates.any();

            // 代表此时 已经获得了这些段索引文件的输入流
            Set<SegmentCommitInfo> seenSegments = new HashSet<>();

            int iter = 0;
            // 总计读取了多少 segment
            int totalSegmentCount = 0;
            long totalDelCount = 0;

            boolean finished = false;

            // Optimistic concurrency: assume we are free to resolve the deletes against all current segments in the index, despite that
            // concurrent merges are running.  Once we are done, we check to see if a merge completed while we were running.  If so, we must retry
            // resolving against the newly merged segment(s).  Eventually no merge finishes while we were running and we are done.
            // 下面是一套乐观锁的逻辑 乐观删除以及merge
            while (true) {
                String messagePrefix;
                if (iter == 0) {
                    messagePrefix = "";
                } else {
                    messagePrefix = "iter " + iter;
                }

                long iterStartNS = System.nanoTime();

                // 获取上一次merge完成的年代
                long mergeGenStart = mergeFinishedGen.get();

                // 代表之后会被处理的所有文件
                Set<String> delFiles = new HashSet<>();
                BufferedUpdatesStream.SegmentState[] segStates;

                // 这个同步块会与 commitMerge相互作用
                synchronized (this) {
                    /**
                     * 在下面检测到 mergeGen发生变化后  会进入第二次循环
                     */

                    // 确定会影响到的范围
                    List<SegmentCommitInfo> infos = getInfosToApply(updates);
                    if (infos == null) {
                        break;
                    }

                    // 将本次涉及到的所有索引文件加入到容器中
                    for (SegmentCommitInfo info : infos) {
                        delFiles.addAll(info.files());
                    }

                    // Must open while holding IW lock so that e.g. segments are not merged
                    // away, dropped from 100% deletions, etc., before we can open the readers
                    // 因为本次段对象中 可能包含新创建的 所以要生成对应的reader 对象 确保能读取之前索引文件的数据
                    segStates = openSegmentStates(infos, seenSegments, updates.delGen());

                    if (segStates.length == 0) {

                        if (infoStream.isEnabled("BD")) {
                            infoStream.message("BD", "packet matches no segments");
                        }
                        break;
                    }

                    if (infoStream.isEnabled("BD")) {
                        infoStream.message("BD", String.format(Locale.ROOT,
                                messagePrefix + "now apply del packet (%s) to %d segments, mergeGen %d",
                                this, segStates.length, mergeGenStart));
                    }

                    totalSegmentCount += segStates.length;

                    // Important, else IFD may try to delete our files while we are still using them,
                    // if e.g. a merge finishes on some of the segments we are resolving on:
                    // 增加文件引用计数 避免被其他线程关闭
                    deleter.incRef(delFiles);
                }

                // 用于记录结果的变量
                AtomicBoolean success = new AtomicBoolean();
                long delCount;

                // 这里就是将 update对象作用在目标 segment的过程   只要至少删除了一个 doc 那么就会标记成需要merge
                try (Closeable finalizer = () -> finishApply(segStates, success.get(), delFiles)) {
                    assert finalizer != null; // access the finalizer to prevent a warning
                    // don't hold IW monitor lock here so threads are free concurrently resolve deletes/updates:
                    // 这里会根据 update内部的 deleteTerm/deleteQuery/fieldUpdates 定位到所有需要删除的doc   并将del信息写入到 PendingDeletes中
                    delCount = updates.apply(segStates);
                    success.set(true);
                    // 在这里才触发 finishApply
                }

                // Since we just resolved some more deletes/updates, now is a good time to write them:
                // 当此时update占用的内存开销比较大  且开启自动刷盘的情况
                // 为了避免oom 而尝试将一些内存开销大的update处理掉 并持久化最新的 docValue， fieldInfo  在更新过程中要修改gen
                writeSomeDocValuesUpdates();

                // It's OK to add this here, even if the while loop retries, because delCount only includes newly
                // deleted documents, on the segments we didn't already do in previous iterations:
                // 累加删除的总数
                totalDelCount += delCount;

                if (infoStream.isEnabled("BD")) {
                    infoStream.message("BD", String.format(Locale.ROOT,
                            messagePrefix + "done inner apply del packet (%s) to %d segments; %d new deletes/updates; took %.3f sec",
                            this, segStates.length, delCount, (System.nanoTime() - iterStartNS) / 1000000000.));
                }
                // 代表该update 仅针对单个segment
                if (updates.privateSegment != null) {
                    // No need to retry for a segment-private packet: the merge that folds in our private segment already waits for all deletes to
                    // be applied before it kicks off, so this private segment must already not be in the set of merging segments
                    // 私有段必须等待 更新/删除处理完后 才允许被加入到merge候选对象中  所以这里不需要考虑段在merge期间发生变化
                    break;
                }

                // Must sync on writer here so that IW.mergeCommit is not running concurrently, so that if we exit, we know mergeCommit will succeed
                // in pulling all our delGens into a merge:
                synchronized (this) {

                    // 代表处理 apply() 与merge 发生了并发 那么此时merge后的段在同步merge期间发生的update/delete时 就有可能丢失数据 就要重新执行一次 apply
                    long mergeGenCur = mergeFinishedGen.get();

                    // 本次segment都是可靠的 没有因为merge导致segment的变化
                    if (mergeGenCur == mergeGenStart) {

                        // Must do this while still holding IW lock else a merge could finish and skip carrying over our updates:

                        // Record that this packet is finished:
                        // 做一些后置处理 代表这个对象已经处理完毕了
                        bufferedUpdatesStream.finished(updates);

                        finished = true;

                        // No merge finished while we were applying, so we are done!
                        break;
                    }
                }

                if (infoStream.isEnabled("BD")) {
                    infoStream.message("BD", messagePrefix + "concurrent merges finished; move to next iter");
                }

                // A merge completed while we were running.  In this case, that merge may have picked up some of the updates we did, but not
                // necessarily all of them, so we cycle again, re-applying all our updates to the newly merged segment.

                iter++;
            }

            // 针对单个segment的update对象 不会在上面的逻辑中循环处理

            // 针对单个段的情况 必然是false   能够退出循环的就是2种情况 私有段 以false的形式退出循环 而 针对所有segment的update必须要等待finished为true时才会退出上面的循环
            if (finished == false) {
                // Record that this packet is finished:
                // 私有段在这个时候已经认为处理完毕了  推测是这样的 针对单个segment下doc的删除 不会强制刷盘  而是等到多个segment都改动后 一次性将多个segment merge
                bufferedUpdatesStream.finished(updates);
            }

            if (infoStream.isEnabled("BD")) {
                String message = String.format(Locale.ROOT,
                        "done apply del packet (%s) to %d segments; %d new deletes/updates; took %.3f sec",
                        this, totalSegmentCount, totalDelCount, (System.nanoTime() - startNS) / 1000000000.);
                if (iter > 0) {
                    message += "; " + (iter + 1) + " iters due to concurrent merges";
                }
                message += "; " + bufferedUpdatesStream.getPendingUpdatesCount() + " packets remain";
                infoStream.message("BD", message);
            }
        } finally {
            updates.unlock();
        }
    }

    /**
     * Returns the {@link SegmentCommitInfo} that this packet is supposed to apply its deletes to, or null
     * if the private segment was already merged away.
     * 需要确定 本次 update 会影响到的范围  一般情况下 每次所有perThread刷盘完成后会先发布  这样就能确保globalSlice能作用到所有perThread对象上了
     */
    private synchronized List<SegmentCommitInfo> getInfosToApply(FrozenBufferedUpdates updates) {
        final List<SegmentCommitInfo> infos;
        // 如果设置了私有段 代表本次更新的范围只针对这个 segment      通过perThread 的 deleteSlice生成的 updates对象仅处理 perThread对象的段
        if (updates.privateSegment != null) {
            // 必须确保此时段信息已经发布了  (即 已经添加到 segmentInfos中)
            if (segmentInfos.contains(updates.privateSegment)) {
                infos = Collections.singletonList(updates.privateSegment);
            } else {
                if (infoStream.isEnabled("BD")) {
                    infoStream.message("BD", "private segment already gone; skip processing updates");
                }
                infos = null;
            }
        } else {
            // 代表影响范围是 该 IndexWriter关联的所有段
            infos = segmentInfos.asList();
        }
        return infos;
    }

    /**
     * 当apply 逻辑执行完后的清理工作
     *
     * @param segStates  本次处理delete时涉及到的所有段
     * @param success  代表本次处理是否成功
     * @param delFiles  本次要处理的所有段下相关的索引文件
     * @throws IOException
     */
    private void finishApply(BufferedUpdatesStream.SegmentState[] segStates,
                             boolean success, Set<String> delFiles) throws IOException {
        synchronized (this) {

            BufferedUpdatesStream.ApplyDeletesResult result;
            try {
                result = closeSegmentStates(segStates, success);
            } finally {
                // Matches the incRef we did above, but we must do the decRef after closing segment states else
                // IFD can't delete still-open files
                deleter.decRef(delFiles);
            }

            if (result.anyDeletes) {
                // 只要有任意数据被删除了 就标记成需要merge
                maybeMerge.set(true);
                // 主要是更新 segment的版本
                checkpoint();
            }

            // 代表有些段的 所有文档都要删除  这个段被认为应当丢弃
            if (result.allDeleted != null) {
                if (infoStream.isEnabled("IW")) {
                    infoStream.message("IW", "drop 100% deleted segments: " + segString(result.allDeleted));
                }
                // 关闭当前段对应的 输入流
                for (SegmentCommitInfo info : result.allDeleted) {
                    dropDeletedSegment(info);
                }
                checkpoint();
            }
        }
    }

    /**
     * Close segment states previously opened with openSegmentStates
     * 在 delete.apply 之后触发   检测是否某些段下所有的doc都已经被标记成删除了  是的话就没必要保留segment了
     *
     * @param segStates
     * @param success
     * @return
     * @throws IOException
     */
    private BufferedUpdatesStream.ApplyDeletesResult closeSegmentStates(BufferedUpdatesStream.SegmentState[] segStates, boolean success) throws IOException {
        List<SegmentCommitInfo> allDeleted = null;
        long totDelCount = 0;
        try {
            for (BufferedUpdatesStream.SegmentState segState : segStates) {
                if (success) {
                    // 获取被 delete处理后新增的 删除数量 并进行累加
                    totDelCount += segState.rld.getDelCount() - segState.startDelCount;
                    // 代表此时一共删除了多少doc 包含待删除的
                    int fullDelCount = segState.rld.getDelCount();
                    assert fullDelCount <= segState.rld.info.info.maxDoc() : fullDelCount + " > " + segState.rld.info.info.maxDoc();
                    // 代表此时删除的doc量已经 == maxDoc了 (也即所有的doc都需要被删除 那么该段也没有保留的必要了)
                    if (segState.rld.isFullyDeleted() && getConfig().getMergePolicy().keepFullyDeletedSegment(() -> segState.reader) == false) {
                        if (allDeleted == null) {
                            allDeleted = new ArrayList<>();
                        }
                        allDeleted.add(segState.reader.getOriginalSegmentInfo());
                    }
                }
            }
        } finally {
            IOUtils.close(segStates);
        }
        if (infoStream.isEnabled("BD")) {
            infoStream.message("BD", "closeSegmentStates: " + totDelCount + " new deleted documents; pool " + bufferedUpdatesStream.getPendingUpdatesCount() + " packets; bytesUsed=" + readerPool.ramBytesUsed());
        }

        return new BufferedUpdatesStream.ApplyDeletesResult(totDelCount > 0, allDeleted);
    }

    /**
     * Opens SegmentReader and inits SegmentState for each segment.
     * 因为之后要处理存储在这些 segment 下的数据    所以现在要先获取它们的输入流
     *
     * @param infos    本次所有需要获取输入流的段对象
     * @param alreadySeenSegments 某些段的输入流可能之前已经获取了 那么此时就不需要再读取了 并且本次新读取的段也会插入到这个容器中
     * @param delGen   每个updates对象在插入到任务队列前会由  updateStream 对象分配一个delGen 顺序递增
     * @return
     * @throws IOException
     */
    private BufferedUpdatesStream.SegmentState[] openSegmentStates(List<SegmentCommitInfo> infos,
                                                                   Set<SegmentCommitInfo> alreadySeenSegments, long delGen) throws IOException {
        List<BufferedUpdatesStream.SegmentState> segStates = new ArrayList<>();
        try {
            for (SegmentCommitInfo info : infos) {
                // 只有当段被标记的delGen <= 当前update对象对应的delGen 时 才会被处理
                // 记得每个 perThread被创建时 就同步了deleteQueue中的tail节点  随着deleteQueue收集全局update信息 会维护一个链表结构
                // 每当刷盘的时候 会将globalSlice 与链表同步 并生成update  同时还会为 perThread 单独同步一次链表 并生成专属于这个 segment的 update
                // 梳理下来结果就是这样  因为每个perThread 自创建起 开始与 deleteQueue 同步 所以之后采集的删除信息必然能作用到该 perThread上   但是在刷盘完成后 deleteQueue可能又采集到了新的删除信息
                // 为了能影响到之前的segment 新的 update信息delGen 一定会大于之前 segment的 delGen
                // 并且 为了删除动作不会被重复的作用  info.getBufferedDeletesGen()对应的update对象必然就是当前perThread对应的deleteSlice  而大于它的delGen的update  只采集了之后新的node信息
                if (info.getBufferedDeletesGen() <= delGen && alreadySeenSegments.contains(info) == false) {
                    // 从 readPool 对象中初始化有关该 segment的reader对象 内部包含 直接读取索引文件内容的各种对象
                    segStates.add(new BufferedUpdatesStream.SegmentState(getPooledInstance(info, true), this::release, info));
                    // 因为该segment对应的inputStream已经生成 为了避免之后反复创建输入流  就添加到容器中
                    alreadySeenSegments.add(info);
                }
            }
        } catch (Throwable t) {
            try {
                IOUtils.close(segStates);
            } catch (Throwable t1) {
                t.addSuppressed(t1);
            }
            throw t;
        }

        return segStates.toArray(new BufferedUpdatesStream.SegmentState[0]);
    }

    /**
     * Tests should override this to enable test points. Default is <code>false</code>.
     */
    protected boolean isEnableTestPoints() {
        return false;
    }

    private void validate(SegmentCommitInfo info) {
        if (info.info.dir != directoryOrig) {
            throw new IllegalArgumentException("SegmentCommitInfo must be from the same directory");
        }
    }

    /**
     * Tests should use this method to snapshot the current segmentInfos to have a consistent view
     */
    final synchronized SegmentInfos cloneSegmentInfos() {
        return segmentInfos.clone();
    }

    /**
     * Returns accurate {@link DocStats} form this writer. The numDoc for instance can change after maxDoc is fetched
     * that causes numDocs to be greater than maxDoc which makes it hard to get accurate document stats from IndexWriter.
     */
    public synchronized DocStats getDocStats() {
        ensureOpen();
        int numDocs = docWriter.getNumDocs();
        int maxDoc = numDocs;
        for (final SegmentCommitInfo info : segmentInfos) {
            maxDoc += info.info.maxDoc();
            numDocs += info.info.maxDoc() - numDeletedDocs(info);
        }
        assert maxDoc >= numDocs : "maxDoc is less than numDocs: " + maxDoc + " < " + numDocs;
        return new DocStats(maxDoc, numDocs);
    }

    /**
     * DocStats for this index
     */
    public static final class DocStats {
        /**
         * The total number of docs in this index, including
         * docs not yet flushed (still in the RAM buffer),
         * not counting deletions.
         */
        public final int maxDoc;
        /**
         * The total number of docs in this index, including
         * docs not yet flushed (still in the RAM buffer), and
         * including deletions.  <b>NOTE:</b> buffered deletions
         * are not counted.  If you really need these to be
         * counted you should call {@link IndexWriter#commit()} first.
         */
        public final int numDocs;

        private DocStats(int maxDoc, int numDocs) {
            this.maxDoc = maxDoc;
            this.numDocs = numDocs;
        }
    }

    /**
     * 负责产生能被merge的对象 (oneMerge)  同时还包含了merge逻辑
     */
    private static class IndexWriterMergeSource implements MergeScheduler.MergeSource {
        private final IndexWriter writer;

        private IndexWriterMergeSource(IndexWriter writer) {
            this.writer = writer;
        }

        @Override
        public MergePolicy.OneMerge getNextMerge() {
            // 从 writer的等待队列中弹出一个元素
            MergePolicy.OneMerge nextMerge = writer.getNextMerge();
            if (nextMerge != null) {
                // 如果允许打印日志
                if (writer.mergeScheduler.verbose()) {
                    writer.mergeScheduler.message("  checked out merge " + writer.segString(nextMerge.segments));
                }
            }
            return nextMerge;
        }

        // 在这里相关实现都转发给了 writer对象
        @Override
        public void onMergeFinished(MergePolicy.OneMerge merge) {
            writer.mergeFinish(merge);
        }

        @Override
        public boolean hasPendingMerges() {
            return writer.hasPendingMerges();
        }

        /**
         * 使用 MergeScheduler 处理OneMerge对象 最终会委托到IndexWriter执行任务
         * @param merge
         * @throws IOException
         */
        @Override
        public void merge(MergePolicy.OneMerge merge) throws IOException {
            assert Thread.holdsLock(writer) == false;
            writer.merge(merge);
        }

        public String toString() {
            return writer.segString();
        }
    }
}
