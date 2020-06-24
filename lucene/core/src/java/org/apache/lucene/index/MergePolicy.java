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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.InfoStream;

/**
 * <p>Expert: a MergePolicy determines the sequence of
 * primitive merge operations.</p>
 *
 * <p>Whenever the segments in an index have been altered by
 * {@link IndexWriter}, either the addition of a newly
 * flushed segment, addition of many segments from
 * addIndexes* calls, or a previous merge that may now need
 * to cascade, {@link IndexWriter} invokes {@link
 * #findMerges} to give the MergePolicy a chance to pick
 * merges that are now required.  This method returns a
 * {@link MergeSpecification} instance describing the set of
 * merges that should be done, or null if no merges are
 * necessary.  When IndexWriter.forceMerge is called, it calls
 * {@link #findForcedMerges(SegmentInfos, int, Map, MergeContext)} and the MergePolicy should
 * then return the necessary merges.</p>
 *
 * <p>Note that the policy can return more than one merge at
 * a time.  In this case, if the writer is using {@link
 * SerialMergeScheduler}, the merges will be run
 * sequentially but if it is using {@link
 * ConcurrentMergeScheduler} they will be run concurrently.</p>
 *
 * <p>The default MergePolicy is {@link
 * TieredMergePolicy}.</p>
 *
 * @lucene.experimental 用于定义 2个 index 如何融合
 */
public abstract class MergePolicy {

    /**
     * Progress and state for an executing merge. This class
     * encapsulates the logic to pause and resume the merge thread
     * or to abort the merge entirely.
     *
     * @lucene.experimental 通过该对象可以影响当前merge的进程
     */
    public static class OneMergeProgress {
        /**
         * Reason for pausing the merge thread.
         */
        // 暂停merge的枚举
        public static enum PauseReason {
            /**
             * Stopped (because of throughput rate set to 0, typically).
             */
            STOPPED,
            /**
             * Temporarily paused because of exceeded throughput rate.
             */
            PAUSED,
            /**
             * Other reason.
             */
            OTHER
        }

        ;

        private final ReentrantLock pauseLock = new ReentrantLock();
        private final Condition pausing = pauseLock.newCondition();

        /**
         * Pause times (in nanoseconds) for each {@link PauseReason}.
         * value 用于累加基于各种原因导致的 merge暂停时间
         */
        private final EnumMap<PauseReason, AtomicLong> pauseTimesNS;

        /**
         * 是否已经禁止融合
         */
        private volatile boolean aborted;

        /**
         * This field is for sanity-check purposes only. Only the same thread that invoked
         * {@link OneMerge#mergeInit()} is permitted to be calling
         * {@link #pauseNanos}. This is always verified at runtime.
         * 代表用于执行 merge 的线程
         */
        private Thread owner;

        /**
         * Creates a new merge progress info.
         */
        // 当该对象被初始化时  每种暂停原因对应的时间都为0
        public OneMergeProgress() {
            // Place all the pause reasons in there immediately so that we can simply update values.
            pauseTimesNS = new EnumMap<PauseReason, AtomicLong>(PauseReason.class);
            for (PauseReason p : PauseReason.values()) {
                pauseTimesNS.put(p, new AtomicLong());
            }
        }

        /**
         * Abort the merge this progress tracks at the next
         * possible moment.
         * 当本次merge 任务被终止时   唤醒之前阻塞的线程   (因为本任务已经停止了  其他线程在被唤醒后应该会判断  aborted标识)
         */
        public void abort() {
            aborted = true;
            wakeup(); // wakeup any paused merge thread.
        }

        /**
         * Return the aborted state of this merge.
         */
        public boolean isAborted() {
            return aborted;
        }

        /**
         * Pauses the calling thread for at least <code>pauseNanos</code> nanoseconds
         * unless the merge is aborted or the external condition returns <code>false</code>,
         * in which case control returns immediately.
         * <p>
         * The external condition is required so that other threads can terminate the pausing immediately,
         * before <code>pauseNanos</code> expires. We can't rely on just {@link Condition#awaitNanos(long)} alone
         * because it can return due to spurious wakeups too.
         *
         * @param condition The pause condition that should return false if immediate return from this
         *                  method is needed. Other threads can wake up any sleeping thread by calling
         *                  {@link #wakeup}, but it'd fall to sleep for the remainder of the requested time if this     该condition必须返回false才会真正从暂停状态解除
         *                  传入一个暂停原因 以及暂停的时间
         */
        public void pauseNanos(long pauseNanos, PauseReason reason, BooleanSupplier condition) throws InterruptedException {
            // 只有执行merge的线程可以从内部暂停
            if (Thread.currentThread() != owner) {
                throw new RuntimeException("Only the merge owner thread can call pauseNanos(). This thread: "
                        + Thread.currentThread().getName() + ", owner thread: "
                        + owner);
            }

            long start = System.nanoTime();
            // 获取暂停时间
            AtomicLong timeUpdate = pauseTimesNS.get(reason);
            pauseLock.lock();
            try {
                // 每次被唤醒时 需要通过 condition 判断是否满足唤醒条件 否则还是继续沉睡
                while (pauseNanos > 0 && !aborted && condition.getAsBoolean()) {
                    pauseNanos = pausing.awaitNanos(pauseNanos);
                }
            } finally {
                pauseLock.unlock();
                timeUpdate.addAndGet(System.nanoTime() - start);
            }
        }

        /**
         * Request a wakeup for any threads stalled in {@link #pauseNanos}.
         * 外部线程暂停的merge 线程
         */
        public void wakeup() {
            pauseLock.lock();
            try {
                pausing.signalAll();
            } finally {
                pauseLock.unlock();
            }
        }

        /**
         * Returns pause reasons and associated times in nanoseconds.
         */
        // 返回由于各种原因暂停的时长
        public Map<PauseReason, Long> getPauseTimes() {
            Set<Entry<PauseReason, AtomicLong>> entries = pauseTimesNS.entrySet();
            return entries.stream()
                    .collect(Collectors.toMap(
                            (e) -> e.getKey(),
                            (e) -> e.getValue().get()));
        }

        /**
         * 指定执行merge的线程
         *
         * @param owner
         */
        final void setMergeThread(Thread owner) {
            assert this.owner == null;
            this.owner = owner;
        }
    }

    /**
     * OneMerge provides the information necessary to perform
     * an individual primitive merge operation, resulting in
     * a single new segment.  The merge spec includes the
     * subset of segments to be merged as well as whether the
     * new segment should use the compound file format.
     *
     * @lucene.experimental
     */
    // 描述一次merge操作必备的各种属性
    public static class OneMerge {
        // 推测是merge的结果
        SegmentCommitInfo info;         // used by IndexWriter
        boolean registerDone;           // used by IndexWriter
        /**
         * merge 也有年代的概念???
         */
        long mergeGen;                  // used by IndexWriter
        boolean isExternal;             // used by IndexWriter
        /**
         * 单次最多merge 多少个 segment
         */
        int maxNumSegments = -1;        // used by IndexWriter

        /**
         * Estimated size in bytes of the merged segment.
         */
        // 预计会融合多少byte
        public volatile long estimatedMergeBytes;       // used by IndexWriter

        // Sum of sizeInBytes of all SegmentInfos; set by IW.mergeInit
        // 代表总的融合数
        volatile long totalMergeBytes;

        /**
         * 这里还存放着一组 用于读取 segment的reader
         */
        List<SegmentReader> readers;        // used by IndexWriter
        List<Bits> hardLiveDocs;        // used by IndexWriter

        /**
         * Segments to be merged.
         */
        // 这里是一组将要被merger的segment 信息
        public final List<SegmentCommitInfo> segments;

        /**
         * Control used to pause/stop/resume the merge thread.
         * 用于控制 merge线程的状态
         */
        private final OneMergeProgress mergeProgress;

        /**
         * 记录merge的起始时间
         */
        volatile long mergeStartNS = -1;

        /**
         * Total number of documents in segments to be merged, not accounting for deletions.
         */
        // 一共融合了多少个doc  每个segment 内部包含多个 doc
        public final int totalMaxDoc;
        Throwable error;

        /**
         * Sole constructor.
         *
         * @param segments List of {@link SegmentCommitInfo}s
         *                 to be merged.
         */
        // 通过传入一个待merge 的commitInfo 进行初始化
        public OneMerge(List<SegmentCommitInfo> segments) {
            if (0 == segments.size()) {
                throw new RuntimeException("segments must include at least one segment");
            }
            // clone the list, as the in list may be based off original SegmentInfos and may be modified
            this.segments = new ArrayList<>(segments);
            int count = 0;
            for (SegmentCommitInfo info : segments) {
                count += info.info.maxDoc();
            }
            // 计算总计要merge 多少doc
            totalMaxDoc = count;

            mergeProgress = new OneMergeProgress();
        }

        /**
         * Called by {@link IndexWriter} after the merge started and from the
         * thread that will be executing the merge.
         * 初始化merge工作  也就是为mergeProgress 设置merge线程
         */
        public void mergeInit() throws IOException {
            mergeProgress.setMergeThread(Thread.currentThread());
        }

        /**
         * Called by {@link IndexWriter} after the merge is done and all readers have been closed.
         */
        public void mergeFinished() throws IOException {
        }

        /**
         * Wrap the reader in order to add/remove information to the merged segment.
         */
        // 使用merge对象 包装 codecReader 默认直接返回reader
        public CodecReader wrapForMerge(CodecReader reader) throws IOException {
            return reader;
        }

        /**
         * Expert: Sets the {@link SegmentCommitInfo} of the merged segment.
         * Allows sub-classes to e.g. set diagnostics properties.
         * 设置merge的结果
         */
        public void setMergeInfo(SegmentCommitInfo info) {
            this.info = info;
        }

        /**
         * Returns the {@link SegmentCommitInfo} for the merged segment,
         * or null if it hasn't been set yet.
         */
        public SegmentCommitInfo getMergeInfo() {
            return info;
        }

        /**
         * Record that an exception occurred while executing
         * this merge
         */
        synchronized void setException(Throwable error) {
            this.error = error;
        }

        /**
         * Retrieve previous exception set by {@link
         * #setException}.
         */
        synchronized Throwable getException() {
            return error;
        }

        /**
         * Returns a readable description of the current merge
         * state.
         */
        // 将内部信息 格式化输出
        public String segString() {
            StringBuilder b = new StringBuilder();
            final int numSegments = segments.size();
            for (int i = 0; i < numSegments; i++) {
                if (i > 0) {
                    b.append(' ');
                }
                b.append(segments.get(i).toString());
            }
            if (info != null) {
                b.append(" into ").append(info.info.name);
            }
            if (maxNumSegments != -1) {
                b.append(" [maxNumSegments=").append(maxNumSegments).append(']');
            }
            if (isAborted()) {
                b.append(" [ABORTED]");
            }
            return b.toString();
        }

        /**
         * Returns the total size in bytes of this merge. Note that this does not
         * indicate the size of the merged segment, but the
         * input total size. This is only set once the merge is
         * initialized by IndexWriter.
         */
        public long totalBytesSize() {
            return totalMergeBytes;
        }

        /**
         * Returns the total number of documents that are included with this merge.
         * Note that this does not indicate the number of documents after the merge.
         * 返回本次merge 的doc总数
         */
        public int totalNumDocs() {
            int total = 0;
            for (SegmentCommitInfo info : segments) {
                total += info.info.maxDoc();
            }
            return total;
        }

        /**
         * Return {@link MergeInfo} describing this merge.
         */
        // 将内部信息包装成一个用于描述 merge的 javaBean
        public MergeInfo getStoreMergeInfo() {
            return new MergeInfo(totalMaxDoc, estimatedMergeBytes, isExternal, maxNumSegments);
        }

        // 与merge的终止相关

        /**
         * Returns true if this merge was or should be aborted.
         */
        public boolean isAborted() {
            return mergeProgress.isAborted();
        }

        /**
         * Marks this merge as aborted. The merge thread should terminate at the soonest possible moment.
         */
        public void setAborted() {
            this.mergeProgress.abort();
        }

        /**
         * Checks if merge has been aborted and throws a merge exception if so.
         */
        public void checkAborted() throws MergeAbortedException {
            if (isAborted()) {
                throw new MergePolicy.MergeAbortedException("merge is aborted: " + segString());
            }
        }

        /**
         * Returns a {@link OneMergeProgress} instance for this merge, which provides
         * statistics of the merge threads (run time vs. sleep time) if merging is throttled.
         */
        public OneMergeProgress getMergeProgress() {
            return mergeProgress;
        }
    }

    /**
     * A MergeSpecification instance provides the information
     * necessary to perform multiple merges.  It simply
     * contains a list of {@link OneMerge} instances.
     * 描述某次参数merge的所有片段
     */
    public static class MergeSpecification {

        /**
         * The subset of segments to be included in the primitive merge.
         * 内部包含一组 oneMerge对象
         */
        public final List<OneMerge> merges = new ArrayList<>();

        /**
         * Sole constructor.  Use {@link
         * #add(MergePolicy.OneMerge)} to add merges.
         */
        public MergeSpecification() {
        }

        /**
         * Adds the provided {@link OneMerge} to this
         * specification.
         */
        // 添加一个merge 对象
        public void add(OneMerge merge) {
            merges.add(merge);
        }

        /**
         * Returns a description of the merges in this specification.
         */
        public String segString(Directory dir) {
            StringBuilder b = new StringBuilder();
            b.append("MergeSpec:\n");
            final int count = merges.size();
            for (int i = 0; i < count; i++) {
                b.append("  ").append(1 + i).append(": ").append(merges.get(i).segString());
            }
            return b.toString();
        }
    }

    /**
     * Exception thrown if there are any problems while executing a merge.
     */
    // 用于描述 merge 过程中出现的异常
    public static class MergeException extends RuntimeException {
        /**
         * Create a {@code MergeException}.
         */
        public MergeException(String message) {
            super(message);
        }

        /**
         * Create a {@code MergeException}.
         */
        public MergeException(Throwable exc) {
            super(exc);
        }
    }

    /**
     * Thrown when a merge was explicitly aborted because
     * {@link IndexWriter#abortMerges} was called.  Normally
     * this exception is privately caught and suppressed by
     * {@link IndexWriter}.
     */
    // 代表本次merge 操作终止
    public static class MergeAbortedException extends IOException {
        /**
         * Create a {@link MergeAbortedException}.
         */
        public MergeAbortedException() {
            super("merge is aborted");
        }

        /**
         * Create a {@link MergeAbortedException} with a
         * specified message.
         */
        public MergeAbortedException(String message) {
            super(message);
        }
    }

    /**
     * Default ratio for compound file system usage. Set to <code>1.0</code>, always use
     * compound file system.
     * 通过Lucene的，用Java编写的文本搜索引擎库使用CFS文件。它用于保存多个索引文件成一个复合存档。
     */
    protected static final double DEFAULT_NO_CFS_RATIO = 1.0;

    /**
     * Default max segment size in order to use compound file system. Set to {@link Long#MAX_VALUE}.
     * 在复合文件中 segment的最大长度
     */
    protected static final long DEFAULT_MAX_CFS_SEGMENT_SIZE = Long.MAX_VALUE;

    /**
     * If the size of the merge segment exceeds this ratio of
     * the total index size then it will remain in
     * non-compound format
     * 如果段相关的索引信息超过一定的大小 那么不采用 cfs格式存储
     */
    protected double noCFSRatio = DEFAULT_NO_CFS_RATIO;

    /**
     * If the size of the merged segment exceeds
     * this value then it will not use compound file format.
     */
    protected long maxCFSSegmentSize = DEFAULT_MAX_CFS_SEGMENT_SIZE;

    /**
     * Creates a new merge policy instance.
     */
    public MergePolicy() {
        this(DEFAULT_NO_CFS_RATIO, DEFAULT_MAX_CFS_SEGMENT_SIZE);
    }

    /**
     * Creates a new merge policy instance with default settings for noCFSRatio
     * and maxCFSSegmentSize. This ctor should be used by subclasses using different
     * defaults than the {@link MergePolicy}
     */
    protected MergePolicy(double defaultNoCFSRatio, long defaultMaxCFSSegmentSize) {
        this.noCFSRatio = defaultNoCFSRatio;
        this.maxCFSSegmentSize = defaultMaxCFSSegmentSize;
    }

    /**
     * Determine what set of merge operations are now necessary on the index.
     * {@link IndexWriter} calls this whenever there is a change to the segments.
     * This call is always synchronized on the {@link IndexWriter} instance so
     * only one thread at a time will call this method.
     *
     * @param mergeTrigger the event that triggered the merge
     * @param segmentInfos the total set of segments in the index
     * @param mergeContext the IndexWriter to find the merges on
     *                     从索引上找到哪些数据需要被merge
     */
    public abstract MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
            throws IOException;

    /**
     * Determine what set of merge operations is necessary in
     * order to merge to {@code <=} the specified segment count. {@link IndexWriter} calls this when its
     * {@link IndexWriter#forceMerge} method is called. This call is always
     * synchronized on the {@link IndexWriter} instance so only one thread at a
     * time will call this method.
     *
     * @param segmentInfos    the total set of segments in the index
     * @param maxSegmentCount requested maximum number of segments in the index (currently this
     *                        is always 1)
     * @param segmentsToMerge contains the specific SegmentInfo instances that must be merged
     *                        away. This may be a subset of all
     *                        SegmentInfos.  If the value is True for a
     *                        given SegmentInfo, that means this segment was
     *                        an original segment present in the
     *                        to-be-merged index; else, it was a segment
     *                        produced by a cascaded merge.
     * @param mergeContext    the IndexWriter to find the merges on
     */
    public abstract MergeSpecification findForcedMerges(
            SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext)
            throws IOException;

    /**
     * Determine what set of merge operations is necessary in order to expunge all
     * deletes from the index.
     *
     * @param segmentInfos the total set of segments in the index
     * @param mergeContext the IndexWriter to find the merges on
     *                     找到哪些数据是需要从索引上删除的
     */
    public abstract MergeSpecification findForcedDeletesMerges(
            SegmentInfos segmentInfos, MergeContext mergeContext) throws IOException;

    /**
     * Returns true if a new segment (regardless of its origin) should use the
     * compound file format. The default implementation returns <code>true</code>
     * iff the size of the given mergedInfo is less or equal to
     * {@link #getMaxCFSSegmentSizeMB()} and the size is less or equal to the
     * TotalIndexSize * {@link #getNoCFSRatio()} otherwise <code>false</code>.
     * 判断是否写入到复合文件
     */
    public boolean useCompoundFile(SegmentInfos infos, SegmentCommitInfo mergedInfo, MergeContext mergeContext) throws IOException {
        if (getNoCFSRatio() == 0.0) {
            return false;
        }
        // 计算需要被merge的长度 (可能有部分数据需要被删除)
        long mergedInfoSize = size(mergedInfo, mergeContext);
        // 超过了预定值 无法进行merge
        if (mergedInfoSize > maxCFSSegmentSize) {
            return false;
        }
        if (getNoCFSRatio() >= 1.0) {
            return true;
        }
        // 计算merge 的总大小
        long totalSize = 0;
        for (SegmentCommitInfo info : infos) {
            totalSize += size(info, mergeContext);
        }
        // 小于所有 infos的总和才行
        return mergedInfoSize <= getNoCFSRatio() * totalSize;
    }

    /**
     * Return the byte size of the provided {@link
     * SegmentCommitInfo}, pro-rated by percentage of
     * non-deleted documents is set.
     * 计算写入的数据长度   需要考虑 删除的数据长度
     */
    protected long size(SegmentCommitInfo info, MergeContext mergeContext) throws IOException {
        // 先获取该段原始长度
        long byteSize = info.sizeInBytes();
        // 计算segment 在merge过程中会删除多少数据
        int delCount = mergeContext.numDeletesToMerge(info);
        assert assertDelCount(delCount, info);
        // 计算被删除的数据占用的 百分比
        double delRatio = info.info.maxDoc() <= 0 ? 0d : (double) delCount / (double) info.info.maxDoc();
        assert delRatio <= 1.0;
        // 只返回未被删除的部分
        return (info.info.maxDoc() <= 0 ? byteSize : (long) (byteSize * (1.0 - delRatio)));
    }

    /**
     * Asserts that the delCount for this SegmentCommitInfo is valid
     */
    protected final boolean assertDelCount(int delCount, SegmentCommitInfo info) {
        assert delCount >= 0 : "delCount must be positive: " + delCount;
        assert delCount <= info.info.maxDoc() : "delCount: " + delCount
                + " must be leq than maxDoc: " + info.info.maxDoc();
        return true;
    }

    /**
     * Returns true if this single info is already fully merged (has no
     * pending deletes, is in the same dir as the
     * writer, and matches the current compound file setting
     * 判断merge 是否已经结束
     */
    protected final boolean isMerged(SegmentInfos infos, SegmentCommitInfo info, MergeContext mergeContext) throws IOException {
        assert mergeContext != null;
        // 计算被删除的长度
        int delCount = mergeContext.numDeletesToMerge(info);
        assert assertDelCount(delCount, info);
        // 如果当前计算出来删除长度为0 且 同时使用复合文件 或者同时不使用
        return delCount == 0 &&
                useCompoundFile(infos, info, mergeContext) == info.info.getUseCompoundFile();
    }

    /**
     * Returns current {@code noCFSRatio}.
     *
     * @see #setNoCFSRatio
     */
    public double getNoCFSRatio() {
        return noCFSRatio;
    }

    /**
     * If a merged segment will be more than this percentage
     * of the total size of the index, leave the segment as
     * non-compound file even if compound file is enabled.
     * Set to 1.0 to always use CFS regardless of merge
     * size.
     */
    public void setNoCFSRatio(double noCFSRatio) {
        if (noCFSRatio < 0.0 || noCFSRatio > 1.0) {
            throw new IllegalArgumentException("noCFSRatio must be 0.0 to 1.0 inclusive; got " + noCFSRatio);
        }
        this.noCFSRatio = noCFSRatio;
    }

    /**
     * Returns the largest size allowed for a compound file segment
     * 复合文件中 每个segment 最大允许使用多少 MB
     */
    public double getMaxCFSSegmentSizeMB() {
        return maxCFSSegmentSize / 1024 / 1024.;
    }

    /**
     * If a merged segment will be more than this value,
     * leave the segment as
     * non-compound file even if compound file is enabled.
     * Set this to Double.POSITIVE_INFINITY (default) and noCFSRatio to 1.0
     * to always use CFS regardless of merge size.
     */
    public void setMaxCFSSegmentSizeMB(double v) {
        if (v < 0.0) {
            throw new IllegalArgumentException("maxCFSSegmentSizeMB must be >=0 (got " + v + ")");
        }
        v *= 1024 * 1024;
        this.maxCFSSegmentSize = v > Long.MAX_VALUE ? Long.MAX_VALUE : (long) v;
    }

    /**
     * Returns true if the segment represented by the given CodecReader should be keep even if it's fully deleted.
     * This is useful for testing of for instance if the merge policy implements retention policies for soft deletes.
     */
    public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
        return false;
    }

    /**
     * Returns the number of deletes that a merge would claim on the given segment. This method will by default return
     * the sum of the del count on disk and the pending delete count. Yet, subclasses that wrap merge readers
     * might modify this to reflect deletes that are carried over to the target segment in the case of soft deletes.
     * <p>
     * Soft deletes all deletes to survive across merges in order to control when the soft-deleted data is claimed.
     *
     * @param info           the segment info that identifies the segment
     * @param delCount       the number deleted documents for this segment
     * @param readerSupplier a supplier that allows to obtain a {@link CodecReader} for this segment
     * @see IndexWriter#softUpdateDocument(Term, Iterable, Field...)
     * @see IndexWriterConfig#setSoftDeletesField(String)
     * 获取在merge 过程中删除的数量
     */
    public int numDeletesToMerge(SegmentCommitInfo info, int delCount,
                                 IOSupplier<CodecReader> readerSupplier) throws IOException {
        return delCount;
    }

    /**
     * Builds a String representation of the given SegmentCommitInfo instances
     */
    protected final String segString(MergeContext mergeContext, Iterable<SegmentCommitInfo> infos) {
        return StreamSupport.stream(infos.spliterator(), false)
                .map(info -> info.toString(mergeContext.numDeletedDocs(info) - info.getDelCount()))
                .collect(Collectors.joining(" "));
    }

    /**
     * Print a debug message to {@link MergeContext}'s {@code
     * infoStream}.
     */
    protected final void message(String message, MergeContext mergeContext) {
        if (verbose(mergeContext)) {
            mergeContext.getInfoStream().message("MP", message);
        }
    }

    /**
     * Returns <code>true</code> if the info-stream is in verbose mode
     *
     * @see #message(String, MergeContext)
     */
    protected final boolean verbose(MergeContext mergeContext) {
        return mergeContext.getInfoStream().isEnabled("MP");
    }

    /**
     * This interface represents the current context of the merge selection process.
     * It allows to access real-time information like the currently merging segments or
     * how many deletes a segment would claim back if merged. This context might be stateful
     * and change during the execution of a merge policy's selection processes.
     *
     * @lucene.experimental
     * merge的上下文对象
     */
    public interface MergeContext {

        /**
         * Returns the number of deletes a merge would claim back if the given segment is merged.
         *
         * @param info the segment to get the number of deletes for
         * @see MergePolicy#numDeletesToMerge(SegmentCommitInfo, int, org.apache.lucene.util.IOSupplier)
         * 计算该segment 在merge过程中需要删除多少doc
         */
        int numDeletesToMerge(SegmentCommitInfo info) throws IOException;

        /**
         * Returns the number of deleted documents in the given segments.
         * 删除的文档数
         */
        int numDeletedDocs(SegmentCommitInfo info);

        /**
         * Returns the info stream that can be used to log messages
         * 返回一个日志对象
         */
        InfoStream getInfoStream();

        /**
         * Returns an unmodifiable set of segments that are currently merging.
         * 返回正在merge中的一组数据
         */
        Set<SegmentCommitInfo> getMergingSegments();
    }
}
