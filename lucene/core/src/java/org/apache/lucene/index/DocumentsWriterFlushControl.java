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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * This class controls {@link DocumentsWriterPerThread} flushing during
 * indexing. It tracks the memory consumption per
 * {@link DocumentsWriterPerThread} and uses a configured {@link FlushPolicy} to
 * decide if a {@link DocumentsWriterPerThread} must flush.
 * <p>
 * In addition to the {@link FlushPolicy} the flush control might set certain
 * {@link DocumentsWriterPerThread} as flush pending iff a
 * {@link DocumentsWriterPerThread} exceeds the
 * {@link IndexWriterConfig#getRAMPerThreadHardLimitMB()} to prevent address
 * space exhaustion.
 * 该对象是刷盘的控制器
 */
final class DocumentsWriterFlushControl implements Accountable, Closeable {

    private final long hardMaxBytesPerDWPT;

    /**
     * 代表当前待刷盘的数据有多少byte
     */
    private long activeBytes = 0;
    /**
     * 本次要刷盘的 bytes  而当刷盘完成后 该值会减小 (因为此时数据已经存入到磁盘了  同时 flushBytes + activeBytes 才是占用的总内存)
     */
    private volatile long flushBytes = 0;
    /**
     * 当前有多少待刷盘 perThread
     */
    private volatile int numPending = 0;
    private int numDocsSinceStalled = 0; // only with assert
    private final AtomicBoolean flushDeletes = new AtomicBoolean(false);
    private boolean fullFlush = false;
    private boolean fullFlushMarkDone = false; // only for assertion that we don't get stale DWPTs from the pool

    /**
     * The flushQueue is used to concurrently distribute DWPTs that are ready to be flushed ie. when a full flush is in
     * progress. This might be triggered by a commit or NRT refresh. The trigger will only walk all eligible DWPTs and
     * mark them as flushable putting them in the flushQueue ready for other threads (ie. indexing threads) to help flushing
     * 这里存放的是需要被刷盘的perThread
     */
    private final Queue<DocumentsWriterPerThread> flushQueue = new LinkedList<>();
    // only for safety reasons if a DWPT is close to the RAM limit
    private final Queue<DocumentsWriterPerThread> blockedFlushes = new LinkedList<>();
    // flushingWriters holds all currently flushing writers. There might be writers in this list that
    // are also in the flushQueue which means that writers in the flushingWriters list are not necessarily
    // already actively flushing. They are only in the state of flushing and might be picked up in the future by
    // polling the flushQueue
    // 记录此时正在刷盘的 perThread 不同于 flushQueue
    private final List<DocumentsWriterPerThread> flushingWriters = new ArrayList<>();

    private double maxConfiguredRamBuffer = 0;
    private long peakActiveBytes = 0;// only with assert
    private long peakFlushBytes = 0;// only with assert
    private long peakNetBytes = 0;// only with assert
    private long peakDelta = 0; // only with assert
    private boolean flushByRAMWasDisabled; // only with assert
    final DocumentsWriterStallControl stallControl = new DocumentsWriterStallControl();
    private final DocumentsWriterPerThreadPool perThreadPool;

    /**
     * 实际上刷盘动作如何执行 等还是依托于 flushPolicy
     */
    private final FlushPolicy flushPolicy;
    private boolean closed = false;
    private final DocumentsWriter documentsWriter;
    private final LiveIndexWriterConfig config;
    private final InfoStream infoStream;

    /**
     * 在  writer 对象被创建时  就会创建该对象
     *
     * @param documentsWriter
     * @param config
     */
    DocumentsWriterFlushControl(DocumentsWriter documentsWriter, LiveIndexWriterConfig config) {
        this.infoStream = config.getInfoStream();
        this.perThreadPool = documentsWriter.perThreadPool;
        this.flushPolicy = config.getFlushPolicy();
        this.config = config;
        this.hardMaxBytesPerDWPT = config.getRAMPerThreadHardLimitMB() * 1024 * 1024;
        this.documentsWriter = documentsWriter;
    }

    public synchronized long activeBytes() {
        return activeBytes;
    }

    long getFlushingBytes() {
        return flushBytes;
    }

    /**
     * 当前占用的总内存
     *
     * @return
     */
    synchronized long netBytes() {
        return flushBytes + activeBytes;
    }

    /**
     * 应该是这个意思 如果开启了自动刷盘 那么当此时内存占用达到2倍的 ramMB时 强制刷盘
     * 如果关闭自动刷盘功能的话  那么触发 stall(暂用) 的值就是 MAX_VALUE 也就是永远不会主动暂停写入doc
     *
     * @return
     */
    private long stallLimitBytes() {
        // 代表最多允许使用多少内存
        final double maxRamMB = config.getRAMBufferSizeMB();
        return maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH ? (long) (2 * (maxRamMB * 1024 * 1024)) : Long.MAX_VALUE;
    }

    // TODO 断言相关的忽略
    private boolean assertMemory() {
        final double maxRamMB = config.getRAMBufferSizeMB();
        // We can only assert if we have always been flushing by RAM usage; otherwise the assert will false trip if e.g. the
        // flush-by-doc-count * doc size was large enough to use far more RAM than the sudden change to IWC's maxRAMBufferSizeMB:
        if (maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH && flushByRAMWasDisabled == false) {
            // for this assert we must be tolerant to ram buffer changes!
            maxConfiguredRamBuffer = Math.max(maxRamMB, maxConfiguredRamBuffer);
            final long ram = flushBytes + activeBytes;
            final long ramBufferBytes = (long) (maxConfiguredRamBuffer * 1024 * 1024);
            // take peakDelta into account - worst case is that all flushing, pending and blocked DWPT had maxMem and the last doc had the peakDelta

            // 2 * ramBufferBytes -> before we stall we need to cross the 2xRAM Buffer border this is still a valid limit
            // (numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta) -> those are the total number of DWPT that are not active but not yet fully flushed
            // all of them could theoretically be taken out of the loop once they crossed the RAM buffer and the last document was the peak delta
            // (numDocsSinceStalled * peakDelta) -> at any given time there could be n threads in flight that crossed the stall control before we reached the limit and each of them could hold a peak document
            final long expected = (2 * ramBufferBytes) + ((numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta) + (numDocsSinceStalled * peakDelta);
            // the expected ram consumption is an upper bound at this point and not really the expected consumption
            if (peakDelta < (ramBufferBytes >> 1)) {
                /*
                 * if we are indexing with very low maxRamBuffer like 0.1MB memory can
                 * easily overflow if we check out some DWPT based on docCount and have
                 * several DWPT in flight indexing large documents (compared to the ram
                 * buffer). This means that those DWPT and their threads will not hit
                 * the stall control before asserting the memory which would in turn
                 * fail. To prevent this we only assert if the the largest document seen
                 * is smaller than the 1/2 of the maxRamBufferMB
                 */
                assert ram <= expected : "actual mem: " + ram + " byte, expected mem: " + expected
                        + " byte, flush mem: " + flushBytes + ", active mem: " + activeBytes
                        + ", pending DWPT: " + numPending + ", flushing DWPT: "
                        + numFlushingDWPT() + ", blocked DWPT: " + numBlockedFlushes()
                        + ", peakDelta mem: " + peakDelta + " bytes, ramBufferBytes=" + ramBufferBytes
                        + ", maxConfiguredRamBuffer=" + maxConfiguredRamBuffer;
            }
        } else {
            flushByRAMWasDisabled = true;
        }
        return true;
    }

    private synchronized void commitPerThreadBytes(DocumentsWriterPerThread perThread) {
        // 自上次刷盘后 增加的byte占用量
        final long delta = perThread.commitLastBytesUsed();
        /*
         * We need to differentiate here if we are pending since setFlushPending
         * moves the perThread memory to the flushBytes and we could be set to
         * pending during a delete
         */
        if (perThread.isFlushPending()) {
            flushBytes += delta;
        } else {
            activeBytes += delta;
        }
        assert updatePeaks(delta);
    }

    // only for asserts  TODO 忽略断言相关的
    private boolean updatePeaks(long delta) {
        peakActiveBytes = Math.max(peakActiveBytes, activeBytes);
        peakFlushBytes = Math.max(peakFlushBytes, flushBytes);
        peakNetBytes = Math.max(peakNetBytes, netBytes());
        peakDelta = Math.max(peakDelta, delta);

        return true;
    }

    /**
     * 在某个 perThread 执行完 updateDocument 后触发  (此时只是在内存结构中生成了便于存储到索引中的数据格式)
     *
     * @param perThread 处理本次写入的 perThread对象
     * @param isUpdate  本次更新是否携带了   Node  (node代表删除某些doc 或者更新doc的信息)
     * @return
     */
    synchronized DocumentsWriterPerThread doAfterDocument(DocumentsWriterPerThread perThread, boolean isUpdate) {
        try {
            // 更新此时 activeBytes/flushBytes
            commitPerThreadBytes(perThread);
            // 此时线程还未处于刷盘阶段
            if (!perThread.isFlushPending()) {
                // 如果此时发生了更新操作 那么会删除一些doc
                if (isUpdate) {
                    flushPolicy.onUpdate(this, perThread);
                } else {
                    // 只有插入操作
                    flushPolicy.onInsert(this, perThread);
                }
                // 代表此时单个 DWPT使用的byte 已经超过了预计值  需要触发刷盘
                if (!perThread.isFlushPending() && perThread.bytesUsed() > hardMaxBytesPerDWPT) {
                    // Safety check to prevent a single DWPT exceeding its RAM limit. This
                    // is super important since we can not address more than 2048 MB per DWPT
                    setFlushPending(perThread);
                }
            }
            return checkout(perThread, false);
        } finally {
            boolean stalled = updateStallState();
            assert assertNumDocsSinceStalled(stalled) && assertMemory();
        }
    }

    /**
     * @param perThread
     * @param markPending 是否需要标记成 待刷盘
     * @return
     */
    private DocumentsWriterPerThread checkout(DocumentsWriterPerThread perThread, boolean markPending) {
        assert Thread.holdsLock(this);
        // 如果本次打算将所有 perThread 都进行刷盘
        if (fullFlush) {
            if (perThread.isFlushPending()) {
                checkoutAndBlock(perThread);
                return nextPendingFlush();
            }
        } else {
            if (markPending) {
                assert perThread.isFlushPending() == false;
                setFlushPending(perThread);
            }

            // 将 perThread从待刷盘改成刷盘中
            if (perThread.isFlushPending()) {
                return checkOutForFlush(perThread);
            }
        }
        return null;
    }

    // TODO 忽略断言
    private boolean assertNumDocsSinceStalled(boolean stalled) {
        /*
         *  updates the number of documents "finished" while we are in a stalled state.
         *  this is important for asserting memory upper bounds since it corresponds
         *  to the number of threads that are in-flight and crossed the stall control
         *  check before we actually stalled.
         *  see #assertMemory()
         */
        if (stalled) {
            numDocsSinceStalled++;
        } else {
            numDocsSinceStalled = 0;
        }
        return true;
    }

    /**
     * 代表某个刷盘操作完成  (实际上也可能是刷盘操作终止)
     *
     * @param dwpt
     */
    synchronized void doAfterFlush(DocumentsWriterPerThread dwpt) {
        assert flushingWriters.contains(dwpt);
        try {
            // 因为本次刷盘任务已经完成了 所以刷盘中队列移除本元素
            flushingWriters.remove(dwpt);
            flushBytes -= dwpt.getLastCommittedBytesUsed();
            assert assertMemory();
        } finally {
            try {
                // 检测当前是否可以解除写doc到内存的线程阻塞
                updateStallState();
            } finally {
                notifyAll();
            }
        }
    }

    /**
     * 记录 状态修改为stall的时间戳
     */
    private long stallStartNS;

    /**
     * 检测是否需要修改 stall(暂用) 状态
     *
     * @return
     */
    private boolean updateStallState() {

        assert Thread.holdsLock(this);
        // 获取 强制刷盘的上限   当此时内存中占用的bytes数超过该值时 就保持 stall 状态
        final long limit = stallLimitBytes();
        /*
         * we block indexing threads if net byte grows due to slow flushes
         * yet, for small ram buffers and large documents we can easily
         * reach the limit without any ongoing flushes. we need to ensure
         * that we don't stall/block if an ongoing or pending flush can
         * not free up enough memory to release the stall lock.
         * flushBytes 代表刷盘中的内存  activeBytes + flushBytes 代表当前占用的总内存
         */
        final boolean stall = (activeBytes + flushBytes) > limit &&  // 代表此时占用的总内存过多
                activeBytes < limit &&    // 如果 activeBytes > limit  代表此时有过多数据囤积在内存中 反而允许直接提交刷盘任务
                !closed;

        if (infoStream.isEnabled("DWFC")) {
            if (stall != stallControl.anyStalledThreads()) {
                if (stall) {
                    infoStream.message("DW", String.format(Locale.ROOT, "now stalling flushes: netBytes: %.1f MB flushBytes: %.1f MB fullFlush: %b",
                            netBytes() / 1024. / 1024., getFlushingBytes() / 1024. / 1024., fullFlush));
                    stallStartNS = System.nanoTime();
                } else {
                    infoStream.message("DW", String.format(Locale.ROOT, "done stalling flushes for %.1f msec: netBytes: %.1f MB flushBytes: %.1f MB fullFlush: %b",
                            (System.nanoTime() - stallStartNS) / 1000000., netBytes() / 1024. / 1024., getFlushingBytes() / 1024. / 1024., fullFlush));
                }
            }
        }

        stallControl.updateStalled(stall);
        return stall;
    }

    /**
     * 等待所有刷盘中的任务完成
     */
    public synchronized void waitForFlush() {
        while (flushingWriters.size() != 0) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new ThreadInterruptedException(e);
            }
        }
    }

    /**
     * Sets flush pending state on the given {@link DocumentsWriterPerThread}. The
     * {@link DocumentsWriterPerThread} must have indexed at least on Document and must not be
     * already pending.
     * 将当前线程标记成 刷盘中
     */
    public synchronized void setFlushPending(DocumentsWriterPerThread perThread) {
        assert !perThread.isFlushPending();
        if (perThread.getNumDocsInRAM() > 0) {
            perThread.setFlushPending(); // write access synced
            // 代表本次会将多少byte 写入到磁盘中
            final long bytes = perThread.getLastCommittedBytesUsed();
            flushBytes += bytes;
            activeBytes -= bytes;
            numPending++; // write access synced
            assert assertMemory();
        } // don't assert on numDocs since we could hit an abort excp. while selecting that dwpt for flushing

    }

    /**
     * @param perThread
     */
    synchronized void doOnAbort(DocumentsWriterPerThread perThread) {
        try {
            assert perThreadPool.isRegistered(perThread);
            assert perThread.isHeldByCurrentThread();
            // 将 flushBytes 和 activeBytes 恢复到之前的值
            if (perThread.isFlushPending()) {
                flushBytes -= perThread.getLastCommittedBytesUsed();
            } else {
                activeBytes -= perThread.getLastCommittedBytesUsed();
            }
            assert assertMemory();
            // Take it out of the loop this DWPT is stale
        } finally {
            updateStallState();
            boolean checkedOut = perThreadPool.checkout(perThread);
            assert checkedOut;
        }
    }

    /**
     * 将某个 待刷盘的 perThread 转移到一个blockFlush 队列中
     *
     * @param perThread
     */
    private void checkoutAndBlock(DocumentsWriterPerThread perThread) {
        assert perThreadPool.isRegistered(perThread);
        assert perThread.isHeldByCurrentThread();
        assert perThread.isFlushPending() : "can not block non-pending threadstate";
        assert fullFlush : "can not block if fullFlush == false";
        numPending--;
        blockedFlushes.add(perThread);
        // 从 pool中释放 perThread
        boolean checkedOut = perThreadPool.checkout(perThread);
        assert checkedOut;
    }

    /**
     * 将某个 perThread对象从 pendingFlush 变成 flushing  同时从pool中移除
     *
     * @param perThread
     * @return
     */
    private synchronized DocumentsWriterPerThread checkOutForFlush(DocumentsWriterPerThread perThread) {
        assert Thread.holdsLock(this);
        assert perThread.isFlushPending();
        assert perThread.isHeldByCurrentThread();
        assert perThreadPool.isRegistered(perThread);
        try {
            addFlushingDWPT(perThread);
            // 减少待刷盘的数量
            numPending--; // write access synced
            // 将 perThread 从pool中移除
            boolean checkedOut = perThreadPool.checkout(perThread);
            assert checkedOut;
            return perThread;
        } finally {
            updateStallState();
        }
    }

    /**
     * 增加一个刷盘中的 perThread
     *
     * @param perThread
     */
    private void addFlushingDWPT(DocumentsWriterPerThread perThread) {
        assert flushingWriters.contains(perThread) == false : "DWPT is already flushing";
        // Record the flushing DWPT to reduce flushBytes in doAfterFlush
        flushingWriters.add(perThread);
    }

    @Override
    public String toString() {
        return "DocumentsWriterFlushControl [activeBytes=" + activeBytes
                + ", flushBytes=" + flushBytes + "]";
    }

    /**
     * 返回下一个待刷盘的线程
     *
     * @return
     */
    DocumentsWriterPerThread nextPendingFlush() {
        int numPending;
        boolean fullFlush;
        synchronized (this) {
            final DocumentsWriterPerThread poll;
            // 如果当前有待刷盘的线程 直接返回
            if ((poll = flushQueue.poll()) != null) {
                // 检测当前对内存的占用是否超过限制  超过的话 选择暂停文档写入线程
                updateStallState();
                return poll;
            }
            fullFlush = this.fullFlush;
            numPending = this.numPending;
        }
        if (numPending > 0 && fullFlush == false) { // don't check if we are doing a full flush
            for (final DocumentsWriterPerThread next : perThreadPool) {
                if (next.isFlushPending()) {
                    if (next.tryLock()) {
                        try {
                            if (perThreadPool.isRegistered(next)) {
                                return checkOutForFlush(next);
                            }
                        } finally {
                            next.unlock();
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public synchronized void close() {
        // set by DW to signal that we are closing. in this case we try to not stall any threads anymore etc.
        closed = true;
    }

    /**
     * Returns an iterator that provides access to all currently active {@link DocumentsWriterPerThread}s
     */
    public Iterator<DocumentsWriterPerThread> allActiveWriters() {
        return perThreadPool.iterator();
    }

    /**
     * 触发删除操作时 转发给flushPolicy对象
     */
    synchronized void doOnDelete() {
        // pass null this is a global delete no update
        flushPolicy.onDelete(this, null);
    }

    /**
     * Returns heap bytes currently consumed by buffered deletes/updates that would be
     * freed if we pushed all deletes.  This does not include bytes consumed by
     * already pushed delete/update packets.
     */
    public long getDeleteBytesUsed() {
        return documentsWriter.deleteQueue.ramBytesUsed();
    }

    @Override
    public long ramBytesUsed() {
        // TODO: improve this to return more detailed info?
        return getDeleteBytesUsed() + netBytes();
    }

    /**
     * 返回此时正在刷盘的数量
     *
     * @return
     */
    synchronized int numFlushingDWPT() {
        return flushingWriters.size();
    }

    // flushDelete标识的作用是???

    public boolean getAndResetApplyAllDeletes() {
        return flushDeletes.getAndSet(false);
    }

    public void setApplyAllDeletes() {
        flushDeletes.set(true);
    }

    DocumentsWriterPerThread obtainAndLock() throws IOException {
        while (closed == false) {
            // 这里先尝试从空闲队列中重用 perThread 如果 无法重用就创建一个新的perThread 对象
            final DocumentsWriterPerThread perThread = perThreadPool.getAndLock();
            // 直到找到批次相同的perThread 因为触发了 markForFullFlush 后  documentsWriter.deleteQueue 会更新 与之前创建的perThread 就会不一样
            if (perThread.deleteQueue == documentsWriter.deleteQueue) {
                // simply return the DWPT even in a flush all case since we already hold the lock and the DWPT is not stale
                // since it has the current delete queue associated with it. This means we have established a happens-before
                // relationship and all docs indexed into this DWPT are guaranteed to not be flushed with the currently
                // progress full flush.
                return perThread;
            } else {
                try {
                    // we must first assert otherwise the full flush might make progress once we unlock the dwpt
                    assert fullFlush && fullFlushMarkDone == false :
                            "found a stale DWPT but full flush mark phase is already done fullFlush: "
                                    + fullFlush + " markDone: " + fullFlushMarkDone;
                } finally {
                    perThread.unlock();
                    // There is a flush-all in process and this DWPT is
                    // now stale - try another one
                }
            }
        }
        throw new AlreadyClosedException("flush control is closed");
    }

    /**
     * 标记成需要 fullFlush  并且尽可能将满足条件的待刷盘队列加入到 flushQueue 中
     * @return
     */
    long markForFullFlush() {
        final DocumentsWriterDeleteQueue flushingQueue;
        long seqNo;
        // 首先更新 docWriter 内部的deleteQueue
        synchronized (this) {
            assert fullFlush == false : "called DWFC#markForFullFlush() while full flush is still running";
            assert fullFlushMarkDone == false : "full flush collection marker is still set to true";
            fullFlush = true;
            flushingQueue = documentsWriter.deleteQueue;
            // Set a new delete queue - all subsequent DWPT will use this queue until
            // we do another full flush
            // 设置标识位 这样pool 就不能分配新的 perThread了
            perThreadPool.lockNewWriters(); // no new thread-states while we do a flush otherwise the seqNo accounting might be off
            try {
                // Insert a gap in seqNo of current active thread count, in the worst case each of those threads now have one operation in flight.  It's fine
                // if we have some sequence numbers that were never assigned:
                // 生成一个新队列
                DocumentsWriterDeleteQueue newQueue = documentsWriter.deleteQueue.advanceQueue(perThreadPool.size());
                seqNo = documentsWriter.deleteQueue.getMaxSeqNo();
                documentsWriter.resetDeleteQueue(newQueue);
            } finally {
                perThreadPool.unlockNewWriters();
            }
        }

        // 存储 更新成刷盘中的 perThread
        final List<DocumentsWriterPerThread> fullFlushBuffer = new ArrayList<>();
        // deleteQueue 相同的 perThread 是同一时期的  获取并加锁
        for (final DocumentsWriterPerThread next : perThreadPool.filterAndLock(dwpt -> dwpt.deleteQueue == flushingQueue)) {
            try {
                assert next.deleteQueue == flushingQueue
                        || next.deleteQueue == documentsWriter.deleteQueue : " flushingQueue: "
                        + flushingQueue
                        + " currentqueue: "
                        + documentsWriter.deleteQueue
                        + " perThread queue: "
                        + next.deleteQueue
                        + " numDocsInRam: " + next.getNumDocsInRAM();

                // 只要这些 perThread 写入了数据  批量刷盘
                if (next.getNumDocsInRAM() > 0) {
                    final DocumentsWriterPerThread flushingDWPT;
                    synchronized (this) {
                        if (next.isFlushPending() == false) {
                            setFlushPending(next);
                        }
                        flushingDWPT = checkOutForFlush(next);
                    }
                    assert flushingDWPT != null : "DWPT must never be null here since we hold the lock and it holds documents";
                    assert next == flushingDWPT : "flushControl returned different DWPT";
                    fullFlushBuffer.add(flushingDWPT);
                } else {
                    // it's possible that we get a DWPT with 0 docs if we flush concurrently to
                    // threads getting DWPTs from the pool. In this case we simply remove it from
                    // the pool and drop it on the floor.
                    // 没有需要写入的数据 直接移除就好
                    boolean checkout = perThreadPool.checkout(next);
                    assert checkout;
                }
            } finally {
                next.unlock();
            }
        }
        synchronized (this) {
            /* make sure we move all DWPT that are where concurrently marked as
             * pending and moved to blocked are moved over to the flushQueue. There is
             * a chance that this happens since we marking DWPT for full flush without
             * blocking indexing.*/
            // 将阻塞中的队列 转移到 刷盘中
            pruneBlockedQueue(flushingQueue);
            assert assertBlockedFlushes(documentsWriter.deleteQueue);
            // 将之前设置为刷盘中的 perThread 都加入到 flushQueue 中
            flushQueue.addAll(fullFlushBuffer);
            updateStallState();
            fullFlushMarkDone = true; // at this point we must have collected all DWPTs that belong to the old delete queue
        }
        assert assertActiveDeleteQueue(documentsWriter.deleteQueue);
        assert flushingQueue.getLastSequenceNumber() <= flushingQueue.getMaxSeqNo();
        return seqNo;
    }

    private boolean assertActiveDeleteQueue(DocumentsWriterDeleteQueue queue) {
        for (final DocumentsWriterPerThread next : perThreadPool) {
            assert next.deleteQueue == queue : "numDocs: " + next.getNumDocsInRAM();
        }
        return true;
    }

    /**
     * Prunes the blockedQueue by removing all DWPTs that are associated with the given flush queue.
     * 将阻塞中的刷盘任务 返回到刷盘中队列
     */
    private void pruneBlockedQueue(final DocumentsWriterDeleteQueue flushingQueue) {
        assert Thread.holdsLock(this);
        Iterator<DocumentsWriterPerThread> iterator = blockedFlushes.iterator();
        while (iterator.hasNext()) {
            DocumentsWriterPerThread blockedFlush = iterator.next();
            if (blockedFlush.deleteQueue == flushingQueue) {
                iterator.remove();
                addFlushingDWPT(blockedFlush);
                // don't decr pending here - it's already done when DWPT is blocked
                flushQueue.add(blockedFlush);
            }
        }
    }

    /**
     * 将阻塞的刷盘任务转移到 flushQueue中
     * 应该是之后想插入的任务发现 此时已经进入刷盘状态了 直接先添加到  blockedFlushed 中
     */
    synchronized void finishFullFlush() {
        assert fullFlush;
        assert flushQueue.isEmpty();
        assert flushingWriters.isEmpty();
        try {
            if (!blockedFlushes.isEmpty()) {
                assert assertBlockedFlushes(documentsWriter.deleteQueue);
                pruneBlockedQueue(documentsWriter.deleteQueue);
                assert blockedFlushes.isEmpty();
            }
        } finally {
            fullFlushMarkDone = fullFlush = false;

            updateStallState();
        }
    }

    boolean assertBlockedFlushes(DocumentsWriterDeleteQueue flushingQueue) {
        for (DocumentsWriterPerThread blockedFlush : blockedFlushes) {
            assert blockedFlush.deleteQueue == flushingQueue;
        }
        return true;
    }

    synchronized void abortFullFlushes() {
        try {
            abortPendingFlushes();
        } finally {
            fullFlushMarkDone = fullFlush = false;
        }
    }

    /**
     * 终止所有待刷盘的对象
     */
    synchronized void abortPendingFlushes() {
        try {
            // 找到刷盘队列中的perThread
            for (DocumentsWriterPerThread dwpt : flushQueue) {
                try {
                    // 将文档数同步到 docWriter
                    documentsWriter.subtractFlushedNumDocs(dwpt.getNumDocsInRAM());
                    dwpt.abort();
                } catch (Exception ex) {
                    // that's fine we just abort everything here this is best effort
                } finally {
                    // 触发钩子
                    doAfterFlush(dwpt);
                }
            }
            // 此次刷盘失败 连同被阻塞的刷盘任务一起关闭
            for (DocumentsWriterPerThread blockedFlush : blockedFlushes) {
                try {
                    addFlushingDWPT(blockedFlush); // add the blockedFlushes for correct accounting in doAfterFlush
                    documentsWriter.subtractFlushedNumDocs(blockedFlush.getNumDocsInRAM());
                    blockedFlush.abort();
                } catch (Exception ex) {
                    // that's fine we just abort everything here this is best effort
                } finally {
                    doAfterFlush(blockedFlush);
                }
            }
        } finally {
            flushQueue.clear();
            blockedFlushes.clear();
            updateStallState();
        }
    }

    /**
     * Returns <code>true</code> if a full flush is currently running
     */
    synchronized boolean isFullFlush() {
        return fullFlush;
    }

    /**
     * Returns the number of flushes that are already checked out but not yet
     * actively flushing
     */
    synchronized int numQueuedFlushes() {
        return flushQueue.size();
    }

    /**
     * Returns the number of flushes that are checked out but not yet available
     * for flushing. This only applies during a full flush if a DWPT needs
     * flushing but must not be flushed until the full flush has finished.
     */
    synchronized int numBlockedFlushes() {
        return blockedFlushes.size();
    }

    /**
     * This method will block if too many DWPT are currently flushing and no
     * checked out DWPT are available
     */
    void waitIfStalled() {
        stallControl.waitIfStalled();
    }

    /**
     * Returns <code>true</code> iff stalled
     */
    boolean anyStalledThreads() {
        return stallControl.anyStalledThreads();
    }

    /**
     * Returns the {@link IndexWriter} {@link InfoStream}
     */
    public InfoStream getInfoStream() {
        return infoStream;
    }

    /**
     * 这里遍历所有的线程 找到写入doc最多的线程
     *
     * @return
     */
    synchronized DocumentsWriterPerThread findLargestNonPendingWriter() {
        DocumentsWriterPerThread maxRamUsingWriter = null;
        long maxRamSoFar = 0;
        int count = 0;
        for (DocumentsWriterPerThread next : perThreadPool) {
            if (next.isFlushPending() == false && next.getNumDocsInRAM() > 0) {
                final long nextRam = next.bytesUsed();
                if (infoStream.isEnabled("FP")) {
                    infoStream.message("FP", "thread state has " + nextRam + " bytes; docInRAM=" + next.getNumDocsInRAM());
                }
                count++;
                if (nextRam > maxRamSoFar) {
                    maxRamSoFar = nextRam;
                    maxRamUsingWriter = next;
                }
            }
        }
        if (infoStream.isEnabled("FP")) {
            infoStream.message("FP", count + " in-use non-flushing threads states");
        }
        return maxRamUsingWriter;
    }

    /**
     * Returns the largest non-pending flushable DWPT or <code>null</code> if there is none.
     */
    final DocumentsWriterPerThread checkoutLargestNonPendingWriter() {
        DocumentsWriterPerThread largestNonPendingWriter = findLargestNonPendingWriter();
        if (largestNonPendingWriter != null) {
            // we only lock this very briefly to swap it's DWPT out - we don't go through the DWPTPool and it's free queue
            largestNonPendingWriter.lock();
            try {
                // 如果目标线程已经注册到 pool中了
                if (perThreadPool.isRegistered(largestNonPendingWriter)) {
                    synchronized (this) {
                        try {
                            return checkout(largestNonPendingWriter, largestNonPendingWriter.isFlushPending() == false);
                        } finally {
                            updateStallState();
                        }
                    }
                }
            } finally {
                largestNonPendingWriter.unlock();
            }
        }
        return null;
    }

    long getPeakActiveBytes() {
        return peakActiveBytes;
    }

    long getPeakNetBytes() {
        return peakNetBytes;
    }
}
