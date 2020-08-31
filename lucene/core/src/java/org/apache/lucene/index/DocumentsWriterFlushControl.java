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

    /**
     * 单个PerThread 如果占用的内存超过该值 会强制刷盘
     */
    private final long hardMaxBytesPerDWPT;

    /**
     * 代表此时内存中有多少bytes  解析doc后 数据先累加到 activeBytes 当perThread进入待刷盘状态时 就会将对应的activeBytes数据转移到flushBytes 上
     */
    private long activeBytes = 0;
    /**
     * 本次要刷盘的 bytes  而当刷盘完成后 该值会减小 (因为此时数据已经存入到磁盘了  同时 flushBytes + activeBytes 才是占用的总内存)
     */
    private volatile long flushBytes = 0;
    /**
     * 当前有多少待刷盘 perThread  一旦perThread 移动到flushingWriters 后就不认为是待刷盘了
     */
    private volatile int numPending = 0;
    private int numDocsSinceStalled = 0; // only with assert
    /**
     * 代表在本次刷盘中 需要执行一些删除操作
     */
    private final AtomicBoolean flushDeletes = new AtomicBoolean(false);
    private boolean fullFlush = false;
    private boolean fullFlushMarkDone = false; // only for assertion that we don't get stale DWPTs from the pool

    /**
     * The flushQueue is used to concurrently distribute DWPTs that are ready to be flushed ie. when a full flush is in
     * progress. This might be triggered by a commit or NRT refresh. The trigger will only walk all eligible DWPTs and
     * mark them as flushable putting them in the flushQueue ready for other threads (ie. indexing threads) to help flushing
     * 该容器的定位是 每次调用 doFlush前从该对象中获取需要执行刷盘的 perThread对象
     */
    private final Queue<DocumentsWriterPerThread> flushQueue = new LinkedList<>();
    // only for safety reasons if a DWPT is close to the RAM limit
    private final Queue<DocumentsWriterPerThread> blockedFlushes = new LinkedList<>();
    // flushingWriters holds all currently flushing writers. There might be writers in this list that
    // are also in the flushQueue which means that writers in the flushingWriters list are not necessarily
    // already actively flushing. They are only in the state of flushing and might be picked up in the future by
    // polling the flushQueue
    // 该容器的定位是  当某个 perThread 完成了刷盘后 才从该容器中移除  通过检测该容器是否为空可以确定此时是否还有未完成的刷盘任务  主要是配合waitForFlush() 实现外部线程阻塞等待所有perThread 完成的功能
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
     * 解析doc 意味着会有大量暂存在内存中
     * 在没有开启自动刷盘的情况下  最多只允许占用一部分内存  直到手动触发刷盘 才允许继续解析doc
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
     * @param isUpdate  本次更新是否携带了   Node  (node代表删除某些doc 或者更新doc的信息)   如果node的类型是 DocValuesUpdatesNode 该值也是false
     * @return
     */
    synchronized DocumentsWriterPerThread doAfterDocument(DocumentsWriterPerThread perThread, boolean isUpdate) {
        try {
            // 更新此时 activeBytes/flushBytes
            commitPerThreadBytes(perThread);
            // 此时线程还未处于刷盘阶段才进行下面的处理  也就是如果已经在刷盘中了 必然已经将该更新的都更新掉了
            if (!perThread.isFlushPending()) {
                // 如果此时发生了更新操作 那么会删除一些doc
                if (isUpdate) {
                    // 实际上就是连续触发 OnInsert 和OnDelete
                    // 在lucene的默认实现中 FlushPolicy 会在当前线程待写入的doc过多 或者此时总的内存占用过大时 将当前线程/待写入doc最多的线程标记成待刷盘状态
                    // 在onDelete中 如果发现此时待删除的数据过多 就会设置flushDeletes为true
                    flushPolicy.onUpdate(this, perThread);
                } else {
                    // 只有插入操作
                    flushPolicy.onInsert(this, perThread);
                }
                if (!perThread.isFlushPending() && perThread.bytesUsed() > hardMaxBytesPerDWPT) {
                    // Safety check to prevent a single DWPT exceeding its RAM limit. This
                    // is super important since we can not address more than 2048 MB per DWPT
                    setFlushPending(perThread);
                }
            }
            // 将待刷盘线程移动到刷盘中线程 以及从pool中移除
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
        // 从下面的逻辑可以推断 当一个perThread 将要刷盘前 会将自身从 pool中移除 避免被使用者再次获取 并写入doc

        // 此时是否处于一个全刷盘的状态 此时会创建一个新的deleteQueue 并将同一时期的所有perThread的数据刷盘
        if (fullFlush) {
            if (perThread.isFlushPending()) {
                // 将 perThread转移到一个 block容器中 同时从pool中移除该线程
                checkoutAndBlock(perThread);
                // TODO
                return nextPendingFlush();
            }
        } else {
            // 将该线程标记成 待刷盘
            if (markPending) {
                assert perThread.isFlushPending() == false;
                setFlushPending(perThread);
            }

            if (perThread.isFlushPending()) {
                // 将线程从 待刷盘移动到刷盘中 同时从 pool中移除
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
     * 代表某个perThread刷盘操作完成  (无论成功失败)
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
                // 有一个对外暴露的api   调用线程会阻塞直到flushingWriters 内所有对象都完成刷盘 ，这里就是尝试唤醒阻塞的线程
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
         * 首先 (activeBytes + flushBytes) <= limit  是没有达到内存上限 必然可以继续解析doc 并往内存中写入数据
         * 当 activeBytes < limit 时 代表有部分内存已经处于即将要写入磁盘的状态了 那么此时一种增加解析doc的速度方式 就是激进的认为这些数据会很快的刷盘成功 这样就不会阻止新的doc 解析 (stall为true 会导致尝试解析doc的线程被阻塞1秒)
         */
        final boolean stall = (activeBytes + flushBytes) > limit &&
                activeBytes < limit &&
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
        // 只有此时线程中确实有未刷盘的doc时 才会处理
        if (perThread.getNumDocsInRAM() > 0) {
            // 状态先变成待刷盘中
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
     * 当某个perThread 被禁止时触发
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
     * 将待刷盘的 perThread 变成刷盘中
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
        // 当没有任务在刷盘队列中时 进行检测   但是如果正处在 fullFlush状态 那么不进行检测   因为在fullFlush中已经确保把同一deleteQueue时期的 perThread都转移到 flushQueue中了 所以不需要再检测池中的perThread对象
        if (numPending > 0 && fullFlush == false) { // don't check if we are doing a full flush
            for (final DocumentsWriterPerThread next : perThreadPool) {
                // 发现待刷盘的线程 只要他现在没有被其他线程占用 就转移到刷盘队列 并从池中移除
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

    /**
     * @return
     */
    public boolean getAndResetApplyAllDeletes() {
        return flushDeletes.getAndSet(false);
    }

    public void setApplyAllDeletes() {
        flushDeletes.set(true);
    }

    /**
     * 此时分配一个新的 docWriter线程 用于往索引文件中写入数据
     * @return
     * @throws IOException
     */
    DocumentsWriterPerThread obtainAndLock() throws IOException {
        // 确保此时 刷盘控制器还没有被关闭
        while (closed == false) {
            // 这里先尝试从空闲队列中重用 perThread 如果 无法重用就创建一个新的perThread 对象
            final DocumentsWriterPerThread perThread = perThreadPool.getAndLock();
            // 可以看到 从freeList 获取到的 perThread 可能是已经过期的  此时docWriter关联的deleteQueue可能已经替换过了
            // 那么忽略这个线程 因为它的 更新/删除动作无法被 queue采集
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
     * 此时进入了 fullFlush 状态
     * @return
     */
    long markForFullFlush() {
        final DocumentsWriterDeleteQueue flushingQueue;
        long seqNo;
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
                // 这里生成了一个新的queue 并使用新的 序列号 / 同时内部的 generate也会增加
                DocumentsWriterDeleteQueue newQueue = documentsWriter.deleteQueue.advanceQueue(perThreadPool.size());
                // 返回当前队列最大的序列号
                seqNo = documentsWriter.deleteQueue.getMaxSeqNo();
                // 更新内部的队列
                documentsWriter.resetDeleteQueue(newQueue);
            } finally {
                // 当新的队列生成时 就允许创建新的 perThread对象  相当于此时新的线程只会将node写入到新队列中 不会对本次的flush造成影响  这样锁的粒度是最小的 也不会丢失删除/更新请求
                perThreadPool.unlockNewWriters();
            }
        }

        // 存储本次参与fullFlush的所有线程
        final List<DocumentsWriterPerThread> fullFlushBuffer = new ArrayList<>();
        // 获取同一时期的线程 做到本次刷盘与之后的写入 解耦
        // 返回此时还未刷盘的所有 PerThread
        // 在 filterAndLock 方法中 会通过 lock 阻塞获取某个线程 如果此时该线程正在被其他人使用 那么会等待索引生成完毕  那些线程在发现此时已经进入了 fullFlush状态 就会将自己设置到 blockFlush队列中
        // 在下面会从blockFlush中将线程取出来 并执行任务  如果刚好未检测到 fullFlush标识 那么就会直到刷盘完成
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
                        // 如果此时线程没有被标记成待刷盘状态  更新状态   (从等待刷盘 到刷盘完成 都属于 flushPending)
                        if (next.isFlushPending() == false) {
                            setFlushPending(next);
                        }
                        // 将线程转移到刷盘队列中 同时从pool移除
                        flushingDWPT = checkOutForFlush(next);
                    }
                    assert flushingDWPT != null : "DWPT must never be null here since we hold the lock and it holds documents";
                    assert next == flushingDWPT : "flushControl returned different DWPT";
                    fullFlushBuffer.add(flushingDWPT);
                } else {
                    // it's possible that we get a DWPT with 0 docs if we flush concurrently to
                    // threads getting DWPTs from the pool. In this case we simply remove it from
                    // the pool and drop it on the floor.
                    // 如果此时没有需要刷盘的数据  直接从pool中移除
                    boolean checkout = perThreadPool.checkout(next);
                    assert checkout;
                }
            } finally {
                // 因为此时已经从pool中移除了 不会被其他线程访问到 所以可以解锁
                next.unlock();
            }
        }
        synchronized (this) {
            /* make sure we move all DWPT that are where concurrently marked as
             * pending and moved to blocked are moved over to the flushQueue. There is
             * a chance that this happens since we marking DWPT for full flush without
             * blocking indexing.*/
            // TODO 找到一些因为特殊原因在刷盘过程中被阻塞的perThread对象 并重新加入到 刷盘队列中
            pruneBlockedQueue(flushingQueue);
            assert assertBlockedFlushes(documentsWriter.deleteQueue);
            // 将之前设置为刷盘中的 perThread 都加入到 flushQueue 中  这样这些 PerThread 就同时在 flushQueue 和 flushWriting 队列中了   在调用 doFlush时 就是循环从  flushQueue中获取perThread 并执行刷盘操作
            flushQueue.addAll(fullFlushBuffer);
            updateStallState();
            // 断言相关的先忽略
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
     * 找到阻塞中的刷盘任务  只要与该删除队列属于同一generate 就转移到刷盘队列中
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
     * 代表 fullFlush 已经完成 无论成功/失败
     */
    synchronized void finishFullFlush() {
        assert fullFlush;
        assert flushQueue.isEmpty();
        assert flushingWriters.isEmpty();
        try {
            // 将之前因为某些原因被阻塞的 perThread 对象   重新加入到 待刷盘队列中
            // TODO 推测由于此时处于 fullFlush 状态  所以之后准备flush的perThread 对象都会被阻塞
            if (!blockedFlushes.isEmpty()) {
                assert assertBlockedFlushes(documentsWriter.deleteQueue);
                pruneBlockedQueue(documentsWriter.deleteQueue);
                assert blockedFlushes.isEmpty();
            }
        } finally {
            // 代表本次全刷盘任务已经完成 同时根据当前内存占用情况 尝试解除对解析doc请求线程的阻塞
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

    /**
     * 代表以异常形式结束了本次 fullFlush  那么就将此时存储在 待刷盘队列中的对象
     */
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
            // 注意 flushQueue 中维护的是待取出 并执行的  perThread对象  而   flushingWriters 中维护的是正在刷盘的对象  当刷盘完成后会从该队列中移除
            // 这里只是放弃 还未从队列中取出的 perThread对象
            for (DocumentsWriterPerThread dwpt : flushQueue) {
                try {
                    // 因为该对象即将被废弃 所以该对象维护在内存中的文档数需要下降
                    documentsWriter.subtractFlushedNumDocs(dwpt.getNumDocsInRAM());
                    // 将内部数据清空 以及删除已经产生的索引文件
                    dwpt.abort();
                } catch (Exception ex) {
                    // that's fine we just abort everything here this is best effort
                } finally {
                    // 触发钩子  这里会同时将 perThread 从 flushingWriters 中移除
                    doAfterFlush(dwpt);
                }
            }
            // 这些被阻塞的对象 推测是检测到当前处在 fullFlush中 所以延迟刷盘  这里会将这些待处理任务一起清除
            for (DocumentsWriterPerThread blockedFlush : blockedFlushes) {
                try {
                    // 先将对象添加到 pendingWrite队列中  在doAfterFlush中 又会将对象从 pendingWriter中移除
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
     * 这里遍历所有的线程 找到待刷盘doc最多的 perThread
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
