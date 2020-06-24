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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimitedIndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * A {@link MergeScheduler} that runs each merge using a
 * separate thread.
 *
 * <p>Specify the max number of threads that may run at
 * once, and the maximum number of simultaneous merges
 * with {@link #setMaxMergesAndThreads}.</p>
 *
 * <p>If the number of merges exceeds the max number of threads
 * then the largest merges are paused until one of the smaller
 * merges completes.</p>
 *
 * <p>If more than {@link #getMaxMergeCount} merges are
 * requested then this class will forcefully throttle the
 * incoming threads by pausing until one more merges
 * complete.</p>
 *
 * <p>This class attempts to detect whether the index is
 * on rotational storage (traditional hard drive) or not
 * (e.g. solid-state disk) and changes the default max merge
 * and thread count accordingly.  This detection is currently
 * Linux-only, and relies on the OS to put the right value
 * into /sys/block/&lt;dev&gt;/block/rotational.  For all
 * other operating systems it currently assumes a rotational
 * disk for backwards compatibility.  To enable default
 * settings for spinning or solid state disks for such
 * operating systems, use {@link #setDefaultMaxMergesAndThreads(boolean)}.
 * 并发merge 对象
 */
public class ConcurrentMergeScheduler extends MergeScheduler {

    /**
     * Dynamic default for {@code maxThreadCount} and {@code maxMergeCount},
     * used to detect whether the index is backed by an SSD or rotational disk and
     * set {@code maxThreadCount} accordingly.  If it's an SSD,
     * {@code maxThreadCount} is set to {@code max(1, min(4, cpuCoreCount/2))},
     * otherwise 1.  Note that detection only currently works on
     * Linux; other platforms will assume the index is not on an SSD.
     */
    public static final int AUTO_DETECT_MERGES_AND_THREADS = -1;

    /**
     * Used for testing.
     *
     * @lucene.internal
     */
    public static final String DEFAULT_CPU_CORE_COUNT_PROPERTY = "lucene.cms.override_core_count";

    /**
     * Used for testing.
     *
     * @lucene.internal
     */
    public static final String DEFAULT_SPINS_PROPERTY = "lucene.cms.override_spins";

    /**
     * List of currently active {@link MergeThread}s.
     */
    protected final List<MergeThread> mergeThreads = new ArrayList<>();

    // Max number of merge threads allowed to be running at
    // once.  When there are more merges then this, we
    // forcefully pause the larger ones, letting the smaller
    // ones run, up until maxMergeCount merges at which point
    // we forcefully pause incoming threads (that presumably
    // are the ones causing so much merging).
    // 仅是针对 大块的segment 的merge工作  如果数量超过了 maxThreadCount 那么只有部分大任务能正常执行
    private int maxThreadCount = AUTO_DETECT_MERGES_AND_THREADS;

    // Max number of merges we accept before forcefully
    // throttling the incoming threads
    private int maxMergeCount = AUTO_DETECT_MERGES_AND_THREADS;

    /**
     * How many {@link MergeThread}s have kicked off (this is use
     * to name them).
     */
    protected int mergeThreadCount;

    /**
     * Floor for IO write rate limit (we will never go any lower than this)
     */
    private static final double MIN_MERGE_MB_PER_SEC = 5.0;

    /**
     * Ceiling for IO write rate limit (we will never go any higher than this)
     * 虽然是一个有效值  但是一般情况下实际上无法超越这个IO限制值了  ==  不做限制
     */
    private static final double MAX_MERGE_MB_PER_SEC = 10240.0;

    /**
     * Initial value for IO write rate limit when doAutoIOThrottle is true
     */
    private static final double START_MB_PER_SEC = 20.0;

    /**
     * Merges below this size are not counted in the maxThreadCount, i.e. they can freely run in their own thread (up until maxMergeCount).
     * 超过该值的segment的合并优先级会比较低  每当有一个相对较小的segment 合并任务被创建时  会优先执行小任务 而暂停大任务
     * 并且只针对超过该大小的segment的合并采用限流策略
     */
    private static final double MIN_BIG_MERGE_MB = 50.0;

    /**
     * Current IO writes throttle rate
     * 正常的 IO 吞吐量
     */
    protected double targetMBPerSec = START_MB_PER_SEC;

    /**
     * true if we should rate-limit writes for each merge
     */
    private boolean doAutoIOThrottle = true;

    /**
     * 当触发强制写入时  对IO 没有限制
     */
    private double forceMergeMBPerSec = Double.POSITIVE_INFINITY;

    /**
     * Sole constructor, with all settings set to default
     * values.
     */
    public ConcurrentMergeScheduler() {
    }

    /**
     * Expert: directly set the maximum number of merge threads and
     * simultaneous merges allowed.
     * 设置 merge 并行度
     *
     * @param maxMergeCount  the max # simultaneous merges that are allowed.
     *                       If a merge is necessary yet we already have this many
     *                       threads running, the incoming thread (that is calling
     *                       add/updateDocument) will block until a merge thread
     *                       has completed.  Note that we will only run the
     *                       smallest <code>maxThreadCount</code> merges at a time.
     * @param maxThreadCount the max # simultaneous merge threads that should
     *                       be running at once.  This must be &lt;= <code>maxMergeCount</code>
     */
    public synchronized void setMaxMergesAndThreads(int maxMergeCount, int maxThreadCount) {
        if (maxMergeCount == AUTO_DETECT_MERGES_AND_THREADS && maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
            // OK
            this.maxMergeCount = AUTO_DETECT_MERGES_AND_THREADS;
            this.maxThreadCount = AUTO_DETECT_MERGES_AND_THREADS;
            // 必须同时设置成 -1 才有效 否则认为是参数设置异常
        } else if (maxMergeCount == AUTO_DETECT_MERGES_AND_THREADS) {
            throw new IllegalArgumentException("both maxMergeCount and maxThreadCount must be AUTO_DETECT_MERGES_AND_THREADS");
        } else if (maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
            throw new IllegalArgumentException("both maxMergeCount and maxThreadCount must be AUTO_DETECT_MERGES_AND_THREADS");
        } else {
            if (maxThreadCount < 1) {
                throw new IllegalArgumentException("maxThreadCount should be at least 1");
            }
            if (maxMergeCount < 1) {
                throw new IllegalArgumentException("maxMergeCount should be at least 1");
            }
            if (maxThreadCount > maxMergeCount) {
                throw new IllegalArgumentException("maxThreadCount should be <= maxMergeCount (= " + maxMergeCount + ")");
            }
            this.maxThreadCount = maxThreadCount;
            this.maxMergeCount = maxMergeCount;
        }
    }

    /**
     * Sets max merges and threads to proper defaults for rotational
     * or non-rotational storage.
     *
     * @param spins true to set defaults best for traditional rotatational storage (spinning disks),
     *              else false (e.g. for solid-state disks)
     *              根据是否是 机械硬盘/固态硬盘  设置写入线程数
     */
    public synchronized void setDefaultMaxMergesAndThreads(boolean spins) {
        if (spins) {
            // 实际上因为 机械硬盘的旋转机制 是不适合使用多线程执行IO 的 所以这里指定了线程数为 1
            maxThreadCount = 1;
            maxMergeCount = 6;
        } else {
            // 只有固态硬盘这种才适合真正使用多线程进行 IO 操作
            int coreCount = Runtime.getRuntime().availableProcessors();

            // Let tests override this to help reproducing a failure on a machine that has a different
            // core count than the one where the test originally failed:
            try {
                String value = System.getProperty(DEFAULT_CPU_CORE_COUNT_PROPERTY);
                if (value != null) {
                    coreCount = Integer.parseInt(value);
                }
            } catch (Throwable ignored) {
            }

            maxThreadCount = Math.max(1, Math.min(4, coreCount / 2));
            maxMergeCount = maxThreadCount + 5;
        }
    }

    /**
     * Set the per-merge IO throttle rate for forced merges (default: {@code Double.POSITIVE_INFINITY}).
     */
    // 当触发强制merge时 使用的吞吐量
    public synchronized void setForceMergeMBPerSec(double v) {
        forceMergeMBPerSec = v;
        // 该方法会清理 已经失活的线程  以及根据当前参数重新计算 rateLimiter限制值的功能
        updateMergeThreads();
    }

    /**
     * Get the per-merge IO throttle rate for forced merges.
     */
    public synchronized double getForceMergeMBPerSec() {
        return forceMergeMBPerSec;
    }

    /**
     * Turn on dynamic IO throttling, to adaptively rate limit writes
     * bytes/sec to the minimal rate necessary so merges do not fall behind.
     * By default this is enabled.
     */
    // 开启动态调节阀门的功能  也就是开启限流
    public synchronized void enableAutoIOThrottle() {
        doAutoIOThrottle = true;
        targetMBPerSec = START_MB_PER_SEC;
        updateMergeThreads();
    }

    /**
     * Turn off auto IO throttling.
     *
     * @see #enableAutoIOThrottle
     */
    public synchronized void disableAutoIOThrottle() {
        doAutoIOThrottle = false;
        updateMergeThreads();
    }

    /**
     * Returns true if auto IO throttling is currently enabled.
     */
    public synchronized boolean getAutoIOThrottle() {
        return doAutoIOThrottle;
    }

    /**
     * Returns the currently set per-merge IO writes rate limit, if {@link #enableAutoIOThrottle}
     * was called, else {@code Double.POSITIVE_INFINITY}.
     */
    // 返回当前的阀门限值  如果没有开启限流功能 返回  无限
    public synchronized double getIORateLimitMBPerSec() {
        if (doAutoIOThrottle) {
            return targetMBPerSec;
        } else {
            return Double.POSITIVE_INFINITY;
        }
    }

    /**
     * Returns {@code maxThreadCount}.
     *
     * @see #setMaxMergesAndThreads(int, int)
     */
    public synchronized int getMaxThreadCount() {
        return maxThreadCount;
    }

    /**
     * See {@link #setMaxMergesAndThreads}.
     */
    public synchronized int getMaxMergeCount() {
        return maxMergeCount;
    }

    /**
     * Removes the calling thread from the active merge threads.
     */
    // 看来每个merge线程都会关联到同一个 scheduler对象 然后每个线程在执行完任务后 主动调用该方法 从而释放引用 (否则线程长时间无法释放 会占用资源  所以很多维护线程的都是配合 WeakReference )
    synchronized void removeMergeThread() {
        Thread currentThread = Thread.currentThread();
        // Paranoia: don't trust Thread.equals:
        for (int i = 0; i < mergeThreads.size(); i++) {
            if (mergeThreads.get(i) == currentThread) {
                mergeThreads.remove(i);
                return;
            }
        }

        assert false : "merge thread " + currentThread + " was not found";
    }

    /**
     * 将merge 对象包装成 一个 directory 这样就可以使用一些类似 过滤器之类的来处理该对象
     *
     * @param merge
     * @param in
     * @return
     */
    @Override
    public Directory wrapForMerge(OneMerge merge, Directory in) {
        // 必须由merge线程自己调用
        Thread mergeThread = Thread.currentThread();
        if (!MergeThread.class.isInstance(mergeThread)) {
            throw new AssertionError("wrapForMerge should be called from MergeThread. Current thread: "
                    + mergeThread);
        }

        // Return a wrapped Directory which has rate-limited output.    获取线程携带的 限流对象
        RateLimiter rateLimiter = ((MergeThread) mergeThread).rateLimiter;
        return new FilterDirectory(in) {

            /**
             * 这里增强了往目录中写入的逻辑
             * @param name
             * @param context
             * @return
             * @throws IOException
             */
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                ensureOpen();

                // This Directory is only supposed to be used during merging,
                // so all writes should have MERGE context, else there is a bug
                // somewhere that is failing to pass down the right IOContext:
                assert context.context == IOContext.Context.MERGE : "got context=" + context.context;

                // Because rateLimiter is bound to a particular merge thread, this method should
                // always be called from that context. Verify this.
                assert mergeThread == Thread.currentThread() : "Not the same merge thread, current="
                        + Thread.currentThread() + ", expected=" + mergeThread;

                // 当往目标目录写入数据的时候 是对 merge进行限流 好像跟当前的写入是2个任务  也就是执行A任务(正常写入)  会抑制 B任务(merge) ???
                return new RateLimitedIndexOutput(rateLimiter, in.createOutput(name, context));
            }
        };
    }

    /**
     * Called whenever the running merges have changed, to set merge IO limits.
     * This method sorts the merge threads by their merge size in
     * descending order and then pauses/unpauses threads from first to last --
     * that way, smaller merges are guaranteed to run before larger ones.
     * 更新 merge线程内部的
     */

    protected synchronized void updateMergeThreads() {

        // Only look at threads that are alive & not in the
        // process of stopping (ie have an active merge):
        final List<MergeThread> activeMerges = new ArrayList<>();

        int threadIdx = 0;
        while (threadIdx < mergeThreads.size()) {
            // 顺便移除失活的线程  实际上每个mergeThread 在完成任务后 会将自己从队列中移除
            final MergeThread mergeThread = mergeThreads.get(threadIdx);
            if (!mergeThread.isAlive()) {
                // Prune any dead threads
                mergeThreads.remove(threadIdx);
                continue;
            }
            activeMerges.add(mergeThread);
            threadIdx++;
        }

        // Sort the merge threads, largest first:
        // 按照线程会merge的大小进行排序
        CollectionUtil.timSort(activeMerges);

        final int activeMergeCount = activeMerges.size();

        // 下面是从小到大排序  实际上就跟那种枚举的方式一样的  就是记录有多少个超过 MIN_BIG_MERGE_MB*1024*1024 大小的线程
        int bigMergeCount = 0;

        for (threadIdx = activeMergeCount - 1; threadIdx >= 0; threadIdx--) {
            MergeThread mergeThread = activeMerges.get(threadIdx);
            if (mergeThread.merge.estimatedMergeBytes > MIN_BIG_MERGE_MB * 1024 * 1024) {
                bigMergeCount = 1 + threadIdx;
                break;
            }
        }

        long now = System.nanoTime();

        StringBuilder message;
        if (verbose()) {
            message = new StringBuilder();
            message.append(String.format(Locale.ROOT, "updateMergeThreads ioThrottle=%s targetMBPerSec=%.1f MB/sec", doAutoIOThrottle, targetMBPerSec));
        } else {
            message = null;
        }

        // 从大到小 开始遍历 merge线程
        for (threadIdx = 0; threadIdx < activeMergeCount; threadIdx++) {
            MergeThread mergeThread = activeMerges.get(threadIdx);

            OneMerge merge = mergeThread.merge;

            // pause the thread if maxThreadCount is smaller than the number of merge threads.
            // 每次同时执行的线程数 是固定的   因为这本身会受限于 IO 所以即使提高并行度 也无法提升性能
            // 这里的逻辑是这样 bigMergeCount - maxThreadCount 对应需要暂停的数量 也就是前几个比较大的merge会先被暂停
            // 当 bigMergeCount 与 maxThreadCount数量持平的时候 是不会阻塞任何线程的 并且小任务是不会被阻塞的
            final boolean doPause = threadIdx < bigMergeCount - maxThreadCount;

            double newMBPerSec;
            // 如果需要暂停 那么IO 吞吐就是0
            if (doPause) {
                newMBPerSec = 0.0;
                // TODO 这神马意思啊  如果指定了  merge的最大段数量 反而要使用 force对应的IO限制  并且这个限制默认是∞ （也就是无限制）
                // TODO 应该是 maxNumSegments 字段只会在 执行forceMergeMBPerSec 时才设置
            } else if (merge.maxNumSegments != -1) {
                newMBPerSec = forceMergeMBPerSec;
                // 无阀门限制 就是 随意写入 不限制吞吐量
            } else if (doAutoIOThrottle == false) {
                newMBPerSec = Double.POSITIVE_INFINITY;
                // 写入的大小本身比较小的情况 就不做限制
            } else if (merge.estimatedMergeBytes < MIN_BIG_MERGE_MB * 1024 * 1024) {
                // Don't rate limit small merges:
                newMBPerSec = Double.POSITIVE_INFINITY;
            } else {
                // 使用正常的IO吞吐量
                newMBPerSec = targetMBPerSec;
            }

            MergeRateLimiter rateLimiter = mergeThread.rateLimiter;
            double curMBPerSec = rateLimiter.getMBPerSec();

            // 输出日志  忽略
            if (verbose()) {
                long mergeStartNS = merge.mergeStartNS;
                if (mergeStartNS == -1) {
                    // IndexWriter didn't start the merge yet:
                    mergeStartNS = now;
                }
                message.append('\n');
                message.append(String.format(Locale.ROOT, "merge thread %s estSize=%.1f MB (written=%.1f MB) runTime=%.1fs (stopped=%.1fs, paused=%.1fs) rate=%s\n",
                        mergeThread.getName(),
                        bytesToMB(merge.estimatedMergeBytes),
                        bytesToMB(rateLimiter.getTotalBytesWritten()),
                        nsToSec(now - mergeStartNS),
                        nsToSec(rateLimiter.getTotalStoppedNS()),
                        nsToSec(rateLimiter.getTotalPausedNS()),
                        rateToString(rateLimiter.getMBPerSec())));

                if (newMBPerSec != curMBPerSec) {
                    if (newMBPerSec == 0.0) {
                        message.append("  now stop");
                    } else if (curMBPerSec == 0.0) {
                        if (newMBPerSec == Double.POSITIVE_INFINITY) {
                            message.append("  now resume");
                        } else {
                            message.append(String.format(Locale.ROOT, "  now resume to %.1f MB/sec", newMBPerSec));
                        }
                    } else {
                        message.append(String.format(Locale.ROOT, "  now change from %.1f MB/sec to %.1f MB/sec", curMBPerSec, newMBPerSec));
                    }
                } else if (curMBPerSec == 0.0) {
                    message.append("  leave stopped");
                } else {
                    message.append(String.format(Locale.ROOT, "  leave running at %.1f MB/sec", curMBPerSec));
                }
            }

            // 设置 阀门的限制
            rateLimiter.setMBPerSec(newMBPerSec);
        }
        if (verbose()) {
            message(message.toString());
        }
    }

    /**
     * 使用一个特殊目录来初始化 merge任务
     *
     * @param directory
     * @throws IOException
     */
    private synchronized void initDynamicDefaults(Directory directory) throws IOException {
        // 确保当前并发度还没有设置   该值默认为-1
        if (maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
            // 非linux系统 默认为true 也就是默认为机械硬盘   linux系统应该是提供了特定的API 便于查询
            boolean spins = IOUtils.spins(directory);

            // Let tests override this to help reproducing a failure on a machine that has a different
            // core count than the one where the test originally failed:
            try {
                String value = System.getProperty(DEFAULT_SPINS_PROPERTY);
                if (value != null) {
                    spins = Boolean.parseBoolean(value);
                }
            } catch (Exception ignored) {
                // that's fine we might hit a SecurityException etc. here just continue
            }
            // 初始化时 先设置默认值 用户可以手动调用setXXX方法设置属性
            setDefaultMaxMergesAndThreads(spins);
            if (verbose()) {
                message("initDynamicDefaults spins=" + spins + " maxThreadCount=" + maxThreadCount + " maxMergeCount=" + maxMergeCount);
            }
        }
    }

    private static String rateToString(double mbPerSec) {
        if (mbPerSec == 0.0) {
            return "stopped";
        } else if (mbPerSec == Double.POSITIVE_INFINITY) {
            return "unlimited";
        } else {
            return String.format(Locale.ROOT, "%.1f MB/sec", mbPerSec);
        }
    }

    /**
     * 当关闭该对象时 必须等待所有merge任务结束
     */
    @Override
    public void close() {
        sync();
    }

    /**
     * Wait for any running merge threads to finish. This call is not interruptible as used by {@link #close()}.
     */
    public void sync() {
        boolean interrupted = false;
        try {
            while (true) {
                MergeThread toSync = null;
                synchronized (this) {
                    for (MergeThread t : mergeThreads) {
                        // In case a merge thread is calling us, don't try to sync on
                        // itself, since that will never finish!
                        if (t.isAlive() && t != Thread.currentThread()) {
                            toSync = t;
                            break;
                        }
                    }
                }
                if (toSync != null) {
                    try {
                        // 当前线程要等待目标线程结束
                        toSync.join();
                    } catch (InterruptedException ie) {
                        // ignore this Exception, we will retry until all threads are dead
                        interrupted = true;
                    }
                } else {
                    break;
                }
            }
        } finally {
            // finally, restore interrupt status:
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the number of merge threads that are alive, ignoring the calling thread
     * if it is a merge thread.  Note that this number is &le; {@link #mergeThreads} size.
     *
     * @lucene.internal 返回当前还有效的 mergeThread 数量
     */
    public synchronized int mergeThreadCount() {
        Thread currentThread = Thread.currentThread();
        int count = 0;
        for (MergeThread mergeThread : mergeThreads) {
            if (currentThread != mergeThread && mergeThread.isAlive() && mergeThread.merge.isAborted() == false) {
                count++;
            }
        }
        return count;
    }

    @Override
    void initialize(InfoStream infoStream, Directory directory) throws IOException {
        super.initialize(infoStream, directory);
        initDynamicDefaults(directory);

    }

    /**
     * 执行merge操作  会专门分配一个merge线程 负责处理任务
     *
     * @param mergeSource the {@link IndexWriter} to obtain the merges from.
     * @param trigger     the {@link MergeTrigger} that caused this merge to happen
     * @throws IOException
     */
    @Override
    public synchronized void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {

        if (trigger == MergeTrigger.CLOSING) {
            // Disable throttling on close:
            // 当本次是基于关闭索引触发时 不做IO限制
            targetMBPerSec = MAX_MERGE_MB_PER_SEC;
            updateMergeThreads();
        }

        // First, quickly run through the newly proposed merges
        // and add any orthogonal merges (ie a merge not
        // involving segments already pending to be merged) to
        // the queue.  If we are way behind on merging, many of
        // these newly proposed merges will likely already be
        // registered.

        if (verbose()) {
            message("now merge");
            message("  index(source): " + mergeSource.toString());
        }

        // Iterate, pulling from the IndexWriter's queue of
        // pending merges, until it's empty:
        while (true) {

            // 返回false 代表当前执行任务的线程数过多  忽略本次merge请求
            // 在 maybeStall方法中已经通过不断自旋争取到stall了 下面只要创建对应的任务线程就好
            if (maybeStall(mergeSource) == false) {
                break;
            }

            // 获取下一个待处理的对象
            OneMerge merge = mergeSource.getNextMerge();
            if (merge == null) {
                if (verbose()) {
                    message("  no more merges pending; now return");
                }
                return;
            }

            boolean success = false;
            try {
                // OK to spawn a new merge thread to handle this
                // merge:  就是 new Thread()
                final MergeThread newMergeThread = getMergeThread(mergeSource, merge);
                mergeThreads.add(newMergeThread);

                // 更新rateLimiter阀值
                updateIOThrottle(newMergeThread.merge, newMergeThread.rateLimiter);

                if (verbose()) {
                    message("    launch new thread [" + newMergeThread.getName() + "]");
                }

                newMergeThread.start();
                // 这里是考虑本次传入的merge任务对应的segment大小 因为 并行的merge任务有上限 并且小任务优先执行 这时就可能会暂停一些大任务
                updateMergeThreads();

                success = true;
            } finally {
                if (!success) {
                    // 当任务执行失败时  提前触发钩子
                    mergeSource.onMergeFinished(merge);
                }
            }
        }
    }

    /**
     * This is invoked by {@link #merge} to possibly stall the incoming
     * thread when there are too many merges running or pending.  The
     * default behavior is to force this thread, which is producing too
     * many segments for merging to keep up, to wait until merges catch
     * up. Applications that can take other less drastic measures, such
     * as limiting how many threads are allowed to index, can do nothing
     * here and throttle elsewhere.
     * <p>
     * If this method wants to stall but the calling thread is a merge
     * thread, it should return false to tell caller not to kick off
     * any new merges.
     * 是否占据摊位成功
     * 该方法是起保护作用的  避免在某一时刻触发了太多的merge
     */
    protected synchronized boolean maybeStall(MergeSource mergeSource) {
        long startStallTime = 0;
        // 当前已经有一些等待中的merge任务了  并且 当前merge线程数也达到了限制值 不应该继续创建merge线程
        while (mergeSource.hasPendingMerges() && mergeThreadCount() >= maxMergeCount) {

            // This means merging has fallen too far behind: we
            // have already created maxMergeCount threads, and
            // now there's at least one more merge pending.
            // Note that only maxThreadCount of
            // those created merge threads will actually be
            // running; the rest will be paused (see
            // updateMergeThreads).  We stall this producer
            // thread to prevent creation of new segments,
            // until merging has caught up:

            // 每个merge线程在完成任务后会尝试创建新的merge任务  当线程数过多的时候 就拒绝merge线程的自创建过程
            if (mergeThreads.contains(Thread.currentThread())) {
                // Never stall a merge thread since this blocks the thread from
                // finishing and calling updateMergeThreads, and blocking it
                // accomplishes nothing anyway (it's not really a segment producer):
                return false;
            }

            if (verbose() && startStallTime == 0) {
                message("    too many merges; stalling...");
            }
            startStallTime = System.currentTimeMillis();
            // wait 等待空出摊位 （释放锁）
            doStall();
        }

        if (verbose() && startStallTime != 0) {
            message("  stalled for " + (System.currentTimeMillis() - startStallTime) + " msec");
        }

        return true;
    }

    /**
     * Called from {@link #maybeStall} to pause the calling thread for a bit.
     */
    protected synchronized void doStall() {
        try {
            // Defensively wait for only .25 seconds in case we are missing a .notify/All somewhere:
            // 当某个线程完成任务时 调用 notifyAll()
            wait(250);
        } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
        }
    }

    /**
     * Does the actual merge, by calling {@link org.apache.lucene.index.MergeScheduler.MergeSource#merge}
     */
    protected void doMerge(MergeSource mergeSource, OneMerge merge) throws IOException {
        mergeSource.merge(merge);
    }

    /**
     * Create and return a new MergeThread
     */
    protected synchronized MergeThread getMergeThread(MergeSource mergeSource, OneMerge merge) throws IOException {
        final MergeThread thread = new MergeThread(mergeSource, merge);
        thread.setDaemon(true);
        thread.setName("Lucene Merge Thread #" + mergeThreadCount++);
        return thread;
    }

    /**
     * merge结束时触发该方法
     *
     * @param mergeSource
     */
    synchronized void runOnMergeFinished(MergeSource mergeSource) {
        // the merge call as well as the merge thread handling in the finally
        // block must be sync'd on CMS otherwise stalling decisions might cause
        // us to miss pending merges
        assert mergeThreads.contains(Thread.currentThread()) : "caller is not a merge thread";
        // Let CMS run new merges if necessary:
        try {
            merge(mergeSource, MergeTrigger.MERGE_FINISHED);
        } catch (AlreadyClosedException ace) {
            // OK
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        } finally {
            // 本线程已经完成使命 所以从队列中移除
            removeMergeThread();
            updateMergeThreads();
            // In case we had stalled indexing, we can now wake up
            // and possibly unstall:
            notifyAll();
        }
    }

    /**
     * Runs a merge thread to execute a single merge, then exits.
     */
    // 每个线程在执行完任务后会自动从 Source中获取下一个任务  并生成一个新的线程
    protected class MergeThread extends Thread implements Comparable<MergeThread> {
        final MergeSource mergeSource;
        final OneMerge merge;
        final MergeRateLimiter rateLimiter;

        /**
         * Sole constructor.
         */
        public MergeThread(MergeSource mergeSource, OneMerge merge) {
            this.mergeSource = mergeSource;
            this.merge = merge;
            // rateLimiter 不会直接操作线程 而是通过 Progress  并且 progress 内部还统计了基于各种原因暂停的时长
            this.rateLimiter = new MergeRateLimiter(merge.getMergeProgress());
        }

        @Override
        public int compareTo(MergeThread other) {
            // Larger merges sort first:
            return Long.compare(other.merge.estimatedMergeBytes, merge.estimatedMergeBytes);
        }

        @Override
        public void run() {
            try {
                if (verbose()) {
                    message("  merge thread: start");
                }

                // 就是 source.merge(merge)   (委托)
                doMerge(mergeSource, merge);

                if (verbose()) {
                    message("  merge thread: done");
                }
                // 这里会触发新一轮的merge 并创建新的线程  然后释放原来的线程
                runOnMergeFinished(mergeSource);
            } catch (Throwable exc) {
                if (exc instanceof MergePolicy.MergeAbortedException) {
                    // OK to ignore
                } else if (suppressExceptions == false) {
                    // suppressExceptions is normally only set during
                    // testing.
                    handleMergeException(exc);
                }
            }
        }
    }

    /**
     * Called when an exception is hit in a background merge
     * thread
     */
    protected void handleMergeException(Throwable exc) {
        throw new MergePolicy.MergeException(exc);
    }

    private boolean suppressExceptions;

    /**
     * Used for testing
     */
    void setSuppressExceptions() {
        if (verbose()) {
            message("will suppress merge exceptions");
        }
        suppressExceptions = true;
    }

    /**
     * Used for testing
     */
    void clearSuppressExceptions() {
        if (verbose()) {
            message("will not suppress merge exceptions");
        }
        suppressExceptions = false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName() + ": ");
        sb.append("maxThreadCount=").append(maxThreadCount).append(", ");
        sb.append("maxMergeCount=").append(maxMergeCount).append(", ");
        sb.append("ioThrottle=").append(doAutoIOThrottle);
        return sb.toString();
    }

    /**
     * 是否是挤压的数据
     *
     * @param now
     * @param merge
     * @return
     */
    private boolean isBacklog(long now, OneMerge merge) {
        double mergeMB = bytesToMB(merge.estimatedMergeBytes);
        for (MergeThread mergeThread : mergeThreads) {
            // 获取每个 merge任务的开始时间
            long mergeStartNS = mergeThread.merge.mergeStartNS;

            if (mergeThread.isAlive() &&  // 当前merge还在工作中
                    mergeThread.merge != merge && // merge对象不同
                    mergeStartNS != -1 &&  // 当前任务已经开始了
                    mergeThread.merge.estimatedMergeBytes >= MIN_BIG_MERGE_MB * 1024 * 1024 &&  // 本次merge的是一个大segment  (大块segment的merge操作是会被暂停的)
                    nsToSec(now - mergeStartNS) > 3.0) {  // 代表该segment首次启动的时间到现在已经超过3秒了  也就认为该任务在中途被暂停过
                double otherMergeMB = bytesToMB(mergeThread.merge.estimatedMergeBytes);
                double ratio = otherMergeMB / mergeMB;   // 本次要处理的大小 是之前segment的 0.3 和 3.0 之间   啥意思???
                if (ratio > 0.3 && ratio < 3.0) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Tunes IO throttle when a new merge starts.
     * 更新IO阀值
     */
    private synchronized void updateIOThrottle(OneMerge newMerge, MergeRateLimiter rateLimiter) throws IOException {
        if (doAutoIOThrottle == false) {
            return;
        }

        double mergeMB = bytesToMB(newMerge.estimatedMergeBytes);
        // 本次merge的不是一个大块的segment 不需要进行限流
        if (mergeMB < MIN_BIG_MERGE_MB) {
            // Only watch non-trivial merges for throttling; this is safe because the MP must eventually
            // have to do larger merges:
            return;
        }

        long now = System.nanoTime();

        // 看来下面的逻辑 跟新传入的merge大小有很大关系 比如 应该是之前的积压才导致传入的 segment 特别大之类的

        // Simplistic closed-loop feedback control: if we find any other similarly
        // sized merges running, then we are falling behind, so we bump up the
        // IO throttle, else we lower it:
        // 代表有某些大块的segment 任务  执行了很长时间 (可能是因为中断的原因)
        boolean newBacklog = isBacklog(now, newMerge);

        boolean curBacklog = false;

        // 检测就当前执行的任务 中是否有积压数据    TODO 思考下 当前几个segment中有几个的大小相近有什么深层含义  应该是这样  如果正常执行那么某个segment的大小应该会明显小于其他的segment
        if (newBacklog == false) {
            // 当前线程数超过了最大值  就认为肯定发生了积压事件
            if (mergeThreads.size() > maxThreadCount) {
                // If there are already more than the maximum merge threads allowed, count that as backlog:
                curBacklog = true;
            } else {
                // Now see if any still-running merges are backlog'd:
                // 检测是否某个任务处于积压状态
                for (MergeThread mergeThread : mergeThreads) {
                    if (isBacklog(now, mergeThread.merge)) {
                        curBacklog = true;
                        break;
                    }
                }
            }
        }

        double curMBPerSec = targetMBPerSec;

        // 如果新的merge任务 检测出了积压状态
        if (newBacklog) {
            // This new merge adds to the backlog: increase IO throttle by 20%
            // 提高IO的阀值 避免被限流
            targetMBPerSec *= 1.20;
            if (targetMBPerSec > MAX_MERGE_MB_PER_SEC) {
                targetMBPerSec = MAX_MERGE_MB_PER_SEC;
            }
            if (verbose()) {
                if (curMBPerSec == targetMBPerSec) {
                    message(String.format(Locale.ROOT, "io throttle: new merge backlog; leave IO rate at ceiling %.1f MB/sec", targetMBPerSec));
                } else {
                    message(String.format(Locale.ROOT, "io throttle: new merge backlog; increase IO rate to %.1f MB/sec", targetMBPerSec));
                }
            }
            // 当前已经处在积压状态 只是打印日志
        } else if (curBacklog) {
            // We still have an existing backlog; leave the rate as is:
            if (verbose()) {
                message(String.format(Locale.ROOT, "io throttle: current merge backlog; leave IO rate at %.1f MB/sec",
                        targetMBPerSec));
            }
            // 没有发生积压 那么就减小IO 阀值
        } else {
            // We are not falling behind: decrease IO throttle by 10%
            targetMBPerSec /= 1.10;
            if (targetMBPerSec < MIN_MERGE_MB_PER_SEC) {
                targetMBPerSec = MIN_MERGE_MB_PER_SEC;
            }
            if (verbose()) {
                if (curMBPerSec == targetMBPerSec) {
                    message(String.format(Locale.ROOT, "io throttle: no merge backlog; leave IO rate at floor %.1f MB/sec", targetMBPerSec));
                } else {
                    message(String.format(Locale.ROOT, "io throttle: no merge backlog; decrease IO rate to %.1f MB/sec", targetMBPerSec));
                }
            }
        }

        double rate;

        if (newMerge.maxNumSegments != -1) {
            rate = forceMergeMBPerSec;
        } else {
            rate = targetMBPerSec;
        }
        // 这里设置不重要 因为马上就会调用 updateMergeThread 将所有任务的时间重置
        rateLimiter.setMBPerSec(rate);
        targetMBPerSecChanged();
    }

    /**
     * Subclass can override to tweak targetMBPerSec.
     */
    protected void targetMBPerSecChanged() {
    }

    private static double nsToSec(long ns) {
        return ns / 1000000000.0;
    }

    private static double bytesToMB(long bytes) {
        return bytes / 1024. / 1024.;
    }
}
