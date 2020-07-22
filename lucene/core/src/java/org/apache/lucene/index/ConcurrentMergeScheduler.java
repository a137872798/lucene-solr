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

/** A {@link MergeScheduler} that runs each merge using a
 *  separate thread.
 *
 *  <p>Specify the max number of threads that may run at
 *  once, and the maximum number of simultaneous merges
 *  with {@link #setMaxMergesAndThreads}.</p>
 *
 *  <p>If the number of merges exceeds the max number of threads 
 *  then the largest merges are paused until one of the smaller
 *  merges completes.</p>
 *
 *  <p>If more than {@link #getMaxMergeCount} merges are
 *  requested then this class will forcefully throttle the
 *  incoming threads by pausing until one more merges
 *  complete.</p>
 *
 *  <p>This class attempts to detect whether the index is
 *  on rotational storage (traditional hard drive) or not
 *  (e.g. solid-state disk) and changes the default max merge
 *  and thread count accordingly.  This detection is currently
 *  Linux-only, and relies on the OS to put the right value
 *  into /sys/block/&lt;dev&gt;/block/rotational.  For all
 *  other operating systems it currently assumes a rotational
 *  disk for backwards compatibility.  To enable default
 *  settings for spinning or solid state disks for such
 *  operating systems, use {@link #setDefaultMaxMergesAndThreads(boolean)}.
 *  每个 oneMerge 对应一条后台线程  并行合并
 */ 
public class ConcurrentMergeScheduler extends MergeScheduler {

  /** Dynamic default for {@code maxThreadCount} and {@code maxMergeCount},
   *  used to detect whether the index is backed by an SSD or rotational disk and
   *  set {@code maxThreadCount} accordingly.  If it's an SSD,
   *  {@code maxThreadCount} is set to {@code max(1, min(4, cpuCoreCount/2))},
   *  otherwise 1.  Note that detection only currently works on
   *  Linux; other platforms will assume the index is not on an SSD. */
  public static final int AUTO_DETECT_MERGES_AND_THREADS = -1;

  /** Used for testing.
   *
   * @lucene.internal */
  public static final String DEFAULT_CPU_CORE_COUNT_PROPERTY = "lucene.cms.override_core_count";

  /** Used for testing.
   *
   * @lucene.internal */
  public static final String DEFAULT_SPINS_PROPERTY = "lucene.cms.override_spins";

  /** List of currently active {@link MergeThread}s. */
  // 维护所有正在运行的merge线程
  protected final List<MergeThread> mergeThreads = new ArrayList<>();
  
  // Max number of merge threads allowed to be running at
  // once.  When there are more merges then this, we
  // forcefully pause the larger ones, letting the smaller
  // ones run, up until maxMergeCount merges at which point
  // we forcefully pause incoming threads (that presumably
  // are the ones causing so much merging).
  // 最大工作线程数    默认情况下是 自动检测  比如会根据当前硬盘类型   机械硬盘的话 提升线程数 并不能很好的提升性能
  private int maxThreadCount = AUTO_DETECT_MERGES_AND_THREADS;

  // Max number of merges we accept before forcefully
  // throttling the incoming threads
  // 该值跟maxThreadCount一样是 检测环境后生成的   并且  maxMergeCount = maxThreadCount + 5
  private int maxMergeCount = AUTO_DETECT_MERGES_AND_THREADS;

  /** How many {@link MergeThread}s have kicked off (this is use
   *  to name them). */
  protected int mergeThreadCount;

  /** Floor for IO write rate limit (we will never go any lower than this) */
  private static final double MIN_MERGE_MB_PER_SEC = 5.0;

  /** Ceiling for IO write rate limit (we will never go any higher than this) */
  private static final double MAX_MERGE_MB_PER_SEC = 10240.0;

  /** Initial value for IO write rate limit when doAutoIOThrottle is true */
  private static final double START_MB_PER_SEC = 20.0;

  /** Merges below this size are not counted in the maxThreadCount, i.e. they can freely run in their own thread (up until maxMergeCount). */
  // 代表被判定为大对象的最小大小
  private static final double MIN_BIG_MERGE_MB = 50.0;

  /** Current IO writes throttle rate */
  protected double targetMBPerSec = START_MB_PER_SEC;

  /** true if we should rate-limit writes for each merge */
  private boolean doAutoIOThrottle = true;

  private double forceMergeMBPerSec = Double.POSITIVE_INFINITY;

  /** Sole constructor, with all settings set to default
   *  values. */
  public ConcurrentMergeScheduler() {
  }

  /**
   * Expert: directly set the maximum number of merge threads and
   * simultaneous merges allowed.
   * 
   * @param maxMergeCount the max # simultaneous merges that are allowed.
   *       If a merge is necessary yet we already have this many
   *       threads running, the incoming thread (that is calling
   *       add/updateDocument) will block until a merge thread
   *       has completed.  Note that we will only run the
   *       smallest <code>maxThreadCount</code> merges at a time.
   * @param maxThreadCount the max # simultaneous merge threads that should
   *       be running at once.  This must be &lt;= <code>maxMergeCount</code>
   */
  public synchronized void setMaxMergesAndThreads(int maxMergeCount, int maxThreadCount) {
    if (maxMergeCount == AUTO_DETECT_MERGES_AND_THREADS && maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
      // OK
      this.maxMergeCount = AUTO_DETECT_MERGES_AND_THREADS;
      this.maxThreadCount = AUTO_DETECT_MERGES_AND_THREADS;
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

  /** Sets max merges and threads to proper defaults for rotational
   *  or non-rotational storage.
   *
   * @param spins true to set defaults best for traditional rotatational storage (spinning disks), 
   *        else false (e.g. for solid-state disks)
   *              根据是否是机械硬盘 设置默认的并发度
   */
  public synchronized void setDefaultMaxMergesAndThreads(boolean spins) {
    if (spins) {
      // 机械硬盘的情况下 线程数并不能提升IO性能 所以还是一个线程
      maxThreadCount = 1;
      maxMergeCount = 6;
    } else {
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

      // 对应 cpu核数的一半
      maxThreadCount = Math.max(1, Math.min(4, coreCount/2));
      maxMergeCount = maxThreadCount+5;
    }
  }

  /** Set the per-merge IO throttle rate for forced merges (default: {@code Double.POSITIVE_INFINITY}). */
  public synchronized void setForceMergeMBPerSec(double v) {
    forceMergeMBPerSec = v;
    updateMergeThreads();
  }

  /** Get the per-merge IO throttle rate for forced merges. */
  public synchronized double getForceMergeMBPerSec() {
    return forceMergeMBPerSec;
  }

  /** Turn on dynamic IO throttling, to adaptively rate limit writes
   *  bytes/sec to the minimal rate necessary so merges do not fall behind.
   *  By default this is enabled. */
  public synchronized void enableAutoIOThrottle() {
    doAutoIOThrottle = true;
    targetMBPerSec = START_MB_PER_SEC;
    updateMergeThreads();
  }

  /** Turn off auto IO throttling.
   *
   * @see #enableAutoIOThrottle */
  public synchronized void disableAutoIOThrottle() {
    doAutoIOThrottle = false;
    updateMergeThreads();
  }

  /** Returns true if auto IO throttling is currently enabled. */
  public synchronized boolean getAutoIOThrottle() {
    return doAutoIOThrottle;
  }

  /** Returns the currently set per-merge IO writes rate limit, if {@link #enableAutoIOThrottle}
   *  was called, else {@code Double.POSITIVE_INFINITY}. */
  public synchronized double getIORateLimitMBPerSec() {
    if (doAutoIOThrottle) {
      return targetMBPerSec;
    } else {
      return Double.POSITIVE_INFINITY;
    }
  }

  /** Returns {@code maxThreadCount}.
   *
   * @see #setMaxMergesAndThreads(int, int) */
  public synchronized int getMaxThreadCount() {
    return maxThreadCount;
  }

  /** See {@link #setMaxMergesAndThreads}. */
  public synchronized int getMaxMergeCount() {
    return maxMergeCount;
  }

  /** Removes the calling thread from the active merge threads. */
  synchronized void removeMergeThread() {
    Thread currentThread = Thread.currentThread();
    // Paranoia: don't trust Thread.equals:
    for(int i=0;i<mergeThreads.size();i++) {
      if (mergeThreads.get(i) == currentThread) {
        mergeThreads.remove(i);
        return;
      }
    }
      
    assert false: "merge thread " + currentThread + " was not found";
  }

  /**
   * 对使用的目录进行包装
   * @param merge
   * @param in
   * @return
   */
  @Override
  public Directory wrapForMerge(OneMerge merge, Directory in) {
    Thread mergeThread = Thread.currentThread();
    // 调用此方法的线程必须是merge线程
    if (!MergeThread.class.isInstance(mergeThread)) {
      throw new AssertionError("wrapForMerge should be called from MergeThread. Current thread: "
          + mergeThread);
    }

    // Return a wrapped Directory which has rate-limited output.
    RateLimiter rateLimiter = ((MergeThread) mergeThread).rateLimiter;
    return new FilterDirectory(in) {
      @Override
      public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();

        // This Directory is only supposed to be used during merging,
        // so all writes should have MERGE context, else there is a bug 
        // somewhere that is failing to pass down the right IOContext:
        assert context.context == IOContext.Context.MERGE: "got context=" + context.context;
        
        // Because rateLimiter is bound to a particular merge thread, this method should
        // always be called from that context. Verify this.
        assert mergeThread == Thread.currentThread() : "Not the same merge thread, current="
          + Thread.currentThread() + ", expected=" + mergeThread;

        // 在创建输出流时 额外追加限流的功能
        return new RateLimitedIndexOutput(rateLimiter, in.createOutput(name, context));
      }
    };
  }
  
  /**
   * Called whenever the running merges have changed, to set merge IO limits.
   * This method sorts the merge threads by their merge size in
   * descending order and then pauses/unpauses threads from first to last --
   * that way, smaller merges are guaranteed to run before larger ones.
   * 根据当前情况调整每个merge线程的 流量
   */
  protected synchronized void updateMergeThreads() {

    // Only look at threads that are alive & not in the
    // process of stopping (ie have an active merge):
    final List<MergeThread> activeMerges = new ArrayList<>();

    // 将此时失活的线程移除
    int threadIdx = 0;
    while (threadIdx < mergeThreads.size()) {
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
    // 按照预计merge的大小 倒序排序
    CollectionUtil.timSort(activeMerges);

    final int activeMergeCount = activeMerges.size();

    // 有多少个大块对象
    int bigMergeCount = 0;

    // 从小到大
    for (threadIdx=activeMergeCount-1;threadIdx>=0;threadIdx--) {
      MergeThread mergeThread = activeMerges.get(threadIdx);
      // 发现首个 大块对象 记录下标
      if (mergeThread.merge.estimatedMergeBytes > MIN_BIG_MERGE_MB*1024*1024) {
        bigMergeCount = 1+threadIdx;
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

    // 从大到小 遍历
    for (threadIdx=0;threadIdx<activeMergeCount;threadIdx++) {
      MergeThread mergeThread = activeMerges.get(threadIdx);

      // 获取当前线程正在处理的 merge对象
      OneMerge merge = mergeThread.merge;

      // pause the thread if maxThreadCount is smaller than the number of merge threads.
      // 如果merge大块数据的线程数 小于 要求值 那么都不会被暂停   当merge大块数据的线程数超过限制时 超过的线程将会被暂停
      final boolean doPause = threadIdx < bigMergeCount - maxThreadCount;

      // 这个就是 merge的流量
      double newMBPerSec;
      if (doPause) {
        // 被暂停 流量就是0
        newMBPerSec = 0.0;
        // 如果设置了最大值
      } else if (merge.maxNumSegments != -1) {
        newMBPerSec = forceMergeMBPerSec;
        // 代表不需要启用阀门
      } else if (doAutoIOThrottle == false) {
        newMBPerSec = Double.POSITIVE_INFINITY;
        // 小块数据的merge 不做限制操作
      } else if (merge.estimatedMergeBytes < MIN_BIG_MERGE_MB*1024*1024) {
        // Don't rate limit small merges:
        newMBPerSec = Double.POSITIVE_INFINITY;
      } else {
        // 阀门限流量
        newMBPerSec = targetMBPerSec;
      }

      MergeRateLimiter rateLimiter = mergeThread.rateLimiter;
      double curMBPerSec = rateLimiter.getMBPerSec();

      // 打印日志 忽略
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

      // 设置流量值
      rateLimiter.setMBPerSec(newMBPerSec);
    }
    if (verbose()) {
      message(message.toString());
    }
  }

  /**
   * 初始化动态配置
   * @param directory
   * @throws IOException
   */
  private synchronized void initDynamicDefaults(Directory directory) throws IOException {
    if (maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
      // 检测该硬盘是否是机械硬盘
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

  @Override
  public void close() {
    sync();
  }

  /** Wait for any running merge threads to finish. This call is not interruptible as used by {@link #close()}. */
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
   * @lucene.internal
   * 返回执行merge的线程数量
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

  /**
   * 对该对象进行初始化   标记预备merge的目录 以及 一个打印日志的对象
   * @param infoStream
   * @param directory
   * @throws IOException
   */
  @Override
  void initialize(InfoStream infoStream, Directory directory) throws IOException {
    super.initialize(infoStream, directory);
    initDynamicDefaults(directory);
  }

  /**
   * @param mergeSource 产生 oneMerge对象
   * @param trigger 代表本次merge操作是由什么原因引发的  当某个线程merge结束时也会调用该方法
   *
   */
  @Override
  public synchronized void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {

    // 如果本次是由关闭引起的merge  不使用阀门限制
    if (trigger == MergeTrigger.CLOSING) {
      // Disable throttling on close:
      targetMBPerSec = MAX_MERGE_MB_PER_SEC;
      // 这里就是根据当前情况调整每个merge线程的流量控制
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

      if (maybeStall(mergeSource) == false) {
        break;
      }

      // 当所有任务都执行完毕后 从merge的死循环中解放出来
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
        // merge:
        // 就是开启一个线程调用  mergeSource.merge  会转发到 indexWriter.merge  并在merge结束后重新触发该方法 唤醒下一个执行merge的线程
        final MergeThread newMergeThread = getMergeThread(mergeSource, merge);
        mergeThreads.add(newMergeThread);

        // 根据新插入的 merge任务 动态调节阈值
        updateIOThrottle(newMergeThread.merge, newMergeThread.rateLimiter);

        if (verbose()) {
          message("    launch new thread [" + newMergeThread.getName() + "]");
        }

        // 启动merge线程
        newMergeThread.start();
        // 调节merge线程的限制值
        updateMergeThreads();

        success = true;
      } finally {
        if (!success) {
          mergeSource.onMergeFinished(merge);
        }
      }
    }
  }

  /** This is invoked by {@link #merge} to possibly stall the incoming
   *  thread when there are too many merges running or pending.  The 
   *  default behavior is to force this thread, which is producing too
   *  many segments for merging to keep up, to wait until merges catch
   *  up. Applications that can take other less drastic measures, such
   *  as limiting how many threads are allowed to index, can do nothing
   *  here and throttle elsewhere.
   *
   *  If this method wants to stall but the calling thread is a merge
   *  thread, it should return false to tell caller not to kick off
   *  any new merges.
   *  是否需要阻塞  返回false 代表可以直接执行任务
   *  */
  protected synchronized boolean maybeStall(MergeSource mergeSource) {
    long startStallTime = 0;

    // 每个创建的merge线程 在执行完一个任务后 会回调merge() 这样又会创建下一个merge线程 这样只要在没达到上限时 就会不断创建线程 有点类似线程池的套路
    // 而当线程数达到一个上限值时 这时之前创建的线程就不必再执行了 可以释放资源
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

      // 这里就是旧线程释放资源的过程
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
      doStall();
    }

    if (verbose() && startStallTime != 0) {
      message("  stalled for " + (System.currentTimeMillis()-startStallTime) + " msec");
    }

    return true;
  }

  /** Called from {@link #maybeStall} to pause the calling thread for a bit. */
  protected synchronized void doStall() {
    try {
      // Defensively wait for only .25 seconds in case we are missing a .notify/All somewhere:
      wait(250);
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
  }

  /** Does the actual merge, by calling {@link org.apache.lucene.index.MergeScheduler.MergeSource#merge} */
  protected void doMerge(MergeSource mergeSource, OneMerge merge) throws IOException {
    mergeSource.merge(merge);
  }

  /** Create and return a new MergeThread */
  // 生成一个新的merge线程
  protected synchronized MergeThread getMergeThread(MergeSource mergeSource, OneMerge merge) throws IOException {
    final MergeThread thread = new MergeThread(mergeSource, merge);
    thread.setDaemon(true);
    thread.setName("Lucene Merge Thread #" + mergeThreadCount++);
    return thread;
  }

  /**
   * 当merge 操作结束时触发
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
      removeMergeThread();
      updateMergeThreads();
      // In case we had stalled indexing, we can now wake up
      // and possibly unstall:
      notifyAll();
    }
  }

  /** Runs a merge thread to execute a single merge, then exits. */
  // 该线程负责实际的 merge工作  而 整个Scheduler 作为一个调节者  一个线程只执行一个merge任务
  protected class MergeThread extends Thread implements Comparable<MergeThread> {
    // 应该是多线程共用该对象
    final MergeSource mergeSource;
    // 应该是当前merge的对象
    final OneMerge merge;
    // merge限流器
    final MergeRateLimiter rateLimiter;

    /** Sole constructor. */
    public MergeThread(MergeSource mergeSource, OneMerge merge) {
      this.mergeSource = mergeSource;
      this.merge = merge;
      // rateLimiter 通过操作 progress 来操控 MergeThread 进而暂停merge动作
      this.rateLimiter = new MergeRateLimiter(merge.getMergeProgress());
    }

    /**
     * 这里是倒序
     * @param other
     * @return
     */
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

        // 默认 操作是  MergeSource.merge()  MergeSource 默认实现就是最核心的 IndexWriter
        doMerge(mergeSource, merge);

        if (verbose()) {
          message("  merge thread: done");
        }
        // 触发钩子
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

  /** Called when an exception is hit in a background merge
   *  thread */
  protected void handleMergeException(Throwable exc) {
    throw new MergePolicy.MergeException(exc);
  }

  private boolean suppressExceptions;

  /** Used for testing */
  void setSuppressExceptions() {
    if (verbose()) {
      message("will suppress merge exceptions");
    }
    suppressExceptions = true;
  }

  /** Used for testing */
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
   * 是否发生了积压
   * @param now
   * @param merge
   * @return
   */
  private boolean isBacklog(long now, OneMerge merge) {
    double mergeMB = bytesToMB(merge.estimatedMergeBytes);
    for (MergeThread mergeThread : mergeThreads) {
      // 这是该对象开始处理的时间
      long mergeStartNS = mergeThread.merge.mergeStartNS;
      // 找到其他正在处理中的任务
      if (mergeThread.isAlive() && mergeThread.merge != merge &&
          mergeStartNS != -1 &&
              // 代表该对象需要merge的量很大 同时已经执行了超过3秒
          mergeThread.merge.estimatedMergeBytes >= MIN_BIG_MERGE_MB*1024*1024 &&
          nsToSec(now-mergeStartNS) > 3.0) {
        double otherMergeMB = bytesToMB(mergeThread.merge.estimatedMergeBytes);
        double ratio = otherMergeMB / mergeMB;
        // 这个比率是怎么出来的???
        if (ratio > 0.3 && ratio < 3.0) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Tunes IO throttle when a new merge starts.
   * @param newMerge 新加入的 merge对象
   * @param rateLimiter  对应的限流对象
   * */
  private synchronized void updateIOThrottle(OneMerge newMerge, MergeRateLimiter rateLimiter) throws IOException {
    // 如果没有开启限流 直接忽略
    if (doAutoIOThrottle == false) {
      return;
    }

    double mergeMB = bytesToMB(newMerge.estimatedMergeBytes);
    // 小块的 数据块直接忽略
    if (mergeMB < MIN_BIG_MERGE_MB) {
      // Only watch non-trivial merges for throttling; this is safe because the MP must eventually
      // have to do larger merges:
      return;
    }

    long now = System.nanoTime();

    // Simplistic closed-loop feedback control: if we find any other similarly
    // sized merges running, then we are falling behind, so we bump up the
    // IO throttle, else we lower it:
    // 代表此时有一个执行了较长时间的 merge (并且数据量也比较大)
    boolean newBacklog = isBacklog(now, newMerge);

    // 当前是否发生了积压
    boolean curBacklog = false;

    // 新插入的对象没有与 旧的形成积压
    if (newBacklog == false) {
      // 只要此时merge线程数超量 久认为发生了积压
      if (mergeThreads.size() > maxThreadCount) {
        // If there are already more than the maximum merge threads allowed, count that as backlog:
        curBacklog = true;
      } else {
        // Now see if any still-running merges are backlog'd:
        // 只要有任何一条线程产生了积压
        for (MergeThread mergeThread : mergeThreads) {
          if (isBacklog(now, mergeThread.merge)) {
            curBacklog = true;
            break;
          }
        }
      }
    }

    double curMBPerSec = targetMBPerSec;

    // 如果新的线程产生了积压
    if (newBacklog) {
      // This new merge adds to the backlog: increase IO throttle by 20%
      // 释放放宽阈值
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
    // 如果之前的某个merge对象 产生了积压  只是打印日志
    } else if (curBacklog) {
      // We still have an existing backlog; leave the rate as is:
      if (verbose()) {
        message(String.format(Locale.ROOT, "io throttle: current merge backlog; leave IO rate at %.1f MB/sec",
                              targetMBPerSec));
      }
    } else {
      // We are not falling behind: decrease IO throttle by 10%
      // 减少部分流量  相当于具备动态调节功能
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

    // 本次merge的量 如果做了限制就代表是 forceMerge 所以采用对应的rate
    if (newMerge.maxNumSegments != -1) {
      rate = forceMergeMBPerSec;
    } else {
      // 非force模式 使用另一个阈值
      rate = targetMBPerSec;
    }
    rateLimiter.setMBPerSec(rate);
    targetMBPerSecChanged();
  }

  /** Subclass can override to tweak targetMBPerSec. */
  // 当阈值发生变化的钩子
  protected void targetMBPerSecChanged() {
  }

  private static double nsToSec(long ns) {
    return ns / 1000000000.0;
  }

  private static double bytesToMB(long bytes) {
    return bytes/1024./1024.;
  }
}
