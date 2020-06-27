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


import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.ThreadInterruptedException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.MergePolicy.OneMergeProgress;
import org.apache.lucene.index.MergePolicy.OneMergeProgress.PauseReason;

/** This is the {@link RateLimiter} that {@link IndexWriter} assigns to each running merge, to 
 *  give {@link MergeScheduler}s ionice like control.
 *
 *  @lucene.internal */
// 限流器  SimpleRateLimiter 是针对 IO 进行限流   这个对象是专门针对merge操作进行限流
public class MergeRateLimiter extends RateLimiter {

  private final static int MIN_PAUSE_CHECK_MSEC = 25;

  // 一个最小的暂停时间 低于这个值 忽略暂停
  private final static long MIN_PAUSE_NS = TimeUnit.MILLISECONDS.toNanos(2);
  private final static long MAX_PAUSE_NS = TimeUnit.MILLISECONDS.toNanos(250);

  /**
   * 限定每秒处理多少 mb数据  如果设置成 0.0 那么触发parse时 传入的person 是 stop
   */
  private volatile double mbPerSec;
  /**
   * 每写入多少数据检测是否需要调用 pause
   */
  private volatile long minPauseCheckBytes;

  private long lastNS;

  /**
   * 记录一共写入了多少数据
   */
  private AtomicLong totalBytesWritten = new AtomicLong();

  /**
   * 该对象会记录因各种原因导致的merge 暂停时间 类似一个协调者
   */
  private final OneMergeProgress mergeProgress;

  /** Sole constructor. */
  public MergeRateLimiter(OneMergeProgress mergeProgress) {
    // Initially no IO limit; use setter here so minPauseCheckBytes is set:
    // 这里 InFinity 代表不做限制
    this.mergeProgress = mergeProgress;
    setMBPerSec(Double.POSITIVE_INFINITY);
  }

  @Override
  public void setMBPerSec(double mbPerSec) {
    // Synchronized to make updates to mbPerSec and minPauseCheckBytes atomic. 
    synchronized (this) {
      // 0.0 is allowed: it means the merge is paused
      if (mbPerSec < 0.0) {
        throw new IllegalArgumentException("mbPerSec must be positive; got: " + mbPerSec);
      }
      this.mbPerSec = mbPerSec;
  
      // NOTE: Double.POSITIVE_INFINITY casts to Long.MAX_VALUE
      this.minPauseCheckBytes = Math.min(1024*1024, (long) ((MIN_PAUSE_CHECK_MSEC / 1000.0) * mbPerSec * 1024 * 1024));
      assert minPauseCheckBytes >= 0;
    }

    // 因为重置了相关参数  选择先唤醒阻塞的线程
    mergeProgress.wakeup();
  }

  @Override
  public double getMBPerSec() {
    return mbPerSec;
  }

  /** Returns total bytes written by this merge. */
  public long getTotalBytesWritten() {
    return totalBytesWritten.get();
  }

  /**
   * 就是正常的写入操作 不过如果超过了 阈值 在内部会被阻塞一段时间
   * @param bytes
   * @return
   * @throws MergePolicy.MergeAbortedException
   */
  @Override
  public long pause(long bytes) throws MergePolicy.MergeAbortedException {
    // 每次写入时 增加写入总量
    totalBytesWritten.addAndGet(bytes);

    // While loop because we may wake up and check again when our rate limit
    // is changed while we were pausing:
    long paused = 0;
    long delta;
    // 暂停并累加总的暂停时间
    while ((delta = maybePause(bytes, System.nanoTime())) >= 0) {
      // Keep waiting.
      paused += delta;
    }

    return paused;
  }

  /** Total NS merge was stopped. */
  public long getTotalStoppedNS() {
    return mergeProgress.getPauseTimes().get(PauseReason.STOPPED);
  } 

  /** Total NS merge was paused to rate limit IO. */
  public long getTotalPausedNS() {
    return mergeProgress.getPauseTimes().get(PauseReason.PAUSED);
  } 

  /** 
   * Returns the number of nanoseconds spent in a paused state or <code>-1</code>
   * if no pause was applied. If the thread needs pausing, this method delegates 
   * to the linked {@link OneMergeProgress}.
   * 检测本次写入数据是否要被阻塞
   */
  private long maybePause(long bytes, long curNS) throws MergePolicy.MergeAbortedException {
    // Now is a good time to abort the merge:
    // 如果merge 任务已经被终止了 直接抛出异常就好
    if (mergeProgress.isAborted()) {
      throw new MergePolicy.MergeAbortedException("Merge aborted.");
    }

    double rate = mbPerSec; // read from volatile rate once.
    // 换算成等待时长
    double secondsToPause = (bytes/1024./1024.) / rate;

    // Time we should sleep until; this is purely instantaneous
    // rate (just adds seconds onto the last time we had paused to);
    // maybe we should also offer decayed recent history one?
    long targetNS = lastNS + (long) (1000000000 * secondsToPause);

    long curPauseNS = targetNS - curNS;

    // We don't bother with thread pausing if the pause is smaller than 2 msec.
    // 这是一个最小的 暂停的值 低于这个值  就忽略parse  也是的 连续的短时间暂停只会收到更多的上下文切换带来的性能影响
    if (curPauseNS <= MIN_PAUSE_NS) {
      // Set to curNS, not targetNS, to enforce the instant rate, not
      // the "averaged over all history" rate:
      lastNS = curNS;
      return -1;
    }

    // Defensive: don't sleep for too long; the loop above will call us again if
    // we should keep sleeping and the rate may be adjusted in between.
    // 不能超过上限值
    if (curPauseNS > MAX_PAUSE_NS) {
      curPauseNS = MAX_PAUSE_NS;
    }

    long start = System.nanoTime();
    try {
      // 通知 merge线程 暂停处理数据
      mergeProgress.pauseNanos(
          curPauseNS, 
          rate == 0.0 ? PauseReason.STOPPED : PauseReason.PAUSED,
          () -> rate == mbPerSec);
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
    return System.nanoTime() - start;
  }

  @Override
  public long getMinPauseCheckBytes() {
    return minPauseCheckBytes;
  }
}
