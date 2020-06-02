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
package org.apache.lucene.store;


import java.io.IOException;

import org.apache.lucene.util.ThreadInterruptedException;

/** Abstract base class to rate limit IO.  Typically implementations are
 *  shared across multiple IndexInputs or IndexOutputs (for example
 *  those involved all merging).  Those IndexInputs and
 *  IndexOutputs would call {@link #pause} whenever the have read
 *  or written more than {@link #getMinPauseCheckBytes} bytes. */
public abstract class RateLimiter {

  /**
   * Sets an updated MB per second rate limit.
   * A subclass is allowed to perform dynamic updates of the rate limit
   * during use.
   * 限制每秒更新的 MB 量
   */
  public abstract void setMBPerSec(double mbPerSec);

  /**
   * The current MB per second rate limit.
   */
  public abstract double getMBPerSec();
  
  /** Pauses, if necessary, to keep the instantaneous IO
   *  rate at or below the target. 
   *  <p>
   *  Note: the implementation is thread-safe
   *  </p>
   *  @return the pause time in nano seconds 
   * */
  public abstract long pause(long bytes) throws IOException;
  
  /** How many bytes caller should add up itself before invoking {@link #pause}.
   *  NOTE: The value returned by this method may change over time and is not guaranteed
   *  to be constant throughout the lifetime of the RateLimiter. Users are advised to
   *  refresh their local values with calls to this method to ensure consistency.
   */
  public abstract long getMinPauseCheckBytes();

  /**
   * Simple class to rate limit IO.
   * 一个简单的限流实现类
   */
  public static class SimpleRateLimiter extends RateLimiter {

    private final static int MIN_PAUSE_CHECK_MSEC = 5;

    /**
     * 每秒允许操作多少 MB
     */
    private volatile double mbPerSec;
    /**
     * 应该是有关每多少byte 触发一次检查
     */
    private volatile long minPauseCheckBytes;
    /**
     * 记录上一次触发的纳秒
     */
    private long lastNS;

    /** mbPerSec is the MB/sec max IO rate */
    public SimpleRateLimiter(double mbPerSec) {
      setMBPerSec(mbPerSec);
      lastNS = System.nanoTime();
    }

    /**
     * Sets an updated mb per second rate limit.
     * 计算当每秒写入的数据量超过多少byte 时 推荐触发 pause方法
     */
    @Override
    public void setMBPerSec(double mbPerSec) {
      this.mbPerSec = mbPerSec;
      // mbPerSec * 1024 * 1024 换算一个byte值
      // MIN_PAUSE_CHECK_MSEC 代表推荐每多少毫秒触发一次 pause
      // MIN_PAUSE_CHECK_MSEC / 1000.0 相当于是换算成秒
      minPauseCheckBytes = (long) ((MIN_PAUSE_CHECK_MSEC / 1000.0) * mbPerSec * 1024 * 1024);
    }

    @Override
    public long getMinPauseCheckBytes() {
      return minPauseCheckBytes;
    }

    /**
     * The current mb per second rate limit.
     */
    @Override
    public double getMBPerSec() {
      return this.mbPerSec;
    }
    
    /** Pauses, if necessary, to keep the instantaneous IO
     *  rate at or below the target.  Be sure to only call
     *  this method when bytes &gt; {@link #getMinPauseCheckBytes},
     *  otherwise it will pause way too long!
     *
     *  @return the pause time in nano seconds */
    // 通过暂停的方式 拒绝数据写入 达到限流的目的
    @Override
    public long pause(long bytes) {

      // 记录当前时间
      long startNS = System.nanoTime();

      // 将byte 先换算成mb  然后根据  每1mb等待多少时间 计算出等待时长
      double secondsToPause = (bytes/1024./1024.) / mbPerSec;

      long targetNS;

      // Sync'd to read + write lastNS:
      synchronized (this) {

        // Time we should sleep until; this is purely instantaneous
        // rate (just adds seconds onto the last time we had paused to);
        // maybe we should also offer decayed recent history one?
        // 换算成纳秒后 计算一个sleep时间
        targetNS = lastNS + (long) (1000000000 * secondsToPause);

        // 没有达到阈值  不需要被限流
        if (startNS >= targetNS) {
          // OK, current time is already beyond the target sleep time,
          // no pausing to do.

          // Set to startNS, not targetNS, to enforce the instant rate, not
          // the "averaaged over all history" rate:
          lastNS = startNS;
          return 0;
        }

        lastNS = targetNS;
      }

      long curNS = startNS;

      // While loop because Thread.sleep doesn't always sleep
      // enough:
      while (true) {
        final long pauseNS = targetNS - curNS;
        if (pauseNS > 0) {
          try {
            // NOTE: except maybe on real-time JVMs, minimum realistic sleep time
            // is 1 msec; if you pass just 1 nsec the default impl rounds
            // this up to 1 msec:
            int sleepNS;
            int sleepMS;
            if (pauseNS > 100000L * Integer.MAX_VALUE) {
              // Not really practical (sleeping for 25 days) but we shouldn't overflow int:
              sleepMS = Integer.MAX_VALUE;
              sleepNS = 0;
            } else {
              sleepMS = (int) (pauseNS/1000000);
              sleepNS = (int) (pauseNS % 1000000);
            }
            // 这个api是精确到秒纳秒数
            Thread.sleep(sleepMS, sleepNS);
          } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
          }
          curNS = System.nanoTime();
          continue;
        }
        break;
      }

      return curNS - startNS;
    }
  }
}
