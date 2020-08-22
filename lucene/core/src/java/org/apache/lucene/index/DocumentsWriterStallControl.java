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

import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Controls the health status of a {@link DocumentsWriter} sessions. This class
 * used to block incoming indexing threads if flushing significantly slower than
 * indexing to ensure the {@link DocumentsWriter}s healthiness. If flushing is
 * significantly slower than indexing the net memory used within an
 * {@link IndexWriter} session can increase very quickly and easily exceed the
 * JVM's available memory.
 * <p>
 * To prevent OOM Errors and ensure IndexWriter's stability this class blocks
 * incoming threads from indexing once 2 x number of available
 * {@link DocumentsWriterPerThread}s in {@link DocumentsWriterPerThreadPool} is exceeded.
 * Once flushing catches up and the number of flushing DWPT is equal or lower
 * than the number of active {@link DocumentsWriterPerThread}s threads are released and can
 * continue indexing.
 * 这是 一个 stall 的控制器  应该是暂停的意思
 */
final class DocumentsWriterStallControl {
  
  private volatile boolean stalled;
  /**
   * 当前有多少线程在等待
   */
  private int numWaiting; // only with assert

  private boolean wasStalled; // only with assert
  /**
   * 记录当前正在等待的线程
   */
  private final Map<Thread, Boolean> waiting = new IdentityHashMap<>(); // only with assert

  /**
   * Update the stalled flag status. This method will set the stalled flag to
   * <code>true</code> iff the number of flushing
   * {@link DocumentsWriterPerThread} is greater than the number of active
   * {@link DocumentsWriterPerThread}. Otherwise it will reset the
   * {@link DocumentsWriterStallControl} to healthy and release all threads
   * waiting on {@link #waitIfStalled()}
   * 每当刷盘时 该标识就可能会被修改成true
   */
  synchronized void updateStalled(boolean stalled) {
    if (this.stalled != stalled) {
      this.stalled = stalled;
      if (stalled) {
        wasStalled = true;
      }
      // 每当状态发生变更时 唤醒所有线程  是这样子 既然之前有被阻塞的线程就一定是  stall == true 那么这里发生变化就一定是 stall == false 所以需要唤醒
      notifyAll();
    }
  }
  
  /**
   * Blocks if documents writing is currently in a stalled state.
   * 当此时内存中 已经有大量数据未刷盘时  会尽可能避免新的doc 被解析， 避免OOM   但是这里并没有完全阻塞线程 而是等待1秒
   */
  void waitIfStalled() {
    if (stalled) {
      synchronized (this) {
        if (stalled) { // react on the first wakeup call!
          // don't loop here, higher level logic will re-stall!
          try {
            incWaiters();
            // Defensive, in case we have a concurrency bug that fails to .notify/All our thread:
            // just wait for up to 1 second here, and let caller re-stall if it's still needed:
            wait(1000);
            decrWaiters();
          } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
          }
        }
      }
    }
  }

  /**
   * 返回当前是否属于暂停状态
   * @return
   */
  boolean anyStalledThreads() {
    return stalled;
  }

  /**
   * 增加等待线程数量
   */
  private void incWaiters() {
    numWaiting++;
    assert waiting.put(Thread.currentThread(), Boolean.TRUE) == null;
    assert numWaiting > 0;
  }
  
  private void decrWaiters() {
    numWaiting--;
    assert waiting.remove(Thread.currentThread()) != null;
    assert numWaiting >= 0;
  }
  
  synchronized boolean hasBlocked() { // for tests
    return numWaiting > 0;
  }
  
  boolean isHealthy() { // for tests
    return !stalled; // volatile read!
  }

  /**
   * 某个线程当前是否处在阻塞状态
   * @param t
   * @return
   */
  synchronized boolean isThreadQueued(Thread t) { // for tests
    return waiting.containsKey(t);
  }

  synchronized boolean wasStalled() { // for tests
    return wasStalled;
  }
}
