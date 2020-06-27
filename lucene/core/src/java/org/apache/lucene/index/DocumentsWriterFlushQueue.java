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
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;
import org.apache.lucene.util.IOUtils;

/**
 * @lucene.internal
 * 内部存储了多个 待刷盘的实体
 */
final class DocumentsWriterFlushQueue {

  /**
   * 每个 flushTicket 代表一个待刷盘动作
   */
  private final Queue<FlushTicket> queue = new LinkedList<>();
  // we track tickets separately since count must be present even before the ticket is
  // constructed ie. queue.size would not reflect it.
  private final AtomicInteger ticketCount = new AtomicInteger();
  private final ReentrantLock purgeLock = new ReentrantLock();

  /**
   * 这里添加一个维护了多个term变化的队列对象
   * @param deleteQueue
   * @return
   * @throws IOException
   */
  synchronized boolean addDeletes(DocumentsWriterDeleteQueue deleteQueue) throws IOException {
    // 每个刷盘对象对应一组更新
    incTickets();// first inc the ticket count - freeze opens
                 // a window for #anyChanges to fail
    boolean success = false;
    try {
      // 冻结内部的数据
      FrozenBufferedUpdates frozenBufferedUpdates = deleteQueue.maybeFreezeGlobalBuffer();
      if (frozenBufferedUpdates != null) { // no need to publish anything if we don't have any frozen updates
        queue.add(new FlushTicket(frozenBufferedUpdates, false));
        success = true;
      }
    } finally {
      // 代表该对象已经被处理过了 忽略 同时记得减少误加的值
      if (!success) {
        decTickets();
      }
    }
    return success;
  }
  
  private void incTickets() {
    int numTickets = ticketCount.incrementAndGet();
    assert numTickets > 0;
  }
  
  private void decTickets() {
    int numTickets = ticketCount.decrementAndGet();
    assert numTickets >= 0;
  }

  /**
   * 通过 传入 thread 的方式来增加 flushTicket   以这种方式创建的 flushTicket （hasSegment == true）
   * @param dwpt
   * @return
   * @throws IOException
   */
  synchronized FlushTicket addFlushTicket(DocumentsWriterPerThread dwpt) throws IOException {
    // Each flush is assigned a ticket in the order they acquire the ticketQueue
    // lock
    incTickets();
    boolean success = false;
    try {
      // prepare flush freezes the global deletes - do in synced block!
      final FlushTicket ticket = new FlushTicket(dwpt.prepareFlush(), true);
      queue.add(ticket);
      success = true;
      return ticket;
    } finally {
      if (!success) {
        decTickets();
      }
    }
  }

  /**
   * 为某个 待刷盘任务设置 段信息
   * @param ticket
   * @param segment
   */
  synchronized void addSegment(FlushTicket ticket, FlushedSegment segment) {
    assert ticket.hasSegment;
    // the actual flush is done asynchronously and once done the FlushedSegment
    // is passed to the flush ticket
    ticket.setSegment(segment);
  }

  /**
   * 标记某个刷盘动作失败了
   * @param ticket
   */
  synchronized void markTicketFailed(FlushTicket ticket) {
    assert ticket.hasSegment;
    // to free the queue we mark tickets as failed just to clean up the queue.
    ticket.setFailed();
  }


  boolean hasTickets() {
    assert ticketCount.get() >= 0 : "ticketCount should be >= 0 but was: " + ticketCount.get();
    return ticketCount.get() != 0;
  }

  /**
   * 使用该消费者处理内部所有 待flush 对象
   * @param consumer
   * @throws IOException
   */
  private void innerPurge(IOUtils.IOConsumer<FlushTicket> consumer) throws IOException {
    assert purgeLock.isHeldByCurrentThread();
    while (true) {
      final FlushTicket head;
      final boolean canPublish;
      synchronized (this) {
        head = queue.peek();
        // 检测当前能否刷盘
        canPublish = head != null && head.canPublish(); // do this synced 
      }
      if (canPublish) {
        try {
          /*
           * if we block on publish -> lock IW -> lock BufferedDeletes we don't block
           * concurrent segment flushes just because they want to append to the queue.
           * the downside is that we need to force a purge on fullFlush since there could
           * be a ticket still in the queue. 
           */
          consumer.accept(head);

        } finally {
          synchronized (this) {
            // finally remove the published ticket from the queue
            final FlushTicket poll = queue.poll();
            decTickets();
            // we hold the purgeLock so no other thread should have polled:
            assert poll == head;
          }
        }
      } else {
        break;
      }
    }
  }

  /**
   * 传入的是一个处理 FT 对象的消费者
   * @param consumer
   * @throws IOException
   */
  void forcePurge(IOUtils.IOConsumer<FlushTicket> consumer) throws IOException {
    assert !Thread.holdsLock(this);
    purgeLock.lock();
    try {
      innerPurge(consumer);
    } finally {
      purgeLock.unlock();
    }
  }

  void tryPurge(IOUtils.IOConsumer<FlushTicket> consumer) throws IOException {
    assert !Thread.holdsLock(this);
    if (purgeLock.tryLock()) {
      try {
        innerPurge(consumer);
      } finally {
        purgeLock.unlock();
      }
    }
  }

  int getTicketCount() {
    return ticketCount.get();
  }

  /**
   * 每个 flushTicket 代表一个刷盘动作
   */
  static final class FlushTicket {
    /**
     * 该对象内部 存储了 多次term的更新信息  现在要将这些变化持久化
     */
    private final FrozenBufferedUpdates frozenUpdates;
    private final boolean hasSegment;
    /**
     * 描述一个刷盘完成的段的信息
     */
    private FlushedSegment segment;
    private boolean failed = false;
    private boolean published = false;

    FlushTicket(FrozenBufferedUpdates frozenUpdates, boolean hasSegment) {
      this.frozenUpdates = frozenUpdates;
      this.hasSegment = hasSegment;
    }

    boolean canPublish() {
      return hasSegment == false || segment != null || failed;
    }

    synchronized void markPublished() {
      assert published == false: "ticket was already published - can not publish twice";
      published = true;
    }

    private void setSegment(FlushedSegment segment) {
      assert !failed;
      this.segment = segment;
    }

    private void setFailed() {
      assert segment == null;
      failed = true;
    }

    /**
     * Returns the flushed segment or <code>null</code> if this flush ticket doesn't have a segment. This can be the
     * case if this ticket represents a flushed global frozen updates package.
     */
    FlushedSegment getFlushedSegment() {
      return segment;
    }

    /**
     * Returns a frozen global deletes package.
     */
    FrozenBufferedUpdates getFrozenUpdates() {
      return frozenUpdates;
    }
  }
}
