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
  // 当前有多少正在处理的 ticket
  private final AtomicInteger ticketCount = new AtomicInteger();
  private final ReentrantLock purgeLock = new ReentrantLock();

  /**
   * 传入一个删除队列 此时删除队列内部已经记录了各种包含删除信息的node
   * @param deleteQueue
   * @return
   * @throws IOException
   */
  synchronized boolean addDeletes(DocumentsWriterDeleteQueue deleteQueue) throws IOException {
    incTickets();// first inc the ticket count - freeze opens
                 // a window for #anyChanges to fail
    boolean success = false;
    try {
      FrozenBufferedUpdates frozenBufferedUpdates = deleteQueue.maybeFreezeGlobalBuffer();
      if (frozenBufferedUpdates != null) { // no need to publish anything if we don't have any frozen updates
        queue.add(new FlushTicket(frozenBufferedUpdates, false));
        success = true;
      }
    } finally {
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
   * 为某个等待刷盘的线程申请门票
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
      // FlushTicket  还携带了 本次刷盘同时会删除/更新的doc   主要体现在 FrozenBufferedUpdates 这个类中
      final FlushTicket ticket = new FlushTicket(dwpt.prepareFlush(), true);
      queue.add(ticket);
      success = true;
      return ticket;
    } finally {
      if (!success) {
        // 失败时才释放ticket
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
   * 使用该消费者处理内部所有 已经完成flush任务的对象
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
        // 代表该完成的刷盘任务 是否允许通知到外面
        canPublish = head != null && head.canPublish(); // do this synced 
      }
      // 代表满足发布条件
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
            // 光是执行完 刷盘任务 还不能释放该ticket 只有等到 publish完成时 才触发减少ticket的逻辑
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
   * 每个 flushTicket 代表一个刷盘动作   是一个简单的bean对象
   */
  static final class FlushTicket {
    /**
     * 本次刷盘涉及到的 删除/更新 doc
     */
    private final FrozenBufferedUpdates frozenUpdates;
    private final boolean hasSegment;
    /**
     * 描述一个刷盘完成的段的信息
     */
    private FlushedSegment segment;
    /**
     * 标记本次刷盘的结果
     */
    private boolean failed = false;
    /**
     * 代表本次刷盘结果已经发布  (当刷盘任务结束时 就要进行发布任务)
     */
    private boolean published = false;

    FlushTicket(FrozenBufferedUpdates frozenUpdates, boolean hasSegment) {
      this.frozenUpdates = frozenUpdates;
      this.hasSegment = hasSegment;
    }

    /**
     * 如果本身不需要设置 segment信息
     * 或者已经设置了 segment （代表已经成功）
     * 或者设置了 failed （代表已经失败）
     * @return
     */
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
