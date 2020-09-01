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
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

/**
 * This class accepts multiple added documents and directly
 * writes segment files.
 *
 * Each added document is passed to the indexing chain,
 * which in turn processes the document into the different
 * codec formats.  Some formats write bytes to files
 * immediately, e.g. stored fields and term vectors, while
 * others are buffered by the indexing chain and written
 * only on flush.
 *
 * Once we have used our allowed RAM buffer, or the number
 * of added docs is large enough (in the case we are
 * flushing by doc count instead of RAM usage), we create a
 * real segment and flush it to the Directory.
 *
 * Threads:
 *
 * Multiple threads are allowed into addDocument at once.
 * There is an initial synchronized call to
 * {@link DocumentsWriterFlushControl#obtainAndLock()}
 * which allocates a DWPT for this indexing thread. The same
 * thread will not necessarily get the same DWPT over time.
 * Then updateDocuments is called on that DWPT without
 * synchronization (most of the "heavy lifting" is in this
 * call). Once a DWPT fills up enough RAM or hold enough
 * documents in memory the DWPT is checked out for flush
 * and all changes are written to the directory. Each DWPT
 * corresponds to one segment being written.
 *
 * When flush is called by IndexWriter we check out all DWPTs
 * that are associated with the current {@link DocumentsWriterDeleteQueue}
 * out of the {@link DocumentsWriterPerThreadPool} and write
 * them to disk. The flush process can piggy-back on incoming
 * indexing threads or even block them from adding documents
 * if flushing can't keep up with new documents being added.
 * Unless the stall control kicks in to block indexing threads
 * flushes are happening concurrently to actual index requests.
 *
 *
 * Exceptions:
 *
 * Because this class directly updates in-memory posting
 * lists, and flushes stored fields and term vectors
 * directly to files in the directory, there are certain
 * limited times when an exception can corrupt this state.
 * For example, a disk full while flushing stored fields
 * leaves this file in a corrupt state.  Or, an OOM
 * exception while appending to the in-memory posting lists
 * can corrupt that posting list.  We call such exceptions
 * "aborting exceptions".  In these cases we must call
 * abort() to discard all docs added since the last flush.
 *
 * All other exceptions ("non-aborting exceptions") can
 * still partially update the index structures.  These
 * updates are consistent, but, they represent only a part
 * of the document seen up until the exception was hit.
 * When this happens, we immediately mark the document as
 * deleted so that the document is always atomically ("all
 * or none") added to the index.
 * 该对象是整个 文档体系对外的入口
 */

final class DocumentsWriter implements Closeable, Accountable {
  private final AtomicLong pendingNumDocs;

  /**
   * 由外部设置的监听器  监听writer的动作
   */
  private final FlushNotifications flushNotifications;

  /**
   * 当前writer是否被关闭
   */
  private volatile boolean closed;

  /**
   * 用于输出日志
   */
  private final InfoStream infoStream;

  /**
   * 配置对象
   */
  private final LiveIndexWriterConfig config;

  /**
   * 记录当前内存中有多少文档数
   */
  private final AtomicInteger numDocsInRAM = new AtomicInteger(0);

  // TODO: cut over to BytesRefHash in BufferedDeletes
  // 这里记录了所有 待删除/更新的 node信息
  volatile DocumentsWriterDeleteQueue deleteQueue;

  /**
   * 该对象存储了 多个待刷盘的动作
   */
  private final DocumentsWriterFlushQueue ticketQueue = new DocumentsWriterFlushQueue();
  /*
   * we preserve changes during a full flush since IW might not checkout before
   * we release all changes. NRT Readers otherwise suddenly return true from
   * isCurrent while there are actually changes currently committed. See also
   * #anyChanges() & #flushAllThreads
   * 代表在fullFlush 之前是否有未刷盤的数据
   * 当 fullFlush 结束后 就会将该标识设置为false
   */
  private volatile boolean pendingChangesInCurrentFullFlush;

  /**
   * 该对象可以创建多个 thread
   */
  final DocumentsWriterPerThreadPool perThreadPool;
  /**
   * 刷盘动作本身是 依赖这个控制器来做的   FlushQueue 只是辅助对象
   */
  final DocumentsWriterFlushControl flushControl;

  /**
   *
   * @param flushNotifications  该对象负责监听 写入文档时 发生的各种情况 比如写入失败等  并作出对应的处理
   * @param indexCreatedVersionMajor   代表索引的主版本 用于判断文件是否兼容吧
   * @param pendingNumDocs   有多少待写入的 doc 该值居然是被共享的
   * @param enableTestPoints
   * @param segmentNameSupplier   该对象创建 segment的名字
   * @param config
   * @param directoryOrig   这里指定了原始目录
   * @param directory       在directoryOrig 的基础上增加了文件锁的检测 确保此时只有单进程能处理文件
   * @param globalFieldNumberMap   维护了全局范围内 fieldNum - fieldName 的映射关系  以及某个field的各个标识
   */
  DocumentsWriter(FlushNotifications flushNotifications, int indexCreatedVersionMajor, AtomicLong pendingNumDocs, boolean enableTestPoints,
                  Supplier<String> segmentNameSupplier, LiveIndexWriterConfig config, Directory directoryOrig, Directory directory,
                  FieldInfos.FieldNumbers globalFieldNumberMap) {
    this.config = config;
    this.infoStream = config.getInfoStream();
    // 该对象以 docWriter为单位
    this.deleteQueue = new DocumentsWriterDeleteQueue(infoStream);

    this.perThreadPool = new DocumentsWriterPerThreadPool(() -> {
      // 传入的参数就是线程工厂
      final FieldInfos.Builder infos = new FieldInfos.Builder(globalFieldNumberMap);
      // 每次创建的线程 会携带这些参数
      return new DocumentsWriterPerThread(indexCreatedVersionMajor,
          segmentNameSupplier.get(), directoryOrig,
          directory, config, infoStream, deleteQueue, infos,
          pendingNumDocs, enableTestPoints);
    });
    this.pendingNumDocs = pendingNumDocs;
    // 这里创建刷盘相关的总控对象
    flushControl = new DocumentsWriterFlushControl(this, config);
    this.flushNotifications = flushNotifications;
  }

  /**
   * 将符合查询条件的文档都删除
   * @param queries
   * @return
   * @throws IOException
   */
  long deleteQueries(final Query... queries) throws IOException {
    // 这里的操作 实际上就是 调用  deleteQueue.addDelete(Query... q) 也就是缓存了删除动作
    return applyDeleteOrUpdate(q -> q.addDelete(queries));
  }

  long deleteTerms(final Term... terms) throws IOException {
    return applyDeleteOrUpdate(q -> q.addDelete(terms));
  }

  long updateDocValues(DocValuesUpdate... updates) throws IOException {
    return applyDeleteOrUpdate(q -> q.addDocValuesUpdates(updates));
  }

  /**
   * 追加了一个新的 update/delete 动作  信息将会追加到 queue中
   * @param function
   * @return
   * @throws IOException
   */
  private synchronized long applyDeleteOrUpdate(ToLongFunction<DocumentsWriterDeleteQueue> function) throws IOException {
    // This method is synchronized to make sure we don't replace the deleteQueue while applying this update / delete
    // otherwise we might lose an update / delete if this happens concurrently to a full flush.
    final DocumentsWriterDeleteQueue deleteQueue = this.deleteQueue;
    // 当处理完毕后生成了一个序列号
    long seqNo = function.applyAsLong(deleteQueue);
    // 检测此时内存中的 update数据是否过多 是的话设置标识  会在applyAllDeletes 中将更新信息包装成 ticket 并发布到 IndexWriter中
    flushControl.doOnDelete();
    if (applyAllDeletes()) {
      // 当flush后 会返回一个负数
      seqNo = -seqNo;
    }
    return seqNo;
  }

  /** If buffered deletes are using too much heap, resolve them and write disk and return true.
   * 此时globalBufferUpdate中已经缓存了很多需要删除的数据了 将这些变化写入到磁盘中
   */
  private boolean applyAllDeletes() throws IOException {
    final DocumentsWriterDeleteQueue deleteQueue = this.deleteQueue;
    // 在fullFlush中 会处理掉deleteQueue的相关数据
    if (flushControl.isFullFlush() == false // never apply deletes during full flush this breaks happens before relationship
        && deleteQueue.isOpen() // if it's closed then it's already fully applied and we have a new delete queue
        // 在flushPolicy中 要是检测到此时 deleteQueue中已经存储了很多数据 那么会将该标识设置为true  提示需要强制处理这些数据
        && flushControl.getAndResetApplyAllDeletes()  //
    ) {
      // delete信息也会被包装成一个 ticket
      if (ticketQueue.addDeletes(deleteQueue)) {
        flushNotifications.onDeletesApplied(); // apply deletes event forces a purge
        return true;
      }
    }
    return false;
  }

  /**
   * 每某次doFlush结束  这里代表着可能是将flushQueue中的PerThread执行完了  又或者  中途发现产生了堆积 提前退出flush
   * @param forced
   * @param consumer
   * @throws IOException
   */
  void purgeFlushTickets(boolean forced, IOUtils.IOConsumer<DocumentsWriterFlushQueue.FlushTicket> consumer)
      throws IOException {
    // 当此时 ticketQueue.getTicketCount() >= perThreadPool.size() 时 代表发生了积压， 此时forced为true
    // 积压就意味着 IO 紧张
    if (forced) {
      ticketQueue.forcePurge(consumer);
    } else {
      ticketQueue.tryPurge(consumer);
    }
  }

  /** Returns how many docs are currently buffered in RAM. */
  // 记录当前改动了多少 doc
  int getNumDocs() {
    return numDocsInRAM.get();
  }

  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this DocumentsWriter is closed");
    }
  }

  /** Called if we hit an exception at a bad time (when
   *  updating the index files) and must discard all
   *  currently buffered docs.  This resets our state,
   *  discarding any docs added since last flush.
   *  一般是当 IndexWriter要被关闭时触发 丢弃所有更新数据 以及此时内存中解析doc后生成的数据
   *  */
  synchronized void abort() throws IOException {
    boolean success = false;
    try {
      // 舍弃之前缓存的所有更新信息
      deleteQueue.clear();
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "abort");
      }

      // 阻塞直到抢占到所有 perThread对象
      for (final DocumentsWriterPerThread perThread : perThreadPool.filterAndLock(x -> true)) {
        try {
          // 将perThread 标记成abort
          abortDocumentsWriterPerThread(perThread);
        } finally {
          perThread.unlock();
        }
      }
      flushControl.abortPendingFlushes();
      // 如果此时有正在刷盘的线程 等待线程执行完成
      flushControl.waitForFlush();
      assert perThreadPool.size() == 0
          : "There are still active DWPT in the pool: " + perThreadPool.size();
      success = true;
    } finally {
      if (success) {
        assert flushControl.getFlushingBytes() == 0 : "flushingBytes has unexpected value 0 != " + flushControl.getFlushingBytes();
        assert flushControl.netBytes() == 0 : "netBytes has unexpected value 0 != " + flushControl.netBytes();
      }
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "done abort success=" + success);
      }
    }
  }

  /**
   * 仅对单个  FlushTicket 执行刷盘操作
   * @return
   * @throws IOException
   */
  final boolean flushOneDWPT() throws IOException {
    if (infoStream.isEnabled("DW")) {
      infoStream.message("DW", "startFlushOneDWPT");
    }
    // first check if there is one pending
    DocumentsWriterPerThread documentsWriterPerThread = flushControl.nextPendingFlush();
    if (documentsWriterPerThread == null) {
      documentsWriterPerThread = flushControl.checkoutLargestNonPendingWriter();
    }
    if (documentsWriterPerThread != null) {
      return doFlush(documentsWriterPerThread);
    }
    return false; // we didn't flush anything here
  }

  /** Locks all currently active DWPT and aborts them.
   *  The returned Closeable should be closed once the locks for the aborted
   *  DWPTs can be released. */
  synchronized Closeable lockAndAbortAll() throws IOException {
    if (infoStream.isEnabled("DW")) {
      infoStream.message("DW", "lockAndAbortAll");
    }
    // Make sure we move all pending tickets into the flush queue:
    ticketQueue.forcePurge(ticket -> {
      if (ticket.getFlushedSegment() != null) {
        pendingNumDocs.addAndGet(-ticket.getFlushedSegment().segmentInfo.info.maxDoc());
      }
    });
    List<DocumentsWriterPerThread> writers = new ArrayList<>();
    AtomicBoolean released = new AtomicBoolean(false);
    final Closeable release = () -> {
      // we return this closure to unlock all writers once done
      // or if hit an exception below in the try block.
      // we can't assign this later otherwise the ref can't be final
      if (released.compareAndSet(false, true)) { // only once
        if (infoStream.isEnabled("DW")) {
          infoStream.message("DW", "unlockAllAbortedThread");
        }
        perThreadPool.unlockNewWriters();
        for (DocumentsWriterPerThread writer : writers) {
          writer.unlock();
        }
      }
    };
    try {
      deleteQueue.clear();
      perThreadPool.lockNewWriters();
      writers.addAll(perThreadPool.filterAndLock(x -> true));
      for (final DocumentsWriterPerThread perThread : writers) {
        assert perThread.isHeldByCurrentThread();
        abortDocumentsWriterPerThread(perThread);
      }
      deleteQueue.clear();

      // jump over any possible in flight ops:
      deleteQueue.skipSequenceNumbers(perThreadPool.size() + 1);

      flushControl.abortPendingFlushes();
      flushControl.waitForFlush();
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "finished lockAndAbortAll success=true");
      }
      return release;
    } catch (Throwable t) {
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", "finished lockAndAbortAll success=false");
      }
      try {
        // if something happens here we unlock all states again
        release.close();
      } catch (Throwable t1) {
        t.addSuppressed(t1);
      }
      throw t;
    }
  }
  
  /** Returns how many documents were aborted. */
  // 禁用  DocumentsWriterPerThread
  private void abortDocumentsWriterPerThread(final DocumentsWriterPerThread perThread) throws IOException {
    assert perThread.isHeldByCurrentThread();
    try {
      // 将 docNum  -  该thread 采集到的更新数量
      subtractFlushedNumDocs(perThread.getNumDocsInRAM());
      perThread.abort();
    } finally {
      flushControl.doOnAbort(perThread);
    }
  }

  /** returns the maximum sequence number for all previously completed operations */
  long getMaxCompletedSequenceNumber() {
    return deleteQueue.getMaxCompletedSeqNo();
  }


  /**
   * 检测当前是否有未提交数据
   * @return
   */
  boolean anyChanges() {
    /*
     * changes are either in a DWPT or in the deleteQueue.
     * yet if we currently flush deletes and / or dwpt there
     * could be a window where all changes are in the ticket queue
     * before they are published to the IW. ie we need to check if the 
     * ticket queue has any tickets.
     */
    boolean anyChanges = numDocsInRAM.get() != 0 || anyDeletions() || ticketQueue.hasTickets() || pendingChangesInCurrentFullFlush;
    if (infoStream.isEnabled("DW") && anyChanges) {
      infoStream.message("DW", "anyChanges? numDocsInRam=" + numDocsInRAM.get()
                         + " deletes=" + anyDeletions() + " hasTickets:"
                         + ticketQueue.hasTickets() + " pendingChangesInFullFlush: "
                         + pendingChangesInCurrentFullFlush);
    }
    return anyChanges;
  }
  
  int getBufferedDeleteTermsSize() {
    return deleteQueue.getBufferedUpdatesTermsSize();
  }

  //for testing
  int getNumBufferedDeleteTerms() {
    return deleteQueue.numGlobalTermDeletes();
  }

  boolean anyDeletions() {
    return deleteQueue.anyChanges();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    IOUtils.close(flushControl, perThreadPool);
  }

  /**
   * 检查当前是否准备要更新
   * @return
   * @throws IOException
   */
  private boolean preUpdate() throws IOException {
    ensureOpen();
    boolean hasEvents = false;

    // 已经处于暂停状态 就代表需要执行刷盘       或者待刷盘数量达到一定值 即使没有触发暂停状态 也会执行刷盘
    if (flushControl.anyStalledThreads() || (flushControl.numQueuedFlushes() > 0 && config.checkPendingFlushOnUpdate)) {
      // Help out flushing any queued DWPTs so we can un-stall:
      do {
        // Try pick up pending threads here if possible
        DocumentsWriterPerThread flushingDWPT;
        while ((flushingDWPT = flushControl.nextPendingFlush()) != null) {
          // Don't push the delete here since the update could fail!
          // 将写入线程的数据刷盘
          hasEvents |= doFlush(flushingDWPT);
        }

        // 如果此时内存中占用的数据量比较大 或者说解析后未刷盘的数据量比较大时  无法继续解析doc 必须等待内存中数据固化到磁盘
        flushControl.waitIfStalled(); // block if stalled
        // 直到执行完此时存在的所有flush任务
      } while (flushControl.numQueuedFlushes() != 0); // still queued DWPTs try help flushing
    }
    return hasEvents;
  }

  /**
   * 执行完 updateDocument的后置方法
   * @param flushingDWPT  本次待刷盘的perThread
   * @param hasEvents
   * @return
   * @throws IOException
   */
  private boolean postUpdate(DocumentsWriterPerThread flushingDWPT, boolean hasEvents) throws IOException {
    // 如果此时将 deleteQueue内的数据包装成 flushTicket 发布到eventQueue中
    hasEvents |= applyAllDeletes();
    // 如果存在某个需要刷盘的 perThread 对象 执行刷盘操作
    if (flushingDWPT != null) {
      hasEvents |= doFlush(flushingDWPT);
    // 此时没有传入待刷盘perThread 对象 但是如果设置了需要主动查询
    } else if (config.checkPendingFlushOnUpdate) {
      final DocumentsWriterPerThread nextPendingFlush = flushControl.nextPendingFlush();
      if (nextPendingFlush != null) {
        hasEvents |= doFlush(nextPendingFlush);
      }
    }

    return hasEvents;
  }

  /**
   * 更新doc
   * @param docs   本次要插入的新doc 支持批量处理多个doc
   * @param delNode  将满足条件的doc删除  允许为null
   * @return
   * @throws IOException
   */
  long updateDocuments(final Iterable<? extends Iterable<? extends IndexableField>> docs,
                       final DocumentsWriterDeleteQueue.Node<?> delNode) throws IOException {
    // 首先检测是否有待刷盘的任务  如果开启了前置刷盘功能 那么必须先将待刷盘的任务完成
    boolean hasEvents = preUpdate();

    // 从空闲队列中生成一个新的 perThread对象 或者直接生成一个新对象
    final DocumentsWriterPerThread dwpt = flushControl.obtainAndLock();
    final DocumentsWriterPerThread flushingDWPT;
    long seqNo;

    try {
      // This must happen after we've pulled the DWPT because IW.close
      // waits for all DWPT to be released:
      ensureOpen();
      // 此时的线程可能是从 freeList中获取的 所以可能记录了某些存在于内存的doc
      final int dwptNumDocs = dwpt.getNumDocsInRAM();
      try {
        // 解析doc 并暂存在内存中 同时将node信息插入到 deleteQueue中
        seqNo = dwpt.updateDocuments(docs, delNode, flushNotifications);
      } finally {
        if (dwpt.isAborted()) {
          flushControl.doOnAbort(dwpt);
        }
        // We don't know how many documents were actually
        // counted as indexed, so we must subtract here to
        // accumulate our separate counter:
        // 更新计数器
        numDocsInRAM.addAndGet(dwpt.getNumDocsInRAM() - dwptNumDocs);
      }
      // 当携带了node信息 且node信息是用于删除的时候  isUpdate为true
      final boolean isUpdate = delNode != null && delNode.isDelete();

      /**
       * 首先进行fullFlush时 会将旧的perThread全部设置到刷盘队列  此时如果有新的解析doc请求 只会创建新的perThread
       * 假设此时存在2条线程 一条执行fullFLush 一条尝试解析新的doc  在doAfterDocument中 另一条线程在检测到当前处在fullFlush时 会帮助执行fullFlush的线程分担 perThread的刷盘任务
       * 如果另一条线程解析的doc 占用的bytes 没有达到刷盘阈值 不会发生什么 如果达到阈值 那么此时对应的perThread对象并不会直接进入刷盘相关队列 而是先进入一个 block队列
       */
      flushingDWPT = flushControl.doAfterDocument(dwpt, isUpdate);
    } finally {
      // 解锁以确保其他线程可以获取到该对象
      if (dwpt.isFlushPending() || dwpt.isAborted()) {
        // 通过这个锁来确保此时只有一个线程独占这个对象   在从pool中获取 perThread的逻辑中就要求一定要上锁成功才能返回
        dwpt.unlock();
      } else {
        // 只有在当前perThread 没有处在待刷盘状态时 才能回到pool中 主要触发了刷盘后 该perThread的任务就完成了 无法再被复用
        perThreadPool.marksAsFreeAndUnlock(dwpt);
      }
      assert dwpt.isHeldByCurrentThread() == false : "we didn't release the dwpt even on abort";
    }

    // 执行后置操作
    if (postUpdate(flushingDWPT, hasEvents)) {
      seqNo = -seqNo;
    }
    return seqNo;
  }

  /**
   * 将某个PerThread对象暂存在内存中的索引数据写入磁盘
   * @param flushingDWPT
   * @return
   * @throws IOException
   */
  private boolean doFlush(DocumentsWriterPerThread flushingDWPT) throws IOException {

    // 是否至少处理过一个 perThread  无论是否成功
    boolean hasEvents = false;
    while (flushingDWPT != null) {
      assert flushingDWPT.hasFlushed() == false;
      hasEvents = true;
      boolean success = false;
      DocumentsWriterFlushQueue.FlushTicket ticket = null;
      try {
        assert currentFullFlushDelQueue == null
            || flushingDWPT.deleteQueue == currentFullFlushDelQueue : "expected: "
            + currentFullFlushDelQueue + "but was: " + flushingDWPT.deleteQueue
            + " " + flushControl.isFullFlush();
        /*
         * Since with DWPT the flush process is concurrent and several DWPT
         * could flush at the same time we must maintain the order of the
         * flushes before we can apply the flushed segment and the frozen global
         * deletes it is buffering. The reason for this is that the global
         * deletes mark a certain point in time where we took a DWPT out of
         * rotation and freeze the global deletes.
         * 
         * Example: A flush 'A' starts and freezes the global deletes, then
         * flush 'B' starts and freezes all deletes occurred since 'A' has
         * started. if 'B' finishes before 'A' we need to wait until 'A' is done
         * otherwise the deletes frozen by 'B' are not applied to 'A' and we
         * might miss to deletes documents in 'A'.
         */
        try {
          assert assertTicketQueueModification(flushingDWPT.deleteQueue);
          // Each flush is assigned a ticket in the order they acquire the ticketQueue lock
          // ticket的流程是这样的  每当某个perThread 尝试进行刷盘时会先创建 ticket对象
          ticket = ticketQueue.addFlushTicket(flushingDWPT);
          // 获取当前待刷盘的 doc 数量
          final int flushingDocsInRam = flushingDWPT.getNumDocsInRAM();
          boolean dwptSuccess = false;
          try {
            // flush concurrently without locking
            // 这里会将之前解析doc生成的暂存在内存中的数据刷盘  同时还会存储段索引文件 和 liveDoc 索引文件
            final FlushedSegment newSegment = flushingDWPT.flush(flushNotifications);
            // 回填本次刷盘的结果信息
            ticketQueue.addSegment(ticket, newSegment);
            dwptSuccess = true;
          } finally {
            // 当刷盘失败被捕获到时 会在外层将该ticket标记成failed

            // 也就是每个尝试flush的 perThread对象执行失败后都会添加一个删除索引文件的任务到 IndexWriter的任务队列中
            // 减少当前内存中的文档数
            subtractFlushedNumDocs(flushingDocsInRam);
            // 代表存在某些要删除的文件  TODO 这里对应复合文件的场景 可以先忽略
            if (flushingDWPT.pendingFilesToDelete().isEmpty() == false) {
              Set<String> files = flushingDWPT.pendingFilesToDelete();
              // 通过indexWriter 处理未使用的文件   (这里使用了 事件模型 EventQueue)
              flushNotifications.deleteUnusedFiles(files);
              hasEvents = true;
            }
            // 如果在处理过程中出现异常了
            if (dwptSuccess == false) {
              // 实际上就是删除未完成的索引文件   这些删除动作重要性都不是特别高 所以先存放到任务队列 待会一块执行
              flushNotifications.flushFailed(flushingDWPT.getSegmentInfo());
              hasEvents = true;
            }
          }
          // flush was successful once we reached this point - new seg. has been assigned to the ticket!
          success = true;
        } finally {
          /**
           * 每次进入新的一轮时  ticket会被置空 这种情况就是刷盘出现异常 但是ticket却成功创建 并插入到队列中了 需要标记成失败 这样就认为也可以触发 canPublish
           * canPublish 为true的ticket才能被正常处理
           * 所以总计有3种情况可以确保ticket被正常处理
           * 1.hasSegment == false 代表本次ticket是针对 update/delete 创建的
           * 2.segment != null 代表刷盘完成并且已经回填了flushedSegment
           * 3.failed == true 代表虽然ticket成功创建和入队 但是刷盘失败了
           */
          if (!success && ticket != null) {
            // In the case of a failure make sure we are making progress and
            // apply all the deletes since the segment flush failed since the flush
            // ticket could hold global deletes see FlushTicket#canPublish()
            ticketQueue.markTicketFailed(ticket);
          }
        }
        /*
         * Now we are done and try to flush the ticket queue if the head of the
         * queue has already finished the flush.
         * 此时代表还有很多 ticket还未处理 此时不再继续处理perThread了 要求先发布之前的ticket
         * 也就是ticket的发布也是尽可能批处理的
         */
        if (ticketQueue.getTicketCount() >= perThreadPool.size()) {
          // This means there is a backlog: the one
          // thread in innerPurge can't keep up with all
          // other threads flushing segments.  In this case
          // we forcefully stall the producers.
          // 代表此时发生了积压
          flushNotifications.onTicketBacklog();
          // 退出刷盘循环
          break;
        }
      } finally {
        // 当刷盘完成时触发
        flushControl.doAfterFlush(flushingDWPT);
      }

      // 获取下一个任务
      flushingDWPT = flushControl.nextPendingFlush();
    }

    // 非强制发布
    if (hasEvents) {
      flushNotifications.afterSegmentsFlushed();
    }

    // If deletes alone are consuming > 1/2 our RAM
    // buffer, force them all to apply now. This is to
    // prevent too-frequent flushing of a long tail of
    // tiny segments:
    final double ramBufferSizeMB = config.getRAMBufferSizeMB();
    // 此时 deleteQueue中堆积的数据很多 尝试包装成Ticket 并等待处理
    if (ramBufferSizeMB != IndexWriterConfig.DISABLE_AUTO_FLUSH &&
        flushControl.getDeleteBytesUsed() > (1024*1024*ramBufferSizeMB/2)) {
      hasEvents = true;

      // 代表此时不适合执行删除动作 ，比如正在进行 fullFlush
      if (applyAllDeletes() == false) {
        if (infoStream.isEnabled("DW")) {
          infoStream.message("DW", String.format(Locale.ROOT, "force apply deletes after flush bytesUsed=%.1f MB vs ramBuffer=%.1f MB",
                                                 flushControl.getDeleteBytesUsed()/(1024.*1024.),
                                                 ramBufferSizeMB));
        }
        // 添加一个阻塞处理之前所有囤积ticket的event 到任务队列
        flushNotifications.onDeletesApplied();
      }
    }

    return hasEvents;
  }

  synchronized long getNextSequenceNumber() {
    // this must be synced otherwise the delete queue might change concurrently
    return deleteQueue.getNextSequenceNumber();
  }

  /**
   * 更新内部的队列
   * @param newQueue
   */
  synchronized void resetDeleteQueue(DocumentsWriterDeleteQueue newQueue) {
    assert deleteQueue.isAdvanced();
    assert newQueue.isAdvanced() == false;
    assert deleteQueue.getLastSequenceNumber() <= newQueue.getLastSequenceNumber();
    assert deleteQueue.getMaxSeqNo() <= newQueue.getLastSequenceNumber()
        : "maxSeqNo: " + deleteQueue.getMaxSeqNo() + " vs. " + newQueue.getLastSequenceNumber();
    deleteQueue = newQueue;
  }

  /**
   * 外部设置的监听器对象
   */
  interface FlushNotifications { // TODO maybe we find a better name for this?

    /**
     * Called when files were written to disk that are not used anymore. It's the implementation's responsibility
     * to clean these files up
     * 当某组文件确保不会再被使用时触发
     */
    void deleteUnusedFiles(Collection<String> files);

    /**
     * Called when a segment failed to flush.
     */
    void flushFailed(SegmentInfo info);

    /**
     * Called after one or more segments were flushed to disk.
     */
    void afterSegmentsFlushed() throws IOException;

    /**
     * Should be called if a flush or an indexing operation caused a tragic / unrecoverable event.
     */
    void onTragicEvent(Throwable event, String message);

    /**
     * Called once deletes have been applied either after a flush or on a deletes call
     */
    void onDeletesApplied();

    /**
     * Called once the DocumentsWriter ticket queue has a backlog. This means there is an inner thread
     * that tries to publish flushed segments but can't keep up with the other threads flushing new segments.
     * This likely requires other thread to forcefully purge the buffer to help publishing. This
     * can't be done in-place since we might hold index writer locks when this is called. The caller must ensure
     * that the purge happens without an index writer lock being held.
     *
     * @see DocumentsWriter#purgeFlushTickets(boolean, IOUtils.IOConsumer)
     */
    void onTicketBacklog();
  }

  /**
   * 减少内存中记录的doc 数量
   * @param numFlushed
   */
  void subtractFlushedNumDocs(int numFlushed) {
    int oldValue = numDocsInRAM.get();
    while (numDocsInRAM.compareAndSet(oldValue, oldValue - numFlushed) == false) {
      oldValue = numDocsInRAM.get();
    }
    assert numDocsInRAM.get() >= 0;
  }
  
  // for asserts
  private volatile DocumentsWriterDeleteQueue currentFullFlushDelQueue = null;

  // for asserts
  private synchronized boolean setFlushingDeleteQueue(DocumentsWriterDeleteQueue session) {
    assert currentFullFlushDelQueue == null
        || currentFullFlushDelQueue.isOpen() == false : "Can not replace a full flush queue if the queue is not closed";
    currentFullFlushDelQueue = session;
    return true;
  }

  private boolean assertTicketQueueModification(DocumentsWriterDeleteQueue deleteQueue) {
    // assign it then we don't need to sync on DW
    DocumentsWriterDeleteQueue currentFullFlushDelQueue = this.currentFullFlushDelQueue;
    assert currentFullFlushDelQueue == null || currentFullFlushDelQueue == deleteQueue:
        "only modifications from the current flushing queue are permitted while doing a full flush";
    return true;
  }
  
  /*
   * FlushAllThreads is synced by IW fullFlushLock. Flushing all threads is a
   * two stage operation; the caller must ensure (in try/finally) that finishFlush
   * is called after this method, to release the flush lock in DWFlushControl
   * 将此时所有 perThread的数据写入到磁盘中
   */
  long flushAllThreads()
    throws IOException {
    final DocumentsWriterDeleteQueue flushingDeleteQueue;
    if (infoStream.isEnabled("DW")) {
      infoStream.message("DW", "startFullFlush");
    }

    long seqNo;
    synchronized (this) {
      // 此时是否有待处理的数据
      pendingChangesInCurrentFullFlush = anyChanges();
      flushingDeleteQueue = deleteQueue;
      /* Cutover to a new delete queue.  This must be synced on the flush control
       * otherwise a new DWPT could sneak into the loop with an already flushing
       * delete queue */
      // 生成一个新的 deleteQueue 替代原来的删除队列 并将同一时期创建的 perThread都转移到刷盘队列中 设置 fullFlush标识     并且此时如果 activeBytes 已经变小了 可以尝试将stall变成false 这样就不会阻塞 尝试解析doc的线程
      seqNo = flushControl.markForFullFlush(); // swaps this.deleteQueue synced on FlushControl
      assert setFlushingDeleteQueue(flushingDeleteQueue);
    }
    assert currentFullFlushDelQueue != null;
    assert currentFullFlushDelQueue != deleteQueue;

    // 只要 doFlush(perThread) 对象不为空 必然返回true
    boolean anythingFlushed = false;
    try {
      DocumentsWriterPerThread flushingDWPT;
      // Help out with flushing:
      // 挨个取出所有刷盘队列中的 PerThread 并执行刷盘操作  在刷盘完成时 会往 IndexWriter的任务队列中添加一堆任务  并会将已经完成刷盘的索引文件的添加到 segmentInfos中  以及通过deleter对象 维护索引文件的引用计数 避免被误删除
      // 在这种情况下 即使发生积压 也会确保执行完所有的任务   积压只是使得 doFlush不会一次性处理完所有的perThread 而在外层的while中还是会处理完所有perThread
      while ((flushingDWPT = flushControl.nextPendingFlush()) != null) {
        anythingFlushed |= doFlush(flushingDWPT);
      }
      // If a concurrent flush is still in flight wait for it
      // 如果是并发调用的场景  有可能 perThread 被其他线程抢走了 那么就等待所有perThread 刷盘完毕
      flushControl.waitForFlush();

      // 如果本次没有处理任何东西 (比如没有需要刷盘的 perThread对象) 并且发现有未处理的 update 就包装成 ticket 并设置到 ticketQueue中
      if (anythingFlushed == false && flushingDeleteQueue.anyChanges()) { // apply deletes if we did not flush any document
        if (infoStream.isEnabled("DW")) {
          infoStream.message("DW", Thread.currentThread().getName() + ": flush naked frozen global deletes");
        }
        assert assertTicketQueueModification(flushingDeleteQueue);
        ticketQueue.addDeletes(flushingDeleteQueue);
      }
      // we can't assert that we don't have any tickets in teh queue since we might add a DocumentsWriterDeleteQueue
      // concurrently if we have very small ram buffers this happens quite frequently
      assert !flushingDeleteQueue.anyChanges();
    } finally {
      assert flushingDeleteQueue == currentFullFlushDelQueue;
      flushingDeleteQueue.close(); // all DWPT have been processed and this queue has been fully flushed to the ticket-queue
    }
    if (anythingFlushed) {
      return -seqNo;
    } else {
      return seqNo;
    }
  }

  /**
   * 代表某次全刷盘操作完成
   * @param success   本次操作是否成功
   * @throws IOException
   */
  void finishFullFlush(boolean success) throws IOException {
    try {
      if (infoStream.isEnabled("DW")) {
        infoStream.message("DW", Thread.currentThread().getName() + " finishFullFlush success=" + success);
      }
      assert setFlushingDeleteQueue(null);
      if (success) {
        // Release the flush lock
        // 释放 flush锁 同时将flush过程中添加的任务(在block队列中) 转移到 flush队列中
        flushControl.finishFullFlush();
      } else {
        // 代表以异常形式结束 fullFlush  会将此时 flushQueue 和 blockedFlushes 内所有 perThread之前解析的数据清空 同时将之前已经创建的索引文件删除
        flushControl.abortFullFlushes();
      }
    } finally {
      pendingChangesInCurrentFullFlush = false;
      applyAllDeletes(); // make sure we do execute this since we block applying deletes during full flush
    }
  }

  @Override
  public long ramBytesUsed() {
    return flushControl.ramBytesUsed();
  }

  /**
   * Returns the number of bytes currently being flushed
   *
   * This is a subset of the value returned by {@link #ramBytesUsed()}
   */
  long getFlushingBytes() {
    return flushControl.getFlushingBytes();
  }
}
