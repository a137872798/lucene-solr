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
package org.apache.lucene.search;


import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.store.AlreadyClosedException;

/**
 * Utility class to safely share instances of a certain type across multiple
 * threads, while periodically refreshing them. This class ensures each
 * reference is closed only once all threads have finished using it. It is
 * recommended to consult the documentation of {@link ReferenceManager}
 * implementations for their {@link #maybeRefresh()} semantics.
 * 
 * @param <G>
 *          the concrete type that will be {@link #acquire() acquired} and
 *          {@link #release(Object) released}.
 * 
 * @lucene.experimental
 * 为内部的数据属性定义了一套在多线程环境下 具备刷新能力的模板
 */
public abstract class ReferenceManager<G> implements Closeable {

  private static final String REFERENCE_MANAGER_IS_CLOSED_MSG = "this ReferenceManager is closed";

  /**
   * 此时数据体
   */
  protected volatile G current;
  
  private final Lock refreshLock = new ReentrantLock();

  /**
   * 监听刷新动作
   */
  private final List<RefreshListener> refreshListeners = new CopyOnWriteArrayList<>();

  private void ensureOpen() {
    if (current == null) {
      throw new AlreadyClosedException(REFERENCE_MANAGER_IS_CLOSED_MSG);
    }
  }

  /**
   * 更新内部的数据对象
   * @param newReference
   * @throws IOException
   */
  private synchronized void swapReference(G newReference) throws IOException {
    ensureOpen();
    final G oldReference = current;
    current = newReference;
    release(oldReference);
  }

  /**
   * Decrement reference counting on the given reference. 
   * @throws IOException if reference decrement on the given resource failed.
   * */
  protected abstract void decRef(G reference) throws IOException;
  
  /**
   * Refresh the given reference if needed. Returns {@code null} if no refresh
   * was needed, otherwise a new refreshed reference.
   * @throws AlreadyClosedException if the reference manager has been {@link #close() closed}.
   * @throws IOException if the refresh operation failed
   */
  protected abstract G refreshIfNeeded(G referenceToRefresh) throws IOException;

  /**
   * Try to increment reference counting on the given reference. Return true if
   * the operation was successful.
   * @throws AlreadyClosedException if the reference manager has been {@link #close() closed}. 
   */
  protected abstract boolean tryIncRef(G reference) throws IOException;

  /**
   * Obtain the current reference. You must match every call to acquire with one
   * call to {@link #release}; it's best to do so in a finally clause, and set
   * the reference to {@code null} to prevent accidental usage after it has been
   * released.
   * @throws AlreadyClosedException if the reference manager has been {@link #close() closed}.
   * 在获取内部属性前 需要做一些准备工作
   */
  public final G acquire() throws IOException {
    G ref;

    do {
      if ((ref = current) == null) {
        throw new AlreadyClosedException(REFERENCE_MANAGER_IS_CLOSED_MSG);
      }
      // 因为在多线程环境下使用这个属性 所以要增加引用计数 避免被其他线程意外关闭
      if (tryIncRef(ref)) {
        return ref;
      }
      // 因为多线程间的竞争 本次更新失败就获取此时的最新值  当发现数值归0 或者数据已经被清理了就抛出异常
      if (getRefCount(ref) == 0 && current == ref) {
        assert ref != null;
        /* if we can't increment the reader but we are
           still the current reference the RM is in a
           illegal states since we can't make any progress
           anymore. The reference is closed but the RM still
           holds on to it as the actual instance.
           This can only happen if somebody outside of the RM
           decrements the refcount without a corresponding increment
           since the RM assigns the new reference before counting down
           the reference. */
        throw new IllegalStateException("The managed reference has already closed - this is likely a bug when the reference count is modified outside of the ReferenceManager");
      }
      // 重试直到增加成功
    } while (true);
  }
  
  /**
    * <p>
    * Closes this ReferenceManager to prevent future {@link #acquire() acquiring}. A
    * reference manager should be closed if the reference to the managed resource
    * should be disposed or the application using the {@link ReferenceManager}
    * is shutting down. The managed resource might not be released immediately,
    * if the {@link ReferenceManager} user is holding on to a previously
    * {@link #acquire() acquired} reference. The resource will be released once
    * when the last reference is {@link #release(Object) released}. Those
    * references can still be used as if the manager was still active.
    * </p>
    * <p>
    * Applications should not {@link #acquire() acquire} new references from this
    * manager once this method has been called. {@link #acquire() Acquiring} a
    * resource on a closed {@link ReferenceManager} will throw an
    * {@link AlreadyClosedException}.
    * </p>
    * 
    * @throws IOException
    *           if the underlying reader of the current reference could not be closed
   */
  @Override
  public final synchronized void close() throws IOException {
    if (current != null) {
      // make sure we can call this more than once
      // closeable javadoc says:
      // if this is already closed then invoking this method has no effect.
      swapReference(null);
      afterClose();
    }
  }

  /**
   * Returns the current reference count of the given reference.
   */
  protected abstract int getRefCount(G reference);

  /**
   *  Called after close(), so subclass can free any resources.
   *  @throws IOException if the after close operation in a sub-class throws an {@link IOException} 
   * */
  protected void afterClose() throws IOException {
  }

  /**
   * 进行刷新工作
   * @throws IOException
   */
  private void doMaybeRefresh() throws IOException {
    // it's ok to call lock() here (blocking) because we're supposed to get here
    // from either maybeRefresh() or maybeRefreshBlocking(), after the lock has
    // already been obtained. Doing that protects us from an accidental bug
    // where this method will be called outside the scope of refreshLock.
    // Per ReentrantLock's javadoc, calling lock() by the same thread more than
    // once is ok, as long as unlock() is called a matching number of times.
    refreshLock.lock();
    boolean refreshed = false;
    try {
      // 获取内部的数据体
      final G reference = acquire();
      try {
        // 触发所有前置钩子
        notifyRefreshListenersBefore();
        // 子类通过重写该函数 实现数据的更新功能
        G newReference = refreshIfNeeded(reference);
        if (newReference != null) {
          assert newReference != reference : "refreshIfNeeded should return null if refresh wasn't needed";
          try {
            // 之后的线程都只会获取到新数据了
            swapReference(newReference);
            refreshed = true;
          } finally {
            if (!refreshed) {
              release(newReference);
            }
          }
        }
      } finally {
        // 释放旧数据的引用计数
        release(reference);
        notifyRefreshListenersRefreshed(refreshed);
      }
      afterMaybeRefresh();
    } finally {
      refreshLock.unlock();
    }
  }

  /**
   * You must call this (or {@link #maybeRefreshBlocking()}), periodically, if
   * you want that {@link #acquire()} will return refreshed instances.
   * 
   * <p>
   * <b>Threads</b>: it's fine for more than one thread to call this at once.
   * Only the first thread will attempt the refresh; subsequent threads will see
   * that another thread is already handling refresh and will return
   * immediately. Note that this means if another thread is already refreshing
   * then subsequent threads will return right away without waiting for the
   * refresh to complete.
   * 
   * <p>
   * If this method returns true it means the calling thread either refreshed or
   * that there were no changes to refresh. If it returns false it means another
   * thread is currently refreshing.
   * </p>
   * @throws IOException if refreshing the resource causes an {@link IOException}
   * @throws AlreadyClosedException if the reference manager has been {@link #close() closed}.
   * 尝试进行更新   应该是感知到某个信号后才会主动调用该方法
   */
  public final boolean maybeRefresh() throws IOException {
    ensureOpen();

    // Ensure only 1 thread does refresh at once; other threads just return immediately:
    final boolean doTryRefresh = refreshLock.tryLock();
    if (doTryRefresh) {
      try {
        doMaybeRefresh();
      } finally {
        refreshLock.unlock();
      }
    }

    return doTryRefresh;
  }
  
  /**
   * You must call this (or {@link #maybeRefresh()}), periodically, if you want
   * that {@link #acquire()} will return refreshed instances.
   * 
   * <p>
   * <b>Threads</b>: unlike {@link #maybeRefresh()}, if another thread is
   * currently refreshing, this method blocks until that thread completes. It is
   * useful if you want to guarantee that the next call to {@link #acquire()}
   * will return a refreshed instance. Otherwise, consider using the
   * non-blocking {@link #maybeRefresh()}.
   * @throws IOException if refreshing the resource causes an {@link IOException}
   * @throws AlreadyClosedException if the reference manager has been {@link #close() closed}.
   * 阻塞直到确定调用了刷新函数
   */
  public final void maybeRefreshBlocking() throws IOException {
    ensureOpen();

    // Ensure only 1 thread does refresh at once
    refreshLock.lock();
    try {
      doMaybeRefresh();
    } finally {
      refreshLock.unlock();
    }
  }

  /** Called after a refresh was attempted, regardless of
   *  whether a new reference was in fact created.
   *  @throws IOException if a low level I/O exception occurs  
   **/
  protected void afterMaybeRefresh() throws IOException {
  }
  
  /**
   * Release the reference previously obtained via {@link #acquire()}.
   * <p>
   * <b>NOTE:</b> it's safe to call this after {@link #close()}.
   * @throws IOException if the release operation on the given resource throws an {@link IOException}
   * 在释放旧的数据时 不会直接进行清理 而是采用引用计数的方式  避免影响到其他使用者
   */
  public final void release(G reference) throws IOException {
    assert reference != null;
    decRef(reference);
  }

  private void notifyRefreshListenersBefore() throws IOException {
    for (RefreshListener refreshListener : refreshListeners) {
      refreshListener.beforeRefresh();
    }
  }

  private void notifyRefreshListenersRefreshed(boolean didRefresh) throws IOException {
    for (RefreshListener refreshListener : refreshListeners) {
      refreshListener.afterRefresh(didRefresh);
    }
  }

  /**
   * Adds a listener, to be notified when a reference is refreshed/swapped.
   */
  public void addListener(RefreshListener listener) {
    if (listener == null) {
      throw new NullPointerException("Listener must not be null");
    }
    refreshListeners.add(listener);
  }

  /**
   * Remove a listener added with {@link #addListener(RefreshListener)}.
   */
  public void removeListener(RefreshListener listener) {
    if (listener == null) {
      throw new NullPointerException("Listener must not be null");
    }
    refreshListeners.remove(listener);
  }

  /** Use to receive notification when a refresh has
   *  finished.  See {@link #addListener}.
   *  刷新监听器 在前后植入钩子
   *  */
  public interface RefreshListener {

    /** Called right before a refresh attempt starts. */
    void beforeRefresh() throws IOException;

    /** Called after the attempted refresh; if the refresh
     * did open a new reference then didRefresh will be true
     * and {@link #acquire()} is guaranteed to return the new
     * reference. */
    void afterRefresh(boolean didRefresh) throws IOException;
  }
}
