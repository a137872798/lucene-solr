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
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A guard that is created for every {@link ByteBufferIndexInput} that tries on best effort
 * to reject any access to the {@link ByteBuffer} behind, once it is unmapped. A single instance
 * of this is used for the original and all clones, so once the original is closed and unmapped
 * all clones also throw {@link AlreadyClosedException}, triggered by a {@link NullPointerException}.
 * <p>
 * This code tries to hopefully flush any CPU caches using a store-store barrier. It also yields the
 * current thread to give other threads a chance to finish in-flight requests...
 * 警卫
 */
final class ByteBufferGuard {
  
  /**
   * Pass in an implementation of this interface to cleanup ByteBuffers.
   * MMapDirectory implements this to allow unmapping of bytebuffers with private Java APIs.
   */
  @FunctionalInterface
  static interface BufferCleaner {
    void freeBuffer(String resourceDescription, ByteBuffer b) throws IOException;
  }
  
  private final String resourceDescription;
  /**
   * 该对象负责回收mmap
   */
  private final BufferCleaner cleaner;
  
  /** Not volatile; see comments on visibility below! */
  // 代表当前是否有效
  private boolean invalidated = false;
  
  /** Used as a store-store barrier; see comments below! */
  // 应该是这样的  AtomicInteger 底层使用的是 cas指令 也就是x86指令集中某个指令 可能在底层的实现会涉及到 内存屏障
  private final AtomicInteger barrier = new AtomicInteger();
  
  /**
   * Creates an instance to be used for a single {@link ByteBufferIndexInput} which
   * must be shared by all of its clones.
   */
  public ByteBufferGuard(String resourceDescription, BufferCleaner cleaner) {
    this.resourceDescription = resourceDescription;
    this.cleaner = cleaner;
  }
  
  /**
   * Invalidates this guard and unmaps (if supported).
   * 为一组 buffer 解除映射
   */
  public void invalidateAndUnmap(ByteBuffer... bufs) throws IOException {
    if (cleaner != null) {
      invalidated = true;
      // This call should hopefully flush any CPU caches and as a result make
      // the "invalidated" field update visible to other threads. We specifically
      // don't make "invalidated" field volatile for performance reasons, hoping the
      // JVM won't optimize away reads of that field and hardware should ensure
      // caches are in sync after this call. This isn't entirely "fool-proof" 
      // (see LUCENE-7409 discussion), but it has been shown to work in practice
      // and we count on this behavior.
      // 强制触发一次写屏障 使得缓存数据无效      实际上还没有被证实这样一定有效
      barrier.lazySet(0);
      // we give other threads a bit of time to finish reads on their ByteBuffer...:
      // 看来其他线程在执行任务时 会时不时的检测 invalidated 一旦发现是 false 立即释放byteBuffer 方便这里进行清理
      Thread.yield();
      // finally unmap the ByteBuffers:
      for (ByteBuffer b : bufs) {
        cleaner.freeBuffer(resourceDescription, b);
      }
    }
  }
  
  private void ensureValid() {
    if (invalidated) {
      // this triggers an AlreadyClosedException in ByteBufferIndexInput:
      throw new NullPointerException();
    }
  }

  // 执行任何操作前都要检测  invalidated 标识
  public void getBytes(ByteBuffer receiver, byte[] dst, int offset, int length) {
    ensureValid();
    receiver.get(dst, offset, length);
  }
  
  public byte getByte(ByteBuffer receiver) {
    ensureValid();
    return receiver.get();
  }
  
  public short getShort(ByteBuffer receiver) {
    ensureValid();
    return receiver.getShort();
  }
  
  public int getInt(ByteBuffer receiver) {
    ensureValid();
    return receiver.getInt();
  }
  
  public long getLong(ByteBuffer receiver) {
    ensureValid();
    return receiver.getLong();
  }
  
  public byte getByte(ByteBuffer receiver, int pos) {
    ensureValid();
    return receiver.get(pos);
  }
  
  public short getShort(ByteBuffer receiver, int pos) {
    ensureValid();
    return receiver.getShort(pos);
  }
  
  public int getInt(ByteBuffer receiver, int pos) {
    ensureValid();
    return receiver.getInt(pos);
  }
  
  public long getLong(ByteBuffer receiver, int pos) {
    ensureValid();
    return receiver.getLong(pos);
  }

  public void getLongs(LongBuffer receiver, long[] dst, int offset, int length) {
    ensureValid();
    receiver.get(dst, offset, length);
  }

}
