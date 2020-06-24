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

/**
 * A {@link RateLimiter rate limiting} {@link IndexOutput}
 * 
 * @lucene.internal
 * 对磁盘IO 做限流的 输出流
 */

public final class RateLimitedIndexOutput extends IndexOutput {

  /**
   * 负责干活的对象
   */
  private final IndexOutput delegate;
  /**
   * 限流器
   */
  private final RateLimiter rateLimiter;

  /** How many bytes we've written since we last called rateLimiter.pause. */
  // 记录在被限流前写入了多少数据
  private long bytesSinceLastPause;
  
  /** Cached here not not always have to call RateLimiter#getMinPauseCheckBytes()
   * which does volatile read. */
  // 每写入多少时检测一次
  private long currentMinPauseCheckBytes;

  public RateLimitedIndexOutput(final RateLimiter rateLimiter, final IndexOutput delegate) {
    super("RateLimitedIndexOutput(" + delegate + ")", delegate.getName());
    this.delegate = delegate;
    this.rateLimiter = rateLimiter;
    this.currentMinPauseCheckBytes = rateLimiter.getMinPauseCheckBytes();
  }
  
  @Override
  public void close() throws IOException {
    delegate.close();
  }

  /**
   * 获取当前文件写入的偏移量
   * @return
   */
  @Override
  public long getFilePointer() {
    return delegate.getFilePointer();
  }

  @Override
  public long getChecksum() throws IOException {
    return delegate.getChecksum();
  }

  /**
   * 在写入前 增加 lastPause 的值  并且判断是否 需要暂停
   * @param b
   * @throws IOException
   */
  @Override
  public void writeByte(byte b) throws IOException {
    bytesSinceLastPause++;
    checkRate();
    delegate.writeByte(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    bytesSinceLastPause += length;
    checkRate();
    delegate.writeBytes(b, offset, length);
  }
  
  private void checkRate() throws IOException {
    // 超过检查值 就调用 pause() 方法  该方法会按照当前情况判断是否需要限流
    if (bytesSinceLastPause > currentMinPauseCheckBytes) {
      rateLimiter.pause(bytesSinceLastPause);
      bytesSinceLastPause = 0;
      // 推荐每写入多少 检测一次
      currentMinPauseCheckBytes = rateLimiter.getMinPauseCheckBytes();
    }    
  }
}
