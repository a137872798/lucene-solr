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
 * Base class for file system based locking implementation.
 * This class is explicitly checking that the passed {@link Directory}
 * is an {@link FSDirectory}.
 * 基于文件系统的锁工厂  返回的对象具备抢占文件锁的能力  而操作系统基本上在文件锁上都是实现了进程级别的隔离的
 */
public abstract class FSLockFactory extends LockFactory {
  
  /** Returns the default locking implementation for this platform.
   * This method currently returns always {@link NativeFSLockFactory}.
   * 返回一个锁工厂的默认实现
   */
  public static final FSLockFactory getDefault() {
    return NativeFSLockFactory.INSTANCE;
  }

  /**
   * 基于文件锁的对象 要求目录必须是  文件系统目录
   * @param dir
   * @param lockName name of the lock to be created.
   * @return
   * @throws IOException
   */
  @Override
  public final Lock obtainLock(Directory dir, String lockName) throws IOException {
    if (!(dir instanceof FSDirectory)) {
      throw new UnsupportedOperationException(getClass().getSimpleName() + " can only be used with FSDirectory subclasses, got: " + dir);
    }
    return obtainFSLock((FSDirectory) dir, lockName);
  }
  
  /** 
   * Implement this method to obtain a lock for a FSDirectory instance. 
   * @throws IOException if the lock could not be obtained.
   */
  protected abstract Lock obtainFSLock(FSDirectory dir, String lockName) throws IOException;

}
