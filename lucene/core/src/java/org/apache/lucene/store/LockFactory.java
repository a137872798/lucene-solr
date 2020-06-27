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
 * <p>Base class for Locking implementation.  {@link Directory} uses
 * instances of this class to implement locking.</p>
 *
 * <p>Lucene uses {@link NativeFSLockFactory} by default for
 * {@link FSDirectory}-based index directories.</p>
 *
 * <p>Special care needs to be taken if you change the locking
 * implementation: First be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index. Be sure to do the LockFactory change on all Lucene
 * instances and clean up all leftover lock files before starting
 * the new configuration for the first time. Different implementations
 * can not work together!</p>
 *
 * <p>If you suspect that some LockFactory implementation is
 * not working properly in your environment, you can easily
 * test it by using {@link VerifyingLockFactory}, {@link
 * LockVerifyServer} and {@link LockStressTest}.</p>
 *
 * @see LockVerifyServer
 * @see LockStressTest
 * @see VerifyingLockFactory
 * 锁工厂  通过指定一个目录 和一个标明锁的名称 返回一个锁对象    注意锁的范围分为线程锁和进程锁    进程锁一般是基于对文件的原子操作来实现的  因为一般的操作系统都能够
 * 保证文件锁同一时间只有一个进程可以持有 所以能够实现进程隔离
 * 该对象在lucene的意义就是 同一时间只有一个IndexWriter可以往目录中写入索引    而如果在多线程中使用同一个IndexWriter(该writer已经获取了进程锁)  那么就需要借助JVM提供的线程锁
 * 来实现线程隔离 比如 synchronized 关键字 或者 juc.Lock 实现类
 */

public abstract class LockFactory {

  /**
   * Return a new obtained Lock instance identified by lockName.
   * @param lockName name of the lock to be created.
   * @throws LockObtainFailedException (optional specific exception) if the lock could
   *         not be obtained because it is currently held elsewhere.
   * @throws IOException if any i/o error occurs attempting to gain the lock
   * 抢占指定目录的锁
   */
  public abstract Lock obtainLock(Directory dir, String lockName) throws IOException;

}
