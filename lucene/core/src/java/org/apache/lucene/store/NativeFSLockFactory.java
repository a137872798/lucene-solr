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


import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.util.IOUtils;

/**
 * <p>Implements {@link LockFactory} using native OS file
 * locks.  Note that because this LockFactory relies on
 * java.nio.* APIs for locking, any problems with those APIs
 * will cause locking to fail.  Specifically, on certain NFS
 * environments the java.nio.* locks will fail (the lock can
 * incorrectly be double acquired) whereas {@link
 * SimpleFSLockFactory} worked perfectly in those same
 * environments.  For NFS based access to an index, it's
 * recommended that you try {@link SimpleFSLockFactory}
 * first and work around the one limitation that a lock file
 * could be left when the JVM exits abnormally.</p>
 *
 * <p>The primary benefit of {@link NativeFSLockFactory} is
 * that locks (not the lock file itself) will be properly
 * removed (by the OS) if the JVM has an abnormal exit.</p>
 * 
 * <p>Note that, unlike {@link SimpleFSLockFactory}, the existence of
 * leftover lock files in the filesystem is fine because the OS
 * will free the locks held against these files even though the
 * files still remain. Lucene will never actively remove the lock
 * files, so although you see them, the index may not be locked.</p>
 *
 * <p>Special care needs to be taken if you change the locking
 * implementation: First be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index. Be sure to do the LockFactory change on all Lucene
 * instances and clean up all leftover lock files before starting
 * the new configuration for the first time. Different implementations
 * can not work together!</p>
 *
 * <p>If you suspect that this or any other LockFactory is
 * not working properly in your environment, you can easily
 * test it by using {@link VerifyingLockFactory}, {@link
 * LockVerifyServer} and {@link LockStressTest}.</p>
 * 
 * <p>This is a singleton, you have to use {@link #INSTANCE}.
 *
 * @see LockFactory
 * 可能文件锁本身还有多种实现 这里是基于本地文件锁实现
 * 该对象相比于  SimpleFSLockFactory   当JVM 异常退出时  操作系统会自动释放文件锁 这样其他进程(或者重启lucene应用)可以正常尝试抢占文件锁
 */

public final class NativeFSLockFactory extends FSLockFactory {
  
  /**
   * Singleton instance
   */
  public static final NativeFSLockFactory INSTANCE = new NativeFSLockFactory();

  /**
   * 静态属性 代表进程级别唯一
   * 记录当前哪些 path 被锁定   锁忘了释放怎么办 ???
   */
  private static final Set<String> LOCK_HELD = Collections.synchronizedSet(new HashSet<String>());

  private NativeFSLockFactory() {}

  /**
   * 根据将要被上锁的目录 以及锁的名字 创建一个锁对象
   * @param dir
   * @param lockName
   * @return
   * @throws IOException
   */
  @Override
  protected Lock obtainFSLock(FSDirectory dir, String lockName) throws IOException {
    // 获取对应的目录路径
    Path lockDir = dir.getDirectory();
    
    // Ensure that lockDir exists and is a directory.
    // note: this will fail if lockDir is a symlink
    // 确保当前目录已创建
    Files.createDirectories(lockDir);

    // 将锁名转换成一个路径对象   这个Path 就是标识已经上锁的意思
    Path lockFile = lockDir.resolve(lockName);

    IOException creationException = null;
    try {
      Files.createFile(lockFile);
    } catch (IOException ignore) {
      // we must create the file to have a truly canonical path.
      // if it's already created, we don't care. if it cant be created, it will fail below.
      creationException = ignore;
    }
    
    // fails if the lock file does not exist
    final Path realPath;
    try {
      // 代表创建文件失败了 判定此次上锁失败 抛出异常
      realPath = lockFile.toRealPath();
    } catch (IOException e) {
      // if we couldn't resolve the lock file, it might be because we couldn't create it.
      // so append any exception from createFile as a suppressed exception, in case its useful
      if (creationException != null) {
        e.addSuppressed(creationException);
      }
      throw e;
    }
    
    // used as a best-effort check, to see if the underlying file has changed
    // 获取当前锁文件的创建时间
    final FileTime creationTime = Files.readAttributes(realPath, BasicFileAttributes.class).creationTime();

    // 首次加入代表本lock对象上锁成功   这里只是获得JVM 级别的锁对象 并没有确保进程隔离
    if (LOCK_HELD.add(realPath.toString())) {
      FileChannel channel = null;
      FileLock lock = null;
      try {
        // 这里开始尝试获取进程级别的锁
        channel = FileChannel.open(realPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        lock = channel.tryLock();
        if (lock != null) {
          // 包装文件锁对象
          return new NativeFSLock(lock, channel, realPath, creationTime);
        } else {
          // 抢占进程锁失败
          throw new LockObtainFailedException("Lock held by another program: " + realPath);
        }
      } finally {
        if (lock == null) { // not successful - clear up and move out
          // 本线程 抢占失败的话 就没有必要维持通往该文件的管道了  (只是关闭管道 不会影响到已经获取到锁的进程)
          IOUtils.closeWhileHandlingException(channel); // TODO: addSuppressed
          // 在 Set中的锁 意味着 线程间抢占成功了  这里释放掉 之后其他线程才能有重新竞争锁的资格 否则在判断 .add() 的地方总是返回false
          clearLockHeld(realPath);  // clear LOCK_HELD last 
        }
      }
      // 代表JVM 级别的锁抢占失败(线程级别)
    } else {
      throw new LockObtainFailedException("Lock held by this virtual machine: " + realPath);
    }
  }

  /**
   * 该进程释放文件锁  这样其他进程还有抢占锁的权利
   * @param path
   * @throws IOException
   */
  private static final void clearLockHeld(Path path) throws IOException {
    boolean remove = LOCK_HELD.remove(path.toString());
    if (remove == false) {
      throw new AlreadyClosedException("Lock path was cleared but never marked as held: " + path);
    }
  }

  // TODO: kind of bogus we even pass channel:
  // FileLock has an accessor, but mockfs doesnt yet mock the locks, too scary atm.

  /**
   * 内部是文件锁对象  当某个线程获取到该 Lock对象时 代表已经实现了（进程&&线程）隔离了
   */
  static final class NativeFSLock extends Lock {
    /**
     * 文件锁 对象  维护引用便于在使用完锁后进行释放
     */
    final FileLock lock;
    final FileChannel channel;
    final Path path;
    final FileTime creationTime;
    volatile boolean closed;
    
    NativeFSLock(FileLock lock, FileChannel channel, Path path, FileTime creationTime) {
      this.lock = lock;
      this.channel = channel;
      this.path = path;
      this.creationTime = creationTime;
    }

    /**
     * 确保当前锁是否可用
     * @throws IOException
     */
    @Override
    public void ensureValid() throws IOException {
      if (closed) {
        throw new AlreadyClosedException("Lock instance already released: " + this);
      }
      // check we are still in the locks map (some debugger or something crazy didn't remove us)
      if (!LOCK_HELD.contains(path.toString())) {
        throw new AlreadyClosedException("Lock path unexpectedly cleared from map: " + this);
      }
      // check our lock wasn't invalidated.   锁对象本身具备一个 检测是否有效的方法
      // 操作系统的某些操作可能可以让文件锁强制失效
      if (!lock.isValid()) {
        throw new AlreadyClosedException("FileLock invalidated by an external force: " + this);
      }
      // try to validate the underlying file descriptor.
      // this will throw IOException if something is wrong.
      // 用于生成文件锁的文件 不应该写入数据
      long size = channel.size();
      if (size != 0) {
        throw new AlreadyClosedException("Unexpected lock file size: " + size + ", (lock=" + this + ")");
      }
      // try to validate the backing file name, that it still exists,
      // and has the same creation time as when we obtained the lock. 
      // if it differs, someone deleted our lock file (and we are ineffective)
      // 当创建时间发生了变化 代表在中途发生了某些不可控情况 (操作系统级别)
      FileTime ctime = Files.readAttributes(path, BasicFileAttributes.class).creationTime(); 
      if (!creationTime.equals(ctime)) {
        throw new AlreadyClosedException("Underlying file changed by an external force at " + ctime + ", (lock=" + this + ")");
      }
    }

    /**
     * 释放文件锁
     * @throws IOException
     */
    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        return;
      }
      // NOTE: we don't validate, as unlike SimpleFSLockFactory, we can't break others locks
      // first release the lock, then the channel
      try (FileChannel channel = this.channel;
           FileLock lock = this.lock) {
        assert lock != null;
        assert channel != null;
      } finally {
        closed = true;
        // 清除本进程内 抢占成功的标记  这样其他线程又具备抢占资格
        clearLockHeld(path);
      }
    }

    @Override
    public String toString() {
      return "NativeFSLock(path=" + path + ",impl=" + lock + ",creationTime=" + creationTime + ")"; 
    }
  }
}
