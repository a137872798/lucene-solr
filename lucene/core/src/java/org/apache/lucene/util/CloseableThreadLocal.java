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
package org.apache.lucene.util;


import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Java's builtin ThreadLocal has a serious flaw:
 * it can take an arbitrarily long amount of time to
 * dereference the things you had stored in it, even once the
 * ThreadLocal instance itself is no longer referenced.
 * This is because there is single, master map stored for
 * each thread, which all ThreadLocals share, and that
 * master map only periodically purges "stale" entries.
 * <p>
 * While not technically a memory leak, because eventually
 * the memory will be reclaimed, it can take a long time
 * and you can easily hit OutOfMemoryError because from the
 * GC's standpoint the stale entries are not reclaimable.
 * <p>
 * This class works around that, by only enrolling
 * WeakReference values into the ThreadLocal, and
 * separately holding a hard reference to each stored
 * value.  When you call {@link #close}, these hard
 * references are cleared and then GC is freely able to
 * reclaim space by objects stored in it.
 * <p>
 * We can not rely on {@link ThreadLocal#remove()} as it
 * only removes the value for the caller thread, whereas
 * {@link #close} takes care of all
 * threads.  You should not call {@link #close} until all
 * threads are done using the instance.
 *
 * @lucene.internal
 * 首先 JDK.ThreadLocal 如果主动调用了 remove() 是不会产生内存泄漏的
 * 但是没有手动调用的情况下 只有等待同线程的其他threadLocal 调用get()，set()时 间接清理 这样就可能会引发内存泄漏  使得大块的对象与thread的生命周期绑定
 *
 * 他这个对象的生命周期不是也绑定在线程上吗 ???
 * 不管了 反正它就是多一个 close方法 能够释放所有创建的value  并且调用close后 不应该再使用该对象了
 */
public class CloseableThreadLocal<T> implements Closeable {


    /**
     * 该对象作为一个连接对象   当t对应的threadLocal 被回收时 触发 value的置空 而value本身又被弱引用修饰  (其他对象可以监控到t被回收的动作)
     */
    private ThreadLocal<WeakReference<T>> t = new ThreadLocal<>();

    // Use a WeakHashMap so that if a Thread exits and is
    // GC'able, its entry may be removed:
    // 该对象本身会被并发访问 所以需要在同步块内调用
    // 这里为 监听了thread的回收动作    当thread被回收时 会惰性清除value
    private Map<Thread, T> hardRefs = new WeakHashMap<>();

    // Increase this to decrease frequency of purging in get:
    // 类似于一个因子
    private static int PURGE_MULTIPLIER = 20;

    // On each get or set we decrement this; when it hits 0 we
    // purge.  After purge, we set this to
    // PURGE_MULTIPLIER * stillAliveCount.  This keeps
    // amortized cost of purging linear.
    // 代表达到多少值后进行清洗
    private final AtomicInteger countUntilPurge = new AtomicInteger(PURGE_MULTIPLIER);

    protected T initialValue() {
        return null;
    }

    public T get() {
        WeakReference<T> weakRef = t.get();
        if (weakRef == null) {
            T iv = initialValue();
            if (iv != null) {
                // 将正常生成的值 通过弱引用包裹  确保能够使得value被自动回收
                // 通过 hardRerfs 将数值 与线程的生命周期绑定在一起
                set(iv);
                return iv;
            } else {
                return null;
            }
        } else {
            // 当从该线程变量获取到值时 判断是否需要清洗
            maybePurge();
            return weakRef.get();
        }
    }

    public void set(T object) {
        // 线程隔离的功能还是由 threadLocal 实现 但是写入的对象本身被弱引用包裹
        t.set(new WeakReference<>(object));

        synchronized (hardRefs) {
            // 该容器的作用是 只要线程还存在就能确保数据本身不被清除
            // 同时当线程本身被回收时 value的一个强引用会被释放  如果此时正好是最后一个强引用 那么object就会从t中被清除 避免了内存泄漏
            hardRefs.put(Thread.currentThread(), object);
            maybePurge();
        }
    }

    /**
     * 每当操作该对象多少次时 可能就会触发一次清理动作
     */
    private void maybePurge() {
        if (countUntilPurge.getAndDecrement() == 0) {
            purge();
        }
    }

    // Purge dead threads
    // 这里相当于给 JDK.ThreadLocal 增加了一个清除数据的触发点  也就是每当操作 n次时 进行一次清理
    private void purge() {
        synchronized (hardRefs) {
            int stillAliveCount = 0;
            // 在迭代器过程中 清除掉被释放的线程 以及绑定的强引用对象   进而回收在 t中被弱引用包裹的对象
            for (Iterator<Thread> it = hardRefs.keySet().iterator(); it.hasNext(); ) {
                // 如果强引用队列中存在已经失活的线程 那么主动移除它 会间接触发value的引用释放 就可以 被GC回收
                final Thread t = it.next();
                if (!t.isAlive()) {
                    it.remove();
                } else {
                    stillAliveCount++;
                }
            }
            int nextCount = (1 + stillAliveCount) * PURGE_MULTIPLIER;
            if (nextCount <= 0) {
                // defensive: int overflow!
                nextCount = 1000000;
            }

            countUntilPurge.set(nextCount);
        }
    }

    /**
     * 当某个线程调用该方法后 其他线程不应该再去操作它了
     */
    @Override
    public void close() {
        // Clear the hard refs; then, the only remaining refs to
        // all values we were storing are weak (unless somewhere
        // else is still using them) and so GC may reclaim them:
        // 该容器只会在本对象内被引用 一旦释放 map就会被回收  value的强引用也会释放  然后 t的弱引用也会释放
        hardRefs = null;
        // Take care of the current thread right now; others will be
        // taken care of via the WeakReferences.
        if (t != null) {
            t.remove();
        }
        t = null;
    }
}
