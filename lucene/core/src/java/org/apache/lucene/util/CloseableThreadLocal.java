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
 * @lucene.internal 该对象本身是为了解决 JDK threadLocal的弊端的 也就是对象必须要很久才能被释放 除非主动调用remove
 */
public class CloseableThreadLocal<T> implements Closeable {

    /**
     * 通过该对象维护的值 会被一个弱引用包裹   为什么要这么做   因为 JDK原生的 ThreadLocal 是将自己作为一个 WeakReference key  ，而value是存在内存泄漏问题的
     * 当调用remove ， set ， get 时 会间接检测当前是否有过期的 ThreadLocalMap.entry 这时才可能将之前的对象释放
     * 这里的解决策略是  将value 也是用weak 包裹 这样value在没有其他引用指向时就会自动的被回收了
     * 但是这里又有一个问题  那就是如果key还存在的时候 value却被回收了
     */
    private ThreadLocal<WeakReference<T>> t = new ThreadLocal<>();

    // Use a WeakHashMap so that if a Thread exits and is
    // GC'able, its entry may be removed:
    // 只要当前线程还存活 T 就无法被回收  同时weakHashMap也存在跟ThreadLocal 类似的问题 就是 value都是惰性删除 这样value的删除就依赖于weakHashMap的删除机制了
    // 一旦value被删除 那么 t也自然会被移除
    private Map<Thread, T> hardRefs = new WeakHashMap<>();

    // Increase this to decrease frequency of purging in get:
    // 类似与一个因子
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
        // 如果不使用额外的强引用关联它  那么刚好被 回收了 那么存进去的值也读取不到  就白存了
        WeakReference<T> weakRef = t.get();
        if (weakRef == null) {
            T iv = initialValue();
            if (iv != null) {
                // 将正常生成的值 通过弱引用包裹  确保能够使得value被自动回收   以及 通过weakHashMap强引用关联使得 value不会在不恰当的时机被回收
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

        t.set(new WeakReference<>(object));

        // 追加一个强引用   这里将当前线程与创建的变量绑定在一起 同时  key也是弱引用的  那么在没有其他引用 指向该变量时  只要创建变量的线程还存活 该变量也是不会被回收的
        // 直到创建该变量的线程被回收了  且外部也没有其他引用指向该变量时  该变量就会被真正回收 (实际上能够以正常方式获取到该变量的情况也不存在了 因为正常使用方式就是在
        // 创建该对象的线程下访问 ThreadLocal)
        // 该容器对象本身是会被并发访问的所以需要做同步处理
        synchronized (hardRefs) {
            // put会惰性触发删除
            hardRefs.put(Thread.currentThread(), object);
            maybePurge();
        }
    }

    /**
     * 代表多次获取值 所以要进行清理
     */
    private void maybePurge() {
        if (countUntilPurge.getAndDecrement() == 0) {
            purge();
        }
    }

    // Purge dead threads
    private void purge() {
        synchronized (hardRefs) {
            int stillAliveCount = 0;
            // 同时在迭代过程中 会间接的删除过期的键值对 释放value
            for (Iterator<Thread> it = hardRefs.keySet().iterator(); it.hasNext(); ) {
                // 如果强引用队列中存在已经失活的线程 那么主动移除它 这样对应的value 就可以 被GC回收
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

    @Override
    public void close() {
        // Clear the hard refs; then, the only remaining refs to
        // all values we were storing are weak (unless somewhere
        // else is still using them) and so GC may reclaim them:
        // 当清理强引用时 剩下的value 都被弱引用包裹 自然会被GC回收
        hardRefs = null;
        // Take care of the current thread right now; others will be
        // taken care of via the WeakReferences.
        // 执行close的线程本身还没有被回收 所以要手动移除 value
        if (t != null) {
            t.remove();
        }
        t = null;
    }
}
