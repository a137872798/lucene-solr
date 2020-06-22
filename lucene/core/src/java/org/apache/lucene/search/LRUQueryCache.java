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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;

import static org.apache.lucene.util.RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;

/**
 * A {@link QueryCache} that evicts queries using a LRU (least-recently-used)
 * eviction policy in order to remain under a given maximum size and number of
 * bytes used.
 *
 * This class is thread-safe.
 *
 * Note that query eviction runs in linear time with the total number of
 * segments that have cache entries so this cache works best with
 * {@link QueryCachingPolicy caching policies} that only cache on "large"
 * segments, and it is advised to not share this cache across too many indices.
 *
 * A default query cache and policy instance is used in IndexSearcher. If you want to replace those defaults
 * it is typically done like this:
 * <pre class="prettyprint">
 *   final int maxNumberOfCachedQueries = 256;
 *   final long maxRamBytesUsed = 50 * 1024L * 1024L; // 50MB
 *   // these cache and policy instances can be shared across several queries and readers
 *   // it is fine to eg. store them into static variables
 *   final QueryCache queryCache = new LRUQueryCache(maxNumberOfCachedQueries, maxRamBytesUsed);
 *   final QueryCachingPolicy defaultCachingPolicy = new UsageTrackingQueryCachingPolicy();
 *   indexSearcher.setQueryCache(queryCache);
 *   indexSearcher.setQueryCachingPolicy(defaultCachingPolicy);
 * </pre>
 *
 * This cache exposes some global statistics ({@link #getHitCount() hit count},
 * {@link #getMissCount() miss count}, {@link #getCacheSize() number of cache
 * entries}, {@link #getCacheCount() total number of DocIdSets that have ever
 * been cached}, {@link #getEvictionCount() number of evicted entries}). In
 * case you would like to have more fine-grained statistics, such as per-index
 * or per-query-class statistics, it is possible to override various callbacks:
 * {@link #onHit}, {@link #onMiss},
 * {@link #onQueryCache}, {@link #onQueryEviction},
 * {@link #onDocIdSetCache}, {@link #onDocIdSetEviction} and {@link #onClear}.
 * It is better to not perform heavy computations in these methods though since
 * they are called synchronously and under a lock.
 *
 * @see QueryCachingPolicy
 * @lucene.experimental
 * 基于 LRU 的缓存对象
 */
public class LRUQueryCache implements QueryCache, Accountable {

  private final int maxSize;
  private final long maxRamBytesUsed;
  /**
   * 有关是否应当被缓存的 谓语对象
   */
  private final Predicate<LeafReaderContext> leavesToCache;
  // maps queries that are contained in the cache to a singleton so that this
  // cache does not store several copies of the same query
  private final Map<Query, Query> uniqueQueries;
  // The contract between this set and the per-leaf caches is that per-leaf caches
  // are only allowed to store sub-sets of the queries that are contained in
  // mostRecentlyUsedQueries. This is why write operations are performed under a lock
  private final Set<Query> mostRecentlyUsedQueries;
  private final Map<IndexReader.CacheKey, LeafCache> cache;
  private final ReentrantLock lock;
  private final float skipCacheFactor;

  // these variables are volatile so that we do not need to sync reads
  // but increments need to be performed under the lock
  private volatile long ramBytesUsed;
  private volatile long hitCount;
  private volatile long missCount;
  private volatile long cacheCount;
  private volatile long cacheSize;

  /**
   * Expert: Create a new instance that will cache at most <code>maxSize</code>
   * queries with at most <code>maxRamBytesUsed</code> bytes of memory, only on
   * leaves that satisfy {@code leavesToCache}.
   *
   * Also, clauses whose cost is {@code skipCacheFactor} times more than the cost of the top-level query
   * will not be cached in order to not slow down queries too much.
   */
  public LRUQueryCache(int maxSize, long maxRamBytesUsed,
                       Predicate<LeafReaderContext> leavesToCache, float skipCacheFactor) {
    this.maxSize = maxSize;
    this.maxRamBytesUsed = maxRamBytesUsed;
    this.leavesToCache = leavesToCache;
    if (skipCacheFactor >= 1 == false) { // NaN >= 1 evaluates false
      throw new IllegalArgumentException("skipCacheFactor must be no less than 1, get " + skipCacheFactor);
    }
    this.skipCacheFactor = skipCacheFactor;

    uniqueQueries = new LinkedHashMap<>(16, 0.75f, true);
    mostRecentlyUsedQueries = uniqueQueries.keySet();
    cache = new IdentityHashMap<>();
    lock = new ReentrantLock();
    ramBytesUsed = 0;
  }

  /**
   * Create a new instance that will cache at most <code>maxSize</code> queries
   * with at most <code>maxRamBytesUsed</code> bytes of memory. Queries will
   * only be cached on leaves that have more than 10k documents and have more
   * than 3% of the total number of documents in the index.
   * This should guarantee that all leaves from the upper
   * {@link TieredMergePolicy tier} will be cached while ensuring that at most
   * <code>33</code> leaves can make it to the cache (very likely less than 10 in
   * practice), which is useful for this implementation since some operations
   * perform in linear time with the number of cached leaves.
   * Only clauses whose cost is at most 100x the cost of the top-level query will
   * be cached in order to not hurt latency too much because of caching.
   */
  public LRUQueryCache(int maxSize, long maxRamBytesUsed) {
    this(maxSize, maxRamBytesUsed, new MinSegmentSizePredicate(10000, .03f), 250);
  }

  // pkg-private for testing   默认使用的缓存谓语 需要确保数据本身超过 一定值才需要缓存 并且要求该叶数据占上层数据的一定比重
  static class MinSegmentSizePredicate implements Predicate<LeafReaderContext> {
    private final int minSize;
    private final float minSizeRatio;

    MinSegmentSizePredicate(int minSize, float minSizeRatio) {
      this.minSize = minSize;
      this.minSizeRatio = minSizeRatio;
    }

    @Override
    public boolean test(LeafReaderContext context) {
      final int maxDoc = context.reader().maxDoc();
      if (maxDoc < minSize) {
        return false;
      }
      final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
      final float sizeRatio = (float) context.reader().maxDoc() / topLevelContext.reader().maxDoc();
      return sizeRatio >= minSizeRatio;
    }
  }

  /**
   * Expert: callback when there is a cache hit on a given query.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the query cache.
   * @see #onMiss
   * @lucene.experimental
   */
  protected void onHit(Object readerCoreKey, Query query) {
    assert lock.isHeldByCurrentThread();
    hitCount += 1;
  }

  /**
   * Expert: callback when there is a cache miss on a given query.
   * @see #onHit
   * @lucene.experimental
   */
  protected void onMiss(Object readerCoreKey, Query query) {
    assert lock.isHeldByCurrentThread();
    assert query != null;
    missCount += 1;
  }

  /**
   * Expert: callback when a query is added to this cache.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the query cache.
   * @see #onQueryEviction
   * @lucene.experimental
   */
  protected void onQueryCache(Query query, long ramBytesUsed) {
    assert lock.isHeldByCurrentThread();
    this.ramBytesUsed += ramBytesUsed;
  }

  /**
   * Expert: callback when a query is evicted from this cache.
   * @see #onQueryCache
   * @lucene.experimental
   */
  protected void onQueryEviction(Query query, long ramBytesUsed) {
    assert lock.isHeldByCurrentThread();
    this.ramBytesUsed -= ramBytesUsed;
  }

  /**
   * Expert: callback when a {@link DocIdSet} is added to this cache.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the query cache.
   * @see #onDocIdSetEviction
   * @lucene.experimental
   */
  protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
    assert lock.isHeldByCurrentThread();
    cacheSize += 1;
    cacheCount += 1;
    this.ramBytesUsed += ramBytesUsed;
  }

  /**
   * Expert: callback when one or more {@link DocIdSet}s are removed from this
   * cache.
   * @see #onDocIdSetCache
   * @lucene.experimental
   */
  protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
    assert lock.isHeldByCurrentThread();
    this.ramBytesUsed -= sumRamBytesUsed;
    cacheSize -= numEntries;
  }

  /**
   * Expert: callback when the cache is completely cleared.
   * @lucene.experimental
   */
  protected void onClear() {
    assert lock.isHeldByCurrentThread();
    ramBytesUsed = 0;
    cacheSize = 0;
  }

  /** Whether evictions are required. */
  boolean requiresEviction() {
    assert lock.isHeldByCurrentThread();
    final int size = mostRecentlyUsedQueries.size();
    if (size == 0) {
      return false;
    } else {
      // 当前 recently队列超过指定大小  或者使用bytes 数 超过指定大小
      return size > maxSize || ramBytesUsed() > maxRamBytesUsed;
    }
  }

  /**
   * 通过缓存对象 以及 helper 生成缓存数据集
   * @param key
   * @param cacheHelper
   * @return
   */
  DocIdSet get(Query key, IndexReader.CacheHelper cacheHelper) {
    assert lock.isHeldByCurrentThread();
    assert key instanceof BoostQuery == false;
    assert key instanceof ConstantScoreQuery == false;
    // 找到缓存键
    final IndexReader.CacheKey readerKey = cacheHelper.getKey();
    // 找到缓存对象
    final LeafCache leafCache = cache.get(readerKey);
    if (leafCache == null) {
      onMiss(readerKey, key);
      return null;
    }
    // this get call moves the query to the most-recently-used position
    // 这里做一层额外的映射
    final Query singleton = uniqueQueries.get(key);
    if (singleton == null) {
      onMiss(readerKey, key);
      return null;
    }
    // 找到数据集
    final DocIdSet cached = leafCache.get(singleton);
    if (cached == null) {
      onMiss(readerKey, singleton);
    } else {
      onHit(readerKey, singleton);
    }
    return cached;
  }

  private void putIfAbsent(Query query, DocIdSet set, IndexReader.CacheHelper cacheHelper) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;
    // under a lock to make sure that mostRecentlyUsedQueries and cache remain sync'ed
    lock.lock();
    try {
      Query singleton = uniqueQueries.putIfAbsent(query, query);
      if (singleton == null) {
        onQueryCache(query, LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + QUERY_DEFAULT_RAM_BYTES_USED);
      } else {
        query = singleton;
      }
      final IndexReader.CacheKey key = cacheHelper.getKey();
      LeafCache leafCache = cache.get(key);
      if (leafCache == null) {
        leafCache = new LeafCache(key);
        final LeafCache previous = cache.put(key, leafCache);
        ramBytesUsed += HASHTABLE_RAM_BYTES_PER_ENTRY;
        assert previous == null;
        // we just created a new leaf cache, need to register a close listener
        // 使用者可以主动关闭缓存对象 并通过监听器触发 该cache对象的clear方法
        cacheHelper.addClosedListener(this::clearCoreCacheKey);
      }
      leafCache.putIfAbsent(query, set);
      evictIfNecessary();
    } finally {
      lock.unlock();
    }
  }

  /**
   * 检测是否有数据需要清理
   */
  private void evictIfNecessary() {
    assert lock.isHeldByCurrentThread();
    // under a lock to make sure that mostRecentlyUsedQueries and cache keep sync'ed
    if (requiresEviction()) {

      // 清除 recently 内部的数据是无序的吗
      Iterator<Query> iterator = mostRecentlyUsedQueries.iterator();
      do {
        final Query query = iterator.next();
        final int size = mostRecentlyUsedQueries.size();
        iterator.remove();
        // put只要在锁中 应该不会出现在这种情况吧
        if (size == mostRecentlyUsedQueries.size()) {
          // size did not decrease, because the hash of the query changed since it has been
          // put into the cache
          throw new ConcurrentModificationException("Removal from the cache failed! This " +
              "is probably due to a query which has been modified after having been put into " +
              " the cache or a badly implemented clone(). Query class: [" + query.getClass() +
              "], query: [" + query + "]");
        }
        onEviction(query);
      } while (iterator.hasNext() && requiresEviction());
    }
  }

  /**
   * Remove all cache entries for the given core cache key.
   */
  public void clearCoreCacheKey(Object coreKey) {
    lock.lock();
    try {
      final LeafCache leafCache = cache.remove(coreKey);
      if (leafCache != null) {
        ramBytesUsed -= HASHTABLE_RAM_BYTES_PER_ENTRY;
        final int numEntries = leafCache.cache.size();
        if (numEntries > 0) {
          onDocIdSetEviction(coreKey, numEntries, leafCache.ramBytesUsed);
        } else {
          assert numEntries == 0;
          assert leafCache.ramBytesUsed == 0;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove all cache entries for the given query.
   */
  public void clearQuery(Query query) {
    lock.lock();
    try {
      final Query singleton = uniqueQueries.remove(query);
      if (singleton != null) {
        onEviction(singleton);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * 移除某个 query 对应的缓存数据
   * @param singleton
   */
  private void onEviction(Query singleton) {
    assert lock.isHeldByCurrentThread();
    onQueryEviction(singleton, LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + QUERY_DEFAULT_RAM_BYTES_USED);
    for (LeafCache leafCache : cache.values()) {
      leafCache.remove(singleton);
    }
  }

  /**
   * Clear the content of this cache.
   * 清空缓存内所有数据
   */
  public void clear() {
    lock.lock();
    try {
      cache.clear();
      // Note that this also clears the uniqueQueries map since mostRecentlyUsedQueries is the uniqueQueries.keySet view:
      mostRecentlyUsedQueries.clear();
      onClear();
    } finally {
      lock.unlock();
    }
  }

  // pkg-private for testing   检测一致性
  void assertConsistent() {
    lock.lock();
    try {
      if (requiresEviction()) {
        throw new AssertionError("requires evictions: size=" + mostRecentlyUsedQueries.size()
            + ", maxSize=" + maxSize + ", ramBytesUsed=" + ramBytesUsed() + ", maxRamBytesUsed=" + maxRamBytesUsed);
      }
      for (LeafCache leafCache : cache.values()) {
        Set<Query> keys = Collections.newSetFromMap(new IdentityHashMap<>());
        keys.addAll(leafCache.cache.keySet());
        keys.removeAll(mostRecentlyUsedQueries);
        if (!keys.isEmpty()) {
          throw new AssertionError("One leaf cache contains more keys than the top-level cache: " + keys);
        }
      }
      long recomputedRamBytesUsed =
            HASHTABLE_RAM_BYTES_PER_ENTRY * cache.size()
          + LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY * uniqueQueries.size();
      recomputedRamBytesUsed += mostRecentlyUsedQueries.size() * QUERY_DEFAULT_RAM_BYTES_USED;
      for (LeafCache leafCache : cache.values()) {
        recomputedRamBytesUsed += HASHTABLE_RAM_BYTES_PER_ENTRY * leafCache.cache.size();
        for (DocIdSet set : leafCache.cache.values()) {
          recomputedRamBytesUsed += set.ramBytesUsed();
        }
      }
      if (recomputedRamBytesUsed != ramBytesUsed) {
        throw new AssertionError("ramBytesUsed mismatch : " + ramBytesUsed + " != " + recomputedRamBytesUsed);
      }

      long recomputedCacheSize = 0;
      for (LeafCache leafCache : cache.values()) {
        recomputedCacheSize += leafCache.cache.size();
      }
      if (recomputedCacheSize != getCacheSize()) {
        throw new AssertionError("cacheSize mismatch : " + getCacheSize() + " != " + recomputedCacheSize);
      }
    } finally {
      lock.unlock();
    }
  }

  // pkg-private for testing
  // return the list of cached queries in LRU order
  List<Query> cachedQueries() {
    lock.lock();
    try {
      return new ArrayList<>(mostRecentlyUsedQueries);
    } finally {
      lock.unlock();
    }
  }

  /**
   * 将权重对象包装成一个缓存对象
   * @param weight
   * @param policy
   * @return
   */
  @Override
  public Weight doCache(Weight weight, QueryCachingPolicy policy) {
    while (weight instanceof CachingWrapperWeight) {
      weight = ((CachingWrapperWeight) weight).in;
    }

    return new CachingWrapperWeight(weight, policy);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    lock.lock();
    try {
      return Accountables.namedAccountables("segment", cache);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Default cache implementation: uses {@link RoaringDocIdSet} for sets that
   * have a density &lt; 1% and a {@link BitDocIdSet} over a {@link FixedBitSet}
   * otherwise.
   */
  protected DocIdSet cacheImpl(BulkScorer scorer, int maxDoc) throws IOException {
    if (scorer.cost() * 100 >= maxDoc) {
      // FixedBitSet is faster for dense sets and will enable the random-access
      // optimization in ConjunctionDISI
      // 该位图对象内部使用 FixBitSet
      return cacheIntoBitSet(scorer, maxDoc);
    } else {
      // 该位图对象内部使用 RoaringDocIdSet  按照多个不同的大小创建 short[] 或者 位图对象来存储数据
      // 使用short[] 读取docId
      // short 本身是 4096个  而一个block是 65535 比一下大概是 1:16  也就是最坏的情况 每2个docId 相距16位  而16位刚好就是一个short
      // 那么直接用short比较 占用的位数是差不多的 并且它的读取比位图更快  所以以该大小作为分界线  当然这是理想情况
      return cacheIntoRoaringDocIdSet(scorer, maxDoc);
    }
  }

  /**
   * 返回一个基于位图的存储对象
   * @param scorer
   * @param maxDoc
   * @return
   * @throws IOException
   */
  private static DocIdSet cacheIntoBitSet(BulkScorer scorer, int maxDoc) throws IOException {
    final FixedBitSet bitSet = new FixedBitSet(maxDoc);
    long cost[] = new long[1];
    // 这个动作应该就是将 docId读取到位图
    scorer.score(new LeafCollector() {

      @Override
      public void setScorer(Scorable scorer) throws IOException {}

      /**
       * 每当采集到docId 时 增加总数 以及填充到 bitSet中
       * @param doc
       * @throws IOException
       */
      @Override
      public void collect(int doc) throws IOException {
        cost[0]++;
        bitSet.set(doc);
      }

    }, null);
    return new BitDocIdSet(bitSet, cost[0]);
  }

  /**
   * 构建了一个特殊的位图 (RoaringBitMap)
   * @param scorer
   * @param maxDoc
   * @return
   * @throws IOException
   */
  private static DocIdSet cacheIntoRoaringDocIdSet(BulkScorer scorer, int maxDoc) throws IOException {
    RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
    scorer.score(new LeafCollector() {

      @Override
      public void setScorer(Scorable scorer) throws IOException {}

      @Override
      public void collect(int doc) throws IOException {
        builder.add(doc);
      }

    }, null);
    return builder.build();
  }

  /**
   * Return the total number of times that a {@link Query} has been looked up
   * in this {@link QueryCache}. Note that this number is incremented once per
   * segment so running a cached query only once will increment this counter
   * by the number of segments that are wrapped by the searcher.
   * Note that by definition, {@link #getTotalCount()} is the sum of
   * {@link #getHitCount()} and {@link #getMissCount()}.
   * @see #getHitCount()
   * @see #getMissCount()
   * 计算 缓存命中总数 + 未命中总数
   */
  public final long getTotalCount() {
    return getHitCount() + getMissCount();
  }

  /**
   * Over the {@link #getTotalCount() total} number of times that a query has
   * been looked up, return how many times a cached {@link DocIdSet} has been
   * found and returned.
   * @see #getTotalCount()
   * @see #getMissCount()
   */
  public final long getHitCount() {
    return hitCount;
  }

  /**
   * Over the {@link #getTotalCount() total} number of times that a query has
   * been looked up, return how many times this query was not contained in the
   * cache.
   * @see #getTotalCount()
   * @see #getHitCount()
   */
  public final long getMissCount() {
    return missCount;
  }

  /**
   * Return the total number of {@link DocIdSet}s which are currently stored
   * in the cache.
   * @see #getCacheCount()
   * @see #getEvictionCount()
   */
  public final long getCacheSize() {
    return cacheSize;
  }

  /**
   * Return the total number of cache entries that have been generated and put
   * in the cache. It is highly desirable to have a {@link #getHitCount() hit
   * count} that is much higher than the {@link #getCacheCount() cache count}
   * as the opposite would indicate that the query cache makes efforts in order
   * to cache queries but then they do not get reused.
   * @see #getCacheSize()
   * @see #getEvictionCount()
   */
  public final long getCacheCount() {
    return cacheCount;
  }

  /**
   * Return the number of cache entries that have been removed from the cache
   * either in order to stay under the maximum configured size/ram usage, or
   * because a segment has been closed. High numbers of evictions might mean
   * that queries are not reused or that the {@link QueryCachingPolicy
   * caching policy} caches too aggressively on NRT segments which get merged
   * early.
   * @see #getCacheCount()
   * @see #getCacheSize()
   * 超过的部分代表需要被移除
   */
  public final long getEvictionCount() {
    return getCacheCount() - getCacheSize();
  }

  // this class is not thread-safe, everything but ramBytesUsed needs to be called under a lock
  // 缓存对象
  private class LeafCache implements Accountable {

    private final Object key;
    /**
     * 相同的查询对象总是返回该组 docId
     */
    private final Map<Query, DocIdSet> cache;
    private volatile long ramBytesUsed;

    LeafCache(Object key) {
      this.key = key;
      cache = new IdentityHashMap<>();
      ramBytesUsed = 0;
    }

    private void onDocIdSetCache(long ramBytesUsed) {
      this.ramBytesUsed += ramBytesUsed;
      LRUQueryCache.this.onDocIdSetCache(key, ramBytesUsed);
    }

    private void onDocIdSetEviction(long ramBytesUsed) {
      this.ramBytesUsed -= ramBytesUsed;
      LRUQueryCache.this.onDocIdSetEviction(key, 1, ramBytesUsed);
    }

    DocIdSet get(Query query) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      return cache.get(query);
    }

    void putIfAbsent(Query query, DocIdSet set) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      if (cache.putIfAbsent(query, set) == null) {
        // the set was actually put
        onDocIdSetCache(HASHTABLE_RAM_BYTES_PER_ENTRY + set.ramBytesUsed());
      }
    }

    /**
     * 将某个 query下的数据移除
     * @param query
     */
    void remove(Query query) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      DocIdSet removed = cache.remove(query);
      if (removed != null) {
        onDocIdSetEviction(HASHTABLE_RAM_BYTES_PER_ENTRY + removed.ramBytesUsed());
      }
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }

  }

  /**
   * 某个权重对象的包装器
   */
  private class CachingWrapperWeight extends ConstantScoreWeight {

    /**
     * 内部真正的权重对象
     */
    private final Weight in;
    /**
     * 采用的缓存策略
     */
    private final QueryCachingPolicy policy;
    // we use an AtomicBoolean because Weight.scorer may be called from multiple
    // threads when IndexSearcher is created with threads
    // 该对象本身可能会被并发访问
    private final AtomicBoolean used;

    CachingWrapperWeight(Weight in, QueryCachingPolicy policy) {
      super(in.getQuery(), 1f);
      this.in = in;
      this.policy = policy;
      used = new AtomicBoolean(false);
    }

    /**
     * 判断使用该doc 能否匹配成功
     * @param context the reader's context to create the {@link Matches} for
     * @param doc     the document's id relative to the given context's reader
     * @return
     * @throws IOException
     */
    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      return in.matches(context, doc);
    }

    /**
     * 传入的是 上层context的maxDoc
     * @param maxDoc
     * @return
     */
    private boolean cacheEntryHasReasonableWorstCaseSize(int maxDoc) {
      // The worst-case (dense) is a bit set which needs one bit per document
      // 这里是理想情况 就是每一位都可以填充一个 docId
      final long worstCaseRamUsage = maxDoc / 8;
      final long totalRamAvailable = maxRamBytesUsed;
      // Imagine the worst-case that a cache entry is large than the size of
      // the cache: not only will this entry be trashed immediately but it
      // will also evict all current entries from the cache. For this reason
      // we only cache on an IndexReader if we have available room for
      // 5 different filters on this reader to avoid excessive trashing
      // 如果每个maxDoc 相间5位  并且没有超过缓存的可使用大小 那么允许使用缓存
      return worstCaseRamUsage * 5 < totalRamAvailable;
    }

    /**
     * 为当前segment 对应的数据体建立缓存
     * @param context
     * @return
     * @throws IOException
     */
    private DocIdSet cache(LeafReaderContext context) throws IOException {
      // 该对象在处理后 会将数据读取到 scorer中 在下面的cacheImpl中 会将数据转移到缓存中
      final BulkScorer scorer = in.bulkScorer(context);
      if (scorer == null) {
        return DocIdSet.EMPTY;
      } else {
        return cacheImpl(scorer, context.reader().maxDoc());
      }
    }

    /** Check whether this segment is eligible for caching, regardless of the query. */
    // 检测该数据是否应该被缓存
    private boolean shouldCache(LeafReaderContext context) throws IOException {
      return cacheEntryHasReasonableWorstCaseSize(ReaderUtil.getTopLevelContext(context).reader().maxDoc())
          && leavesToCache.test(context);
    }

    /**
     * 基于当前数据叶 生成打分对象
     * @param context
     * @return
     * @throws IOException
     */
    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      if (used.compareAndSet(false, true)) {
        // 从 weight中找到 query对象  默认的policy实现是 UsageTrackingQueryCachingPolicy  就是在使用前 记录了一下使用痕迹
        policy.onUse(getQuery());
      }

      // 如果当前对象被判断不需要缓存  那么使用原始对象生成 score
      if (in.isCacheable(context) == false) {
        // this segment is not suitable for caching
        return in.scorerSupplier(context);
      }

      // Short-circuit: Check whether this segment is eligible for caching
      // before we take a lock because of #get
      // 不支持缓存的情况直接返回 打分对象
      if (shouldCache(context) == false) {
        return in.scorerSupplier(context);
      }

      // 如果不包含helper对象 不生成缓存
      final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
      if (cacheHelper == null) {
        // this reader has no cache helper
        return in.scorerSupplier(context);
      }

      // If the lock is already busy, prefer using the uncached version than waiting
      // 当前生成缓存的竞争严重就放弃生成缓存包装
      if (lock.tryLock() == false) {
        return in.scorerSupplier(context);
      }

      // 下面是生成缓存的逻辑
      DocIdSet docIdSet;
      try {
        // 找到缓存的数据集
        docIdSet = get(in.getQuery(), cacheHelper);
      } finally {
        lock.unlock();
      }

      // 当没有找到缓存时
      if (docIdSet == null) {
        // 检测是否应当进行缓存   里面还会关注该 query的使用次数  当使用次数比较少的时候 不会生成缓存
        if (policy.shouldCache(in.getQuery())) {
          // 找到原始的 supplier
          final ScorerSupplier supplier = in.scorerSupplier(context);
          if (supplier == null) {
            // 这里缓存一个空数据 并返回null
            putIfAbsent(in.getQuery(), DocIdSet.EMPTY, cacheHelper);
            return null;
          }

          final long cost = supplier.cost();
          // 这里包装原始对象
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
              // skip cache operation which would slow query down too much
              if (cost / skipCacheFactor > leadCost) {
                return supplier.get(leadCost);
              }

              // -------------  这些步骤就是被省略的 ------------- //
              Scorer scorer = supplier.get(Long.MAX_VALUE);
              // 使用 scorer读取 reader内部的数据 然后填充到 docIdSet中
              DocIdSet docIdSet = cacheImpl(new DefaultBulkScorer(scorer), context.reader().maxDoc());
              // 缓存结果集  并且 使用者可以通过 helper对象 主动释放缓存
              putIfAbsent(in.getQuery(), docIdSet, cacheHelper);
              DocIdSetIterator disi = docIdSet.iterator();
              if (disi == null) {
                // docIdSet.iterator() is allowed to return null when empty but we want a non-null iterator here
                disi = DocIdSetIterator.empty();
              }

              // ------------------------------------------------- //

              // 生成一个常量打分对象 将docId结果 固化在对象内部   应该是这样 能够被缓存的对象首先本身就不具备打分功能
              // 而需要返回的又是一个scorer对象 所以这里直接指定score为0 同时将本次涉及到的docId设置进去
              return new ConstantScoreScorer(CachingWrapperWeight.this, 0f, ScoreMode.COMPLETE_NO_SCORES, disi);
            }

            @Override
            public long cost() {
              return cost;
            }
          };
        } else {
          // 不需要生成缓存时直接返回
          return in.scorerSupplier(context);
        }
      }

      // 只有当首次调用  scorerSupplier 才会返回 ConstantScoreScorer  之后调用 可以获取到之前缓存的 docIdSet
      assert docIdSet != null;
      // 为什么这里就允许返回null 了
      if (docIdSet == DocIdSet.EMPTY) {
        return null;
      }
      final DocIdSetIterator disi = docIdSet.iterator();
      if (disi == null) {
        return null;
      }

      // 当存在缓存的时候 直接返回 scorer对象
      return new ScorerSupplier() {
        @Override
        public Scorer get(long LeadCost) throws IOException {
          return new ConstantScoreScorer(CachingWrapperWeight.this, 0f, ScoreMode.COMPLETE_NO_SCORES, disi);
        }

        @Override
        public long cost() {
          return disi.cost();
        }
      };

    }

    /**
     * 基于当前的叶数据 生成打分对象
     * @param context
     *          the {@link org.apache.lucene.index.LeafReaderContext} for which to return the {@link Scorer}.
     *
     * @return
     * @throws IOException
     */
    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      ScorerSupplier scorerSupplier = scorerSupplier(context);
      if (scorerSupplier == null) {
        return null;
      }
      return scorerSupplier.get(Long.MAX_VALUE);
    }

    /**
     * 检测是否支持缓存
     * @param ctx
     * @return
     */
    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return in.isCacheable(ctx);
    }

    /**
     * 生成 bulk打分对象 (什么是大块的打分对象???)  下面的步骤 和 scorerSupplier 类似
     * @param context
     *          the {@link org.apache.lucene.index.LeafReaderContext} for which to return the {@link Scorer}.
     *
     * @return
     * @throws IOException
     */
    @Override
    public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
      if (used.compareAndSet(false, true)) {
        policy.onUse(getQuery());
      }

      if (in.isCacheable(context) == false) {
        // this segment is not suitable for caching
        return in.bulkScorer(context);
      }

      // Short-circuit: Check whether this segment is eligible for caching
      // before we take a lock because of #get
      if (shouldCache(context) == false) {
        return in.bulkScorer(context);
      }

      final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
      if (cacheHelper == null) {
        // this reader has no cacheHelper
        return in.bulkScorer(context);
      }

      // If the lock is already busy, prefer using the uncached version than waiting
      if (lock.tryLock() == false) {
        return in.bulkScorer(context);
      }

      DocIdSet docIdSet;
      try {
        docIdSet = get(in.getQuery(), cacheHelper);
      } finally {
        lock.unlock();
      }

      if (docIdSet == null) {
        if (policy.shouldCache(in.getQuery())) {
          docIdSet = cache(context);
          putIfAbsent(in.getQuery(), docIdSet, cacheHelper);
        } else {
          return in.bulkScorer(context);
        }
      }

      assert docIdSet != null;
      if (docIdSet == DocIdSet.EMPTY) {
        return null;
      }
      final DocIdSetIterator disi = docIdSet.iterator();
      if (disi == null) {
        return null;
      }

      return new DefaultBulkScorer(new ConstantScoreScorer(this, 0f, ScoreMode.COMPLETE_NO_SCORES, disi));
    }

  }
}
