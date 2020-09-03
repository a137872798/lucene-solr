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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

/** Implements search over a single IndexReader.
 *
 * <p>Applications usually need only call the inherited
 * {@link #search(Query,int)} method. For
 * performance reasons, if your index is unchanging, you
 * should share a single IndexSearcher instance across
 * multiple searches instead of creating a new one
 * per-search.  If your index has changed and you wish to
 * see the changes reflected in searching, you should
 * use {@link DirectoryReader#openIfChanged(DirectoryReader)}
 * to obtain a new reader and
 * then create a new IndexSearcher from that.  Also, for
 * low-latency turnaround it's best to use a near-real-time
 * reader ({@link DirectoryReader#open(IndexWriter)}).
 * Once you have a new {@link IndexReader}, it's relatively
 * cheap to create a new IndexSearcher from it.
 *
 * <p><b>NOTE</b>: The {@link #search} and {@link #searchAfter} methods are
 * configured to only count top hits accurately up to {@code 1,000} and may
 * return a {@link TotalHits.Relation lower bound} of the hit count if the
 * hit count is greater than or equal to {@code 1,000}. On queries that match
 * lots of documents, counting the number of hits may take much longer than
 * computing the top hits so this trade-off allows to get some minimal
 * information about the hit count without slowing down search too much. The
 * {@link TopDocs#scoreDocs} array is always accurate however. If this behavior
 * doesn't suit your needs, you should create collectors manually with either
 * {@link TopScoreDocCollector#create} or {@link TopFieldCollector#create} and
 * call {@link #search(Query, Collector)}.
 *
 * <a id="thread-safety"></a><p><b>NOTE</b>: <code>{@link
 * IndexSearcher}</code> instances are completely
 * thread safe, meaning multiple threads can call any of its
 * methods, concurrently.  If your application requires
 * external synchronization, you should <b>not</b>
 * synchronize on the <code>IndexSearcher</code> instance;
 * use your own (non-Lucene) objects instead.</p>
 * 该对象负责从索引中查询数据
 */
public class IndexSearcher {

  /**
   * 组合条件数量  某些query类的查询条件是可以组合的  这个是组合数量的上限
   */
  static int maxClauseCount = 1024;

  /**
   * 结果缓存对象
   */
  private static QueryCache DEFAULT_QUERY_CACHE;
  /**
   * 该对象负责判断 某个query 是否具备缓存的条件
   */
  private static QueryCachingPolicy DEFAULT_CACHING_POLICY = new UsageTrackingQueryCachingPolicy();
  static {
    // 最多仅允许缓存100个query的结果
    final int maxCachedQueries = 1000;
    // min of 32MB or 5% of the heap size
    final long maxRamBytesUsed = Math.min(1L << 25, Runtime.getRuntime().maxMemory() / 20);
    DEFAULT_QUERY_CACHE = new LRUQueryCache(maxCachedQueries, maxRamBytesUsed);
  }
  /**
   * By default we count hits accurately up to 1000. This makes sure that we
   * don't spend most time on computing hit counts
   */
  private static final int TOTAL_HITS_THRESHOLD = 1000;

  /**
   * Thresholds for index slice allocation logic. To change the default, extend
   * <code> IndexSearcher</code> and use custom values
   */
  private static final int MAX_DOCS_PER_SLICE = 250_000;
  private static final int MAX_SEGMENTS_PER_SLICE = 5;

  final IndexReader reader; // package private for testing!
  
  // NOTE: these members might change in incompatible ways
  // in the next release
  protected final IndexReaderContext readerContext;
  protected final List<LeafReaderContext> leafContexts;

  /** used with executor - each slice holds a set of leafs executed within one thread */
  private final LeafSlice[] leafSlices;

  // These are only used for multi-threaded search
  private final Executor executor;

  // Used internally for load balancing threads executing for the query
  private final SliceExecutor sliceExecutor;

  // the default Similarity   打分器对象 获得的doc结果集会按照该对象计算分数
  private static final Similarity defaultSimilarity = new BM25Similarity();

  private QueryCache queryCache = DEFAULT_QUERY_CACHE;
  private QueryCachingPolicy queryCachingPolicy = DEFAULT_CACHING_POLICY;

  /**
   * Expert: returns a default Similarity instance.
   * In general, this method is only called to initialize searchers and writers.
   * User code and query implementations should respect
   * {@link IndexSearcher#getSimilarity()}.
   * @lucene.internal
   */
  public static Similarity getDefaultSimilarity() {
    return defaultSimilarity;
  }

  /**
   * Expert: Get the default {@link QueryCache} or {@code null} if the cache is disabled.
   * @lucene.internal
   */
  public static QueryCache getDefaultQueryCache() {
    return DEFAULT_QUERY_CACHE;
  }

  /**
   * Expert: set the default {@link QueryCache} instance.
   * @lucene.internal
   */
  public static void setDefaultQueryCache(QueryCache defaultQueryCache) {
    DEFAULT_QUERY_CACHE = defaultQueryCache;
  }

  /**
   * Expert: Get the default {@link QueryCachingPolicy}.
   * @lucene.internal
   */
  public static QueryCachingPolicy getDefaultQueryCachingPolicy() {
    return DEFAULT_CACHING_POLICY;
  }

  /**
   * Expert: set the default {@link QueryCachingPolicy} instance.
   * @lucene.internal
   */
  public static void setDefaultQueryCachingPolicy(QueryCachingPolicy defaultQueryCachingPolicy) {
    DEFAULT_CACHING_POLICY = defaultQueryCachingPolicy;
  }

  /** The Similarity implementation used by this searcher. */
  private Similarity similarity = defaultSimilarity;

  /**
   * Creates a searcher searching the provided index.
   * @param r 该对象基于一个读取索引文件的reader对象初始化  可能是一个组合对象 也可能只是针对单个段的读取对象
   * */
  public IndexSearcher(IndexReader r) {
    this(r, null);
  }

  /** Runs searches for each segment separately, using the
   *  provided Executor. NOTE:
   *  if you are using {@link NIOFSDirectory}, do not use
   *  the shutdownNow method of ExecutorService as this uses
   *  Thread.interrupt under-the-hood which can silently
   *  close file descriptors (see <a
   *  href="https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   * 
   * @lucene.experimental */
  public IndexSearcher(IndexReader r, Executor executor) {
    this(r.getContext(), executor);
  }

  /**
   * Creates a searcher searching the provided top-level {@link IndexReaderContext}.
   * <p>
   * Given a non-<code>null</code> {@link Executor} this method runs
   * searches for each segment separately, using the provided Executor.
   * NOTE: if you are using {@link NIOFSDirectory}, do not use the shutdownNow method of
   * ExecutorService as this uses Thread.interrupt under-the-hood which can
   * silently close file descriptors (see <a
   * href="https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   * 
   * @see IndexReaderContext
   * @see IndexReader#getContext()
   * @lucene.experimental
   * @param context  该reader相关的上下文
   * @param executor 支持并发查询    (机械硬盘的话 并发读取也没有优势啊 必须要固态硬盘)
   */
  public IndexSearcher(IndexReaderContext context, Executor executor) {
    this(context, executor, getSliceExecutionControlPlane(executor));
  }

  /**
   *
   * @param context
   * @param executor
   * @param sliceExecutor  通过包装线程池对象生成的  线程池分片  会将部分任务交由线程池执行 部分任务由本线程完成
   */
  IndexSearcher(IndexReaderContext context, Executor executor, SliceExecutor sliceExecutor) {
    assert context.isTopLevel: "IndexSearcher's ReaderContext must be topLevel for reader" + context.reader();
    assert (sliceExecutor == null) == (executor==null);

    reader = context.reader();
    this.executor = executor;
    this.sliceExecutor = sliceExecutor;
    this.readerContext = context;
    // 如果是 compositeReader 那么 leaves 就是叶子节点  如果是 LeafReader 那么leaves 就是自身
    leafContexts = context.leaves();
    // 如果设置了线程池  代表可以并行执行 leafReader的查询功能  这里就创建了一组对应的 LeafSlice
    this.leafSlices = executor == null ? null : slices(leafContexts);
  }

  /**
   * Creates a searcher searching the provided top-level {@link IndexReaderContext}.
   *
   * @see IndexReaderContext
   * @see IndexReader#getContext()
   * @lucene.experimental
   * 支持直接通过上下文对象进行初始化
   */
  public IndexSearcher(IndexReaderContext context) {
    this(context, null);
  }

  /** Return the maximum number of clauses permitted, 1024 by default.
   * Attempts to add more than the permitted number of clauses cause {@link
   * TooManyClauses} to be thrown.
   * @see #setMaxClauseCount(int)
   */
  public static int getMaxClauseCount() { return maxClauseCount; }

  /**
   * Set the maximum number of clauses permitted per Query.
   * Default value is 1024.
   */
  public static void setMaxClauseCount(int value)  {
    if (value < 1) {
      throw new IllegalArgumentException("maxClauseCount must be >= 1");
    }
    maxClauseCount = value;
  }

  /**
   * Set the {@link QueryCache} to use when scores are not needed.
   * A value of {@code null} indicates that query matches should never be
   * cached. This method should be called <b>before</b> starting using this
   * {@link IndexSearcher}.
   * <p>NOTE: When using a query cache, queries should not be modified after
   * they have been passed to IndexSearcher.
   * @see QueryCache
   * @lucene.experimental
   */
  public void setQueryCache(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  /**
   * Return the query cache of this {@link IndexSearcher}. This will be either
   * the {@link #getDefaultQueryCache() default query cache} or the query cache
   * that was last set through {@link #setQueryCache(QueryCache)}. A return
   * value of {@code null} indicates that caching is disabled.
   * @lucene.experimental
   */
  public QueryCache getQueryCache() {
    return queryCache;
  }

  /**
   * Set the {@link QueryCachingPolicy} to use for query caching.
   * This method should be called <b>before</b> starting using this
   * {@link IndexSearcher}.
   * @see QueryCachingPolicy
   * @lucene.experimental
   */
  public void setQueryCachingPolicy(QueryCachingPolicy queryCachingPolicy) {
    this.queryCachingPolicy = Objects.requireNonNull(queryCachingPolicy);
  }

  /**
   * Return the query cache of this {@link IndexSearcher}. This will be either
   * the {@link #getDefaultQueryCachingPolicy() default policy} or the policy
   * that was last set through {@link #setQueryCachingPolicy(QueryCachingPolicy)}.
   * @lucene.experimental
   */
  public QueryCachingPolicy getQueryCachingPolicy() {
    return queryCachingPolicy;
  }

  /**
   * Expert: Creates an array of leaf slices each holding a subset of the given leaves.
   * Each {@link LeafSlice} is executed in a single thread. By default, segments with more than
   * MAX_DOCS_PER_SLICE will get their own thread
   * 为一组叶子节点创建分片
   */
  protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
    return slices(leaves, MAX_DOCS_PER_SLICE, MAX_SEGMENTS_PER_SLICE);
  }

  /**
   * Static method to segregate LeafReaderContexts amongst multiple slices
   * @param maxDocsPerSlice  每个分片最多只能存储这么多doc
   * @param maxSegmentsPerSlice 每个分片最多允许的 segment数量   也就是slice 与 segment 并不是一一对应的
   */
  public static LeafSlice[] slices (List<LeafReaderContext> leaves, int maxDocsPerSlice,
                                    int maxSegmentsPerSlice) {
    // Make a copy so we can sort:
    List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);

    // Sort by maxDoc, descending:
    // 按照doc 数量 倒序排列
    Collections.sort(sortedLeaves,
        Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

    // 第一层就是按照slice 划分的  第二层就是按照 segment(reader) 划分的
    final List<List<LeafReaderContext>> groupedLeaves = new ArrayList<>();
    long docSum = 0;
    List<LeafReaderContext> group = null;
    for (LeafReaderContext ctx : sortedLeaves) {
      if (ctx.reader().maxDoc() > maxDocsPerSlice) {
        assert group == null;
        groupedLeaves.add(Collections.singletonList(ctx));
      } else {
        // 只要group没有被置空 就代表之前还没有达到 maxDocsPerSlice 的限制
        if (group == null) {
          group = new ArrayList<>();
          group.add(ctx);

          groupedLeaves.add(group);
        } else {
          group.add(ctx);
        }

        // 达到限制时 重置相关引用
        docSum += ctx.reader().maxDoc();
        if (group.size() >= maxSegmentsPerSlice || docSum > maxDocsPerSlice) {
          group = null;
          docSum = 0;
        }
      }
    }

    LeafSlice[] slices = new LeafSlice[groupedLeaves.size()];
    int upto = 0;
    // 将以segment为维度划分的list 进行合并
    for (List<LeafReaderContext> currentLeaf : groupedLeaves) {
      slices[upto] = new LeafSlice(currentLeaf);
      ++upto;
    }

    return slices;
  }

  /** Return the {@link IndexReader} this searches. */
  public IndexReader getIndexReader() {
    return reader;
  }

  /** 
   * Sugar for <code>.getIndexReader().document(docID)</code> 
   * @see IndexReader#document(int)
   * 通过指定docId 查询对应的文档   这个docId 是一个 全局性的id 比如reader是CompositeReader 那么就要先通过docId 定位到具体的leafReader 之后再读取
   * 注意这里是明确直到了 docId 是多少后再去查询  并且只能读取到 stored为true的 field   (从索引文件中将他们还原出来 并填写到一个doc中)
   * 但是lucene的精髓肯定还是按照 query来查找
   */
  public Document doc(int docID) throws IOException {
    return reader.document(docID);
  }

  /** 
   * Sugar for <code>.getIndexReader().document(docID, fieldVisitor)</code>
   * @see IndexReader#document(int, StoredFieldVisitor)
   * 通过外部直接指定visitor读取field 并生成doc  用户需要自己调用 visitor.document 得到还原的doc
   */
  public void doc(int docID, StoredFieldVisitor fieldVisitor) throws IOException {
    reader.document(docID, fieldVisitor);
  }

  /** 
   * Sugar for <code>.getIndexReader().document(docID, fieldsToLoad)</code>
   * @see IndexReader#document(int, Set)
   * @param fieldsToLoad  代表只允许加载这些field信息  其余信息即使存储在doc中 且 stored为true 也不会被处理
   */
  public Document doc(int docID, Set<String> fieldsToLoad) throws IOException {
    return reader.document(docID, fieldsToLoad);
  }

  /** Expert: Set the Similarity implementation used by this IndexSearcher.
   *
   */
  public void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
  }

  /** Expert: Get the {@link Similarity} to use to compute scores. This returns the
   *  {@link Similarity} that has been set through {@link #setSimilarity(Similarity)}
   *  or the default {@link Similarity} if none has been set explicitly. */
  public Similarity getSimilarity() {
    return similarity;
  }

  /**
   * Count how many documents match the given query.
   * 返回query命中的doc 总数
   */
  public int count(Query query) throws IOException {
    // 在处理query前需要进行重写  该方法内部同时还会对 query的 clause数量做检测 如果超过maxClauseCount  则会抛出异常
    query = rewrite(query);
    while (true) {
      // remove wrappers that don't matter for counts
      if (query instanceof ConstantScoreQuery) {
        query = ((ConstantScoreQuery) query).getQuery();
      } else {
        break;
      }
    }

    // some counts can be computed in constant time
    if (query instanceof MatchAllDocsQuery) {
      // 如果所有doc 都满足条件 那么根据当前 reader的情况返回doc数量 如果是 LeafReader 返回单个segment的docNum 如果是 CompositeReader 返回的就是所有关联segment的docNum总和
      return reader.numDocs();
      // 如果此时 doc 还全部存活
    } else if (query instanceof TermQuery && reader.hasDeletions() == false) {
      Term term = ((TermQuery) query).getTerm();
      int count = 0;
      // 在每个 leafReader中 都按照term查询docFreq 并进行累加    docFreq 是在 解析doc时计算的
      for (LeafReaderContext leaf : reader.leaves()) {
        count += leaf.reader().docFreq(term);
      }
      return count;
    }

    // general case: create a collector and count matches
    // 在查询时应该还要考虑已经非存活的doc 不能被查询出来
    // CollectorManager 负责创建 collector 以及将多个collector的数据进行整合
    final CollectorManager<TotalHitCountCollector, Integer> collectorManager = new CollectorManager<TotalHitCountCollector, Integer>() {

      /**
       * TotalHitCountCollector 在查询到doc时 会累加 命中的数量
       * @return
       * @throws IOException
       */
      @Override
      public TotalHitCountCollector newCollector() throws IOException {
        return new TotalHitCountCollector();
      }

      /**
       * 将每个子 Collector获得的结果进行累加
       * @param collectors
       * @return
       * @throws IOException
       */
      @Override
      public Integer reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
        int total = 0;
        for (TotalHitCountCollector collector : collectors) {
          total += collector.getTotalHits();
        }
        return total;
      }

    };
    return search(query, collectorManager);
  }

  /** Returns the leaf slices used for concurrent searching, or null if no {@code Executor} was
   *  passed to the constructor.
   *
   * @lucene.experimental */
  public LeafSlice[] getSlices() {
      return leafSlices;
  }
  
  /** Finds the top <code>n</code>
   * hits for <code>query</code> where all results are after a previous 
   * result (<code>after</code>).
   * <p>
   * By passing the bottom result from a previous page as <code>after</code>,
   * this method can be used for efficient 'deep-paging' across potentially
   * large result sets.
   *
   * @throws TooManyClauses If a query would exceed
   *         {@link IndexSearcher#getMaxClauseCount()} clauses.
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, int numHits) throws IOException {
    final int limit = Math.max(1, reader.maxDoc());
    if (after != null && after.doc >= limit) {
      throw new IllegalArgumentException("after.doc exceeds the number of documents in the reader: after.doc="
          + after.doc + " limit=" + limit);
    }

    final int cappedNumHits = Math.min(numHits, limit);

    final CollectorManager<TopScoreDocCollector, TopDocs> manager = new CollectorManager<TopScoreDocCollector, TopDocs>() {

      private final HitsThresholdChecker hitsThresholdChecker = (executor == null || leafSlices.length <= 1) ? HitsThresholdChecker.create(Math.max(TOTAL_HITS_THRESHOLD, numHits)) :
          HitsThresholdChecker.createShared(Math.max(TOTAL_HITS_THRESHOLD, numHits));

      private final MaxScoreAccumulator minScoreAcc = (executor == null || leafSlices.length <= 1) ? null : new MaxScoreAccumulator();

      @Override
      public TopScoreDocCollector newCollector() throws IOException {
        return TopScoreDocCollector.create(cappedNumHits, after, hitsThresholdChecker, minScoreAcc);
      }

      @Override
      public TopDocs reduce(Collection<TopScoreDocCollector> collectors) throws IOException {
        final TopDocs[] topDocs = new TopDocs[collectors.size()];
        int i = 0;
        for (TopScoreDocCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(0, cappedNumHits, topDocs);
      }

    };

    return search(query, manager);
  }

  /** Finds the top <code>n</code>
   * hits for <code>query</code>.
   *
   * @throws TooManyClauses If a query would exceed
   *         {@link IndexSearcher#getMaxClauseCount()} clauses.
   */
  public TopDocs search(Query query, int n)
    throws IOException {
    return searchAfter(null, query, n);
  }

  /** Lower-level search API.
   *
   * <p>{@link LeafCollector#collect(int)} is called for every matching document.
   *
   * @throws TooManyClauses If a query would exceed
   *         {@link IndexSearcher#getMaxClauseCount()} clauses.
   */
  public void search(Query query, Collector results)
    throws IOException {
    // 在查询前先确保query 已经无法被重写
    query = rewrite(query);
    // 从存储数据的 leafReader对应的上下文中读取数据  注意这里还传入了一个 weight对象   weight中携带了 scorer对象 (通过统计field信息和term信息 进行打分)
    search(leafContexts, createWeight(query, results.scoreMode(), 1), results);
  }

  /** Search implementation with arbitrary sorting, plus
   * control over whether hit scores and max score
   * should be computed.  Finds
   * the top <code>n</code> hits for <code>query</code>, and sorting
   * the hits by the criteria in <code>sort</code>.
   * If <code>doDocScores</code> is <code>true</code>
   * then the score of each hit will be computed and
   * returned.  If <code>doMaxScore</code> is
   * <code>true</code> then the maximum score over all
   * collected hits will be computed.
   * 
   * @throws TooManyClauses If a query would exceed
   *         {@link IndexSearcher#getMaxClauseCount()} clauses.
   */
  public TopFieldDocs search(Query query, int n,
      Sort sort, boolean doDocScores) throws IOException {
    return searchAfter(null, query, n, sort, doDocScores);
  }

  /**
   * Search implementation with arbitrary sorting.
   * @param query The query to search for
   * @param n Return only the top n results
   * @param sort The {@link org.apache.lucene.search.Sort} object
   * @return The top docs, sorted according to the supplied {@link org.apache.lucene.search.Sort} instance
   * @throws IOException if there is a low-level I/O error
   */
  public TopFieldDocs search(Query query, int n, Sort sort) throws IOException {
    return searchAfter(null, query, n, sort, false);
  }

  /** Finds the top <code>n</code>
   * hits for <code>query</code> where all results are after a previous
   * result (<code>after</code>).
   * <p>
   * By passing the bottom result from a previous page as <code>after</code>,
   * this method can be used for efficient 'deep-paging' across potentially
   * large result sets.
   *
   * @throws TooManyClauses If a query would exceed
   *         {@link IndexSearcher#getMaxClauseCount()} clauses.
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, int n, Sort sort) throws IOException {
    return searchAfter(after, query, n, sort, false);
  }

  /** Finds the top <code>n</code>
   * hits for <code>query</code> where all results are after a previous
   * result (<code>after</code>), allowing control over
   * whether hit scores and max score should be computed.
   * <p>
   * By passing the bottom result from a previous page as <code>after</code>,
   * this method can be used for efficient 'deep-paging' across potentially
   * large result sets.  If <code>doDocScores</code> is <code>true</code>
   * then the score of each hit will be computed and
   * returned.  If <code>doMaxScore</code> is
   * <code>true</code> then the maximum score over all
   * collected hits will be computed.
   *
   * @throws TooManyClauses If a query would exceed
   *         {@link IndexSearcher#getMaxClauseCount()} clauses.
   */
  public TopFieldDocs searchAfter(ScoreDoc after, Query query, int numHits, Sort sort,
      boolean doDocScores) throws IOException {
    if (after != null && !(after instanceof FieldDoc)) {
      // TODO: if we fix type safety of TopFieldDocs we can
      // remove this
      throw new IllegalArgumentException("after must be a FieldDoc; got " + after);
    }
    return searchAfter((FieldDoc) after, query, numHits, sort, doDocScores);
  }

  private TopFieldDocs searchAfter(FieldDoc after, Query query, int numHits, Sort sort,
      boolean doDocScores) throws IOException {
    final int limit = Math.max(1, reader.maxDoc());
    if (after != null && after.doc >= limit) {
      throw new IllegalArgumentException("after.doc exceeds the number of documents in the reader: after.doc="
          + after.doc + " limit=" + limit);
    }
    final int cappedNumHits = Math.min(numHits, limit);
    final Sort rewrittenSort = sort.rewrite(this);

    final CollectorManager<TopFieldCollector, TopFieldDocs> manager = new CollectorManager<>() {

      private final HitsThresholdChecker hitsThresholdChecker = (executor == null || leafSlices.length <= 1) ? HitsThresholdChecker.create(Math.max(TOTAL_HITS_THRESHOLD, numHits)) :
          HitsThresholdChecker.createShared(Math.max(TOTAL_HITS_THRESHOLD, numHits));

      private final MaxScoreAccumulator minScoreAcc = (executor == null || leafSlices.length <= 1) ? null : new MaxScoreAccumulator();

      @Override
      public TopFieldCollector newCollector() throws IOException {
        // TODO: don't pay the price for accurate hit counts by default
        return TopFieldCollector.create(rewrittenSort, cappedNumHits, after, hitsThresholdChecker, minScoreAcc);
      }

      @Override
      public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
        final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
        int i = 0;
        for (TopFieldCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(rewrittenSort, 0, cappedNumHits, topDocs);
      }

    };

    TopFieldDocs topDocs = search(query, manager);
    if (doDocScores) {
      TopFieldCollector.populateScores(topDocs.scoreDocs, this, query);
    }
    return topDocs;
  }

 /**
  * Lower-level search API.
  * Search all leaves using the given {@link CollectorManager}. In contrast
  * to {@link #search(Query, Collector)}, this method will use the searcher's
  * {@link Executor} in order to parallelize execution of the collection
  * on the configured {@link #leafSlices}.
  * @see CollectorManager
  * @lucene.experimental
  * 该对象的核心方法  根据 query对象查询命中的doc 同时将结果交由collectorManager 处理
  */
  public <C extends Collector, T> T search(Query query, CollectorManager<C, T> collectorManager) throws IOException {
    // 代表单线程执行  因为数据都是从 IO系统中拉取后缓存在内存中的 所以可以通过提高并行度来提高性能
    if (executor == null || leafSlices.length <= 1) {
      final C collector = collectorManager.newCollector();
      search(query, collector);
      return collectorManager.reduce(Collections.singletonList(collector));
    } else {
      final List<C> collectors = new ArrayList<>(leafSlices.length);
      ScoreMode scoreMode = null;
      for (int i = 0; i < leafSlices.length; ++i) {
        final C collector = collectorManager.newCollector();
        collectors.add(collector);
        if (scoreMode == null) {
          scoreMode = collector.scoreMode();
        } else if (scoreMode != collector.scoreMode()) {
          throw new IllegalStateException("CollectorManager does not always produce collectors with the same score mode");
        }
      }
      if (scoreMode == null) {
        // no segments
        scoreMode = ScoreMode.COMPLETE;
      }
      query = rewrite(query);
      final Weight weight = createWeight(query, scoreMode, 1);
      final List<FutureTask<C>> listTasks = new ArrayList<>();
      for (int i = 0; i < leafSlices.length; ++i) {
        final LeafReaderContext[] leaves = leafSlices[i].leaves;
        final C collector = collectors.get(i);
        FutureTask<C> task = new FutureTask<>(() -> {
          search(Arrays.asList(leaves), weight, collector);
          return collector;
        });

        listTasks.add(task);
      }

      sliceExecutor.invokeAll(listTasks);
      final List<C> collectedCollectors = new ArrayList<>();
      for (Future<C> future : listTasks) {
        try {
          collectedCollectors.add(future.get());
        } catch (InterruptedException e) {
          throw new ThreadInterruptedException(e);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
      return collectorManager.reduce(collectors);
    }
  }

  /**
   * Lower-level search API.
   * 
   * <p>
   * {@link LeafCollector#collect(int)} is called for every document. <br>
   * 
   * <p>
   * NOTE: this method executes the searches on all given leaves exclusively.
   * To search across all the searchers leaves use {@link #leafContexts}.
   * 
   * @param leaves 
   *          the searchers leaves to execute the searches on
   * @param weight
   *          to match documents
   * @param collector
   *          to receive hits
   * @throws TooManyClauses If a query would exceed
   *         {@link IndexSearcher#getMaxClauseCount()} clauses.
   *         查询结果并使用 collector处理
   */
  protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector)
      throws IOException {

    // TODO: should we make this
    // threaded...?  the Collector could be sync'd?
    // always use single thread:
    for (LeafReaderContext ctx : leaves) { // search each subreader
      final LeafCollector leafCollector;
      try {
        // 根据context 重置内部相关信息    一般会与 doSetNextReader 产生联动
        leafCollector = collector.getLeafCollector(ctx);
      } catch (CollectionTerminatedException e) {
        // there is no doc of interest in this reader context
        // continue with the following leaf
        continue;
      }
      // 看来每个reader对象都会对应一个 打分的结果
      BulkScorer scorer = weight.bulkScorer(ctx);
      if (scorer != null) {
        try {
          scorer.score(leafCollector, ctx.reader().getLiveDocs());
        } catch (CollectionTerminatedException e) {
          // collection was terminated prematurely
          // continue with the following leaf
        }
      }
    }
  }

  /** Expert: called to re-write queries into primitive queries.
   * @throws TooManyClauses If a query would exceed
   *         {@link IndexSearcher#getMaxClauseCount()} clauses.
   *         在使用query前需要进行重写
   */
  public Query rewrite(Query original) throws IOException {
    Query query = original;
    // 每次rewrite返回的新对象又可以重写出新的对象  当rewriter返回自身时  代表已经无法继续重写了
    for (Query rewrittenQuery = query.rewrite(reader); rewrittenQuery != query;
         rewrittenQuery = query.rewrite(reader)) {
      query = rewrittenQuery;
    }
    // 针对直接使用docId 进行查询 的visitor 是 StoredFieldVisitor  会直接从索引文件中寻找 field.value 并且仅能还原 field.stored为true的 field到doc上
    // 针对query使用的visitor是 QueryVisitor
    query.visit(getNumClausesCheckVisitor());
    return query;
  }

  /** Returns a QueryVisitor which recursively checks the total
   * number of clauses that a query and its children cumulatively
   * have and validates that the total number does not exceed
   * the specified limit
   * 该visitor 对象 实际上并没有从索引文件中读取数据的能力 仅仅具备对clause做限制的功能  比如在 rewrite中 最终返回的是一个BooleanQuery对象 它的各种query都会触发 queryVisitor的 consumerTerms  方法 如果条件数过多 则会抛出异常
   */
  private static QueryVisitor getNumClausesCheckVisitor() {
    return new QueryVisitor() {

      int numClauses;

      @Override
      public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
        // Return this instance even for MUST_NOT and not an empty QueryVisitor
        return this;
      }

      // query.visitor 被触发时 可能通过以下三种方式实现功能   该对象就是专门为组合对象下 query数量做限制

      /**
       * @param query the query
       */
      @Override
      public void visitLeaf(Query query) {
        if (numClauses > maxClauseCount) {
          throw new TooManyClauses();
        }
        ++numClauses;
      }

      /**
       *
       * @param query  the leaf query   定义了查询条件的对象
       * @param terms  the terms the query will match on    该查询对象以什么term作为查询条件
       */
      @Override
      public void consumeTerms(Query query, Term... terms) {
        if (numClauses > maxClauseCount) {
          throw new TooManyClauses();
        }
        ++numClauses;
      }

      @Override
      public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
        if (numClauses > maxClauseCount) {
          throw new TooManyClauses();
        }
        ++numClauses;
      }
    };
  }

  /** Returns an Explanation that describes how <code>doc</code> scored against
   * <code>query</code>.
   *
   * <p>This is intended to be used in developing Similarity implementations,
   * and, for good performance, should not be displayed with every hit.
   * Computing an explanation is as expensive as executing the query over the
   * entire index.
   */
  public Explanation explain(Query query, int doc) throws IOException {
    query = rewrite(query);
    return explain(createWeight(query, ScoreMode.COMPLETE, 1), doc);
  }

  /** Expert: low-level implementation method
   * Returns an Explanation that describes how <code>doc</code> scored against
   * <code>weight</code>.
   *
   * <p>This is intended to be used in developing Similarity implementations,
   * and, for good performance, should not be displayed with every hit.
   * Computing an explanation is as expensive as executing the query over the
   * entire index.
   * <p>Applications should call {@link IndexSearcher#explain(Query, int)}.
   * @throws TooManyClauses If a query would exceed
   *         {@link IndexSearcher#getMaxClauseCount()} clauses.
   */
  protected Explanation explain(Weight weight, int doc) throws IOException {
    int n = ReaderUtil.subIndex(doc, leafContexts);
    final LeafReaderContext ctx = leafContexts.get(n);
    int deBasedDoc = doc - ctx.docBase;
    final Bits liveDocs = ctx.reader().getLiveDocs();
    if (liveDocs != null && liveDocs.get(deBasedDoc) == false) {
      return Explanation.noMatch("Document " + doc + " is deleted");
    }
    return weight.explain(ctx, deBasedDoc);
  }

  /**
   * Creates a {@link Weight} for the given query, potentially adding caching
   * if possible and configured.
   * @lucene.experimental
   * @param query 本次使用的查询条件
   * @param scoreMode 使用的打分模式
   * @param boost 打分的相关参数
   * 创建权重对象
   */
  public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException {
    final QueryCache queryCache = this.queryCache;
    Weight weight = query.createWeight(this, scoreMode, boost);
    if (scoreMode.needsScores() == false && queryCache != null) {
      weight = queryCache.doCache(weight, queryCachingPolicy);
    }
    return weight;
  }

  /**
   * Returns this searchers the top-level {@link IndexReaderContext}.
   * @see IndexReader#getContext()
   */
  /* sugar for #getReader().getTopReaderContext() */
  public IndexReaderContext getTopReaderContext() {
    return readerContext;
  }

  /**
   * A class holding a subset of the {@link IndexSearcher}s leaf contexts to be
   * executed within a single thread.
   * 
   * @lucene.experimental
   * 每个分片对象都包含一组reader
   */
  public static class LeafSlice {

    /** The leaves that make up this slice.
     *
     *  @lucene.experimental */
    public final LeafReaderContext[] leaves;
    
    public LeafSlice(List<LeafReaderContext> leavesList) {
      // 恢复从小到大的顺序
      Collections.sort(leavesList, Comparator.comparingInt(l -> l.docBase));
      this.leaves = leavesList.toArray(new LeafReaderContext[0]);
    }
  }

  @Override
  public String toString() {
    return "IndexSearcher(" + reader + "; executor=" + executor + "; sliceExecutionControlPlane " + sliceExecutor + ")";
  }
  
  /**
   * Returns {@link TermStatistics} for a term.
   * 
   * This can be overridden for example, to return a term's statistics
   * across a distributed collection.
   *
   * @param docFreq The document frequency of the term. It must be greater or equal to 1.
   * @param totalTermFreq The total term frequency.
   * @return A {@link TermStatistics} (never null).
   *
   * @lucene.experimental
   * 生成某个 term的统计信息  这里只是简单做一个包装 因为相关信息已经从参数中获取了
   */
  public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) throws IOException {
    // This constructor will throw an exception if docFreq <= 0.
    return new TermStatistics(term.bytes(), docFreq, totalTermFreq);
  }
  
  /**
   * Returns {@link CollectionStatistics} for a field, or {@code null} if
   * the field does not exist (has no indexed terms)
   * 
   * This can be overridden for example, to return a field's statistics
   * across a distributed collection.
   * @lucene.experimental
   * 基于 field 生成一个统计对象
   */
  public CollectionStatistics collectionStatistics(String field) throws IOException {
    assert field != null;
    long docCount = 0;
    long sumTotalTermFreq = 0;
    long sumDocFreq = 0;
    for (LeafReaderContext leaf : reader.leaves()) {
      final Terms terms = leaf.reader().terms(field);
      if (terms == null) {
        continue;
      }
      // 代表该field出现的doc总数  虽然 计算是通过该field下所有term所在的doc总数 (已根据docId去重)  但是这些term都是以field为单位进行管理的 所以可以确保不会读取到field不存在的doc
      docCount += terms.getDocCount();
      // 下面2个参数 分别记录了 该field下所有term 出现的doc总数和 以及在doc中出现的频率次数总和
      sumTotalTermFreq += terms.getSumTotalTermFreq();
      sumDocFreq += terms.getSumDocFreq();
    }
    if (docCount == 0) {
      return null;
    }
    return new CollectionStatistics(field, reader.maxDoc(), docCount, sumTotalTermFreq, sumDocFreq);
  }

  /**
   * Returns this searchers executor or <code>null</code> if no executor was provided
   */
  public Executor getExecutor() {
    return executor;
  }

  /** Thrown when an attempt is made to add more than {@link
   * #getMaxClauseCount()} clauses. This typically happens if
   * a PrefixQuery, FuzzyQuery, WildcardQuery, or TermRangeQuery
   * is expanded to many terms during search.
   */
  public static class TooManyClauses extends RuntimeException {
    public TooManyClauses() {
      super("maxClauseCount is set to " + maxClauseCount);
    }
  }

  /**
   * Return the SliceExecutionControlPlane instance to be used for this IndexSearcher instance
   * 线程池还有分片的概念???
   */
  private static SliceExecutor getSliceExecutionControlPlane(Executor executor) {
    // 如果传入的线程池为 null 则不需要处理
    if (executor == null) {
      return null;
    }

    if (executor instanceof ThreadPoolExecutor) {
      return new QueueSizeBasedExecutor((ThreadPoolExecutor) executor);
    }

    // 默认情况下 返回一个 线程池分片
    return new SliceExecutor(executor);
  }
}
