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
import java.util.Collection;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MaxScoreAccumulator.DocAndScore;

/**
 * A {@link Collector} implementation that collects the top-scoring hits,
 * returning them as a {@link TopDocs}. This is used by {@link IndexSearcher} to
 * implement {@link TopDocs}-based search. Hits are sorted by score descending
 * and then (when the scores are tied) docID ascending. When you create an
 * instance of this collector you should know in advance whether documents are
 * going to be collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and
 * {@link Float#NEGATIVE_INFINITY} are not valid scores.  This
 * collector will not properly collect hits with such
 * scores.
 * 该对象会根据某种要求 仅采集符合查询条件 且得分最高的某几条数据
 */
public abstract class TopScoreDocCollector extends TopDocsCollector<ScoreDoc> {

  /**
   * 该对象是 具备打分对象的 collector 模板  就是附带一个scorer属性
   */
  abstract static class ScorerLeafCollector implements LeafCollector {

    Scorable scorer;

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer;
    }
  }

  /**
   * 一个 topN 查询结果处理对象  不涉及分页
   */
  private static class SimpleTopScoreDocCollector extends TopScoreDocCollector {

    /**
     *
     * @param numHits  N的上限值
     * @param hitsThresholdChecker   该对象用于检测查询的数量是否达到阈值
     * @param minScoreAcc   一个累加器
     */
    SimpleTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker,
                               MaxScoreAccumulator minScoreAcc) {
      super(numHits, hitsThresholdChecker, minScoreAcc);
    }

    /**
     * 根据当前使用的 reader对象 返回一个合适的 collector
     * @param context
     * @return
     * @throws IOException
     */
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      // reset the minimum competitive score
      docBase = context.docBase;
      // 返回一个具备打分能力的对象
      return new ScorerLeafCollector() {

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          // 为父类设置打分对象
          super.setScorer(scorer);
          minCompetitiveScore = 0f;

          // 只有得分高于该值的doc才有查询的必要

          // 尝试更新此时的最低分数
          updateMinCompetitiveScore(scorer);
          // 当设置了累加器时 尝试更新全局最低分数
          if (minScoreAcc != null) {
            updateGlobalMinCompetitiveScore(scorer);
          }
        }

        /**
         * 当查询到符合条件的数据时 会挨个将doc 交由该对象进行处理  当然返回的doc 已经被 liveDoc过滤过了
         * 那么最后 符合条件 得分较高的doc 都存入到最小堆中了
         * @param doc
         * @throws IOException
         */
        @Override
        public void collect(int doc) throws IOException {
          // 实际上就是委托 BM啥256的进行打分 先忽略吧
          float score = scorer.score();

          // This collector relies on the fact that scorers produce positive values:
          assert score >= 0; // NOTE: false for NaN

          // 增加查询到的结果数
          totalHits++;
          // 增加收集到的值 当采集到的值达到一定数量时 就会更新最小值
          hitsThresholdChecker.incrementHitCount();

          // 代表每当查询到多少doc时 触发一次 更新全局最小分数
          if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
            updateGlobalMinCompetitiveScore(scorer);
          }

          // 这个优先队列还是一个最小堆  但是区别在于 要维护的数据是超过 最小堆最小值的 只有超过堆顶的值才会被处理
          if (score <= pqTop.score) {
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore(scorer);
            }
            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }
          // 此时重新构建堆结构  doc 需要+上docBase 转换成全局doc
          pqTop.doc = doc + docBase;
          pqTop.score = score;
          pqTop = pq.updateTop();
          updateMinCompetitiveScore(scorer);
        }

      };
    }
  }

  /**
   * 该对象相比 Simple 对象 在初始化时  额外传入一个after对象
   */
  private static class PagingTopScoreDocCollector extends TopScoreDocCollector {

    /**
     * 本次选择的数据必须小于该值  起到了分页的作用
     */
    private final ScoreDoc after;
    private int collectedHits;

    PagingTopScoreDocCollector(int numHits, ScoreDoc after, HitsThresholdChecker hitsThresholdChecker,
                               MaxScoreAccumulator minScoreAcc) {
      super(numHits, hitsThresholdChecker, minScoreAcc);
      this.after = after;
      this.collectedHits = 0;
    }

    @Override
    protected int topDocsSize() {
      return collectedHits < pq.size() ? collectedHits : pq.size();
    }

    /**
     * 该方法是返回查询结果的
     * @param results
     * @param start
     * @return
     */
    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      return results == null
              // 父类没有查询到结果 直接返回空数据  而该方法会返回总计扫描了多少数据
          ? new TopDocs(new TotalHits(totalHits, totalHitsRelation), new ScoreDoc[0])
          : new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
    }

    /**
     * 返回该reader相关的 leafCollector
     * @param context
     * @return
     * @throws IOException
     */
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      docBase = context.docBase;
      final int afterDoc = after.doc - context.docBase;

      return new ScorerLeafCollector() {
        @Override
        public void collect(int doc) throws IOException {
          // 为本次查询到的doc 进行打分
          float score = scorer.score();

          // This collector relies on the fact that scorers produce positive values:
          assert score >= 0; // NOTE: false for NaN

          totalHits++;
          hitsThresholdChecker.incrementHitCount();

          // 每当并发更新的 次数达到一定值时 就更新全局的 最小score 这样在遍历doc的过程中 这些doc就会被忽略了
          if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
            updateGlobalMinCompetitiveScore(scorer);
          }

          // 大于after的分数 其实就是之前已经查询到的数据  或者分数相同 但是doc小的  因为之前最小堆的构建已经要求了
          if (score > after.score || (score == after.score && doc <= afterDoc)) {
            // hit was collected on a previous page
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore(scorer);
            }
            return;
          }

          // 本次分数太小 不是合适的选择
          if (score <= pqTop.score) {
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore(scorer);
            }

            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }
          // 符合条件的doc 将会用于构建最小堆
          collectedHits++;
          pqTop.doc = doc + docBase;
          pqTop.score = score;
          pqTop = pq.updateTop();
          updateMinCompetitiveScore(scorer);
        }
      };
    }
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect and the number of hits to count accurately.
   *
   * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly
   * {@code totalHitsThreshold} then this value is accurate. On the other hand,
   * if the {@link TopDocs#totalHits} value is greater than {@code totalHitsThreshold}
   * then its value is a lower bound of the hit count. A value of {@link Integer#MAX_VALUE}
   * will make the hit count accurate but will also likely make query processing slower.
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, int totalHitsThreshold) {
    return create(numHits, null, totalHitsThreshold);
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect, the bottom of the previous page, and the number of hits to count
   * accurately.
   *
   * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly
   * {@code totalHitsThreshold} then this value is accurate. On the other hand,
   * if the {@link TopDocs#totalHits} value is greater than {@code totalHitsThreshold}
   * then its value is a lower bound of the hit count. A value of {@link Integer#MAX_VALUE}
   * will make the hit count accurate but will also likely make query processing slower.
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, ScoreDoc after, int totalHitsThreshold) {
    return create(numHits, after, HitsThresholdChecker.create(Math.max(totalHitsThreshold, numHits)), null);
  }

  static TopScoreDocCollector create(int numHits, ScoreDoc after, HitsThresholdChecker hitsThresholdChecker,
                                     MaxScoreAccumulator minScoreAcc) {

    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    if (hitsThresholdChecker == null) {
      throw new IllegalArgumentException("hitsThresholdChecker must be non null");
    }

    if (after == null) {
      return new SimpleTopScoreDocCollector(numHits, hitsThresholdChecker, minScoreAcc);
    } else {
      // 代表本次返回的结果 必须要先小于 after 主要是做分页用
      return new PagingTopScoreDocCollector(numHits, after, hitsThresholdChecker, minScoreAcc);
    }
  }

  /**
   * Create a CollectorManager which uses a shared hit counter to maintain number of hits
   * and a shared {@link MaxScoreAccumulator} to propagate the minimum score accross segments
   */
  public static CollectorManager<TopScoreDocCollector, TopDocs> createSharedManager(int numHits, FieldDoc after,
                                                                                      int totalHitsThreshold) {
    return new CollectorManager<>() {

      private final HitsThresholdChecker hitsThresholdChecker = HitsThresholdChecker.createShared(Math.max(totalHitsThreshold, numHits));
      private final MaxScoreAccumulator minScoreAcc = new MaxScoreAccumulator();

      @Override
      public TopScoreDocCollector newCollector() throws IOException {
        return TopScoreDocCollector.create(numHits, after, hitsThresholdChecker, minScoreAcc);
      }

      @Override
      public TopDocs reduce(Collection<TopScoreDocCollector> collectors) throws IOException {
        final TopDocs[] topDocs = new TopDocs[collectors.size()];
        int i = 0;
        for (TopScoreDocCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(0, numHits, topDocs);
      }

    };
  }

  int docBase;
  /**
   * 记录当前最小堆中的最小值
   */
  ScoreDoc pqTop;
  final HitsThresholdChecker hitsThresholdChecker;
  final MaxScoreAccumulator minScoreAcc;
  float minCompetitiveScore;

  /**
   * @param numHits  N的上限值
   * @param hitsThresholdChecker   该对象用于检测查询的数量是否达到阈值
   * @param minScoreAcc   一个累加器
   */
  TopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker,
                       MaxScoreAccumulator minScoreAcc) {
    super(new HitQueue(numHits, true));
    assert hitsThresholdChecker != null;

    // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
    // that at this point top() is already initialized.
    // 此时最小值
    pqTop = pq.top();
    this.hitsThresholdChecker = hitsThresholdChecker;
    this.minScoreAcc = minScoreAcc;
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      return EMPTY_TOPDOCS;
    }

    return new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
  }

  @Override
  public ScoreMode scoreMode() {
    return hitsThresholdChecker.scoreMode();
  }

  /**
   * 更新全局最低分数
   * @param scorer
   * @throws IOException
   */
  protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
    assert minScoreAcc != null;
    // 通过多个对象在不同线程中初始化 并将最小值填充到累加器中 此时 minScoreAcc的最小值 就是全局最小值
    DocAndScore maxMinScore = minScoreAcc.get();
    if (maxMinScore != null) {
      // since we tie-break on doc id and collect in doc id order we can require
      // the next float if the global minimum score is set on a document id that is
      // smaller than the ids in the current leaf
      // 返回略大于 score的下一个浮点值
      float score = docBase > maxMinScore.docID ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
      // 更新 最小分数值
      if (score > minCompetitiveScore) {
        assert hitsThresholdChecker.isThresholdReached();
        scorer.setMinCompetitiveScore(score);
        minCompetitiveScore = score;
        totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
      }
    }
  }

  /**
   * 尝试更新最低分数
   * @param scorer
   * @throws IOException
   */
  protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
    // 当此时设置的值已经达到阈值时  且顶部数据是有效的
    if (hitsThresholdChecker.isThresholdReached()
          && pqTop != null
          && pqTop.score != Float.NEGATIVE_INFINITY) { // -Infinity is the score of sentinels
      // since we tie-break on doc id and collect in doc id order, we can require
      // the next float
      float localMinScore = Math.nextUp(pqTop.score);
      // 当此时顶部最低数据 超过  minCompetitiveScore 时 就代表要以localMinScore 作为最低分数
      if (localMinScore > minCompetitiveScore) {
        // 更新scorer对象的最低分数
        scorer.setMinCompetitiveScore(localMinScore);
        totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        minCompetitiveScore = localMinScore;
        if (minScoreAcc != null) {
          // we don't use the next float but we register the document
          // id so that other leaves can require it if they are after
          // the current maximum
          // 该对象负责从多个对象中采集数据 并维护最大的 最小值  每次的并发范围就是一次查询中针对多个readerSlice的查询线程
          minScoreAcc.accumulate(pqTop.doc, pqTop.score);
        }
      }
    }
  }
}
