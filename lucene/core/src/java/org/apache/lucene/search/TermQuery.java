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
import java.util.Objects;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.Similarity;

/**
 * A Query that matches documents containing a term. This may be combined with
 * other terms with a {@link BooleanQuery}.
 * 查询包含该term的所有 query
 */
public class TermQuery extends Query {


  private final Term term;
  private final TermStates perReaderTermState;

  /**
   *  TermQuery 生成的 权重对象
   */
  final class TermWeight extends Weight {

    /**
     * 打分的算法对象
     */
    private final Similarity similarity;
    /**
     * 包含打分的API  如果某个term没有存储freq信息 那么不需要创建该对象
     */
    private final Similarity.SimScorer simScorer;
    private final TermStates termStates;
    private final ScoreMode scoreMode;


    /**
     *
     * @param searcher  该对象是通过 哪个searcher 创建的
     * @param scoreMode   使用的打分模式
     * @param boost      影响到得分的一个权重值
     * @param termStates   存储该term的 posting信息
     * @throws IOException
     */
    public TermWeight(IndexSearcher searcher, ScoreMode scoreMode,
        float boost, TermStates termStates) throws IOException {
      super(TermQuery.this);
      // 如果需要打分 那么必须要生成 state信息 否则无法计算分数
      if (scoreMode.needsScores() && termStates == null) {
        throw new IllegalStateException("termStates are required when scores are needed");
      }
      this.scoreMode = scoreMode;
      this.termStates = termStates;
      this.similarity = searcher.getSimilarity();

      final CollectionStatistics collectionStats;
      final TermStatistics termStats;
      if (scoreMode.needsScores()) {
        // 采集该field 下所有的信息
        collectionStats = searcher.collectionStatistics(term.field());
        // 生成有关term的统计对象   当field没有记录freq信息时 该值好像是-1   那么此时就不会生成 termStats
        termStats = termStates.docFreq() > 0 ? searcher.termStatistics(term, termStates.docFreq(), termStates.totalTermFreq()) : null;
      } else {
        // we do not need the actual stats, use fake stats with docFreq=maxDoc=ttf=1
        // 当不需要计算分数时 生成一个虚拟结果
        collectionStats = new CollectionStatistics(term.field(), 1, 1, 1, 1);
        termStats = new TermStatistics(term.bytes(), 1, 1);
      }

      // 当没有生成 term的统计信息时 不需要创建 simScorer
      if (termStats == null) {
        this.simScorer = null; // term doesn't exist in any segment, we won't use similarity at all
      } else {
        this.simScorer = similarity.scorer(boost, collectionStats, termStats);
      }
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      TermsEnum te = getTermsEnum(context);
      if (te == null) {
        return null;
      }
      if (context.reader().terms(term.field()).hasPositions() == false) {
        return super.matches(context, doc);
      }
      return MatchesUtils.forField(term.field(), () -> {
        PostingsEnum pe = te.postings(null, PostingsEnum.OFFSETS);
        if (pe.advance(doc) != doc) {
          return null;
        }
        return new TermMatchesIterator(getQuery(), pe);
      });
    }

    @Override
    public String toString() {
      return "weight(" + TermQuery.this + ")";
    }

    /**
     * 根据 reader上下文信息 生成打分对象
     * @param context
     *          the {@link org.apache.lucene.index.LeafReaderContext} for which to return the {@link Scorer}.
     *
     * @return
     * @throws IOException
     */
    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      assert termStates == null || termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context)) : "The top-reader used to create Weight is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);;

      // 获取该  Term在该reader下的信息  (需要同时命中 term.field 和 term)
      final TermsEnum termsEnum = getTermsEnum(context);
      if (termsEnum == null) {
        return null;
      }
      // 将标准因子 与 核心的打分对象 simScorer 包装到一起
      LeafSimScorer scorer = new LeafSimScorer(simScorer, context.reader(), term.field(), scoreMode.needsScores());

      // 如果仅top数据参与打分
      if (scoreMode == ScoreMode.TOP_SCORES) {
        return new TermScorer(this, termsEnum.impacts(PostingsEnum.FREQS), scorer);
      } else {
        // 这种情况下 内部会使用一个总是返回固定标准因子的迭代器对象
        return new TermScorer(this, termsEnum.postings(null, scoreMode.needsScores() ? PostingsEnum.FREQS : PostingsEnum.NONE), scorer);
      }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }

    /**
     * Returns a {@link TermsEnum} positioned at this weights Term or null if
     * the term does not exist in the given context
     */
    private TermsEnum getTermsEnum(LeafReaderContext context) throws IOException {
      assert termStates != null;
      assert termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context)) :
          "The top-reader used to create Weight is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);

      // 获取该context下该term的 posting信息
      final TermState state = termStates.get(context);
      if (state == null) { // term is not present in that reader
        assert termNotInReader(context.reader(), term) : "no termstate found but term exists in reader term=" + term;
        return null;
      }
      final TermsEnum termsEnum = context.reader().terms(term.field()).iterator();
      termsEnum.seekExact(term.bytes(), state);
      return termsEnum;
    }

    private boolean termNotInReader(LeafReader reader, Term term) throws IOException {
      // only called from assert
      // System.out.println("TQ.termNotInReader reader=" + reader + " term=" +
      // field + ":" + bytes.utf8ToString());
      return reader.docFreq(term) == 0;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      TermScorer scorer = (TermScorer) scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          float freq = scorer.freq();
          LeafSimScorer docScorer = new LeafSimScorer(simScorer, context.reader(), term.field(), true);
          Explanation freqExplanation = Explanation.match(freq, "freq, occurrences of term within document");
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
          return Explanation.match(
              scoreExplanation.getValue(),
              "weight(" + getQuery() + " in " + doc + ") ["
                  + similarity.getClass().getSimpleName() + "], result of:",
              scoreExplanation);
        }
      }
      return Explanation.noMatch("no matching term");
    }
  }

  /** Constructs a query for the term <code>t</code>. */
  // 通过一个词进行初始化
  public TermQuery(Term t) {
    term = Objects.requireNonNull(t);
    perReaderTermState = null;
  }

  /**
   * Expert: constructs a TermQuery that will use the provided docFreq instead
   * of looking up the docFreq against the searcher.
   */
  public TermQuery(Term t, TermStates states) {
    assert states != null;
    term = Objects.requireNonNull(t);
    perReaderTermState = Objects.requireNonNull(states);
  }

  /** Returns the term of this query. */
  public Term getTerm() {
    return term;
  }

  /**
   * 创建权重对象
   * @param searcher
   * @param scoreMode     How the produced scorers will be consumed.
   * @param boost         The boost that is propagated by the parent queries.
   * @return
   * @throws IOException
   */
  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    // 获取IndexSearcher 的顶层 context  (如果初始化时使用的 reader就是 LeafReader 那么顶层上下文就是该LeafReader 对应的上下文)
    final IndexReaderContext context = searcher.getTopReaderContext();
    final TermStates termState;
    // perReaderTermState 负责为每个reader 构建一个 termState对象
    if (perReaderTermState == null
        || perReaderTermState.wasBuiltFor(context) == false) {
      // 为该context 创建 termState
      termState = TermStates.build(context, term, scoreMode.needsScores());
    } else {
      // PRTS was pre-build for this IS
      // 可以在构建 QueryTerm时 提前设置 perReaderTermState
      termState = this.perReaderTermState;
    }

    // 看来权重对象就是用来打分的
    return new TermWeight(searcher, scoreMode, boost, termState);
  }

  /**
   * 只看该对象的实现  因为大体流程都是一样的 其他的query等使用的时候再看
   * @param visitor a QueryVisitor to be called by each query in the tree
   */
  @Override
  public void visit(QueryVisitor visitor) {
    // 首先要检测 该visitor 是否允许处理该field  就跟 StoredFieldVisitor一样 只能处理被允许的field
    if (visitor.acceptField(term.field())) {
      // visitor 直接有处理term的接口吗  那么任何查询看来在重写后最终都会转换成针对term的查询
      visitor.consumeTerms(this, term);
    }
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append(term.text());
    return buffer.toString();
  }

  /** Returns the {@link TermStates} passed to the constructor, or null if it was not passed.
   *
   * @lucene.experimental */
  public TermStates getTermStates() {
    return perReaderTermState;
  }

  /** Returns true iff <code>other</code> is equal to <code>this</code>. */
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           term.equals(((TermQuery) other).term);
  }

  @Override
  public int hashCode() {
    return classHash() ^ term.hashCode();
  }
}
