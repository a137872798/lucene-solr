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


import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause.Occur;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

/**
 * A Query that matches documents matching boolean combinations of other
 * queries, e.g. {@link TermQuery}s, {@link PhraseQuery}s or other
 * BooleanQuerys.
 * 该对象通过组合多个 Query生成  每个被组合的 Query 被包装成 BooleanClause
 */
public class BooleanQuery extends Query implements Iterable<BooleanClause> {

    /**
     * Thrown when an attempt is made to add more than {@link
     * #getMaxClauseCount()} clauses. This typically happens if
     * a PrefixQuery, FuzzyQuery, WildcardQuery, or TermRangeQuery
     * is expanded to many terms during search.
     *
     * @deprecated use {@link IndexSearcher.TooManyClauses}
     */
    @Deprecated // Remove in Lucene 10
    public static class TooManyClauses extends IndexSearcher.TooManyClauses {
    }

    /**
     * Return the maximum number of clauses permitted, 1024 by default.
     * Attempts to add more than the permitted number of clauses cause {@link
     * TooManyClauses} to be thrown.
     *
     * @see IndexSearcher#setMaxClauseCount(int)
     * @deprecated use {@link IndexSearcher#getMaxClauseCount()}
     */
    @Deprecated // Remove in Lucene 10
    public static int getMaxClauseCount() {
        return IndexSearcher.getMaxClauseCount();
    }

    /**
     * Set the maximum number of clauses permitted per BooleanQuery.
     * Default value is 1024.
     *
     * @deprecated use {@link IndexSearcher#setMaxClauseCount(int)}
     */
    @Deprecated // Remove in Lucene 10
    public static void setMaxClauseCount(int maxClauseCount) {
        IndexSearcher.setMaxClauseCount(maxClauseCount);
    }

    /**
     * A builder for boolean queries.
     */
    // 提供API 便于直接构建 BooleanQuery
    public static class Builder {

        private int minimumNumberShouldMatch;
        // 组成该 BooleanQuery的一组 Query(每个Query对象都会被包装成 Clause)
        private final List<BooleanClause> clauses = new ArrayList<>();

        /**
         * Sole constructor.
         */
        public Builder() {
        }

        /**
         * Specifies a minimum number of the optional BooleanClauses
         * which must be satisfied.
         *
         * <p>
         * By default no optional clauses are necessary for a match
         * (unless there are no required clauses).  If this method is used,
         * then the specified number of clauses is required.
         * </p>
         * <p>
         * Use of this method is totally independent of specifying that
         * any specific clauses are required (or prohibited).  This number will
         * only be compared against the number of matching optional clauses.
         * </p>
         *
         * @param min the number of optional clauses that must match
         */
        public Builder setMinimumNumberShouldMatch(int min) {
            this.minimumNumberShouldMatch = min;
            return this;
        }

        /**
         * Add a new clause to this {@link Builder}. Note that the order in which
         * clauses are added does not have any impact on matching documents or query
         * performance.
         *
         * @throws IndexSearcher.TooManyClauses if the new number of clauses exceeds the maximum clause number
         */
        public Builder add(BooleanClause clause) {
            // We do the final deep check for max clauses count limit during
            //<code>IndexSearcher.rewrite</code> but do this check to short
            // circuit in case a single query holds more than numClauses
            // 如果查询的条件过多 会抛出异常
            if (clauses.size() >= IndexSearcher.maxClauseCount) {
                throw new IndexSearcher.TooManyClauses();
            }
            clauses.add(clause);
            return this;
        }

        /**
         * Add a new clause to this {@link Builder}. Note that the order in which
         * clauses are added does not have any impact on matching documents or query
         * performance.
         *
         * @throws IndexSearcher.TooManyClauses if the new number of clauses exceeds the maximum clause number
         *                                      外部一般会调用该api    将组合条件和查询对象包装成一个 BooleanClause 并 添加到query中
         */
        public Builder add(Query query, Occur occur) {
            return add(new BooleanClause(query, occur));
        }

        /**
         * Create a new {@link BooleanQuery} based on the parameters that have
         * been set on this builder.
         */
        public BooleanQuery build() {
            return new BooleanQuery(minimumNumberShouldMatch, clauses.toArray(new BooleanClause[0]));
        }

    }

    /**
     * 代表当查询的条件中 包含should时  至少要满足多少should 才可以
     * 比如有5个 should条件  但是该值为3  也就是只要3个should满足 就允许查询该值
     */
    private final int minimumNumberShouldMatch;
    private final List<BooleanClause> clauses;              // used for toString() and getClauses()
    /**
     * key 对应组合条件 value 对应查询对象set
     */
    private final Map<Occur, Collection<Query>> clauseSets; // used for equals/hashcode

    /**
     * 通过 Builder 构建出 BooleanQuery 对象
     *
     * @param minimumNumberShouldMatch
     * @param clauses
     */
    private BooleanQuery(int minimumNumberShouldMatch,
                         BooleanClause[] clauses) {
        // 针对查询条件中有 Should的情况   要求必须满足 minimumNumberShouldMatch 数量的词
        this.minimumNumberShouldMatch = minimumNumberShouldMatch;
        this.clauses = Collections.unmodifiableList(Arrays.asList(clauses));
        clauseSets = new EnumMap<>(Occur.class);
        // duplicates matter for SHOULD and MUST      这2种重复添加时 会累加
        clauseSets.put(Occur.SHOULD, new Multiset<>());
        clauseSets.put(Occur.MUST, new Multiset<>());
        // but not for FILTER and MUST_NOT            这2种重复添加时 会覆盖
        clauseSets.put(Occur.FILTER, new HashSet<>());
        clauseSets.put(Occur.MUST_NOT, new HashSet<>());
        // 将clause 分派到所属的数组中
        for (BooleanClause clause : clauses) {
            clauseSets.get(clause.getOccur()).add(clause.getQuery());
        }
    }

    /**
     * Gets the minimum number of the optional BooleanClauses
     * which must be satisfied.
     */
    public int getMinimumNumberShouldMatch() {
        return minimumNumberShouldMatch;
    }

    /**
     * Return a list of the clauses of this {@link BooleanQuery}.
     */
    public List<BooleanClause> clauses() {
        return clauses;
    }

    /**
     * Return the collection of queries for the given {@link Occur}.
     */
    Collection<Query> getClauses(Occur occur) {
        return clauseSets.get(occur);
    }

    /**
     * Whether this query is a pure disjunction, ie. it only has SHOULD clauses
     * and it is enough for a single clause to match for this boolean query to match.
     */
    boolean isPureDisjunction() {
        return clauses.size() == getClauses(Occur.SHOULD).size()
                && minimumNumberShouldMatch <= 1;
    }

    /**
     * Returns an iterator on the clauses in this query. It implements the {@link Iterable} interface to
     * make it possible to do:
     * <pre class="prettyprint">for (BooleanClause clause : booleanQuery) {}</pre>
     */
    @Override
    public final Iterator<BooleanClause> iterator() {
        return clauses.iterator();
    }

    /**
     * 修改成不需要打分
     *
     * @return
     */
    private BooleanQuery rewriteNoScoring() {
        // 当getMinimumNumberShouldMatch == 0 的时候就代表不需要should类型的 clause了
        boolean keepShould = getMinimumNumberShouldMatch() > 0
                || (clauseSets.get(Occur.MUST).size() + clauseSets.get(Occur.FILTER).size() == 0);

        // 没有MUST 的条件时 就要保持原样
        if (clauseSets.get(Occur.MUST).size() == 0 && keepShould) {
            return this;
        }
        BooleanQuery.Builder newQuery = new BooleanQuery.Builder();

        newQuery.setMinimumNumberShouldMatch(getMinimumNumberShouldMatch());
        for (BooleanClause clause : clauses) {
            switch (clause.getOccur()) {
                // 将MUST 类型修改成 FILTER  (filter类型的 clause是不参与打分的)
                case MUST: {
                    newQuery.add(clause.getQuery(), Occur.FILTER);
                    break;
                }
                case SHOULD: {
                    if (keepShould) {
                        newQuery.add(clause);
                    }
                    break;
                }
                default: {
                    newQuery.add(clause);
                }
            }
        }

        return newQuery.build();
    }

    /**
     * 根据 searcher对象  创建权重对象
     *
     * @param searcher
     * @param scoreMode How the produced scorers will be consumed.
     * @param boost     The boost that is propagated by the parent queries.
     * @return
     * @throws IOException
     */
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        BooleanQuery query = this;
        // 如果不需要打分
        if (scoreMode.needsScores() == false) {
            // 将must 都转换成 filter
            query = rewriteNoScoring();
        }
        return new BooleanWeight(query, searcher, scoreMode, boost);
    }

    /**
     * 原本用于查询的 query本身可能不够清晰 比如 BooleanQuery传入时 无法识别查询条件是什么 在这里就要将BooleanQuery 拆解成内部详细的query对象
     * 当返回原本的对象时 调用方则会退出rewrite
     *
     * @param reader
     * @return
     * @throws IOException
     */
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (clauses.size() == 0) {
            return new MatchNoDocsQuery("empty BooleanQuery");
        }

        // optimize 1-clause queries
        // 如果内部仅包含一个Query
        if (clauses.size() == 1) {
            BooleanClause c = clauses.get(0);
            Query query = c.getQuery();
            // 如果使用的 Occur是 Should 且数量满足时 允许被返回
            if (minimumNumberShouldMatch == 1 && c.getOccur() == Occur.SHOULD) {
                return query;
                // 代表不对 should做数量限制
            } else if (minimumNumberShouldMatch == 0) {
                switch (c.getOccur()) {
                    // 这时 should 和must 是一样的
                    case SHOULD:
                    case MUST:
                        return query;
                    // 返回一个得分始终为0的对象
                    case FILTER:
                        // no scoring clauses, so return a score of 0
                        return new BoostQuery(new ConstantScoreQuery(query), 0);
                    // 仅有一个Query 且是  MUST_NOT时 无法定位要查询的内容  （只声明不能查到什么时是无法返回内容的）
                    case MUST_NOT:
                        // no positive clauses
                        // 该对象代表无法匹配到任何 doc
                        return new MatchNoDocsQuery("pure negative BooleanQuery");
                    default:
                        throw new AssertionError();
                }
            }
        }

        // recursively rewrite
        // 相当于这里的目地是将所有 clause重写一次 只要有一个重写成功就认为该BooleanQuery本身重写成功   之后调用方还会不断的调用rewrite 直到无法再重写为止
        {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.setMinimumNumberShouldMatch(getMinimumNumberShouldMatch());
            boolean actuallyRewritten = false;
            for (BooleanClause clause : this) {
                Query query = clause.getQuery();
                // 将每个 子 Query对象都进行重写
                Query rewritten = query.rewrite(reader);
                if (rewritten != query) {
                    // rewrite clause
                    actuallyRewritten = true;
                    builder.add(rewritten, clause.getOccur());
                } else {
                    // leave as-is
                    builder.add(clause);
                }
            }
            // 只要有一个query重写成功了 就返回新的 BooleanQuery
            if (actuallyRewritten) {
                return builder.build();
            }
        }

        // remove duplicate FILTER and MUST_NOT clauses
        // 此时内部的clauses本身 无法再重写了  就开始检测是否有重复的查询条件 并进行剔除
        {
            int clauseCount = 0;
            for (Collection<Query> queries : clauseSets.values()) {
                clauseCount += queries.size();
            }
            // 数值不等的时候意味着重复插入了  NOT_MUST 和 FILTER
            if (clauseCount != clauses.size()) {
                // since clauseSets implicitly deduplicates FILTER and MUST_NOT
                // clauses, this means there were duplicates
                BooleanQuery.Builder rewritten = new BooleanQuery.Builder();
                rewritten.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
                for (Map.Entry<Occur, Collection<Query>> entry : clauseSets.entrySet()) {
                    final Occur occur = entry.getKey();
                    for (Query query : entry.getValue()) {
                        rewritten.add(query, occur);
                    }
                }
                // 返回的 BooleanQuery.clauses 中 NOT_MUST 和 FILTER 不会重复
                return rewritten.build();
            }
        }

        // Check whether some clauses are both required and excluded
        final Collection<Query> mustNotClauses = clauseSets.get(Occur.MUST_NOT);
        if (!mustNotClauses.isEmpty()) {
            final Predicate<Query> p = clauseSets.get(Occur.MUST)::contains;
            // 代表 MUST_NOT 与 MUST 刚好冲突了  或者 MUST_NOT 与 FILTER 冲突了  (Filter和must 都代表文档必须包含目标词)    重写后直接生成无法找到任何doc的query
            if (mustNotClauses.stream().anyMatch(p.or(clauseSets.get(Occur.FILTER)::contains))) {
                return new MatchNoDocsQuery("FILTER or MUST clause also in MUST_NOT");
            }
            // 如果不匹配所有的query 转换成对应语义的对象
            if (mustNotClauses.contains(new MatchAllDocsQuery())) {
                return new MatchNoDocsQuery("MUST_NOT clause is MatchAllDocsQuery");
            }
        }

        // remove FILTER clauses that are also MUST clauses
        // or that match all documents
        // 这里已经将 Must , MustNot , Filter 语义冲突的去除了
        if (clauseSets.get(Occur.MUST).size() > 0 && clauseSets.get(Occur.FILTER).size() > 0) {
            final Set<Query> filters = new HashSet<>(clauseSets.get(Occur.FILTER));
            boolean modified = filters.remove(new MatchAllDocsQuery());
            // 代表 filter 与 must 有交集
            modified |= filters.removeAll(clauseSets.get(Occur.MUST));
            // 需要重新构建 Query
            if (modified) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.setMinimumNumberShouldMatch(getMinimumNumberShouldMatch());
                for (BooleanClause clause : clauses) {
                    if (clause.getOccur() != Occur.FILTER) {
                        builder.add(clause);
                    }
                }
                // 这里的filters 已经移除了交集的部分 将剩余的写入builder  也就是当must与 filter冲突时 只保留must
                for (Query filter : filters) {
                    builder.add(filter, Occur.FILTER);
                }
                return builder.build();
            }
        }

        // convert FILTER clauses that are also SHOULD clauses to MUST clauses
        if (clauseSets.get(Occur.SHOULD).size() > 0 && clauseSets.get(Occur.FILTER).size() > 0) {
            final Collection<Query> filters = clauseSets.get(Occur.FILTER);
            final Collection<Query> shoulds = clauseSets.get(Occur.SHOULD);

            // 如果should 和 filter有交集
            Set<Query> intersection = new HashSet<>(filters);
            intersection.retainAll(shoulds);

            if (intersection.isEmpty() == false) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                int minShouldMatch = getMinimumNumberShouldMatch();

                for (BooleanClause clause : clauses) {
                    // 存在与该交集的 should 需要转换成 must   因为既要求filter 有要求 should 实际上就是必须存在
                    if (intersection.contains(clause.getQuery())) {
                        // 将 should 转换成 must
                        if (clause.getOccur() == Occur.SHOULD) {
                            builder.add(new BooleanClause(clause.getQuery(), Occur.MUST));
                            minShouldMatch--;
                        }
                    } else {
                        builder.add(clause);
                    }
                }

                // 同时减少了 minShould的值
                builder.setMinimumNumberShouldMatch(Math.max(0, minShouldMatch));
                return builder.build();
            }
        }

        // Deduplicate SHOULD clauses by summing up their boosts
        if (clauseSets.get(Occur.SHOULD).size() > 0 && minimumNumberShouldMatch <= 1) {
            Map<Query, Double> shouldClauses = new HashMap<>();
            for (Query query : clauseSets.get(Occur.SHOULD)) {
                double boost = 1;
                // 寻找嵌套的 BoostQuery 将他们的 boost 累乘
                while (query instanceof BoostQuery) {
                    BoostQuery bq = (BoostQuery) query;
                    boost *= bq.getBoost();
                    // BoostQuery 内部嵌套的 query  也有可能是  BoostQuery
                    query = bq.getQuery();
                }
                // 当遇到key 相同的情况下 会用叠加的value 去覆盖
                shouldClauses.put(query, shouldClauses.getOrDefault(query, 0d) + boost);
            }
            // 代表有些should是 BoostQuery 且内部嵌套的query 是一样的 在这里被去重
            // 换句话说 如果没有发生去重 就不用rewrite
            if (shouldClauses.size() != clauseSets.get(Occur.SHOULD).size()) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder()
                        .setMinimumNumberShouldMatch(minimumNumberShouldMatch);
                // 使用覆盖后的query 构建 BooleanQuery
                for (Map.Entry<Query, Double> entry : shouldClauses.entrySet()) {
                    Query query = entry.getKey();
                    float boost = entry.getValue().floatValue();
                    // 当boost是1时 就没必要被包裹了  引用默认就是1
                    if (boost != 1f) {
                        query = new BoostQuery(query, boost);
                    }
                    builder.add(query, Occur.SHOULD);
                }
                // 其余的 原封不动
                for (BooleanClause clause : clauses) {
                    if (clause.getOccur() != Occur.SHOULD) {
                        builder.add(clause);
                    }
                }
                return builder.build();
            }
        }

        // Deduplicate MUST clauses by summing up their boosts
        // 合并must
        if (clauseSets.get(Occur.MUST).size() > 0) {
            Map<Query, Double> mustClauses = new HashMap<>();
            for (Query query : clauseSets.get(Occur.MUST)) {
                double boost = 1;
                while (query instanceof BoostQuery) {
                    BoostQuery bq = (BoostQuery) query;
                    boost *= bq.getBoost();
                    query = bq.getQuery();
                }
                mustClauses.put(query, mustClauses.getOrDefault(query, 0d) + boost);
            }
            if (mustClauses.size() != clauseSets.get(Occur.MUST).size()) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder()
                        .setMinimumNumberShouldMatch(minimumNumberShouldMatch);
                for (Map.Entry<Query, Double> entry : mustClauses.entrySet()) {
                    Query query = entry.getKey();
                    float boost = entry.getValue().floatValue();
                    if (boost != 1f) {
                        query = new BoostQuery(query, boost);
                    }
                    builder.add(query, Occur.MUST);
                }
                for (BooleanClause clause : clauses) {
                    if (clause.getOccur() != Occur.MUST) {
                        builder.add(clause);
                    }
                }
                return builder.build();
            }
        }

        // Rewrite queries whose single scoring clause is a MUST clause on a
        // MatchAllDocsQuery to a ConstantScoreQuery
        // 检测是否有包含 MatchAllDocsQuery  如果有的话 实际上可以简化query
        {
            final Collection<Query> musts = clauseSets.get(Occur.MUST);
            final Collection<Query> filters = clauseSets.get(Occur.FILTER);
            if (musts.size() == 1
                    && filters.size() > 0) {
                Query must = musts.iterator().next();
                float boost = 1f;
                if (must instanceof BoostQuery) {
                    BoostQuery boostQuery = (BoostQuery) must;
                    must = boostQuery.getQuery();
                    boost = boostQuery.getBoost();
                }
                // 如果 must 仅仅被一层 BoostQuery 包裹   或者未被包裹 且是MatchAllDocsQuery
                if (must.getClass() == MatchAllDocsQuery.class) {
                    // our single scoring clause matches everything: rewrite to a CSQ on the filter
                    // ignore SHOULD clause for now
                    BooleanQuery.Builder builder = new BooleanQuery.Builder();
                    for (BooleanClause clause : clauses) {
                        switch (clause.getOccur()) {
                            case FILTER:
                            case MUST_NOT:
                                builder.add(clause);
                                break;
                            default:
                                // ignore
                                break;
                        }
                    }
                    Query rewritten = builder.build();
                    // TODO 这段逻辑的核心结果就是该对象  相当于将一个 MUST  MatchAllDocsQuery 与其他非SHOULD条件结合成 MUST ConstantScoreQuery
                    rewritten = new ConstantScoreQuery(rewritten);
                    if (boost != 1f) {
                        rewritten = new BoostQuery(rewritten, boost);
                    }

                    // now add back the SHOULD clauses
                    builder = new BooleanQuery.Builder()
                            .setMinimumNumberShouldMatch(getMinimumNumberShouldMatch())
                            .add(rewritten, Occur.MUST);
                    for (Query query : clauseSets.get(Occur.SHOULD)) {
                        builder.add(query, Occur.SHOULD);
                    }
                    rewritten = builder.build();
                    return rewritten;
                }
            }
        }

        // Flatten nested disjunctions, this is important for block-max WAND to perform well
        if (minimumNumberShouldMatch <= 1) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
            boolean actuallyRewritten = false;
            for (BooleanClause clause : clauses) {
                if (clause.getOccur() == Occur.SHOULD && clause.getQuery() instanceof BooleanQuery) {
                    BooleanQuery innerQuery = (BooleanQuery) clause.getQuery();
                    // 代表全是 should 且 minShould <=1    相当于将嵌套结构平铺了
                    if (innerQuery.isPureDisjunction()) {
                        actuallyRewritten = true;
                        for (BooleanClause innerClause : innerQuery.clauses()) {
                            builder.add(innerClause);
                        }
                    } else {
                        builder.add(clause);
                    }
                } else {
                    builder.add(clause);
                }
            }
            if (actuallyRewritten) {
                return builder.build();
            }
        }

        return super.rewrite(reader);
    }

    /**
     * 使用visitor从索引文件中抽取信息
     * @param visitor a QueryVisitor to be called by each query in the tree
     */
    @Override
    public void visit(QueryVisitor visitor) {
        QueryVisitor sub = visitor.getSubVisitor(Occur.MUST, this);
        for (BooleanClause.Occur occur : clauseSets.keySet()) {
            // 首先确保存在这种类型的 query
            if (clauseSets.get(occur).size() > 0) {
                // 遇到must 类型会全部处理
                if (occur == Occur.MUST) {
                    for (Query q : clauseSets.get(occur)) {
                        q.visit(sub);
                    }
                } else {
                    // 如果是其余类型的 会先获得 subVisitor 之后再挨个处理   但是这些子visitor都是基于 MUST生成的
                    QueryVisitor v = sub.getSubVisitor(occur, this);
                    for (Query q : clauseSets.get(occur)) {
                        q.visit(v);
                    }
                }
            }
        }
    }

    /**
     * Prints a user-readable version of this query.
     */
    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        boolean needParens = getMinimumNumberShouldMatch() > 0;
        if (needParens) {
            buffer.append("(");
        }

        int i = 0;
        for (BooleanClause c : this) {
            buffer.append(c.getOccur().toString());

            Query subQuery = c.getQuery();
            if (subQuery instanceof BooleanQuery) {  // wrap sub-bools in parens
                buffer.append("(");
                buffer.append(subQuery.toString(field));
                buffer.append(")");
            } else {
                buffer.append(subQuery.toString(field));
            }

            if (i != clauses.size() - 1) {
                buffer.append(" ");
            }
            i += 1;
        }

        if (needParens) {
            buffer.append(")");
        }

        if (getMinimumNumberShouldMatch() > 0) {
            buffer.append('~');
            buffer.append(getMinimumNumberShouldMatch());
        }

        return buffer.toString();
    }

    /**
     * Compares the specified object with this boolean query for equality.
     * Returns true if and only if the provided object<ul>
     * <li>is also a {@link BooleanQuery},</li>
     * <li>has the same value of {@link #getMinimumNumberShouldMatch()}</li>
     * <li>has the same {@link Occur#SHOULD} clauses, regardless of the order</li>
     * <li>has the same {@link Occur#MUST} clauses, regardless of the order</li>
     * <li>has the same set of {@link Occur#FILTER} clauses, regardless of the
     * order and regardless of duplicates</li>
     * <li>has the same set of {@link Occur#MUST_NOT} clauses, regardless of
     * the order and regardless of duplicates</li></ul>
     */
    @Override
    public boolean equals(Object o) {
        return sameClassAs(o) &&
                equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(BooleanQuery other) {
        return getMinimumNumberShouldMatch() == other.getMinimumNumberShouldMatch() &&
                clauseSets.equals(other.clauseSets);
    }

    private int computeHashCode() {
        int hashCode = Objects.hash(minimumNumberShouldMatch, clauseSets);
        if (hashCode == 0) {
            hashCode = 1;
        }
        return hashCode;
    }

    // cached hash code is ok since boolean queries are immutable
    private int hashCode;

    @Override
    public int hashCode() {
        // no need for synchronization, in the worst case we would just compute the hash several times.
        if (hashCode == 0) {
            hashCode = computeHashCode();
            assert hashCode != 0;
        }
        assert hashCode == computeHashCode();
        return hashCode;
    }

}
