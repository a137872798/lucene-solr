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


import java.util.Objects;

/** A clause in a BooleanQuery. */

/**
 * BooleanQuery 作为一种组合查询 下面的子 Query可以单独设置 Occur 用于定义多个子 Query之间的关系
 * 描述组合查询并列关系的
 */
public final class BooleanClause {
  
  /** Specifies how clauses are to occur in matching documents. */
  public static enum Occur {

    /**
     * Use this operator for clauses that <i>must</i> appear in the matching documents.
     * 查询的结果必须命中当前query
     */
    MUST     { @Override public String toString() { return "+"; } },

    /**
     * Like {@link #MUST} except that these clauses do not participate in scoring.
     * 类似于 MUST 要求查询的结果必须命中当前query 但是不会参与打分
     */
    FILTER   { @Override public String toString() { return "#"; } },

    /** Use this operator for clauses that <i>should</i> appear in the 
     * matching documents. For a BooleanQuery with no <code>MUST</code> 
     * clauses one or more <code>SHOULD</code> clauses must match a document 
     * for the BooleanQuery to match.
     * @see BooleanQuery.Builder#setMinimumNumberShouldMatch
     * 只要求在多个 SHOULD中命中minimumNumberShouldMatch个  同时命中的越多分数越高
     */
    SHOULD   { @Override public String toString() { return "";  } },

    /** Use this operator for clauses that <i>must not</i> appear in the matching documents.
     * Note that it is not possible to search for queries that only consist
     * of a <code>MUST_NOT</code> clause. These clauses do not contribute to the
     * score of documents.
     * 必须不能命中query
     */
    MUST_NOT { @Override public String toString() { return "-"; } };

  }

  /** The query whose matching documents are combined by the boolean query.
   * 这是一个原始的 query 对象
   */
  private final Query query;

  /**
   * 一种组合规则
   */
  private final Occur occur;


  /** Constructs a BooleanClause.
  */ 
  public BooleanClause(Query query, Occur occur) {
    this.query = Objects.requireNonNull(query, "Query must not be null");
    this.occur = Objects.requireNonNull(occur, "Occur must not be null");
    
  }

  public Occur getOccur() {
    return occur;
  }

  public Query getQuery() {
    return query;
  }
  
  public boolean isProhibited() {
    return Occur.MUST_NOT == occur;
  }

  public boolean isRequired() {
    return occur == Occur.MUST || occur == Occur.FILTER;
  }

  /**
   * 是否 参与打分
   * @return
   */
  public boolean isScoring() {
    return occur == Occur.MUST || occur == Occur.SHOULD;
  }

  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof BooleanClause))
      return false;
    BooleanClause other = (BooleanClause)o;
    return this.query.equals(other.query)
      && this.occur == other.occur;
  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    return 31 * query.hashCode() + occur.hashCode();
  }


  @Override
  public String toString() {
    return occur.toString() + query.toString();
  }
}
