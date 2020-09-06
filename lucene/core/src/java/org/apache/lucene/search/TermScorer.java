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

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;

/** Expert: A <code>Scorer</code> for documents matching a <code>Term</code>.
 */
final class TermScorer extends Scorer {
  private final PostingsEnum postingsEnum;
  private final ImpactsEnum impactsEnum;
  private final DocIdSetIterator iterator;
  private final LeafSimScorer docScorer;
  private final ImpactsDISI impactsDisi;

  /**
   * Construct a {@link TermScorer} that will iterate all documents.
   * 该对象将会针对 所有 doc 进行打分
   */
  TermScorer(Weight weight, PostingsEnum postingsEnum, LeafSimScorer docScorer) {
    super(weight);
    // 这种情况下 doc迭代器就是 PostingsEnum
    iterator = this.postingsEnum = postingsEnum;
    impactsEnum = new SlowImpactsEnum(postingsEnum);
    impactsDisi = new ImpactsDISI(impactsEnum, impactsEnum, docScorer.getSimScorer());
    this.docScorer = docScorer;
  }

  /**
   * Construct a {@link TermScorer} that will use impacts to skip blocks of
   * non-competitive documents.
   * @param weight 创建该对象的权重对象
   * @param impactsEnum  具备遍历标准因子功能的迭代器
   * @param docScorer  打分对象
   */
  TermScorer(Weight weight, ImpactsEnum impactsEnum, LeafSimScorer docScorer) {
    super(weight);
    postingsEnum = this.impactsEnum = impactsEnum;
    // 将 ImpactsEnum 包装后生成 doc迭代器
    impactsDisi = new ImpactsDISI(impactsEnum, impactsEnum, docScorer.getSimScorer());
    iterator = impactsDisi;
    this.docScorer = docScorer;
  }

  /**
   * 此时读取的docId
   * @return
   */
  @Override
  public int docID() {
    return postingsEnum.docID();
  }

  final int freq() throws IOException {
    return postingsEnum.freq();
  }

  @Override
  public DocIdSetIterator iterator() {
    return iterator;
  }

  /**
   * 为此时遍历到 的doc 进行打分 并返回结果  这里基于什么BM256算法的 先忽略计算过程
   * @return
   * @throws IOException
   */
  @Override
  public float score() throws IOException {
    assert docID() != DocIdSetIterator.NO_MORE_DOCS;
    return docScorer.score(postingsEnum.docID(), postingsEnum.freq());
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    return impactsDisi.advanceShallow(target);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return impactsDisi.getMaxScore(upTo);
  }

  /**
   * 设置最低分数
   * @param minScore
   */
  @Override
  public void setMinCompetitiveScore(float minScore) {
    impactsDisi.setMinCompetitiveScore(minScore);
  }

  /** Returns a string representation of this <code>TermScorer</code>. */
  @Override
  public String toString() { return "scorer(" + weight + ")[" + super.toString() + "]"; }

}
