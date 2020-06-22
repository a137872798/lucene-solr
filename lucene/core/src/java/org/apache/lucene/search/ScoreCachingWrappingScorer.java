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
import java.util.Collections;

/**
 * A {@link Scorer} which wraps another scorer and caches the score of the
 * current document. Successive calls to {@link #score()} will return the same
 * result and will not invoke the wrapped Scorer's score() method, unless the
 * current document has changed.<br>
 * This class might be useful due to the changes done to the {@link Collector}
 * interface, in which the score is not computed for a document by default, only
 * if the collector requests it. Some collectors may need to use the score in
 * several places, however all they have in hand is a {@link Scorer} object, and
 * might end up computing the score of a document more than once.
 * 为打分对象追加缓存功能
 */
public final class ScoreCachingWrappingScorer extends Scorable {

  /**
   * 当前指向的文档号
   */
  private int curDoc = -1;

  /**
   * 当前文档得出的分数
   */
  private float curScore;
  /**
   * 内部实际工作的对象
   */
  private final Scorable in;

  /** Creates a new instance by wrapping the given scorer. */
  public ScoreCachingWrappingScorer(Scorable scorer) {
    this.in = scorer;
  }

  /**
   * 如果当前指向的doc 就是 上次使用过的 那么直接返回已经打好的分数 避免重复打分 (这是一个耗时的过程)
   * @return
   * @throws IOException
   */
  @Override
  public float score() throws IOException {
    int doc = in.docID();
    if (doc != curDoc) {
      curScore = in.score();
      curDoc = doc;
    }

    return curScore;
  }

  /**
   * 设置一个最小的分数  类似默认值
   * @param minScore
   * @throws IOException
   */
  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    in.setMinCompetitiveScore(minScore);
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public Collection<ChildScorable> getChildren() {
    return Collections.singleton(new ChildScorable(in, "CACHED"));
  }
}
