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


/**
 * Just counts the total number of hits.
 * 仅包含命中总数的收集器
 */

public class TotalHitCountCollector extends SimpleCollector {
  private int totalHits;

  /** Returns how many hits matched the search. */
  public int getTotalHits() {
    return totalHits;
  }

  /**
   * 每当命中某个doc 时  增加查询命中总数
   * @param doc
   */
  @Override
  public void collect(int doc) {
    totalHits++;
  }

  /**
   * 该对象查询出来的结果不需要打分
   * @return
   */
  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }
}
