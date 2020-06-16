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

package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;

/**
 * Defines a group, for use by grouping collectors
 *
 * A GroupSelector acts as an iterator over documents.  For each segment, clients
 * should call {@link #setNextReader(LeafReaderContext)}, and then {@link #advanceTo(int)}
 * for each matching document.
 *
 * @param <T> the type of the group value
 *           鉴别某个文档是否属于某个组
 */
public abstract class GroupSelector<T> {

  /**
   * What to do with the current value
   * 代表当前doc 是需要跳过还是需要被收集
   */
  public enum State { SKIP, ACCEPT }

  /**
   * Set the LeafReaderContext
   * 更新reader对象 这样会采集到新的数据
   */
  public abstract void setNextReader(LeafReaderContext readerContext) throws IOException;

  /**
   * Set the current Scorer
   * 设置打分对象
   */
  public abstract void setScorer(Scorable scorer) throws IOException;

  /**
   * Advance the GroupSelector's iterator to the given document
   * 传入文档id 并返回处理的结果 (不属于该组需要跳过 还是属于该组需要被收集)
   */
  public abstract State advanceTo(int doc) throws IOException;

  /**
   * Get the group value of the current document
   *
   * N.B. this object may be reused, for a persistent version use {@link #copyValue()}
   * 返回当前文档
   */
  public abstract T currentValue() throws IOException;

  /**
   * @return a copy of the group value of the current document
   * 拷贝一份当前文档的副本
   */
  public abstract T copyValue() throws IOException;

  /**
   * Set a restriction on the group values returned by this selector
   *
   * If the selector is positioned on a document whose group value is not contained
   * within this set, then {@link #advanceTo(int)} will return {@link State#SKIP}
   *
   * @param groups a set of {@link SearchGroup} objects to limit selections to
   *               设置被预先设定的组  该对象就是基于这些组来筛选的
   */
  public abstract void setGroups(Collection<SearchGroup<T>> groups);

}
