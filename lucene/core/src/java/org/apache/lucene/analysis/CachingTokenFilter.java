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
package org.apache.lucene.analysis;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.util.AttributeSource;

/**
 * This class can be used if the token attributes of a TokenStream
 * are intended to be consumed more than once. It caches
 * all token attribute states locally in a List when the first call to
 * {@link #incrementToken()} is called. Subsequent calls will used the cache.
 * <p>
 * <em>Important:</em> Like any proper TokenFilter, {@link #reset()} propagates
 * to the input, although only before {@link #incrementToken()} is called the
 * first time. Prior to  Lucene 5, it was never propagated.
 */
public final class CachingTokenFilter extends TokenFilter {
  /**
   * 内部维护一组状态对象 而每个state又是一个链表 每个节点上携带一个 attribute 对象
   */
  private List<AttributeSource.State> cache = null;
  private Iterator<AttributeSource.State> iterator = null;
  /**
   * 代表调用 end 后 获取到的state
   */
  private AttributeSource.State finalState;
  
  /**
   * Create a new CachingTokenFilter around <code>input</code>. As with
   * any normal TokenFilter, do <em>not</em> call reset on the input; this filter
   * will do it normally.
   */
  public CachingTokenFilter(TokenStream input) {
    super(input);
  }

  /**
   * Propagates reset if incrementToken has not yet been called. Otherwise
   * it rewinds the iterator to the beginning of the cached list.
   * 当缓存还未创建时  重置内部的 input  否则获取一个迭代器的引用
   */
  @Override
  public void reset() throws IOException {
    if (cache == null) {//first time
      input.reset();
    } else {
      iterator = cache.iterator();
    }
  }

  /** The first time called, it'll read and cache all tokens from the input. */
  @Override
  public final boolean incrementToken() throws IOException {
    // 创建 cache 对象 以及填充cache
    if (cache == null) {//first-time
      // fill cache lazily
      cache = new ArrayList<>(64);
      fillCache();
      iterator = cache.iterator();
    }

    // 当迭代器中没有数据时 无法在添加
    if (!iterator.hasNext()) {
      // the cache is exhausted, return false
      return false;
    }
    // Since the TokenFilter can be reset, the tokens need to be preserved as immutable.
    restoreState(iterator.next());
    return true;
  }

  @Override
  public final void end() {
    if (finalState != null) {
      restoreState(finalState);
    }
  }

  /**
   * 填满缓存
   * @throws IOException
   */
  private void fillCache() throws IOException {
    while (input.incrementToken()) {
      // 每当成功往 tokenStream 插入数据时  往缓存中写入数据
      cache.add(captureState());
    }
    // capture final state
    input.end();
    finalState = captureState();
  }

  /** If the underlying token stream was consumed and cached. */
  public boolean isCached() {
    return cache != null;
  }

}
