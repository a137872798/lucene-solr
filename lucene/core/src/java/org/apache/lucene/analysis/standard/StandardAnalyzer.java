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
package org.apache.lucene.analysis.standard;


import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WordlistLoader;

/**
 * Filters {@link StandardTokenizer} with {@link LowerCaseFilter} and
 * {@link StopFilter}, using a configurable list of stop words.
 *
 * @since 3.1
 * 代表一个标准的token 加工器  会找到内部的停词 以及统一成小写
 */
public final class StandardAnalyzer extends StopwordAnalyzerBase {
  
  /** Default maximum allowed token length */
  // 默认情况下 每个token 最大长度为 255
  public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

  private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

  /** Builds an analyzer with the given stop words.
   * @param stopWords stop words */
  // 使用一个指定的停词容器 这样在解析的时候就不会将这些词作为索引
  public StandardAnalyzer(CharArraySet stopWords) {
    super(stopWords);
  }

  /** Builds an analyzer with no stop words.
   * 默认情况下不拦截任何停词
   */
  public StandardAnalyzer() {
    this(CharArraySet.EMPTY_SET);
  }

  /** Builds an analyzer with the stop words from the given reader.
   * @see WordlistLoader#getWordSet(Reader)
   * @param stopwords Reader to read stop words from */
  // 停词从 reader中获取
  public StandardAnalyzer(Reader stopwords) throws IOException {
    this(loadStopwordSet(stopwords));
  }

  /**
   * Set the max allowed token length.  Tokens larger than this will be chopped
   * up at this token length and emitted as multiple tokens.  If you need to
   * skip such large tokens, you could increase this max length, and then
   * use {@code LengthFilter} to remove long tokens.  The default is
   * {@link StandardAnalyzer#DEFAULT_MAX_TOKEN_LENGTH}.
   */
  public void setMaxTokenLength(int length) {
    maxTokenLength = length;
  }
    
  /** Returns the current maximum token length
   * 
   *  @see #setMaxTokenLength */
  public int getMaxTokenLength() {
    return maxTokenLength;
  }

  /**
   * @param fieldName
   *          the name of the fields content passed to the
   *          {@link TokenStreamComponents} sink as a reader

   * @return
   */
  @Override
  protected TokenStreamComponents createComponents(final String fieldName) {
    // 该对象通过读取输入流 解析内部的token
    final StandardTokenizer src = new StandardTokenizer();
    src.setMaxTokenLength(maxTokenLength);
    // 包装成一个新的token流
    TokenStream tok = new LowerCaseFilter(src);
    // 追加停词过滤器
    tok = new StopFilter(tok, stopwords);
    return new TokenStreamComponents(r -> {
      src.setMaxTokenLength(StandardAnalyzer.this.maxTokenLength);
      src.setReader(r);
    }, tok);
  }

  /**
   * 默认情况下 会追加忽略大小写的能力
   * @param fieldName
   * @param in
   * @return
   */
  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    return new LowerCaseFilter(in);
  }
}
