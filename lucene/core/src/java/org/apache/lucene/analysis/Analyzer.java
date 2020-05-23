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


import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.Version;

/**
 * An Analyzer builds TokenStreams, which analyze text.  It thus represents a
 * policy for extracting index terms from text.
 * <p>
 * In order to define what analysis is done, subclasses must define their
 * {@link TokenStreamComponents TokenStreamComponents} in {@link #createComponents(String)}.
 * The components are then reused in each call to {@link #tokenStream(String, Reader)}.
 * <p>
 * Simple example:
 * <pre class="prettyprint">
 * Analyzer analyzer = new Analyzer() {
 *  {@literal @Override}
 *   protected TokenStreamComponents createComponents(String fieldName) {
 *     Tokenizer source = new FooTokenizer(reader);
 *     TokenStream filter = new FooFilter(source);
 *     filter = new BarFilter(filter);
 *     return new TokenStreamComponents(source, filter);
 *   }
 *   {@literal @Override}
 *   protected TokenStream normalize(TokenStream in) {
 *     // Assuming FooFilter is about normalization and BarFilter is about
 *     // stemming, only FooFilter should be applied
 *     return new FooFilter(in);
 *   }
 * };
 * </pre>
 * For more examples, see the {@link org.apache.lucene.analysis Analysis package documentation}.
 * <p>
 * For some concrete implementations bundled with Lucene, look in the analysis modules:
 * <ul>
 *   <li><a href="{@docRoot}/../analyzers-common/overview-summary.html">Common</a>:
 *       Analyzers for indexing content in different languages and domains.
 *   <li><a href="{@docRoot}/../analyzers-icu/overview-summary.html">ICU</a>:
 *       Exposes functionality from ICU to Apache Lucene. 
 *   <li><a href="{@docRoot}/../analyzers-kuromoji/overview-summary.html">Kuromoji</a>:
 *       Morphological analyzer for Japanese text.
 *   <li><a href="{@docRoot}/../analyzers-morfologik/overview-summary.html">Morfologik</a>:
 *       Dictionary-driven lemmatization for the Polish language.
 *   <li><a href="{@docRoot}/../analyzers-phonetic/overview-summary.html">Phonetic</a>:
 *       Analysis for indexing phonetic signatures (for sounds-alike search).
 *   <li><a href="{@docRoot}/../analyzers-smartcn/overview-summary.html">Smart Chinese</a>:
 *       Analyzer for Simplified Chinese, which indexes words.
 *   <li><a href="{@docRoot}/../analyzers-stempel/overview-summary.html">Stempel</a>:
 *       Algorithmic Stemmer for the Polish Language.
 * </ul>
 *
 * @since 3.1
 *
 * 这是一个分析器对象
 */
public abstract class Analyzer implements Closeable {

  /**
   * 代表一种重用策略
   */
  private final ReuseStrategy reuseStrategy;
  /**
   * 当前版本是最新版本
   */
  private Version version = Version.LATEST;

  // non final as it gets nulled if closed; pkg private for access by ReuseStrategy's final helper methods:
  // 该对象内存储的本地线程变量不容易发生内存泄漏
  CloseableThreadLocal<Object> storedValue = new CloseableThreadLocal<>();

  /**
   * Create a new Analyzer, reusing the same set of components per-thread
   * across calls to {@link #tokenStream(String, Reader)}.
   * 默认采用全局策略 也就是 一个 analyzer 对应一个 components
   */
  public Analyzer() {
    this(GLOBAL_REUSE_STRATEGY);
  }

  /**
   * Expert: create a new Analyzer with a custom {@link ReuseStrategy}.
   * <p>
   * NOTE: if you just want to reuse on a per-field basis, it's easier to
   * use a subclass of {@link AnalyzerWrapper} such as 
   * <a href="{@docRoot}/../analyzers-common/org/apache/lucene/analysis/miscellaneous/PerFieldAnalyzerWrapper.html">
   * PerFieldAnalyerWrapper</a> instead.
   */
  public Analyzer(ReuseStrategy reuseStrategy) {
    this.reuseStrategy = reuseStrategy;
  }

  /**
   * Creates a new {@link TokenStreamComponents} instance for this analyzer.
   * 
   * @param fieldName
   *          the name of the fields content passed to the
   *          {@link TokenStreamComponents} sink as a reader

   * @return the {@link TokenStreamComponents} for this analyzer.
   * 通过字段名创建一个 token组件
   */
  protected abstract TokenStreamComponents createComponents(String fieldName);

  /**
   * Wrap the given {@link TokenStream} in order to apply normalization filters.
   * The default implementation returns the {@link TokenStream} as-is. This is
   * used by {@link #normalize(String, String)}.
   * 包装传入的对象并返回 默认情况 直接返回
   */
  protected TokenStream normalize(String fieldName, TokenStream in) {
    return in;
  }

  /**
   * Returns a TokenStream suitable for <code>fieldName</code>, tokenizing
   * the contents of <code>reader</code>.
   * <p>
   * This method uses {@link #createComponents(String)} to obtain an
   * instance of {@link TokenStreamComponents}. It returns the sink of the
   * components and stores the components internally. Subsequent calls to this
   * method will reuse the previously stored components after resetting them
   * through {@link TokenStreamComponents#setReader(Reader)}.
   * <p>
   * <b>NOTE:</b> After calling this method, the consumer must follow the 
   * workflow described in {@link TokenStream} to properly consume its contents.
   * See the {@link org.apache.lucene.analysis Analysis package documentation} for
   * some examples demonstrating this.
   * 
   * <b>NOTE:</b> If your data is available as a {@code String}, use
   * {@link #tokenStream(String, String)} which reuses a {@code StringReader}-like
   * instance internally.
   * 
   * @param fieldName the name of the field the created TokenStream is used for
   * @param reader the reader the streams source reads from
   * @return TokenStream for iterating the analyzed content of <code>reader</code>
   * @throws AlreadyClosedException if the Analyzer is closed.
   * @see #tokenStream(String, String)
   */
  public final TokenStream tokenStream(final String fieldName,
                                       final Reader reader) {
    // 先尝试获取之前缓存的component
    TokenStreamComponents components = reuseStrategy.getReusableComponents(this, fieldName);
    // 利用field 初始化 reader 对象
    final Reader r = initReader(fieldName, reader);
    if (components == null) {
      // 基于 field创建一个 components
      components = createComponents(fieldName);
      // 存储 以便重用
      reuseStrategy.setReusableComponents(this, fieldName, components);
    }
    components.setReader(r);
    // 从组件中返回 stream
    return components.getTokenStream();
  }
  
  /**
   * Returns a TokenStream suitable for <code>fieldName</code>, tokenizing
   * the contents of <code>text</code>.
   * <p>
   * This method uses {@link #createComponents(String)} to obtain an
   * instance of {@link TokenStreamComponents}. It returns the sink of the
   * components and stores the components internally. Subsequent calls to this
   * method will reuse the previously stored components after resetting them
   * through {@link TokenStreamComponents#setReader(Reader)}.
   * <p>
   * <b>NOTE:</b> After calling this method, the consumer must follow the 
   * workflow described in {@link TokenStream} to properly consume its contents.
   * See the {@link org.apache.lucene.analysis Analysis package documentation} for
   * some examples demonstrating this.
   * 
   * @param fieldName the name of the field the created TokenStream is used for
   * @param text the String the streams source reads from
   * @return TokenStream for iterating the analyzed content of <code>reader</code>
   * @throws AlreadyClosedException if the Analyzer is closed.
   * @see #tokenStream(String, Reader)
   * 通过 特殊字段和 一个文本来生成token 流
   */
  public final TokenStream tokenStream(final String fieldName, final String text) {
    TokenStreamComponents components = reuseStrategy.getReusableComponents(this, fieldName);
    @SuppressWarnings("resource") final ReusableStringReader strReader = 
        (components == null || components.reusableStringReader == null) ?
        new ReusableStringReader() : components.reusableStringReader;
    strReader.setValue(text);
    // 通过特殊字段初始化 reader 后重新设置到 components
    final Reader r = initReader(fieldName, strReader);
    if (components == null) {
      components = createComponents(fieldName);
      reuseStrategy.setReusableComponents(this, fieldName, components);
    }

    components.setReader(r);
    components.reusableStringReader = strReader;
    return components.getTokenStream();
  }

  /**
   * Normalize a string down to the representation that it would have in the
   * index.
   * <p>
   * This is typically used by query parsers in order to generate a query on
   * a given term, without tokenizing or stemming, which are undesirable if
   * the string to analyze is a partial word (eg. in case of a wildcard or
   * fuzzy query).
   * <p>
   * This method uses {@link #initReaderForNormalization(String, Reader)} in
   * order to apply necessary character-level normalization and then
   * {@link #normalize(String, TokenStream)} in order to apply the normalizing
   * token filters.
   * 使用特殊字段和文本生成 byte[]
   */
  public final BytesRef normalize(final String fieldName, final String text) {
    try {
      // apply char filters
      final String filteredText;
      try (Reader reader = new StringReader(text)) {
        // 加工reader 后将数据转移到 stringBuilder 中
        Reader filterReader = initReaderForNormalization(fieldName, reader);
        char[] buffer = new char[64];
        StringBuilder builder = new StringBuilder();
        for (;;) {
          final int read = filterReader.read(buffer, 0, buffer.length);
          if (read == -1) {
            break;
          }
          builder.append(buffer, 0, read);
        }
        filteredText = builder.toString();
      } catch (IOException e) {
        throw new IllegalStateException("Normalization threw an unexpected exception", e);
      }

      final AttributeFactory attributeFactory = attributeFactory(fieldName);
      // 生成一个 token 流 并进行包装
      try (TokenStream ts = normalize(fieldName,
          new StringTokenStream(attributeFactory, filteredText, text.length()))) {
        final TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        ts.reset();
        if (ts.incrementToken() == false) {
          throw new IllegalStateException("The normalization token stream is "
              + "expected to produce exactly 1 token, but got 0 for analyzer "
              + this + " and input \"" + text + "\"");
        }
        // 这里拷贝一份数据后返回
        final BytesRef term = BytesRef.deepCopyOf(termAtt.getBytesRef());
        if (ts.incrementToken()) {
          throw new IllegalStateException("The normalization token stream is "
              + "expected to produce exactly 1 token, but got 2+ for analyzer "
              + this + " and input \"" + text + "\"");
        }
        ts.end();
        return term;
      }
    } catch (IOException e) {
      throw new IllegalStateException("Normalization threw an unexpected exception", e);
    }
  }

  /**
   * Override this if you want to add a CharFilter chain.
   * <p>
   * The default implementation returns <code>reader</code>
   * unchanged.
   * 
   * @param fieldName IndexableField name being indexed
   * @param reader original Reader
   * @return reader, optionally decorated with CharFilter(s)
   */
  protected Reader initReader(String fieldName, Reader reader) {
    return reader;
  }

  /** Wrap the given {@link Reader} with {@link CharFilter}s that make sense
   *  for normalization. This is typically a subset of the {@link CharFilter}s
   *  that are applied in {@link #initReader(String, Reader)}. This is used by
   *  {@link #normalize(String, String)}. */
  protected Reader initReaderForNormalization(String fieldName, Reader reader) {
    return reader;
  }

  /** Return the {@link AttributeFactory} to be used for
   *  {@link #tokenStream analysis} and
   *  {@link #normalize(String, String) normalization} on the given
   *  {@code FieldName}. The default implementation returns
   *  {@link TokenStream#DEFAULT_TOKEN_ATTRIBUTE_FACTORY}. */
  protected AttributeFactory attributeFactory(String fieldName) {
    return TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY;
  }

  /**
   * Invoked before indexing a IndexableField instance if
   * terms have already been added to that field.  This allows custom
   * analyzers to place an automatic position increment gap between
   * IndexbleField instances using the same field name.  The default value
   * position increment gap is 0.  With a 0 position increment gap and
   * the typical default token position increment of 1, all terms in a field,
   * including across IndexableField instances, are in successive positions, allowing
   * exact PhraseQuery matches, for instance, across IndexableField instance boundaries.
   *
   * @param fieldName IndexableField name being indexed.
   * @return position increment gap, added to the next token emitted from {@link #tokenStream(String,Reader)}.
   *         This value must be {@code >= 0}.
   */
  public int getPositionIncrementGap(String fieldName) {
    return 0;
  }

  /**
   * Just like {@link #getPositionIncrementGap}, except for
   * Token offsets instead.  By default this returns 1.
   * This method is only called if the field
   * produced at least one token for indexing.
   *
   * @param fieldName the field just indexed
   * @return offset gap, added to the next token emitted from {@link #tokenStream(String,Reader)}.
   *         This value must be {@code >= 0}.
   */
  public int getOffsetGap(String fieldName) {
    return 1;
  }

  /**
   * Returns the used {@link ReuseStrategy}.
   */
  public final ReuseStrategy getReuseStrategy() {
    return reuseStrategy;
  }

  /**
   * Set the version of Lucene this analyzer should mimic the behavior for for analysis.
   */
  public void setVersion(Version v) {
    version = v; // TODO: make write once?
  }

  /**
   * Return the version of Lucene this analyzer will mimic the behavior of for analysis.
   */
  public Version getVersion() {
    return version;
  }

  /** Frees persistent resources used by this Analyzer */
  @Override
  public void close() {
    if (storedValue != null) {
      storedValue.close();
      storedValue = null;
    }
  }

  /**
   * This class encapsulates the outer components of a token stream. It provides
   * access to the source (a {@link Reader} {@link Consumer} and the outer end (sink), an
   * instance of {@link TokenFilter} which also serves as the
   * {@link TokenStream} returned by
   * {@link Analyzer#tokenStream(String, Reader)}.
   * 代表一个token的组件 多个token 能组成一个完整的语句
   */
  public static final class TokenStreamComponents {
    /**
     * Original source of the tokens.
     * 该函数负责使用 java.io.Reader
     */
    protected final Consumer<Reader> source;
    /**
     * Sink tokenstream, such as the outer tokenfilter decorating
     * the chain. This can be the source if there are no filters.
     * 该对象内部是一个链式结构 可以往该对象上添加token
     */
    protected final TokenStream sink;
    
    /** Internal cache only used by {@link Analyzer#tokenStream(String, String)}. */
    // 内部就是一个 string 按char进行读取
    transient ReusableStringReader reusableStringReader;

    /**
     * Creates a new {@link TokenStreamComponents} instance.
     * 
     * @param source
     *          the source to set the reader on
     * @param result
     *          the analyzer's resulting token stream
     */
    public TokenStreamComponents(final Consumer<Reader> source,
        final TokenStream result) {
      this.source = source;
      this.sink = result;
    }

    /**
     * Creates a new {@link TokenStreamComponents} instance
     * @param tokenizer the analyzer's Tokenizer
     * @param result    the analyzer's resulting token stream
     */
    public TokenStreamComponents(final Tokenizer tokenizer, final TokenStream result) {
      this(tokenizer::setReader, result);
    }

    /**
     * Creates a new {@link TokenStreamComponents} from a Tokenizer
     */
    public TokenStreamComponents(final Tokenizer tokenizer) {
      this(tokenizer::setReader, tokenizer);
    }

    /**
     * Resets the encapsulated components with the given reader. If the components
     * cannot be reset, an Exception should be thrown.
     * 
     * @param reader
     *          a reader to reset the source component
     *          实际上就是触发 tokenizer::setReader
     */
    private void setReader(final Reader reader) {
      source.accept(reader);
    }

    /**
     * Returns the sink {@link TokenStream}
     * 
     * @return the sink {@link TokenStream}
     */
    public TokenStream getTokenStream() {
      return sink;
    }

    /**
     * Returns the component's source
     */
    public Consumer<Reader> getSource() {
      return source;
    }
  }

  /**
   * Strategy defining how TokenStreamComponents are reused per call to
   * {@link Analyzer#tokenStream(String, java.io.Reader)}.
   * 代表一个重用策略
   */
  public static abstract class ReuseStrategy {

    /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
    public ReuseStrategy() {}

    /**
     * Gets the reusable TokenStreamComponents for the field with the given name.
     *
     * @param analyzer Analyzer from which to get the reused components. Use
     *        {@link #getStoredValue(Analyzer)} and {@link #setStoredValue(Analyzer, Object)}
     *        to access the data on the Analyzer.
     * @param fieldName Name of the field whose reusable TokenStreamComponents
     *        are to be retrieved
     * @return Reusable TokenStreamComponents for the field, or {@code null}
     *         if there was no previous components for the field
     */
    public abstract TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName);

    /**
     * Stores the given TokenStreamComponents as the reusable components for the
     * field with the give name.
     *
     * @param fieldName Name of the field whose TokenStreamComponents are being set
     * @param components TokenStreamComponents which are to be reused for the field
     */
    public abstract void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents components);

    /**
     * Returns the currently stored value.
     *
     * @return Currently stored value or {@code null} if no value is stored
     * @throws AlreadyClosedException if the Analyzer is closed.
     */
    protected final Object getStoredValue(Analyzer analyzer) {
      if (analyzer.storedValue == null) {
        throw new AlreadyClosedException("this Analyzer is closed");
      }
      return analyzer.storedValue.get();
    }

    /**
     * Sets the stored value.
     *
     * @param storedValue Value to store
     * @throws AlreadyClosedException if the Analyzer is closed.
     */
    protected final void setStoredValue(Analyzer analyzer, Object storedValue) {
      if (analyzer.storedValue == null) {
        throw new AlreadyClosedException("this Analyzer is closed");
      }
      analyzer.storedValue.set(storedValue);
    }

  }

  /**
   * A predefined {@link ReuseStrategy}  that reuses the same components for
   * every field.
   * 这是一种默认的全局策略
   */
  public static final ReuseStrategy GLOBAL_REUSE_STRATEGY = new ReuseStrategy() {

    @Override
    public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
      return (TokenStreamComponents) getStoredValue(analyzer);
    }

    @Override
    public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents components) {
      setStoredValue(analyzer, components);
    }
  };

  /**
   * A predefined {@link ReuseStrategy} that reuses components per-field by
   * maintaining a Map of TokenStreamComponent per field name.
   * 基于每个 field 进行重用
   */
  public static final ReuseStrategy PER_FIELD_REUSE_STRATEGY = new ReuseStrategy() {

    @SuppressWarnings("unchecked")
    @Override
    public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
      // 获取本地线程存储的值    以每个线程为单位 都会维护一个 field 与 components的映射
      Map<String, TokenStreamComponents> componentsPerField = (Map<String, TokenStreamComponents>) getStoredValue(analyzer);
      return componentsPerField != null ? componentsPerField.get(fieldName) : null;
    }

    /**
     * 存储数据 以便重用
     * @param analyzer
     * @param fieldName Name of the field whose TokenStreamComponents are being set
     * @param components TokenStreamComponents which are to be reused for the field
     */
    @SuppressWarnings("unchecked")
    @Override
    public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents components) {
      Map<String, TokenStreamComponents> componentsPerField = (Map<String, TokenStreamComponents>) getStoredValue(analyzer);
      if (componentsPerField == null) {
        componentsPerField = new HashMap<>();
        setStoredValue(analyzer, componentsPerField);
      }
      componentsPerField.put(fieldName, components);
    }
  };

  /**
   * 一个基于字符串 token 流
   */
  private static final class StringTokenStream extends TokenStream {

    /**
     * 内部的字符串
     */
    private final String value;
    private final int length;
    private boolean used = true;
    // 增加2个特殊的 attribute  这里通过工厂对象会自动找到 CharTermAttributeImpl  和  OffsetAttributeImpl
    // 都是简单的bean对象
    private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);

    StringTokenStream(AttributeFactory attributeFactory, String value, int length) {
      super(attributeFactory);
      this.value = value;
      this.length = length;
    }

    @Override
    public void reset() {
      used = false;
    }

    /**
     * 往内部添加token
     * @return
     */
    @Override
    public boolean incrementToken() {
      if (used) {
        return false;
      }
      clearAttributes();
      // 往 char[] 内部填充数据
      termAttribute.append(value);
      // 初始化偏移量
      offsetAttribute.setOffset(0, length);
      used = true;
      return true;
    }

    @Override
    public void end() throws IOException {
      super.end();
      offsetAttribute.setOffset(length, length);
    }
  }
}
