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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeFactory;

/** A grammar-based tokenizer constructed with JFlex.
 * <p>
 * This class implements the Word Break rules from the
 * Unicode Text Segmentation algorithm, as specified in 
 * <a href="http://unicode.org/reports/tr29/">Unicode Standard Annex #29</a>.
 * <p>Many applications have specific tokenizer needs.  If this tokenizer does
 * not suit your application, please consider copying this source code
 * directory to your project and maintaining your own grammar-based tokenizer.
 * 这是一个标准的词法解析器
 */

public final class StandardTokenizer extends Tokenizer {
  /** A private instance of the JFlex-constructed scanner */
  // 该对象负责扫描文本 以及解析token
  private StandardTokenizerImpl scanner;

  // 代表是什么语言类型  对应 scanner 通过dfa扫描出来的类型  scanner本身可以简易的理解成一个正则表达式 然后每种字符都必然要符合某种规则

  /** Alpha/numeric token type */
  public static final int ALPHANUM = 0;
  /** Numeric token type */
  public static final int NUM = 1;
  /** Southeast Asian token type */
  public static final int SOUTHEAST_ASIAN = 2;
  /** Ideographic token type */
  public static final int IDEOGRAPHIC = 3;
  /** Hiragana token type */
  public static final int HIRAGANA = 4;
  /** Katakana token type */
  public static final int KATAKANA = 5;
  /** Hangul token type */
  public static final int HANGUL = 6;
  /** Emoji token type. */
  public static final int EMOJI = 7;
  
  /** String token types that correspond to token type int constants */
  public static final String [] TOKEN_TYPES = new String [] {
    "<ALPHANUM>",
    "<NUM>",
    "<SOUTHEAST_ASIAN>",
    "<IDEOGRAPHIC>",
    "<HIRAGANA>",
    "<KATAKANA>",
    "<HANGUL>",
    "<EMOJI>"
  };
  
  /** Absolute maximum sized token */
  public static final int MAX_TOKEN_LENGTH_LIMIT = 1024 * 1024;

  /**
   * 记录一个标记位 便于之后回到该位置
   */
  private int skippedPositions;

  private int maxTokenLength = StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH;

  /**
   * Set the max allowed token length.  Tokens larger than this will be chopped
   * up at this token length and emitted as multiple tokens.  If you need to
   * skip such large tokens, you could increase this max length, and then
   * use {@code LengthFilter} to remove long tokens.  The default is
   * {@link StandardAnalyzer#DEFAULT_MAX_TOKEN_LENGTH}.
   * 
   * @throws IllegalArgumentException if the given length is outside of the
   *  range [1, {@value #MAX_TOKEN_LENGTH_LIMIT}].
   *  设置token的最大长度 也就是一次最多允许读取多少数据
   */ 
  public void setMaxTokenLength(int length) {
    if (length < 1) {
      throw new IllegalArgumentException("maxTokenLength must be greater than zero");
    } else if (length > MAX_TOKEN_LENGTH_LIMIT) {
      throw new IllegalArgumentException("maxTokenLength may not exceed " + MAX_TOKEN_LENGTH_LIMIT);
    }
    if (length != maxTokenLength) {
      maxTokenLength = length;
      scanner.setBufferSize(length);
    }
  }

  /** Returns the current maximum token length
   * 
   *  @see #setMaxTokenLength */
  public int getMaxTokenLength() {
    return maxTokenLength;
  }

  /**
   * Creates a new instance of the {@link org.apache.lucene.analysis.standard.StandardTokenizer}.  Attaches
   * the <code>input</code> to the newly created JFlex scanner.

   * See http://issues.apache.org/jira/browse/LUCENE-1068
   */
  public StandardTokenizer() {
    init();
  }

  /**
   * Creates a new StandardTokenizer with a given {@link org.apache.lucene.util.AttributeFactory}
   * 这里指定了 使用的 attrFactory
   * 在没有指定 factory的情况下 会使用一个 DefaultAttributeFactory
   */
  public StandardTokenizer(AttributeFactory factory) {
    super(factory);
    init();
  }

  /**
   * 基于传入的文本资源初始化 词法解析器
   */
  private void init() {
    this.scanner = new StandardTokenizerImpl(input);
  }

  // this tokenizer generates three attributes:
  // term offset, positionIncrement and type
  // 标准token解析器有自己固定的一套 attribute对象   实际上在TokenStream这层已经确定了这些attr 会统一返回 PackedTokenAttributeImpl 作为实现类
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);


  /*
   * (non-Javadoc)
   *
   * @see org.apache.lucene.analysis.TokenStream#next()
   * 解析出下一个token
   */
  @Override
  public final boolean incrementToken() throws IOException {
    // 每当读取一个新的token时 需要重置state上所有的 attr
    clearAttributes();
    skippedPositions = 0;

    while(true) {
      // 根据token类型走不同的逻辑
      int tokenType = scanner.getNextToken();

      // 代表读取到末尾了
      if (tokenType == StandardTokenizerImpl.YYEOF) {
        return false;
      }

      // 读取到一个合理的token后 设置token的位置
      if (scanner.yylength() <= maxTokenLength) {
        posIncrAtt.setPositionIncrement(skippedPositions+1);
        // 将解析出来的token 转存到 termAtt 中
        scanner.getText(termAtt);
        final int start = scanner.yychar();
        // 记录起始/终止偏移量
        offsetAtt.setOffset(correctOffset(start), correctOffset(start+termAtt.length()));
        // 找到本次的token类型
        typeAtt.setType(StandardTokenizer.TOKEN_TYPES[tokenType]);
        return true;
      // 太长的token 会被忽略
      } else
        // When we skip a too-long term, we still increment the
        // position increment
        skippedPositions++;
    }
  }

  /**
   * 在reader对象解析完成时调用
   * @throws IOException
   */
  @Override
  public final void end() throws IOException {

    // 清除之前的 attr
    super.end();
    // set final offset
    int finalOffset = correctOffset(scanner.yychar() + scanner.yylength());
    // 最后还设置了一次 pos 和 offset信息

    // 更新偏移量
    offsetAtt.setOffset(finalOffset, finalOffset);
    // adjust any skipped tokens
    posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement()+skippedPositions);
  }

  @Override
  public void close() throws IOException {
    super.close();
    scanner.yyreset(input);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    scanner.yyreset(input);
    skippedPositions = 0;
  }
}
