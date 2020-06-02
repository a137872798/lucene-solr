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
package org.apache.lucene.analysis.util;


import java.io.IOException;
import java.util.Objects;
import java.util.function.IntPredicate;

import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.analysis.CharacterUtils.CharacterBuffer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeFactory;

import static org.apache.lucene.analysis.standard.StandardTokenizer.MAX_TOKEN_LENGTH_LIMIT;

/**
 * An abstract base class for simple, character-oriented tokenizers.
 * <p>
 * The base class also provides factories to create instances of
 * {@code CharTokenizer} using Java 8 lambdas or method references.
 * It is possible to create an instance which behaves exactly like
 * {@link LetterTokenizer}:
 * <pre class="prettyprint lang-java">
 * Tokenizer tok = CharTokenizer.fromTokenCharPredicate(Character::isLetter);
 * </pre>
 * 基于 char字符的词法解析器
 */
public abstract class CharTokenizer extends Tokenizer {

    /**
     * Creates a new {@link CharTokenizer} instance
     */
    public CharTokenizer() {
        this.maxTokenLen = DEFAULT_MAX_WORD_LEN;
    }

    /**
     * Creates a new {@link CharTokenizer} instance
     *
     * @param factory the attribute factory to use for this {@link Tokenizer}   该对象负责创建 Attribute的实现类
     */
    public CharTokenizer(AttributeFactory factory) {
        super(factory);
        this.maxTokenLen = DEFAULT_MAX_WORD_LEN;
    }

    /**
     * Creates a new {@link CharTokenizer} instance
     *
     * @param factory     the attribute factory to use for this {@link Tokenizer}
     * @param maxTokenLen maximum token length the tokenizer will emit.
     *                    Must be greater than 0 and less than MAX_TOKEN_LENGTH_LIMIT (1024*1024)
     * @throws IllegalArgumentException if maxTokenLen is invalid.
     */
    public CharTokenizer(AttributeFactory factory, int maxTokenLen) {
        super(factory);
        if (maxTokenLen > MAX_TOKEN_LENGTH_LIMIT || maxTokenLen <= 0) {
            throw new IllegalArgumentException("maxTokenLen must be greater than 0 and less than " + MAX_TOKEN_LENGTH_LIMIT + " passed: " + maxTokenLen);
        }
        this.maxTokenLen = maxTokenLen;
    }

    /**
     * Creates a new instance of CharTokenizer using a custom predicate, supplied as method reference or lambda expression.
     * The predicate should return {@code true} for all valid token characters.
     * <p>
     * This factory is intended to be used with lambdas or method references. E.g., an elegant way
     * to create an instance which behaves exactly as {@link LetterTokenizer} is:
     * <pre class="prettyprint lang-java">
     * Tokenizer tok = CharTokenizer.fromTokenCharPredicate(Character::isLetter);
     * </pre>
     */
    public static CharTokenizer fromTokenCharPredicate(final IntPredicate tokenCharPredicate) {
        return fromTokenCharPredicate(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, tokenCharPredicate);
    }

    /**
     * Creates a new instance of CharTokenizer with the supplied attribute factory using a custom predicate, supplied as method reference or lambda expression.
     * The predicate should return {@code true} for all valid token characters.
     * <p>
     * This factory is intended to be used with lambdas or method references. E.g., an elegant way
     * to create an instance which behaves exactly as {@link LetterTokenizer} is:
     * <pre class="prettyprint lang-java">
     * Tokenizer tok = CharTokenizer.fromTokenCharPredicate(factory, Character::isLetter);
     * </pre>
     * 通过一个谓语初始化该对象
     */
    public static CharTokenizer fromTokenCharPredicate(AttributeFactory factory, final IntPredicate tokenCharPredicate) {
        Objects.requireNonNull(tokenCharPredicate, "predicate must not be null.");
        return new CharTokenizer(factory) {
            /**
             * 使用谓语判断 给与的值是否满足条件
             * @param c
             * @return
             */
            @Override
            protected boolean isTokenChar(int c) {
                return tokenCharPredicate.test(c);
            }
        };
    }

    /**
     * Creates a new instance of CharTokenizer using a custom predicate, supplied as method reference or lambda expression.
     * The predicate should return {@code true} for all valid token separator characters.
     * This method is provided for convenience to easily use predicates that are negated
     * (they match the separator characters, not the token characters).
     * <p>
     * This factory is intended to be used with lambdas or method references. E.g., an elegant way
     * to create an instance which behaves exactly as {@link WhitespaceTokenizer} is:
     * <pre class="prettyprint lang-java">
     * Tokenizer tok = CharTokenizer.fromSeparatorCharPredicate(Character::isWhitespace);
     * </pre>
     */
    public static CharTokenizer fromSeparatorCharPredicate(final IntPredicate separatorCharPredicate) {
        return fromSeparatorCharPredicate(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, separatorCharPredicate);
    }

    /**
     * Creates a new instance of CharTokenizer with the supplied attribute factory using a custom predicate, supplied as method reference or lambda expression.
     * The predicate should return {@code true} for all valid token separator characters.
     * <p>
     * This factory is intended to be used with lambdas or method references. E.g., an elegant way
     * to create an instance which behaves exactly as {@link WhitespaceTokenizer} is:
     * <pre class="prettyprint lang-java">
     * Tokenizer tok = CharTokenizer.fromSeparatorCharPredicate(factory, Character::isWhitespace);
     * </pre>
     * 取反
     */
    public static CharTokenizer fromSeparatorCharPredicate(AttributeFactory factory, final IntPredicate separatorCharPredicate) {
        return fromTokenCharPredicate(factory, separatorCharPredicate.negate());
    }

    /**
     * 记录某个 数据流的相关标识 而 Tokenizer 就是用于解析数据流的
     */
    private int offset = 0,
            bufferIndex = 0,  // 当前读取到的buffer的偏移量
            dataLen = 0,   // 数据总长度
            finalOffset = 0;
    // 代表某个词的大小
    public static final int DEFAULT_MAX_WORD_LEN = 255;
    private static final int IO_BUFFER_SIZE = 4096;
    private final int maxTokenLen;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    /**
     * 一个简单的 char[] 包装对象
     */
    private final CharacterBuffer ioBuffer = CharacterUtils.newCharacterBuffer(IO_BUFFER_SIZE);

    /**
     * Returns true iff a codepoint should be included in a token. This tokenizer
     * generates as tokens adjacent sequences of codepoints which satisfy this
     * predicate. Codepoints for which this is false are used to define token
     * boundaries and are not included in tokens.
     */
    protected abstract boolean isTokenChar(int c);

    /**
     * 基于当前的数据流 读取一个token
     *
     * @return
     * @throws IOException
     */
    @Override
    public final boolean incrementToken() throws IOException {
        // 将state 链表上所有的 attr 都清空
        clearAttributes();
        int length = 0;
        int start = -1; // this variable is always initialized
        int end = -1;
        // 返回该 attr内部的数组对象  就是用该对象去填装数据
        char[] buffer = termAtt.buffer();
        // 不断读取数据到buffer中
        while (true) {
            // 一开始 bufferIndex 和 dataLen 都是0 所以从输入流中读取数据到 ioBuffer中
            if (bufferIndex >= dataLen) {
                offset += dataLen;
                // 从输入流中读取数据填满 buffer
                CharacterUtils.fill(ioBuffer, input); // read supplementary char aware with CharacterUtils
                // 代表无数据可读了
                if (ioBuffer.getLength() == 0) {
                    dataLen = 0; // so next offset += dataLen won't decrement offset
                    // 代表至少读取到一次数据
                    if (length > 0) {
                        break;
                    } else {
                        // 代表一次数据没有读取到
                        finalOffset = correctOffset(offset);
                        return false;
                    }
                }
                // 更新当前读取的数据长度
                dataLen = ioBuffer.getLength();
                // 重置bufferIndex
                bufferIndex = 0;
            }
            // use CharacterUtils here to support < 3.1 UTF-16 code unit behavior if the char based methods are gone
            // 以 UTF-16 进行解码  他的特性是一般1个char 或者2个char 来表示一个字符  如果使用2个char 那么高位和低位会有一个大小限制
            // 可以通过这种方式判断 读取到的是一个2char的字符  还是一个1char的字符
            // 这里就是从 ioBuffer中读取数据 并尝试转移到 termAtt.buffer 中
            final int c = Character.codePointAt(ioBuffer.getBuffer(), bufferIndex, ioBuffer.getLength());
            // 代表该代码点是由一个 char 还是2个char 组成的  也就是对应UTF-16变长的编码方式
            final int charCount = Character.charCount(c);
            // 这里代表读取了一个char 值
            bufferIndex += charCount;

            // 尝试解析读取到的char (因为UTF-16 可能一个字符占用2个char 所以用int来表示)
            // 这里的意思就是 如果c 满足了token的条件 就存入到termAtt.buffer中
            if (isTokenChar(c)) {               // if it's a token char
                // 代表第一个token
                if (length == 0) {                // start of token
                    assert start == -1;
                    // 这里设置起点和终点
                    start = offset + bufferIndex - charCount;
                    end = start;
                    // 避免容量不足 进行扩容
                } else if (length >= buffer.length - 1) { // check if a supplementary could run out of bounds
                    // 对数组进行扩容
                    buffer = termAtt.resizeBuffer(2 + length); // make sure a supplementary fits in the buffer
                }
                end += charCount;
                // 增加读取的长度 同时将数据写入到buffer中
                length += Character.toChars(c, buffer, length); // buffer it, normalized
                if (length >= maxTokenLen) { // buffer overflow! make sure to check for >= surrogate pair could break == test
                    break;
                }
            } else if (length > 0) {           // at non-Letter w/ chars
                break;                           // return 'em
            }
        }

        // 标记当前数据内数据的长度
        termAtt.setLength(length);
        assert start != -1;
        // 设置偏移量
        offsetAtt.setOffset(correctOffset(start), finalOffset = correctOffset(end));
        return true;

    }

    /**
     * 当调用 end时 修改 offsetAttr的属性
     *
     * @throws IOException
     */
    @Override
    public final void end() throws IOException {
        super.end();
        // set final offset
        offsetAtt.setOffset(finalOffset, finalOffset);
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        bufferIndex = 0;
        offset = 0;
        dataLen = 0;
        finalOffset = 0;
        ioBuffer.reset(); // make sure to reset the IO buffer!!
    }
}
