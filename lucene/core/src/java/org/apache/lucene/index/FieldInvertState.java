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
package org.apache.lucene.index;

import org.apache.lucene.analysis.TokenStream; // javadocs
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 * This class tracks the number and position / offset parameters of terms
 * being added to the index. The information collected in this class is
 * also used to calculate the normalization factor for a field.
 * 
 * @lucene.experimental
 * 存储域的反转信息
 */
public final class FieldInvertState {
  final int indexCreatedVersionMajor;
  final String name;
  final IndexOptions indexOptions;
  /**
   * 代表当前指向的偏移量   每当解析token流时
   */
  int position;
  /**
   * 这个长度信息  每次会叠加 termFreq      长度信息跟 freq应该没关系吧???
   */
  int length;
  /**
   * 当解析某个token时   pos 没有变化  则numOverlap++   代表一种重复现象???
   */
  int numOverlap;
  int offset;
  int maxTermFrequency;
  /**
   * 就是该域下词去重后的数量   因为一个域下可能有多个相同的 term
   * 相同的term 在不同doc重复时 认为是出现了2次
   */
  int uniqueTermCount;
  // we must track these across field instances (multi-valued case)
  // 每当解析完某个token时  在 OffsetAttr中会记录新的 start 之后会设置到该属性中
  int lastStartOffset = 0;
  /**
   * 每当解析某个 term后 该值会与 position 同步
   */
  int lastPosition = 0;
  AttributeSource attributeSource;

  // 下面是 域携带的一些相关属性
  OffsetAttribute offsetAttribute;
  PositionIncrementAttribute posIncrAttribute;
  PayloadAttribute payloadAttribute;
  TermToBytesRefAttribute termAttribute;
  TermFrequencyAttribute termFreqAttribute;

  /** Creates {code FieldInvertState} for the specified
   *  field name.
   * @param indexCreatedVersionMajor 代表索引的版本号
   * @param name fieldName
   * @param indexOptions field中需要存储到索引文件中的信息
   */
  public FieldInvertState(int indexCreatedVersionMajor, String name, IndexOptions indexOptions) {
    this.indexCreatedVersionMajor = indexCreatedVersionMajor;
    this.name = name;
    this.indexOptions = indexOptions;
  }
  
  /** Creates {code FieldInvertState} for the specified
   *  field name and values for all fields. */
  public FieldInvertState(int indexCreatedVersionMajor, String name, IndexOptions indexOptions, int position, int length, int numOverlap, int offset, int maxTermFrequency, int uniqueTermCount) {
    this(indexCreatedVersionMajor, name, indexOptions);
    this.position = position;
    this.length = length;
    this.numOverlap = numOverlap;
    this.offset = offset;
    this.maxTermFrequency = maxTermFrequency;
    this.uniqueTermCount = uniqueTermCount;
  }

  /**
   * Re-initialize the state
   * 每当在一个新的 doc中处理之前出现过的 field 会通过该方法重置内部属性
   */
  void reset() {
    position = -1;
    length = 0;
    numOverlap = 0;
    offset = 0;
    maxTermFrequency = 0;
    uniqueTermCount = 0;
    lastStartOffset = 0;
    lastPosition = 0;
  }

  /**
   * Sets attributeSource to a new instance.
   * 传入一个可以提供属性的  属性源   从中抽取属性并赋值
   * 一般传入的就是 tokenStream
   */
  void setAttributeSource(AttributeSource attributeSource) {
    if (this.attributeSource != attributeSource) {
      this.attributeSource = attributeSource;
      termAttribute = attributeSource.getAttribute(TermToBytesRefAttribute.class);
      termFreqAttribute = attributeSource.addAttribute(TermFrequencyAttribute.class);
      posIncrAttribute = attributeSource.addAttribute(PositionIncrementAttribute.class);
      offsetAttribute = attributeSource.addAttribute(OffsetAttribute.class);
      payloadAttribute = attributeSource.getAttribute(PayloadAttribute.class);
    }
  }

  /**
   * Get the last processed term position.
   * @return the position
   */
  public int getPosition() {
    return position;
  }

  /**
   * Get total number of terms in this field.
   * @return the length
   */
  public int getLength() {
    return length;
  }

  /** Set length value. */
  public void setLength(int length) {
    this.length = length;
  }
  
  /**
   * Get the number of terms with <code>positionIncrement == 0</code>.
   * @return the numOverlap
   */
  public int getNumOverlap() {
    return numOverlap;
  }

  /** Set number of terms with {@code positionIncrement ==
   *  0}. */
  public void setNumOverlap(int numOverlap) {
    this.numOverlap = numOverlap;
  }
  
  /**
   * Get end offset of the last processed term.
   * @return the offset
   */
  public int getOffset() {
    return offset;
  }

  /**
   * Get the maximum term-frequency encountered for any term in the field.  A
   * field containing "the quick brown fox jumps over the lazy dog" would have
   * a value of 2, because "the" appears twice.
   */
  public int getMaxTermFrequency() {
    return maxTermFrequency;
  }
  
  /**
   * Return the number of unique terms encountered in this field.
   */
  public int getUniqueTermCount() {
    return uniqueTermCount;
  }

  /** Returns the {@link AttributeSource} from the {@link
   *  TokenStream} that provided the indexed tokens for this
   *  field. */
  public AttributeSource getAttributeSource() {
    return attributeSource;
  }
  
  /**
   * Return the field's name
   */
  public String getName() {
    return name;
  }

  /**
   * Return the version that was used to create the index, or 6 if it was created before 7.0.
   */
  public int getIndexCreatedVersionMajor() {
    return indexCreatedVersionMajor;
  }
  
  /**
   * Get the index options for this field
   */
  public IndexOptions getIndexOptions() {
    return indexOptions;
  }
}
