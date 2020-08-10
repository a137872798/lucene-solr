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
package org.apache.lucene.analysis.tokenattributes;


import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/** Default implementation of the common attributes used by Lucene:<ul>
 * <li>{@link CharTermAttribute}
 * <li>{@link TypeAttribute}
 * <li>{@link PositionIncrementAttribute}
 * <li>{@link PositionLengthAttribute}
 * <li>{@link OffsetAttribute}
 * <li>{@link TermFrequencyAttribute}
 * </ul>*/
// 该对象已经组合了多种默认的 attr
public class PackedTokenAttributeImpl extends CharTermAttributeImpl   // 该对象就是 char[] 的包装类
                   implements TypeAttribute, PositionIncrementAttribute,
                              PositionLengthAttribute, OffsetAttribute,
                              TermFrequencyAttribute {

  // 代表起始偏移量和终止偏移量
  private int startOffset,endOffset;
  private String type = DEFAULT_TYPE;
  private int positionIncrement = 1;
  private int positionLength = 1;
  private int termFrequency = 1;

  /** Constructs the attribute implementation. */
  public PackedTokenAttributeImpl() {
  }

  /**
   * {@inheritDoc}
   * @see PositionIncrementAttribute
   * 记录某个token相比上一个token position的增量信息 (太长的token 会跳过)
   * 比如 token1 positionIncrement=1 相比token0 增量值为1
   *      token2 太长不记录position
   *      token3 相比token1 增量值就是2  (因为中间还有个token2被跳过了)
   */
  @Override
  public void setPositionIncrement(int positionIncrement) {
    if (positionIncrement < 0) {
      throw new IllegalArgumentException("Increment must be zero or greater: " + positionIncrement);
    }
    this.positionIncrement = positionIncrement;
  }

  /**
   * {@inheritDoc}
   * @see PositionIncrementAttribute
   */
  @Override
  public int getPositionIncrement() {
    return positionIncrement;
  }

  /**
   * {@inheritDoc}
   * @see PositionLengthAttribute
   */
  @Override
  public void setPositionLength(int positionLength) {
    if (positionLength < 1) {
      throw new IllegalArgumentException("Position length must be 1 or greater: got " + positionLength);
    }
    this.positionLength = positionLength;
  }

  /**
   * {@inheritDoc}
   * @see PositionLengthAttribute
   */
  @Override
  public int getPositionLength() {
    return positionLength;
  }

  /**
   * {@inheritDoc}
   * @see OffsetAttribute
   */
  @Override
  public final int startOffset() {
    return startOffset;
  }

  /**
   * {@inheritDoc}
   * @see OffsetAttribute
   */
  @Override
  public final int endOffset() {
    return endOffset;
  }

  /**
   * {@inheritDoc}
   * @see OffsetAttribute
   * 记录当前token 的偏移量信息 同时包含起始位置和终止位置
   */
  @Override
  public void setOffset(int startOffset, int endOffset) {
    if (startOffset < 0 || endOffset < startOffset) {
      throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset; got "
          + "startOffset=" + startOffset + ",endOffset=" + endOffset);
    }
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  /**
   * {@inheritDoc}
   * @see TypeAttribute
   */
  @Override
  public final String type() {
    return type;
  }

  /**
   * {@inheritDoc}
   * @see TypeAttribute
   * 记录本次token对应的类型
   */
  @Override
  public final void setType(String type) {
    this.type = type;
  }

  @Override
  public final void setTermFrequency(int termFrequency) {
    if (termFrequency < 1) {
      throw new IllegalArgumentException("Term frequency must be 1 or greater; got " + termFrequency);
    }
    this.termFrequency = termFrequency;
  }

  @Override
  public final int getTermFrequency() {
    return termFrequency;
  }

  /** Resets the attributes
   */
  @Override
  public void clear() {
    // 将上层的char[] 置空
    super.clear();
    positionIncrement = positionLength = 1;
    termFrequency = 1;
    startOffset = endOffset = 0;
    type = DEFAULT_TYPE;
  }
  
  /** Resets the attributes at end
   * 调用super.end() 会转发到 clear 上 重置内部属性
   */
  @Override
  public void end() {
    super.end();
    // super.end already calls this.clear, so we only set values that are different from clear:
    positionIncrement = 0;
  }

  @Override
  public PackedTokenAttributeImpl clone() {
    return (PackedTokenAttributeImpl) super.clone();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this)
      return true;

    if (obj instanceof PackedTokenAttributeImpl) {
      final PackedTokenAttributeImpl other = (PackedTokenAttributeImpl) obj;
      return (startOffset == other.startOffset &&
          endOffset == other.endOffset && 
          positionIncrement == other.positionIncrement &&
          positionLength == other.positionLength &&
          (type == null ? other.type == null : type.equals(other.type)) &&
          termFrequency == other.termFrequency &&
          super.equals(obj)
      );
    } else
      return false;
  }

  @Override
  public int hashCode() {
    int code = super.hashCode();
    code = code * 31 + startOffset;
    code = code * 31 + endOffset;
    code = code * 31 + positionIncrement;
    code = code * 31 + positionLength;
    if (type != null)
      code = code * 31 + type.hashCode();
    code = code * 31 + termFrequency;;
    return code;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    if (target instanceof PackedTokenAttributeImpl) {
      final PackedTokenAttributeImpl to = (PackedTokenAttributeImpl) target;
      to.copyBuffer(buffer(), 0, length());
      to.positionIncrement = positionIncrement;
      to.positionLength = positionLength;
      to.startOffset = startOffset;
      to.endOffset = endOffset;
      to.type = type;
      to.termFrequency = termFrequency;
    } else {
      super.copyTo(target);
      ((OffsetAttribute) target).setOffset(startOffset, endOffset);
      ((PositionIncrementAttribute) target).setPositionIncrement(positionIncrement);
      ((PositionLengthAttribute) target).setPositionLength(positionLength);
      ((TypeAttribute) target).setType(type);
      ((TermFrequencyAttribute) target).setTermFrequency(termFrequency);
    }
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    super.reflectWith(reflector);
    reflector.reflect(OffsetAttribute.class, "startOffset", startOffset);
    reflector.reflect(OffsetAttribute.class, "endOffset", endOffset);
    reflector.reflect(PositionIncrementAttribute.class, "positionIncrement", positionIncrement);
    reflector.reflect(PositionLengthAttribute.class, "positionLength", positionLength);
    reflector.reflect(TypeAttribute.class, "type", type);
    reflector.reflect(TermFrequencyAttribute.class, "termFrequency", termFrequency);
  }
}
