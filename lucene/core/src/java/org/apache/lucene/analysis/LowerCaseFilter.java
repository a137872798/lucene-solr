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

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Normalizes token text to lower case.
 * 该对象在处理数据前都会转换成小写
 */
public class LowerCaseFilter extends TokenFilter {

  /**
   * 在 super(in) 中 termAtt 会复用in.termAtt 所以      CharacterUtils.toLowerCase(termAtt.buffer(), 0, termAtt.length()); 实际上termAtt.buffer() 读取的是被包装的in解析的后填充token的容器
   */
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  
  /**
   * Create a new LowerCaseFilter, that normalizes token text to lower case.
   * 
   * @param in TokenStream to filter
   */
  public LowerCaseFilter(TokenStream in) {
    super(in);
  }
  
  @Override
  public final boolean incrementToken() throws IOException {
    // 当下游解析到token后
    if (input.incrementToken()) {
      CharacterUtils.toLowerCase(termAtt.buffer(), 0, termAtt.length());
      return true;
    } else
      return false;
  }
}
