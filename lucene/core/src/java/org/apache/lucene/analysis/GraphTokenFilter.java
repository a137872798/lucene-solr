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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 * An abstract TokenFilter that exposes its input stream as a graph
 *
 * Call {@link #incrementBaseToken()} to move the root of the graph to the next
 * position in the TokenStream, {@link #incrementGraphToken()} to move along
 * the current graph, and {@link #incrementGraph()} to reset to the next graph
 * based at the current root.
 *
 * For example, given the stream 'a b/c:2 d e`, then with the base token at
 * 'a', incrementGraphToken() will produce the stream 'a b d e', and then
 * after calling incrementGraph() will produce the stream 'a c e'.
 * 图过滤器
 */
public abstract class GraphTokenFilter extends TokenFilter {

  /**
   * 对象池 可以从这里取出token 并通过reset进行初始化   就是为了减少GC开销
   */
  private final Deque<Token> tokenPool = new ArrayDeque<>();
  private final List<Token> currentGraph = new ArrayList<>();

  /**
   * The maximum permitted number of routes through a graph
   * 代表一个图链路中最多存在多少token
   */
  public static final int MAX_GRAPH_STACK_SIZE = 1000;

  /**
   * The maximum permitted read-ahead in the token stream
   */
  public static final int MAX_TOKEN_CACHE_SIZE = 100;

  /**
   * 起点
   */
  private Token baseToken;
  private int graphDepth;
  private int graphPos;
  private int trailingPositions = -1;
  private int finalOffsets = -1;

  private int stackSize;
  private int cacheSize;

  private final PositionIncrementAttribute posIncAtt;
  private final OffsetAttribute offsetAtt;

  /**
   * Create a new GraphTokenFilter
   */
  public GraphTokenFilter(TokenStream input) {
    super(input);
    this.posIncAtt = input.addAttribute(PositionIncrementAttribute.class);
    this.offsetAtt = input.addAttribute(OffsetAttribute.class);
  }

  /**
   * Move the root of the graph to the next token in the wrapped TokenStream
   *
   * @return {@code false} if the underlying stream is exhausted
   * 移动起点
   */
  protected final boolean incrementBaseToken() throws IOException {
    stackSize = 0;
    graphDepth = 0;
    graphPos = 0;
    Token oldBase = baseToken;
    // 将baseToken 指向新的token
    baseToken = nextTokenInStream(baseToken);
    if (baseToken == null) {
      return false;
    }
    // 将当前图清除后 重新将 root(baseToken) 添加到图中
    currentGraph.clear();
    currentGraph.add(baseToken);
    // 将当前属性赋予到 baseToken.attr上
    baseToken.attSource.copyTo(this);
    recycleToken(oldBase);
    return true;
  }

  /**
   * Move to the next token in the current route through the graph
   *
   * @return {@code false} if there are not more tokens in the current graph
   * 从图结构中 获取token
   */
  protected final boolean incrementGraphToken() throws IOException {
    if (graphPos < graphDepth) {
      graphPos++;
      currentGraph.get(graphPos).attSource.copyTo(this);
      return true;
    }
    // 获取图中的下一个token
    Token token = nextTokenInGraph(currentGraph.get(graphDepth));
    if (token == null) {
      return false;
    }
    graphDepth++;
    graphPos++;
    currentGraph.add(graphDepth, token);
    token.attSource.copyTo(this);
    return true;
  }

  /**
   * Reset to the root token again, and move down the next route through the graph
   *
   * @return false if there are no more routes through the graph
   */
  protected final boolean incrementGraph() throws IOException {
    if (baseToken == null) {
      return false;
    }
    graphPos = 0;
    for (int i = graphDepth; i >= 1; i--) {
      // 从下往上不断读取图中保存的token
      if (lastInStack(currentGraph.get(i)) == false) {
        currentGraph.set(i, nextTokenInStream(currentGraph.get(i)));
        for (int j = i + 1; j < graphDepth; j++) {
          currentGraph.set(j, nextTokenInGraph(currentGraph.get(j)));
        }
        if (stackSize++ > MAX_GRAPH_STACK_SIZE) {
          throw new IllegalStateException("Too many graph paths (> " + MAX_GRAPH_STACK_SIZE + ")");
        }
        currentGraph.get(0).attSource.copyTo(this);
        graphDepth = i;
        return true;
      }
    }
    return false;
  }

  /**
   * Return the number of trailing positions at the end of the graph
   *
   * NB this should only be called after {@link #incrementGraphToken()} has returned {@code false}
   */
  public int getTrailingPositions() {
    return trailingPositions;
  }

  @Override
  public void end() throws IOException {
    if (trailingPositions == -1) {
      input.end();
      trailingPositions = posIncAtt.getPositionIncrement();
      finalOffsets = offsetAtt.endOffset();
    }
    else {
      endAttributes();
      this.posIncAtt.setPositionIncrement(trailingPositions);
      this.offsetAtt.setOffset(finalOffsets, finalOffsets);
    }
  }

  @Override
  public void reset() throws IOException {
    input.reset();
    // new attributes can be added between reset() calls, so we can't reuse
    // token objects from a previous run
    tokenPool.clear();
    cacheSize = 0;
    graphDepth = 0;
    trailingPositions = -1;
    finalOffsets = -1;
    baseToken = null;
  }

  int cachedTokenCount() {
    return cacheSize;
  }

  private Token newToken() {
    if (tokenPool.size() == 0) {
      cacheSize++;
      if (cacheSize > MAX_TOKEN_CACHE_SIZE) {
        throw new IllegalStateException("Too many cached tokens (> " + MAX_TOKEN_CACHE_SIZE + ")");
      }
      return new Token(this.cloneAttributes());
    }
    Token token = tokenPool.removeFirst();
    token.reset(input);
    return token;
  }

  /**
   * 将给与的token 存入到池中
   * @param token
   */
  private void recycleToken(Token token) {
    if (token == null)
      return;
    token.nextToken = null;
    tokenPool.add(token);
  }

  /**
   * 获取当前token构成的图中的下一个token
   * @param token
   * @return
   * @throws IOException
   */
  private Token nextTokenInGraph(Token token) throws IOException {
    int remaining = token.length();
    do {
      token = nextTokenInStream(token);
      if (token == null) {
        return null;
      }
      remaining -= token.posInc();
    } while (remaining > 0);
    return token;
  }

  // check if the next token in the tokenstream is at the same position as this one
  private boolean lastInStack(Token token) throws IOException {
    Token next = nextTokenInStream(token);
    return next == null || next.posInc() != 0;
  }

  private Token nextTokenInStream(Token token) throws IOException {
    if (token != null && token.nextToken != null) {
      return token.nextToken;
    }
    if (this.trailingPositions != -1) {
      // already hit the end
      return null;
    }
    if (input.incrementToken() == false) {
      input.end();
      trailingPositions = posIncAtt.getPositionIncrement();
      finalOffsets = offsetAtt.endOffset();
      return null;
    }
    if (token == null) {
      return newToken();
    }
    token.nextToken = newToken();
    return token.nextToken;
  }

  /**
   * 代表一个token对象
   */
  private static class Token {

    /**
     * 每个token 绑定一个 source 对象
     */
    final AttributeSource attSource;
    final PositionIncrementAttribute posIncAtt;
    final PositionLengthAttribute lengthAtt;
    /**
     * token 本身构成一个链
     */
    Token nextToken;

    Token(AttributeSource attSource) {
      this.attSource = attSource;
      this.posIncAtt = attSource.addAttribute(PositionIncrementAttribute.class);
      boolean hasLengthAtt = attSource.hasAttribute(PositionLengthAttribute.class);
      this.lengthAtt = hasLengthAtt ? attSource.addAttribute(PositionLengthAttribute.class) : null;
    }

    int posInc() {
      return this.posIncAtt.getPositionIncrement();
    }

    int length() {
      if (this.lengthAtt == null) {
        return 1;
      }
      return this.lengthAtt.getPositionLength();
    }

    void reset(AttributeSource attSource) {
      attSource.copyTo(this.attSource);
      this.nextToken = null;
    }

    @Override
    public String toString() {
      return attSource.toString();
    }
  }

}
