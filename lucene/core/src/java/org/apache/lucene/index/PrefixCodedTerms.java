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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.StringHelper;

/**
 * Prefix codes term instances (prefixes are shared). This is expected to be
 * faster to build than a FST and might also be more compact if there are no
 * common suffixes.
 * @lucene.internal
 * 代表一个 共享前缀的对象     使用特殊的规则读取内部的数据  因为内部的数据是已经省略掉相同前缀的
 */
public class PrefixCodedTerms implements Accountable {
  private final List<ByteBuffer> content;
  private final long size;
  /**
   * 描述该对象关联的 删除年代
   */
  private long delGen;
  private int lazyHash;

  private PrefixCodedTerms(List<ByteBuffer> content, long size) {
    this.content = Objects.requireNonNull(content);
    this.size = size;
  }

  @Override
  public long ramBytesUsed() {
    return content.stream().mapToLong(buf -> buf.capacity()).sum() + 2 * Long.BYTES; 
  }

  /** Records del gen for this packet. */
  public void setDelGen(long delGen) {
    this.delGen = delGen;
  }
  
  /** Builds a PrefixCodedTerms: call add repeatedly, then finish. */
  // 该对象用于构建 PrefixCodedTerms
  public static class Builder {

    /**
     * 所有的数据都会写入到这里 如果出现相同前缀 就不会重复写入
     */
    private ByteBuffersDataOutput output = new ByteBuffersDataOutput();
    /**
     * 对应最后一个term
     */
    private Term lastTerm = new Term("");
    /**
     * 对应最后一个写入的 term 的全部数据
     */
    private BytesRefBuilder lastTermBytes = new BytesRefBuilder();
    /**
     * 记录当前总计写入了多少 term
     */
    private long size;

    /** Sole constructor. */
    public Builder() {}

    /** add a term */
    public void add(Term term) {
      add(term.field(), term.bytes());
    }

    /**
     * add a term.  This fully consumes in the incoming {@link BytesRef}.
     * @param field  term所属的 field
     * @param bytes  term的数据     该方法会抽取 term的公共前缀
     */
    public void add(String field, BytesRef bytes) {
      assert lastTerm.equals(new Term("")) || new Term(field, bytes).compareTo(lastTerm) > 0;

      try {
        final int prefix;
        // 当本次写入的 term.field 与上次写入的相同时
        if (size > 0 && field.equals(lastTerm.field)) {
          // same field as the last term
          // 找到相同前缀的长度  通过 << 1 与无前缀时的 1 区分开  之后从output读取的时候 可以反向推导之前写入的数据
          prefix = StringHelper.bytesDifference(lastTerm.bytes, bytes);
          output.writeVInt(prefix << 1);
        } else {
          // field change
          // 首次写入 会将全部数据作为前缀
          prefix = 0;
          output.writeVInt(1);
          output.writeString(field);
        }

        // 获取后缀的部分  、
        int suffix = bytes.length - prefix;
        output.writeVInt(suffix);
        // 从这里可以看出 prefix 是不需要写入的  直接写入suffix 就好
        output.writeBytes(bytes.bytes, bytes.offset + prefix, suffix);
        lastTermBytes.copyBytes(bytes);
        lastTerm.bytes = lastTermBytes.get();
        lastTerm.field = field;
        size += 1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    /** return finalized form */
    // 对象构造完成
    public PrefixCodedTerms finish() {
      return new PrefixCodedTerms(output.toBufferList(), size);
    }
  }

  /** An iterator over the list of terms stored in a {@link PrefixCodedTerms}. */
  // 该对象负责遍历 content内部的数据  (按照特殊的规则写入数据 所以解读时也要按照特殊的规则)
  public static class TermIterator extends FieldTermIterator {
    final ByteBuffersDataInput input;
    final BytesRefBuilder builder = new BytesRefBuilder();
    final BytesRef bytes = builder.get();
    final long end;
    final long delGen;
    String field = "";

    private TermIterator(long delGen, ByteBuffersDataInput input) {
      this.input = input;
      end = input.size();
      this.delGen = delGen;
    }

    /**
     * 读取下一个数据
     * @return
     */
    @Override
    public BytesRef next() {
      if (input.position() < end) {
        try {
          // code 标明之前无公共前缀 又或者有 并且长度是多少
          int code = input.readVInt();
          // 代表之前无公共前缀
          boolean newField = (code & 1) != 0;
          if (newField) {
            // 更新此时的 field
            field = input.readString();
          }
          int prefix = code >>> 1;
          // 下一个值就是后缀的长度
          int suffix = input.readVInt();
          // 将前缀长度 + 后缀长度的数据读取到 term 中
          readTermBytes(prefix, suffix);
          return bytes;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        field = null;
        return null;
      }
    }

    // TODO: maybe we should freeze to FST or automaton instead?
    private void readTermBytes(int prefix, int suffix) throws IOException {
      // 扩容确保 bytes中有足够的空间
      builder.grow(prefix + suffix);
      // 从 bytes的 prefix 开始写入   也就是发生了 公共前缀的时候 bytes 中的一部分数据可以留到下次使用 也就是加上 suffix 这段逻辑
      input.readBytes(builder.bytes(), prefix, suffix);
      builder.setLength(prefix + suffix);
    }

    // Copied from parent-class because javadoc doesn't do it for some reason
    /** Returns current field.  This method should not be called
     *  after iteration is done.  Note that you may use == to
     *  detect a change in field. */
    @Override
    public String field() {
      return field;
    }

    // Copied from parent-class because javadoc doesn't do it for some reason
    /** Del gen of the current term. */
    @Override
    public long delGen() {
      return delGen;
    }
  }

  /** Return an iterator over the terms stored in this {@link PrefixCodedTerms}. */
  public TermIterator iterator() {
    return new TermIterator(delGen, new ByteBuffersDataInput(content));
  }

  /** Return the number of terms stored in this {@link PrefixCodedTerms}. */
  public long size() {
    return size;
  }

  @Override
  public int hashCode() {
    if (lazyHash == 0) {
      int h = 1;
      for (ByteBuffer bb : content) {
        h = h + 31 * bb.hashCode();
      }
      h = 31 * h + (int) (delGen ^ (delGen >>> 32));
      lazyHash = h;
    }
    return lazyHash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) { 
      return false;
    }

    PrefixCodedTerms other = (PrefixCodedTerms) obj;
    return delGen == other.delGen &&
           this.content.equals(other.content);
  }
}
