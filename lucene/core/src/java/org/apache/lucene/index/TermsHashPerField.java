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

import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRefHash.BytesStartArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/**
 * 对 TermsHash 调用 addField 时 会返回该对象的子类
 */
abstract class TermsHashPerField implements Comparable<TermsHashPerField> {

  private static final int HASH_INIT_SIZE = 4;

  /**
   * 该对象存储于哪个 hash桶
   */
  final TermsHash termsHash;

  /**
   * 目前只有一种情况 就是  freqPerField 下面连接 termVectorPerField
   */
  final TermsHashPerField nextPerField;
  protected final DocumentsWriterPerThread.DocState docState;
  protected final FieldInvertState fieldState;
  TermToBytesRefAttribute termAtt;
  protected TermFrequencyAttribute termFreqAtt;

  // Copied from our perThread
  // 这些对象 使用 thread 的 allocator进行初始化 从 TermHash 传过来
  /**
   * 该对象存储了 bytePool 的 endIndex
   */
  final IntBlockPool intPool;
  final ByteBlockPool bytePool;
  final ByteBlockPool termBytePool;

  final int streamCount;
  final int numPostingInt;

  protected final FieldInfo fieldInfo;

  /**
   * 该对象借助 pool 对象写入数据  利用了pool自动扩容且不需要大量的数据拷贝的特点
   * 该对象本身只维护了  hash到 id 到 pool偏移量的映射关系
   */
  final BytesRefHash bytesHash;

  /**
   * 该对象内部就是很多int[] 每个数组负责存储一种数据
   */
  ParallelPostingsArray postingsArray;
  private final Counter bytesUsed;

  /** streamCount: how many streams this field stores per term.
   * E.g. doc(+freq) is 1 stream, prox+offset is a second. */

  /**
   *
   * @param streamCount 代表存储几种数据流  只有频率时是1   除此之外如果有 offset position 之类的就是2
   * @param fieldState
   * @param termsHash
   * @param nextPerField   该对象本身也是链式结构  目前只有 FreqProxTermsWriterPerField 有下游对象
   * @param fieldInfo
   */
  public TermsHashPerField(int streamCount, FieldInvertState fieldState, TermsHash termsHash, TermsHashPerField nextPerField, FieldInfo fieldInfo) {
    intPool = termsHash.intPool;
    bytePool = termsHash.bytePool;
    termBytePool = termsHash.termBytePool;
    docState = termsHash.docState;
    this.termsHash = termsHash;
    bytesUsed = termsHash.bytesUsed;
    this.fieldState = fieldState;
    this.streamCount = streamCount;
    numPostingInt = 2*streamCount;
    this.fieldInfo = fieldInfo;
    this.nextPerField = nextPerField;
    PostingsBytesStartArray byteStarts = new PostingsBytesStartArray(this, bytesUsed);
    // 默认情况下 存储 byteRefId 与 pool偏移量关系的对象是  DirectBytesStartArray 对象 而这里替换成了 PostingsBytesStartArray
    bytesHash = new BytesRefHash(termBytePool, HASH_INIT_SIZE, byteStarts);
  }

  /**
   * 重置 hash桶内部的数据 同时将重置指令传递到下游
   */
  void reset() {
    bytesHash.clear(false);
    if (nextPerField != null) {
      nextPerField.reset();
    }
  }

  /**
   *
   * @param reader   此时reader对象还没有初始化
   * @param termID  这里传入了某个词的id  看来一个文档被分词器处理后 每个词应该有自己的id
   * @param stream
   */
  public void initReader(ByteSliceReader reader, int termID, int stream) {
    assert stream < streamCount;
    // 将id 转换成 pool的 绝对偏移量   也就是所有的词都存储在 pool 对象中
    int intStart = postingsArray.intStarts[termID];
    // 存储int 类型数据的 block
    final int[] ints = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
    final int upto = intStart & IntBlockPool.INT_BLOCK_MASK;
    reader.init(bytePool,   // 通过 pool对象进行初始化
                postingsArray.byteStarts[termID]+stream*ByteBlockPool.FIRST_LEVEL_SIZE,  // 这里是 startIndex
                ints[upto+stream]);  // endIndex 是从 ints中获取的
  }

  int[] sortedTermIDs;

  /** Collapse the hash table and sort in-place; also sets
   * this.sortedTermIDs to the results */
  public int[] sortPostings() {
    sortedTermIDs = bytesHash.sort();
    return sortedTermIDs;
  }

  private boolean doNextCall;

  // Secondary entry point (for 2nd & subsequent TermsHash),
  // because token text has already been "interned" into
  // textStart, so we hash by textStart.  term vectors use
  // this API.
  public void add(int textStart) throws IOException {
    int termID = bytesHash.addByPoolOffset(textStart);
    if (termID >= 0) {      // New posting
      // First time we are seeing this token since we last
      // flushed the hash.
      // Init stream slices
      if (numPostingInt + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
        intPool.nextBuffer();
      }

      if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt*ByteBlockPool.FIRST_LEVEL_SIZE) {
        bytePool.nextBuffer();
      }

      intUptos = intPool.buffer;
      intUptoStart = intPool.intUpto;
      intPool.intUpto += streamCount;

      postingsArray.intStarts[termID] = intUptoStart + intPool.intOffset;

      for(int i=0;i<streamCount;i++) {
        final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        intUptos[intUptoStart+i] = upto + bytePool.byteOffset;
      }
      postingsArray.byteStarts[termID] = intUptos[intUptoStart];

      newTerm(termID);

    } else {
      termID = (-termID)-1;
      int intStart = postingsArray.intStarts[termID];
      intUptos = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
      intUptoStart = intStart & IntBlockPool.INT_BLOCK_MASK;
      addTerm(termID);
    }
  }

  /** Called once per inverted token.  This is the primary
   *  entry point (for first TermsHash); postings use this
   *  API. */
  void add() throws IOException {
    // We are first in the chain so we must "intern" the
    // term text into textStart address
    // Get the text & hash of this term.
    int termID = bytesHash.add(termAtt.getBytesRef());
      
    //System.out.println("add term=" + termBytesRef.utf8ToString() + " doc=" + docState.docID + " termID=" + termID);

    if (termID >= 0) {// New posting
      bytesHash.byteStart(termID);
      // Init stream slices
      if (numPostingInt + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
        intPool.nextBuffer();
      }

      if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt*ByteBlockPool.FIRST_LEVEL_SIZE) {
        bytePool.nextBuffer();
      }

      intUptos = intPool.buffer;
      intUptoStart = intPool.intUpto;
      intPool.intUpto += streamCount;

      postingsArray.intStarts[termID] = intUptoStart + intPool.intOffset;

      for(int i=0;i<streamCount;i++) {
        final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        intUptos[intUptoStart+i] = upto + bytePool.byteOffset;
      }
      postingsArray.byteStarts[termID] = intUptos[intUptoStart];

      newTerm(termID);

    } else {
      termID = (-termID)-1;
      int intStart = postingsArray.intStarts[termID];
      intUptos = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
      intUptoStart = intStart & IntBlockPool.INT_BLOCK_MASK;
      addTerm(termID);
    }

    if (doNextCall) {
      nextPerField.add(postingsArray.textStarts[termID]);
    }
  }

  int[] intUptos;
  int intUptoStart;

  void writeByte(int stream, byte b) {
    int upto = intUptos[intUptoStart+stream];
    byte[] bytes = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert bytes != null;
    int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;
    if (bytes[offset] != 0) {
      // End of slice; allocate a new one
      offset = bytePool.allocSlice(bytes, offset);
      bytes = bytePool.buffer;
      intUptos[intUptoStart+stream] = offset + bytePool.byteOffset;
    }
    bytes[offset] = b;
    (intUptos[intUptoStart+stream])++;
  }

  public void writeBytes(int stream, byte[] b, int offset, int len) {
    // TODO: optimize
    final int end = offset + len;
    for(int i=offset;i<end;i++)
      writeByte(stream, b[i]);
  }

  void writeVInt(int stream, int i) {
    assert stream < streamCount;
    while ((i & ~0x7F) != 0) {
      writeByte(stream, (byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte(stream, (byte) i);
  }

  /**
   * 该对象在 BytesRefHash 中使用
   */
  private static final class PostingsBytesStartArray extends BytesStartArray {

    /**
     * 该对象存储了某个域的信息 以及 term信息
     */
    private final TermsHashPerField perField;
    private final Counter bytesUsed;

    private PostingsBytesStartArray(
        TermsHashPerField perField, Counter bytesUsed) {
      this.perField = perField;
      this.bytesUsed = bytesUsed;
    }

    @Override
    public int[] init() {
      if (perField.postingsArray == null) {
        perField.postingsArray = perField.createPostingsArray(2);
        perField.newPostingsArray();
        bytesUsed.addAndGet(perField.postingsArray.size * perField.postingsArray.bytesPerPosting());
      }
      return perField.postingsArray.textStarts;
    }

    @Override
    public int[] grow() {
      ParallelPostingsArray postingsArray = perField.postingsArray;
      final int oldSize = perField.postingsArray.size;
      postingsArray = perField.postingsArray = postingsArray.grow();
      perField.newPostingsArray();
      bytesUsed.addAndGet((postingsArray.bytesPerPosting() * (postingsArray.size - oldSize)));
      return postingsArray.textStarts;
    }

    @Override
    public int[] clear() {
      if (perField.postingsArray != null) {
        bytesUsed.addAndGet(-(perField.postingsArray.size * perField.postingsArray.bytesPerPosting()));
        perField.postingsArray = null;
        perField.newPostingsArray();
      }
      return null;
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }

  @Override
  public int compareTo(TermsHashPerField other) {
    return fieldInfo.name.compareTo(other.fieldInfo.name);
  }

  /** Finish adding all instances of this field to the
   *  current document. */
  void finish() throws IOException {
    if (nextPerField != null) {
      nextPerField.finish();
    }
  }

  /** Start adding a new field instance; first is true if
   *  this is the first time this field name was seen in the
   *  document. */
  boolean start(IndexableField field, boolean first) {
    termAtt = fieldState.termAttribute;
    termFreqAtt = fieldState.termFreqAttribute;
    if (nextPerField != null) {
      doNextCall = nextPerField.start(field, first);
    }

    return true;
  }

  /** Called when a term is seen for the first time. */
  abstract void newTerm(int termID) throws IOException;

  /** Called when a previously seen term is seen again. */
  abstract void addTerm(int termID) throws IOException;

  /** Called when the postings array is initialized or
   *  resized. */
  abstract void newPostingsArray();

  /** Creates a new postings array of the specified size. */
  abstract ParallelPostingsArray createPostingsArray(int size);
}
