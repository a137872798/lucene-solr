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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.FreqProxTermsWriterPerField.FreqProxPostingsArray;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/** Implements limited (iterators only, no stats) {@link
 *  Fields} interface over the in-RAM buffered
 *  fields/terms/postings, to flush postings through the
 *  PostingsFormat. */

class FreqProxFields extends Fields {
  final Map<String,FreqProxTermsWriterPerField> fields = new LinkedHashMap<>();

  public FreqProxFields(List<FreqProxTermsWriterPerField> fieldList) {
    // NOTE: fields are already sorted by field name
    for(FreqProxTermsWriterPerField field : fieldList) {
      fields.put(field.fieldInfo.name, field);
    }
  }

  public Iterator<String> iterator() {
    return fields.keySet().iterator();
  }

  /**
   * 通过指定 field 返回属于该field下所有的 term
   * @param field
   * @return
   * @throws IOException
   */
  @Override
  public Terms terms(String field) throws IOException {
    FreqProxTermsWriterPerField perField = fields.get(field);
    return perField == null ? null : new FreqProxTerms(perField);
  }

  @Override
  public int size() {
    //return fields.size();
    throw new UnsupportedOperationException();
  }

  /**
   * 遍历某个field 下所有的term
   */
  private static class FreqProxTerms extends Terms {
    final FreqProxTermsWriterPerField terms;

    public FreqProxTerms(FreqProxTermsWriterPerField terms) {
      this.terms = terms;
    }

    /**
     * 生成一个 term 迭代器
     * @return
     */
    @Override
    public TermsEnum iterator() {
      FreqProxTermsEnum termsEnum = new FreqProxTermsEnum(terms);
      termsEnum.reset();
      return termsEnum;
    }

    @Override
    public long size() {
      //return terms.termsHashPerField.bytesHash.size();
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumTotalTermFreq() {
      //return terms.sumTotalTermFreq;
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() {
      //return terms.sumDocFreq;
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() {
      //return terms.docCount;
      throw new UnsupportedOperationException();
    }
  
    @Override
    public boolean hasFreqs() {
      return terms.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;      
    }

    @Override
    public boolean hasOffsets() {
      // NOTE: the in-memory buffer may have indexed offsets
      // because that's what FieldInfo said when we started,
      // but during indexing this may have been downgraded:
      return terms.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;      
    }
  
    @Override
    public boolean hasPositions() {
      // NOTE: the in-memory buffer may have indexed positions
      // because that's what FieldInfo said when we started,
      // but during indexing this may have been downgraded:
      return terms.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }
  
    @Override
    public boolean hasPayloads() {
      return terms.sawPayloads;
    }
  }

  /**
   * 该对象是 term迭代器模板
   */
  private static class FreqProxTermsEnum extends BaseTermsEnum {
    /**
     * 这里已经存储了 有关term的各种属性
     */
    final FreqProxTermsWriterPerField terms;
    /**
     * 下标是大小顺序 内部的元素代表排在该位置的 termId 是多少
     */
    final int[] sortedTermIDs;
    /**
     * 该对象内部存储了多组数据 可以通过termId 找到对应的属性
     */
    final FreqProxPostingsArray postingsArray;
    /**
     * 用于存储临时数据
     */
    final BytesRef scratch = new BytesRef();
    /**
     * terms 内部有一个 byteRefHash对象  每当添加一个新的term时 就会增加该值
     */
    final int numTerms;
    /**
     * 迭代器的光标
     */
    int ord;

    public FreqProxTermsEnum(FreqProxTermsWriterPerField terms) {
      this.terms = terms;
      this.numTerms = terms.bytesHash.size();
      // sortedTermIDs 代表 term按照文本内容排序后  每个term对应的id     一开始按插入term的顺序生成 termID
      sortedTermIDs = terms.sortedTermIDs;
      assert sortedTermIDs != null;
      postingsArray = (FreqProxPostingsArray) terms.postingsArray;
    }

    public void reset() {
      ord = -1;
    }

    /**
     * 查询当前对象中是否有包含某个 term的数据
     * @param text
     * @return  SeekStatus 描述一个查询的结果
     */
    public SeekStatus seekCeil(BytesRef text) {
      // TODO: we could instead keep the BytesRefHash
      // intact so this is a hash lookup

      // binary search:
      int lo = 0;
      int hi = numTerms - 1;
      while (hi >= lo) {
        int mid = (lo + hi) >>> 1;
        // sortedTermIDs  已经按照文本大小进行排序了  先找到 中间term在 blockPool中的起始偏移量
        int textStart = postingsArray.textStarts[sortedTermIDs[mid]];
        // 读取 term的信息
        terms.bytePool.setBytesRef(scratch, textStart);
        // 通过比较文本 确定查找范围
        int cmp = scratch.compareTo(text);
        if (cmp < 0) {
          lo = mid + 1;
        } else if (cmp > 0) {
          hi = mid - 1;
        } else {
          // found:
          ord = mid;
          assert term().compareTo(text) == 0;
          return SeekStatus.FOUND;
        }
      }

      // 进入到这里已经代表没有找到了
      // not found:
      ord = lo;
      if (ord >= numTerms) {
        return SeekStatus.END;
      } else {
        // 代表没有找到
        int textStart = postingsArray.textStarts[sortedTermIDs[ord]];
        terms.bytePool.setBytesRef(scratch, textStart);
        assert term().compareTo(text) > 0;
        return SeekStatus.NOT_FOUND;
      }
    }

    /**
     * 因为 sortedTermIDs 已经按照 term值大小 为 termId 做好排序了 ord 能够直接定位到某个term
     * @param ord
     */
    public void seekExact(long ord) {
      this.ord = (int) ord;
      int textStart = postingsArray.textStarts[sortedTermIDs[this.ord]];
      terms.bytePool.setBytesRef(scratch, textStart);
    }

    /**
     * 读取某个term
     * @return
     */
    @Override
    public BytesRef next() {
      ord++;
      if (ord >= numTerms) {
        return null;
      } else {
        // 找到 term 在 termHash的起始偏移量
        int textStart = postingsArray.textStarts[sortedTermIDs[ord]];
        terms.bytePool.setBytesRef(scratch, textStart);
        return scratch;
      }
    }

    /**
     * 当遍历到当前term时 获取term携带的数据
     * @return
     */
    @Override
    public BytesRef term() {
      return scratch;
    }

    /**
     * 获取当前term 在 该field下所有term的顺序
     * @return
     */
    @Override
    public long ord() {
      return ord;
    }

    @Override
    public int docFreq() {
      // We do not store this per-term, and we cannot
      // implement this at merge time w/o an added pass
      // through the postings:
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() {
      // We do not store this per-term, and we cannot
      // implement this at merge time w/o an added pass
      // through the postings:
      throw new UnsupportedOperationException();
    }

    /**
     * 这里有多种 PostingsEnum
     * @param reuse pass a prior PostingsEnum for possible reuse  该对象本身会被复用
     * @param flags specifies which optional per-document values
     *        you require; see {@link PostingsEnum#FREQS}    代表是否需要携带其他信息
     * @return
     */
    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) {
      // 如果需要携带 position信息
      if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
        FreqProxPostingsEnum posEnum;

        // 代表需要携带 position  但是field 当时没有存储该信息
        if (!terms.hasProx) {
          // Caller wants positions but we didn't index them;
          // don't lie:
          throw new IllegalArgumentException("did not index positions");
        }

        // 如果需要查询 offset 信息 而之前没有存储
        if (!terms.hasOffsets && PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS)) {
          // Caller wants offsets but we didn't index them;
          // don't lie:
          throw new IllegalArgumentException("did not index offsets");
        }

        // 如果该对象已经是 存储position的对象了 选择直接复用
        if (reuse instanceof FreqProxPostingsEnum) {
          posEnum = (FreqProxPostingsEnum) reuse;
          // 内部的数组不同 无法复用
          if (posEnum.postingsArray != postingsArray) {
            posEnum = new FreqProxPostingsEnum(terms, postingsArray);
          }
        } else {
          // reuse 类型不匹配  选择创建一个新对象
          posEnum = new FreqProxPostingsEnum(terms, postingsArray);
        }
        // 根据当前 termId  将sliceReader 定位到对应位置
        posEnum.reset(sortedTermIDs[ord]);
        return posEnum;
      }

      // 进入到这里代表 本次不需要读取position相关的信息
      FreqProxDocsEnum docsEnum;

      // 如果需要查询频率信息 而未存储  抛出异常
      if (!terms.hasFreq && PostingsEnum.featureRequested(flags, PostingsEnum.FREQS)) {
        // Caller wants freqs but we didn't index them;
        // don't lie:
        throw new IllegalArgumentException("did not index freq");
      }

      if (reuse instanceof FreqProxDocsEnum) {
        docsEnum = (FreqProxDocsEnum) reuse;
        if (docsEnum.postingsArray != postingsArray) {
          docsEnum = new FreqProxDocsEnum(terms, postingsArray);
        }
      } else {
        docsEnum = new FreqProxDocsEnum(terms, postingsArray);
      }
      docsEnum.reset(sortedTermIDs[ord]);
      return docsEnum;
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * Expert: Returns the TermsEnums internal state to position the TermsEnum
     * without re-seeking the term dictionary.
     * <p>
     * NOTE: A seek by {@link TermState} might not capture the
     * {@link AttributeSource}'s state. Callers must maintain the
     * {@link AttributeSource} states separately
     * 
     * @see TermState
     * @see #seekExact(BytesRef, TermState)
     */
    public TermState termState() throws IOException {
      return new TermState() {
        @Override
        public void copyFrom(TermState other) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  /**
   * docId 迭代器
   */
  private static class FreqProxDocsEnum extends PostingsEnum {

    final FreqProxTermsWriterPerField terms;
    final FreqProxPostingsArray postingsArray;
    final ByteSliceReader reader = new ByteSliceReader();
    final boolean readTermFreq;
    int docID = -1;
    int freq;
    boolean ended;

    /**
     * 此时分片数据是关于哪个 term的
     */
    int termID;

    public FreqProxDocsEnum(FreqProxTermsWriterPerField terms, FreqProxPostingsArray postingsArray) {
      this.terms = terms;
      this.postingsArray = postingsArray;
      this.readTermFreq = terms.hasFreq;
    }

    /**
     * 将 sliceReader 的指针指向某个term起始位置
     * @param termID
     */
    public void reset(int termID) {
      this.termID = termID;
      // 这里读取的都是第一个维度的数据
      terms.initReader(reader, termID, 0);
      ended = false;
      docID = -1;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      // Don't lie here ... don't want codecs writings lots
      // of wasted 1s into the index:
      if (!readTermFreq) {
        throw new IllegalStateException("freq was not indexed");
      } else {
        return freq;
      }
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    /**
     * 遍历该term 所在的所有doc
     * @return
     * @throws IOException
     */
    @Override
    public int nextDoc() throws IOException {
      if (docID == -1) {
        docID = 0;
      }
      // 代表存储相关数据的容器已经读取到末尾
      if (reader.eof()) {
        if (ended) {
          return NO_MORE_DOCS;
        } else {
          ended = true;
          // 获取该 term最后一次出现的doc
          docID = postingsArray.lastDocIDs[termID];
          // 默认情况下 只要存储了频率信息 就顺便读取频率信息
          if (readTermFreq) {
            // 代表该term在最后一个文档中出现的频率
            freq = postingsArray.termFreqs[termID];
          }
        }
      } else {
        // 当不需要存储频率信息时  code 就是 2个docId 之间的差值   写入逻辑对应  FreqProxTermsWriterPerField.addTerm()
        int code = reader.readVInt();
        // 所以这里要记录是否存储了频率信息
        if (!readTermFreq) {
          // 还原docId
          docID += code;
        } else {
          docID += code >>> 1;
          // 代表该term 在当前文档中只出现了一次
          if ((code & 1) != 0) {
            freq = 1;
          // 这种情况是 连续写入了 docId 差值 与当前文档中term出现的频率
          } else {
            freq = reader.readVInt();
          }
        }

        assert docID != postingsArray.lastDocIDs[termID];
      }

      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * 该对象 包含了某个 term 的 pos/freq/offset 等信息
   */
  private static class FreqProxPostingsEnum extends PostingsEnum {

    final FreqProxTermsWriterPerField terms;

    /**
     * term抽取出来的相关信息都暂存在该数组 以及 2个slice中
     */
    final FreqProxPostingsArray postingsArray;

    /**
     * 每当调整当前 term时 对应的分片读取对象会移动到 pool对应的起点
     */
    final ByteSliceReader reader = new ByteSliceReader();
    final ByteSliceReader posReader = new ByteSliceReader();
    final boolean readOffsets;
    int docID = -1;
    int freq;
    int pos;
    int startOffset;
    int endOffset;
    int posLeft;
    int termID;
    boolean ended;
    boolean hasPayload;
    BytesRefBuilder payload = new BytesRefBuilder();

    /**
     * 该对象内部存储了 某个field下所有的term
     * @param terms
     * @param postingsArray   以termId 为下标可以找到该term绑定的其他属性
     */
    public FreqProxPostingsEnum(FreqProxTermsWriterPerField terms, FreqProxPostingsArray postingsArray) {
      this.terms = terms;
      this.postingsArray = postingsArray;
      this.readOffsets = terms.hasOffsets;
      assert terms.hasProx;
      assert terms.hasFreq;
    }

    /**
     * 通过传入一个起始的termId 定位起始位置
     * @param termID
     */
    public void reset(int termID) {
      this.termID = termID;
      // stream 代表统计的数据维度  针对一个term 可以存储多个维度的数据
      terms.initReader(reader, termID, 0);
      terms.initReader(posReader, termID, 1);
      ended = false;
      docID = -1;
      posLeft = 0;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int nextDoc() throws IOException {
      if (docID == -1) {
        docID = 0;
      }
      while (posLeft != 0) {
        nextPosition();
      }

      if (reader.eof()) {
        if (ended) {
          return NO_MORE_DOCS;
        } else {
          ended = true;
          docID = postingsArray.lastDocIDs[termID];
          freq = postingsArray.termFreqs[termID];
        }
      } else {
        int code = reader.readVInt();
        docID += code >>> 1;
        if ((code & 1) != 0) {
          freq = 1;
        } else {
          freq = reader.readVInt();
        }

        assert docID != postingsArray.lastDocIDs[termID];
      }

      posLeft = freq;
      pos = 0;
      startOffset = 0;
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextPosition() throws IOException {
      assert posLeft > 0;
      posLeft--;
      int code = posReader.readVInt();
      pos += code >>> 1;
      if ((code & 1) != 0) {
        hasPayload = true;
        // has a payload
        payload.setLength(posReader.readVInt());
        payload.grow(payload.length());
        posReader.readBytes(payload.bytes(), 0, payload.length());
      } else {
        hasPayload = false;
      }

      if (readOffsets) {
        startOffset += posReader.readVInt();
        endOffset = startOffset + posReader.readVInt();
      }

      return pos;
    }

    @Override
    public int startOffset() {
      if (!readOffsets) {
        throw new IllegalStateException("offsets were not indexed");
      }
      return startOffset;
    }

    @Override
    public int endOffset() {
      if (!readOffsets) {
        throw new IllegalStateException("offsets were not indexed");
      }
      return endOffset;
    }

    @Override
    public BytesRef getPayload() {
      if (hasPayload) {
        return payload.get();
      } else {
        return null;
      }
    }
  }
}
