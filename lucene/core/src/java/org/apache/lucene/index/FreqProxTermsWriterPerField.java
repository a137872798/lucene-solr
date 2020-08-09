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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N

/**
 * 该对象是上游对象 下接TermVectorsConsumerPerField    该对象使用的一个常见的编码技巧就是 << 1
 * 这样通过判断最低一位是否为0 可以得到某种信息 (如果不这么做可能需要额外存储一个boolean 就会加大内存开销)
 *
 * 该对象从term，docState，fieldState 等对象中抽取属性 并存储到 pool中
 */
final class FreqProxTermsWriterPerField extends TermsHashPerField {

  /**
   * 该对象内部有多个 int[]  每个数组应该就是用来存储描述term的某个属性
   */
  private FreqProxPostingsArray freqProxPostingsArray;
  /**
   * 根据field的 IndexOptional 确定是否要存储频率
   */
  final boolean hasFreq;
  /**
   * 是否要存储 position信息
   */
  final boolean hasProx;
  /**
   * 是否要存储offset信息
   */
  final boolean hasOffsets;
  PayloadAttribute payloadAttribute;
  OffsetAttribute offsetAttribute;
  long sumTotalTermFreq;
  long sumDocFreq;

  // How many docs have this field:   代表该field 已经处理了多少doc  换一个角度描述就是 已经检测到有多少文档包含该 field
  int docCount;

  /** Set to true if any token had a payload in the current
   *  segment. */
  // 代表还存储了 payload信息   从存储position的逻辑中可以看到
  boolean sawPayloads;

  /**
   *
   * @param invertState
   * @param termsHash  该对象绑定的 存储term的容器
   * @param fieldInfo  代表从哪个field 中抽取属性
   * @param nextPerField  下游对象
   */
  public FreqProxTermsWriterPerField(FieldInvertState invertState, TermsHash termsHash, FieldInfo fieldInfo, TermsHashPerField nextPerField) {
    // 当该field 需要存储的属性枚举 大于该值时 代表需要存储2种数据 频率和position 否则只存储频率
    super(fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 ? 2 : 1, invertState, termsHash, nextPerField, fieldInfo);
    IndexOptions indexOptions = fieldInfo.getIndexOptions();
    assert indexOptions != IndexOptions.NONE;
    // 根据索引配置 确定是否要存储某些属性
    hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
  }

  /**
   * 每当处理完一个文档后 调用该方法
   * @throws IOException
   */
  @Override
  void finish() throws IOException {
    // 这里会先触发下游的 finish
    super.finish();
    sumDocFreq += fieldState.uniqueTermCount;
    sumTotalTermFreq += fieldState.length;
    if (fieldState.length > 0) {
      docCount++;
    }

    if (sawPayloads) {
      fieldInfo.setStorePayloads();
    }
  }

  /**
   * 代表此时正在处理一个新的 doc
   * @param f
   * @param first
   * @return
   */
  @Override
  boolean start(IndexableField f, boolean first) {
    // super 方法会先触发下游的 start
    super.start(f, first);
    payloadAttribute = fieldState.payloadAttribute;
    offsetAttribute = fieldState.offsetAttribute;
    return true;
  }

  /**
   * 代表还需要写入 position 信息   position写入的逻辑是这样  先读取第一个int  并且 int >> 1 才是position的值 如果最低位不为0 那么代表还存储了 payload的信息
   * @param termID
   * @param proxCode
   */
  void writeProx(int termID, int proxCode) {
    // 根据是否携带 payload 分为2种情况
    if (payloadAttribute == null) {
      // 不包含payload
      writeVInt(1, proxCode<<1);
    } else {
      BytesRef payload = payloadAttribute.getPayload();
      if (payload != null && payload.length > 0) {
        // 如果低位不为0 就代表存在 payload
        writeVInt(1, (proxCode<<1)|1);
        // 写入payload 的长度
        writeVInt(1, payload.length);
        writeBytes(1, payload.bytes, payload.offset, payload.length);
        sawPayloads = true;
      } else {
        // 当payload信息无效时  当作没有payload的写入逻辑
        writeVInt(1, proxCode<<1);
      }
    }

    assert postingsArray == freqProxPostingsArray;
    // 同时维护上一次 出现的 position
    freqProxPostingsArray.lastPositions[termID] = fieldState.position;
  }

  /**
   * 这里记录偏移量信息
   * @param termID
   * @param offsetAccum  偏移量是一个增量数据
   */
  void writeOffsets(int termID, int offsetAccum) {
    final int startOffset = offsetAccum + offsetAttribute.startOffset();
    final int endOffset = offsetAccum + offsetAttribute.endOffset();
    assert startOffset - freqProxPostingsArray.lastOffsets[termID] >= 0;
    // 第一个写入的值是  start - lastStart
    writeVInt(1, startOffset - freqProxPostingsArray.lastOffsets[termID]);
    // end - start
    writeVInt(1, endOffset - startOffset);
    freqProxPostingsArray.lastOffsets[termID] = startOffset;
  }

  /**
   * 当首次添加某个term时    可以看到需要写入的属性都是从 docState，termState 这种直接获取的
   * @param termID
   */
  @Override
  void newTerm(final int termID) {
    // First time we're seeing this term since the last
    // flush
    final FreqProxPostingsArray postings = freqProxPostingsArray;

    // 记录该词 属于哪个 doc
    postings.lastDocIDs[termID] = docState.docID;
    // 如果不需要记录 频率信息
    if (!hasFreq) {
      assert postings.termFreqs == null;
      postings.lastDocCodes[termID] = docState.docID;
      // 保持原封不动 如果数据异常则使用1
      fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
    } else {
      // 最低位为0 代表需要记录频率信息
      postings.lastDocCodes[termID] = docState.docID << 1;
      // 获取频率信息 并记录
      postings.termFreqs[termID] = getTermFreq();
      if (hasProx) {
        writeProx(termID, fieldState.position);
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.maxTermFrequency = Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
    }
    fieldState.uniqueTermCount++;
  }

  /**
   * 代表该 term之前已经出现过
   * @param termID
   */
  @Override
  void addTerm(final int termID) {
    final FreqProxPostingsArray postings = freqProxPostingsArray;
    assert !hasFreq || postings.termFreqs[termID] > 0;

    if (!hasFreq) {
      assert postings.termFreqs == null;
      if (termFreqAtt.getTermFrequency() != 1) {
        throw new IllegalStateException("field \"" + fieldInfo.name + "\": must index term freq while using custom TermFrequencyAttribute");
      }
      // 代表这个词 上次出现是在另外的文档
      if (docState.docID != postings.lastDocIDs[termID]) {
        // New document; now encode docCode for previous doc:
        assert docState.docID > postings.lastDocIDs[termID];
        // 该值就是 lastDoc 经过处理后的值
        writeVInt(0, postings.lastDocCodes[termID]);
        // 这里修改成 docId的差值
        postings.lastDocCodes[termID] = docState.docID - postings.lastDocIDs[termID];
        postings.lastDocIDs[termID] = docState.docID;
        fieldState.uniqueTermCount++;
      }
      // 在需要记录频率的前提下   并且发生了跨文档
    } else if (docState.docID != postings.lastDocIDs[termID]) {
      assert docState.docID > postings.lastDocIDs[termID]:"id: "+docState.docID + " postings ID: "+ postings.lastDocIDs[termID] + " termID: "+termID;
      // Term not yet seen in the current doc but previously
      // seen in other doc(s) since the last flush

      // Now that we know doc freq for previous doc,
      // write it & lastDocCode
      if (1 == postings.termFreqs[termID]) {
        // 最低位为1的时候 应该就是代表频率为1  否则 最低为是0  TODO 需要看写入的数据是怎么读取的
        writeVInt(0, postings.lastDocCodes[termID]|1);
      } else {
        // 最低位为0 时 代表需要记录频率信息 然后立即读取一个int 代表频率值   这里写入的都是上一次的数据
        writeVInt(0, postings.lastDocCodes[termID]);
        writeVInt(0, postings.termFreqs[termID]);
      }

      // Init freq for the current document
      // 更新本次信息
      postings.termFreqs[termID] = getTermFreq();
      fieldState.maxTermFrequency = Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
      postings.lastDocCodes[termID] = (docState.docID - postings.lastDocIDs[termID]) << 1;
      postings.lastDocIDs[termID] = docState.docID;
      if (hasProx) {
        writeProx(termID, fieldState.position);
        if (hasOffsets) {
          postings.lastOffsets[termID] = 0;
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      // 跨文档 看作独立的 term
      fieldState.uniqueTermCount++;
      // 代表在同一文档中 并且 需要记录频率
    } else {
      postings.termFreqs[termID] = Math.addExact(postings.termFreqs[termID], getTermFreq());
      fieldState.maxTermFrequency = Math.max(fieldState.maxTermFrequency, postings.termFreqs[termID]);
      if (hasProx) {
        writeProx(termID, fieldState.position-postings.lastPositions[termID]);
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      }
    }
  }

  private int getTermFreq() {
    int freq = termFreqAtt.getTermFrequency();
    if (freq != 1) {
      if (hasProx) {
        throw new IllegalStateException("field \"" + fieldInfo.name + "\": cannot index positions while using custom TermFrequencyAttribute");
      }
    }

    return freq;
  }

  @Override
  public void newPostingsArray() {
    freqProxPostingsArray = (FreqProxPostingsArray) postingsArray;
  }

  /**
   * 该方法创建的数组 会变成父类的 postingsArray
   * @param size
   * @return
   */
  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    IndexOptions indexOptions = fieldInfo.getIndexOptions();
    assert indexOptions != IndexOptions.NONE;
    // 根据域的 索引选项判断是否要记录某些信息
    boolean hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    return new FreqProxPostingsArray(size, hasFreq, hasProx, hasOffsets);
  }

  /**
   * 存储频率信息的对象
   */
  static final class FreqProxPostingsArray extends ParallelPostingsArray {

    /**
     *
     * @param size
     * @param writeFreqs
     * @param writeProx
     * @param writeOffsets
     */
    public FreqProxPostingsArray(int size, boolean writeFreqs, boolean writeProx, boolean writeOffsets) {
      super(size);
      if (writeFreqs) {
        termFreqs = new int[size];
      }
      lastDocIDs = new int[size];
      lastDocCodes = new int[size];
      if (writeProx) {
        lastPositions = new int[size];
        if (writeOffsets) {
          lastOffsets = new int[size];
        }
      } else {
        assert !writeOffsets;
      }
      //System.out.println("PA init freqs=" + writeFreqs + " pos=" + writeProx + " offs=" + writeOffsets);
    }

    /**
     * 当需要记录 频率信息时 就要初始化该对象
     */
    int termFreqs[];                                   // # times this term occurs in the current doc

    // 这些last属性 都是以 termId 为下标  相同的term可能会在一个doc中出现多次 下面的数组就是记录某个term 最近一次相关的值

    int lastDocIDs[];                                  // Last docID where this term occurred
    /**
     * 当不需要记录频率信息的时候 就是 （docId - lastDocId)   否则是 (docId - lastDocId) << 1
     */
    int lastDocCodes[];                                // Code for prior doc
    // 同样只有需要存储时 才初始化容器
    int lastPositions[];                               // Last position where this term occurred
    int lastOffsets[];                                 // Last endOffset where this term occurred

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new FreqProxPostingsArray(size, termFreqs != null, lastPositions != null, lastOffsets != null);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof FreqProxPostingsArray;
      FreqProxPostingsArray to = (FreqProxPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(lastDocIDs, 0, to.lastDocIDs, 0, numToCopy);
      System.arraycopy(lastDocCodes, 0, to.lastDocCodes, 0, numToCopy);
      // 只有在初始化时设置了需要记录某属性 才会初始化数组 这里拷贝数组内的数据
      if (lastPositions != null) {
        assert to.lastPositions != null;
        System.arraycopy(lastPositions, 0, to.lastPositions, 0, numToCopy);
      }
      if (lastOffsets != null) {
        assert to.lastOffsets != null;
        System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, numToCopy);
      }
      if (termFreqs != null) {
        assert to.termFreqs != null;
        System.arraycopy(termFreqs, 0, to.termFreqs, 0, numToCopy);
      }
    }

    /**
     * 父类3个int[] 需要的值就是 3*int.bytes   这里又多出了几个数组  需要的值就是加上这些新字段
     * @return
     */
    @Override
    int bytesPerPosting() {
      int bytes = ParallelPostingsArray.BYTES_PER_POSTING + 2 * Integer.BYTES;
      if (lastPositions != null) {
        bytes += Integer.BYTES;
      }
      if (lastOffsets != null) {
        bytes += Integer.BYTES;
      }
      if (termFreqs != null) {
        bytes += Integer.BYTES;
      }

      return bytes;
    }
  }
}
