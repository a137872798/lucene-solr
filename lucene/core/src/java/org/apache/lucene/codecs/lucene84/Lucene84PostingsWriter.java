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
package org.apache.lucene.codecs.lucene84;

import static org.apache.lucene.codecs.lucene84.ForUtil.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.DOC_CODEC;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.MAX_SKIP_LEVELS;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.PAY_CODEC;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.POS_CODEC;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.TERMS_CODEC;
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.VERSION_CURRENT;

import java.io.IOException;
import java.nio.ByteOrder;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Concrete class that writes docId(maybe frq,pos,offset,payloads) list
 * with postings format.
 *
 * Postings list for each term will be stored separately. 
 *
 * @see Lucene84SkipWriter for details about skipping setting and postings layout.
 * @lucene.experimental
 */
public final class Lucene84PostingsWriter extends PushPostingsWriterBase {

  /**
   * 这个输出流 用于存储doc
   */
  IndexOutput docOut;
  /**
   * 负责存储 prox 信息
   */
  IndexOutput posOut;
  /**
   * 存储 offset/payload 信息
   */
  IndexOutput payOut;

  final static IntBlockTermState emptyState = new IntBlockTermState();
  IntBlockTermState lastState;

  // Holds starting file pointers for current term:
  // 每当尝试写入某个term时 相关标识要更新
  private long docStartFP;
  private long posStartFP;
  private long payStartFP;

  /**
   * 对应当前block下每个doc与上一个doc的delta
   */
  final long[] docDeltaBuffer;
  /**
   * 存储当前block下 doc的频率
   */
  final long[] freqBuffer;
  /**
   * 记录此时缓存区已经处理的doc数量
   */
  private int docBufferUpto;

  /**
   * 存储同一个doc下 每次position与lastPosition的增量值
   */
  final long[] posDeltaBuffer;
  /**
   * 描述 position携带的 payload长度信息
   */
  final long[] payloadLengthBuffer;
  /**
   * 存储payload数据
   */
  private byte[] payloadBytes;
  /**
   * 对应 startOffset的偏移量差值
   */
  final long[] offsetStartDeltaBuffer;
  /**
   * 对应position的 endOffset-startOffset
   */
  final long[] offsetLengthBuffer;
  /**
   * 同一个doc下 当前读取了多少个position
   */
  private int posBufferUpto;


  private int payloadByteUpto;

  /**
   * 上次 doc block结尾对应的docId
   */
  private int lastBlockDocID;
  /**
   * 上次 doc block结尾对应的 position文件偏移量
   */
  private long lastBlockPosFP;
  /**
   * 上次 doc block结尾对应的 payload文件偏移量
   */
  private long lastBlockPayFP;
  /**
   * 上次 doc block结尾对应的 payload block下标
   */
  private int lastBlockPosBufferUpto;
  private int lastBlockPayloadByteUpto;

  private int lastDocID;
  private int lastPosition;
  private int lastStartOffset;
  /**
   * 针对某一term 下所有doc数量
   */
  private int docCount;

  private final PForUtil pforUtil;
  private final ForDeltaUtil forDeltaUtil;
  private final Lucene84SkipWriter skipWriter;

  private boolean fieldHasNorms;
  /**
   * 每当处理某个term时 会刷新正在使用的标准因子
   */
  private NumericDocValues norms;

  /**
   * 该对象会在处理过程中 记录标准因子和 freq等影响打分的参数
   */
  private final CompetitiveImpactAccumulator competitiveFreqNormAccumulator = new CompetitiveImpactAccumulator();

  /** Creates a postings writer */
  // 通过一个描述段信息的对象初始化
  public Lucene84PostingsWriter(SegmentWriteState state) throws IOException {

    // 生成存储doc 的文件名
    String docFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene84PostingsFormat.DOC_EXTENSION);
    docOut = state.directory.createOutput(docFileName, state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(docOut, DOC_CODEC, VERSION_CURRENT, 
                                   state.segmentInfo.getId(), state.segmentSuffix);
      ByteOrder byteOrder = ByteOrder.nativeOrder();
      // 大端法/小端法
      if (byteOrder == ByteOrder.BIG_ENDIAN) {
        docOut.writeByte((byte) 'B');
      } else if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
        docOut.writeByte((byte) 'L');
      } else {
        throw new Error();
      }
      final ForUtil forUtil = new ForUtil();
      forDeltaUtil = new ForDeltaUtil(forUtil);
      pforUtil = new PForUtil(forUtil);
      // 只要有任意一个 field 设置了 prox
      if (state.fieldInfos.hasProx()) {
        // 初始大小为128
        posDeltaBuffer = new long[BLOCK_SIZE];
        // 创建存储 pos 的文件
        String posFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene84PostingsFormat.POS_EXTENSION);
        posOut = state.directory.createOutput(posFileName, state.context);
        CodecUtil.writeIndexHeader(posOut, POS_CODEC, VERSION_CURRENT,
                                     state.segmentInfo.getId(), state.segmentSuffix);

        // 如果field 携带了payload信息
        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new long[BLOCK_SIZE];
        } else {
          payloadBytes = null;
          payloadLengthBuffer = null;
        }

        // 如果携带了 offset 信息
        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new long[BLOCK_SIZE];
          offsetLengthBuffer = new long[BLOCK_SIZE];
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
        }

        // 只要存在 offset 或者 payload 任意一项 就创建对应的索引文件
        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene84PostingsFormat.PAY_EXTENSION);
          payOut = state.directory.createOutput(payFileName, state.context);
          CodecUtil.writeIndexHeader(payOut, PAY_CODEC, VERSION_CURRENT,
                                       state.segmentInfo.getId(), state.segmentSuffix);
        }
      } else {
        posDeltaBuffer = null;
        payloadLengthBuffer = null;
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        payloadBytes = null;
      }
      this.payOut = payOut;
      this.posOut = posOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
    }

    docDeltaBuffer = new long[BLOCK_SIZE];
    freqBuffer = new long[BLOCK_SIZE];

    // TODO: should we try skipping every 2/4 blocks...?
    // 基于跳表结构 将相关数据存储到3个输出流中
    skipWriter = new Lucene84SkipWriter(MAX_SKIP_LEVELS,
                                        BLOCK_SIZE, 
                                        state.segmentInfo.maxDoc(),
                                        docOut,
                                        posOut,
                                        payOut);
  }

  @Override
  public IntBlockTermState newTermState() {
    return new IntBlockTermState();
  }

  /**
   * 使用一个索引文件进行初始化
   * @param termsOut
   * @param state
   * @throws IOException
   */
  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeIndexHeader(termsOut, TERMS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
    termsOut.writeVInt(BLOCK_SIZE);
  }

  /**
   * 代表此时开始处理某个 field   将内部相关属性都更改成与这个field 挂钩的
   * @param fieldInfo
   */
  @Override
  public void setField(FieldInfo fieldInfo) {
    super.setField(fieldInfo);
    skipWriter.setField(writePositions, writeOffsets, writePayloads);
    // 重置状态
    lastState = emptyState;
    fieldHasNorms = fieldInfo.hasNorms();
  }

  /**
   * 开始处理某个term
   * @param norms  同时需要处理的标准因子
   */
  @Override
  public void startTerm(NumericDocValues norms) {
    // 根据当前field是否记录了某些信息   更新相关偏移量
    docStartFP = docOut.getFilePointer();
    if (writePositions) {
      posStartFP = posOut.getFilePointer();
      if (writePayloads || writeOffsets) {
        payStartFP = payOut.getFilePointer();
      }
    }
    lastDocID = 0;
    lastBlockDocID = -1;
    // 将跳跃表内相关信息也进行更新
    skipWriter.resetSkip();
    this.norms = norms;
    competitiveFreqNormAccumulator.clear();
  }

  /**
   * 开始写入某个doc的值   调用顺序是 setField -> startTerm -> startDoc
   * @param docID
   * @param termDocFreq  当前doc出现的频率
   * @throws IOException
   */
  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    // Have collected a block of docs, and get a new doc. 
    // Should write skip data as well as postings list for
    // current block.
    // 代表此时处理的doc数量满足一个 block大小   这时将数据写入到skip结构中
    if (lastBlockDocID != -1 && docBufferUpto == 0) {
      skipWriter.bufferSkip(lastBlockDocID, competitiveFreqNormAccumulator, docCount,
          lastBlockPosFP, lastBlockPayFP, lastBlockPosBufferUpto, lastBlockPayloadByteUpto);
      competitiveFreqNormAccumulator.clear();
    }

    // 如果是merge的情况 会对doc做整理  也就是doc会变成连续的 delta始终是1
    // 如果是正常情况  如果发生了 删除doc的事件 那么doc就不是连续的
    final int docDelta = docID - lastDocID;

    if (docID < 0 || (docCount > 0 && docDelta <= 0)) {
      throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )", docOut);
    }

    docDeltaBuffer[docBufferUpto] = docDelta;
    if (writeFreqs) {
      freqBuffer[docBufferUpto] = termDocFreq;
    }

    // 增加当前block 处理的doc数量
    docBufferUpto++;
    // 增加总的 doc 处理数
    docCount++;

    // 代表此时大小已经满足一个block 需要将数据持久化
    if (docBufferUpto == BLOCK_SIZE) {
      forDeltaUtil.encodeDeltas(docDeltaBuffer, docOut);
      if (writeFreqs) {
        // 将频率信息写入 out
        pforUtil.encode(freqBuffer, docOut);
      }
      // NOTE: don't set docBufferUpto back to 0 here;
      // finishDoc will do so (because it needs to see that
      // the block was filled so it can save skip data)
    }


    // 记录上次处理的 doc
    lastDocID = docID;
    lastPosition = 0;
    lastStartOffset = 0;

    long norm;
    // 如果该field 携带了标准因子
    if (fieldHasNorms) {
      boolean found = norms.advanceExact(docID);
      if (found == false) {
        // This can happen if indexing hits a problem after adding a doc to the
        // postings but before buffering the norm. Such documents are written
        // deleted and will go away on the first merge.
        norm = 1L;
      } else {
        // 获取该文档对应的标准因子
        norm = norms.longValue();
        assert norm != 0 : docID;
      }
    } else {
      norm = 1L;
    }

    // 累加 doc出现频率 以及携带的标准因子
    competitiveFreqNormAccumulator.add(writeFreqs ? termDocFreq : 1, norm);
  }

  /**
   * 将描述某个doc位置信息的数据写入索引文件   一个doc会根据它的freq存在等量的 position
   * @param position
   * @param payload
   * @param startOffset
   * @param endOffset
   * @throws IOException
   */
  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
    if (position > IndexWriter.MAX_POSITION) {
      throw new CorruptIndexException("position=" + position + " is too large (> IndexWriter.MAX_POSITION=" + IndexWriter.MAX_POSITION + ")", docOut);
    }
    if (position < 0) {
      throw new CorruptIndexException("position=" + position + " is < 0", docOut);
    }

    // 记录距上次调用 addPosition 的增量
    posDeltaBuffer[posBufferUpto] = position - lastPosition;
    // 如果有携带 负载信息
    if (writePayloads) {
      // 传入的负载为null
      if (payload == null || payload.length == 0) {
        // no payload
        payloadLengthBuffer[posBufferUpto] = 0;
      } else {
        payloadLengthBuffer[posBufferUpto] = payload.length;
        if (payloadByteUpto + payload.length > payloadBytes.length) {
          payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payload.length);
        }
        System.arraycopy(payload.bytes, payload.offset, payloadBytes, payloadByteUpto, payload.length);
        payloadByteUpto += payload.length;
      }
    }

    // 如果携带了 offset信息
    if (writeOffsets) {
      assert startOffset >= lastStartOffset;
      assert endOffset >= startOffset;
      offsetStartDeltaBuffer[posBufferUpto] = startOffset - lastStartOffset;
      offsetLengthBuffer[posBufferUpto] = endOffset - startOffset;
      lastStartOffset = startOffset;
    }

    // 增加当前block 已经处理的 position 数量
    posBufferUpto++;
    lastPosition = position;
    if (posBufferUpto == BLOCK_SIZE) {
      // 也是一种基于位的存储方式
      pforUtil.encode(posDeltaBuffer, posOut);

      if (writePayloads) {
        pforUtil.encode(payloadLengthBuffer, payOut);
        payOut.writeVInt(payloadByteUpto);
        payOut.writeBytes(payloadBytes, 0, payloadByteUpto);
        payloadByteUpto = 0;
      }
      if (writeOffsets) {
        pforUtil.encode(offsetStartDeltaBuffer, payOut);
        pforUtil.encode(offsetLengthBuffer, payOut);
      }
      posBufferUpto = 0;
    }
  }

  /**
   * 代表某个doc 处理完毕了
   * @throws IOException
   */
  @Override
  public void finishDoc() throws IOException {
    // Since we don't know df for current term, we had to buffer
    // those skip data for each block, and when a new doc comes, 
    // write them to skip file.
    // 如果此时刚好填满了一个block
    if (docBufferUpto == BLOCK_SIZE) {
      lastBlockDocID = lastDocID;
      if (posOut != null) {
        if (payOut != null) {
          lastBlockPayFP = payOut.getFilePointer();
        }
        lastBlockPosFP = posOut.getFilePointer();
        lastBlockPosBufferUpto = posBufferUpto;
        lastBlockPayloadByteUpto = payloadByteUpto;
      }
      // 将该值标记成0 后 下次调用 startDoc 就会将数据写入到 skip中
      docBufferUpto = 0;
    }
  }

  /**
   * Called when we are done adding docs to this term
   * 代表当前数据写入完成  这里是将一些当前状态回填到 state中
   */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    assert state.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert state.docFreq == docCount: state.docFreq + " vs " + docCount;
    
    // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to it.
    // 如果该term只出现在一个doc中 那么只存在一个docId
    final int singletonDocID;
    // 代表该term只在一个doc中出现过
    if (state.docFreq == 1) {
      // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
      singletonDocID = (int) docDeltaBuffer[0];
    } else {
      singletonDocID = -1;
      // vInt encode the remaining doc deltas and freqs:
      // 注意docDeltaBuffer 存储的是据上一个doc的差值信息    倒排索引的doc一般情况下是非连续的
      for(int i=0;i<docBufferUpto;i++) {
        final int docDelta = (int) docDeltaBuffer[i];
        // 这个频率应该是指term在单个doc中出现了多少次
        final int freq = (int) freqBuffer[i];
        if (!writeFreqs) {
          docOut.writeVInt(docDelta);
        } else if (freq == 1) {
          // 最低位为1 代表该doc下term出现的频率为1
          docOut.writeVInt((docDelta<<1)|1);
        } else {
          // 最低位为0 代表还需要额外读取一个频率值
          docOut.writeVInt(docDelta<<1);
          docOut.writeVInt(freq);
        }
      }
    }

    final long lastPosBlockOffset;

    // 如果写入了position信息
    if (writePositions) {
      // totalTermFreq is just total number of positions(or payloads, or offsets)
      // associated with current term.
      assert state.totalTermFreq != -1;
      // 代表该term在所有doc中出现的次数总和 超过了128  如果超过了一个block的大小 就记录开始写入该term 到写完该term所有相关信息后的总偏移量变化
      if (state.totalTermFreq > BLOCK_SIZE) {
        // record file offset for last pos in last block
        lastPosBlockOffset = posOut.getFilePointer() - posStartFP;
      } else {
        lastPosBlockOffset = -1;
      }
      // 这里记录了在某个doc下term出现的位置数量
      if (posBufferUpto > 0) {       
        // TODO: should we send offsets/payloads to
        // .pay...?  seems wasteful (have to store extra
        // vLong for low (< BLOCK_SIZE) DF terms = vast vast
        // majority)

        // vInt encode the remaining positions/payloads/offsets:
        int lastPayloadLength = -1;  // force first payload length to be written
        int lastOffsetLength = -1;   // force first offset length to be written

        // 记录payloadBytes此时写入的偏移量
        int payloadBytesReadUpto = 0;
        // 开始遍历出现的所有位置
        for(int i=0;i<posBufferUpto;i++) {
          // 这里记录的是位置的增量值
          final int posDelta = (int) posDeltaBuffer[i];
          // 如果额外写入了负载信息
          if (writePayloads) {
            // 记录负载信息的长度
            final int payloadLength = (int) payloadLengthBuffer[i];
            if (payloadLength != lastPayloadLength) {
              lastPayloadLength = payloadLength;
              // 最低位为1 代表存储了负载长度
              posOut.writeVInt((posDelta<<1)|1);
              posOut.writeVInt(payloadLength);
            } else {
              // 最低位为1 代表该position对应的负载数据长度与上一个一致
              posOut.writeVInt(posDelta<<1);
            }

            // 在最后将负载数据写入
            if (payloadLength != 0) {
              posOut.writeBytes(payloadBytes, payloadBytesReadUpto, payloadLength);
              payloadBytesReadUpto += payloadLength;
            }
          } else {
            // 如果没有携带负载信息 那么只要写入 position 增量信息就好
            posOut.writeVInt(posDelta);
          }

          // 如果还携带了 offset 信息
          if (writeOffsets) {
            // 代表 startOffset的增量数据
            int delta = (int) offsetStartDeltaBuffer[i];
            // 代表 endOffset - startOffset
            int length = (int) offsetLengthBuffer[i];
            // 如果长度都是一致的 最低位使用1 代表复用之前的长度 跟上面同样的套路
            if (length == lastOffsetLength) {
              posOut.writeVInt(delta << 1);
            } else {
              posOut.writeVInt(delta << 1 | 1);
              posOut.writeVInt(length);
              lastOffsetLength = length;
            }
          }
        }

        if (writePayloads) {
          assert payloadBytesReadUpto == payloadByteUpto;
          // 重置指针 方便之后数据写入时 覆盖payload 数据
          payloadByteUpto = 0;
        }
      }
    } else {
      // -1代表没有携带 position 信息
      lastPosBlockOffset = -1;
    }

    long skipOffset;
    // 代表该term关联的doc总数 超过了一个 block的大小
    if (docCount > BLOCK_SIZE) {
      // 每个跳跃表记录的数据 以 term为单位  每当处理超一个block长度的数据时 会生成一些差值数据 并存储到跳跃表中 这里将数据写回到output中
      // skipOffset 代表 处理该term写入的总长度
      skipOffset = skipWriter.writeSkip(docOut) - docStartFP;
    } else {
      // 没有跨block的情况 就不需要写入这些信息
      skipOffset = -1;
    }

    // 将开始写该term时 相关的一些起始偏移量设置到 state上
    state.docStartFP = docStartFP;
    state.posStartFP = posStartFP;
    state.payStartFP = payStartFP;
    state.singletonDocID = singletonDocID;
    state.skipOffset = skipOffset;
    state.lastPosBlockOffset = lastPosBlockOffset;
    // 重置相关参数
    docBufferUpto = 0;
    posBufferUpto = 0;
    lastDocID = 0;
    docCount = 0;
  }
  
  @Override
  public void encodeTerm(DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute) throws IOException {
    IntBlockTermState state = (IntBlockTermState)_state;
    if (absolute) {
      lastState = emptyState;
      assert lastState.docStartFP == 0;
    }

    if (lastState.singletonDocID != -1 && state.singletonDocID != -1 && state.docStartFP == lastState.docStartFP) {
      // With runs of rare values such as ID fields, the increment of pointers in the docs file is often 0.
      // Furthermore some ID schemes like auto-increment IDs or Flake IDs are monotonic, so we encode the delta
      // between consecutive doc IDs to save space.
      final long delta = (long) state.singletonDocID - lastState.singletonDocID;
      out.writeVLong((BitUtil.zigZagEncode(delta) << 1) | 0x01);
    } else {
      out.writeVLong((state.docStartFP - lastState.docStartFP) << 1);
      if (state.singletonDocID != -1) {
        out.writeVInt(state.singletonDocID);
      }
    }

    if (writePositions) {
      out.writeVLong(state.posStartFP - lastState.posStartFP);
      if (writePayloads || writeOffsets) {
        out.writeVLong(state.payStartFP - lastState.payStartFP);
      }
    }
    if (writePositions) {
      if (state.lastPosBlockOffset != -1) {
        out.writeVLong(state.lastPosBlockOffset);
      }
    }
    if (state.skipOffset != -1) {
      out.writeVLong(state.skipOffset);
    }
    lastState = state;
  }

  @Override
  public void close() throws IOException {
    // TODO: add a finish() at least to PushBase? DV too...?
    boolean success = false;
    try {
      if (docOut != null) {
        CodecUtil.writeFooter(docOut);
      }
      if (posOut != null) {
        CodecUtil.writeFooter(posOut);
      }
      if (payOut != null) {
        CodecUtil.writeFooter(payOut);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(docOut, posOut, payOut);
      } else {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
      docOut = posOut = payOut = null;
    }
  }
}
