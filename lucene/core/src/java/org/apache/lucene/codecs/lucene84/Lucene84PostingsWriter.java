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
import static org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.*;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat.*;
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

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Concrete class that writes docId(maybe frq,pos,offset,payloads) list
 * with postings format.
 * <p>
 * Postings list for each term will be stored separately.
 *
 * @lucene.experimental
 * @see Lucene84SkipWriter for details about skipping setting and postings layout.
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

    /**
     * 记录上一个写入的term.state
     */
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
     * 对应当前应该写入的  位置信息相关的几个数组的下标 position/payloadLength/offsetStart/offsetLength
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

    /**
     * Creates a postings writer
     *
     * @param state 描述本次写入段数据的 上下文信息
     */
    public Lucene84PostingsWriter(SegmentWriteState state) throws IOException {

        // 生成文件名 .doc
        String docFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene84PostingsFormat.DOC_EXTENSION);
        // 创建输出流
        docOut = state.directory.createOutput(docFileName, state.context);
        IndexOutput posOut = null;
        IndexOutput payOut = null;
        boolean success = false;
        // 写入文件头
        try {
            CodecUtil.writeIndexHeader(docOut, DOC_CODEC, VERSION_CURRENT,
                    state.segmentInfo.getId(), state.segmentSuffix);
            ByteOrder byteOrder = ByteOrder.nativeOrder();
            // 写入 大端法/小端法
            if (byteOrder == ByteOrder.BIG_ENDIAN) {
                docOut.writeByte((byte) 'B');
            } else if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
                docOut.writeByte((byte) 'L');
            } else {
                throw new Error();
            }
            // 就简单看作是加工数据的好了
            final ForUtil forUtil = new ForUtil();
            forDeltaUtil = new ForDeltaUtil(forUtil);
            pforUtil = new PForUtil(forUtil);
            // 代表有field 包含 position 信息
            if (state.fieldInfos.hasProx()) {
                // 初始大小为128
                posDeltaBuffer = new long[BLOCK_SIZE];
                // 创建 .pos文件
                String posFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene84PostingsFormat.POS_EXTENSION);
                posOut = state.directory.createOutput(posFileName, state.context);
                CodecUtil.writeIndexHeader(posOut, POS_CODEC, VERSION_CURRENT,
                        state.segmentInfo.getId(), state.segmentSuffix);

                // 如果field 携带了payload信息
                if (state.fieldInfos.hasPayloads()) {
                    // 分别用于写入 payload  以及记录 payload的长度
                    payloadBytes = new byte[128];
                    payloadLengthBuffer = new long[BLOCK_SIZE];
                } else {
                    payloadBytes = null;
                    payloadLengthBuffer = null;
                }

                // 如果携带了 offset 信息
                if (state.fieldInfos.hasOffsets()) {
                    // 用于记录 startOff 以及  (endOff - startOff)
                    offsetStartDeltaBuffer = new long[BLOCK_SIZE];
                    offsetLengthBuffer = new long[BLOCK_SIZE];
                } else {
                    offsetStartDeltaBuffer = null;
                    offsetLengthBuffer = null;
                }

                // 当存在 payload 或者 offset时  创建 .pay 文件
                if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
                    String payFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Lucene84PostingsFormat.PAY_EXTENSION);
                    payOut = state.directory.createOutput(payFileName, state.context);
                    CodecUtil.writeIndexHeader(payOut, PAY_CODEC, VERSION_CURRENT,
                            state.segmentInfo.getId(), state.segmentSuffix);
                }
            } else {
                // 当不存在 position 时  相关容器都不需要设置了
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
        // 使用跳跃表结构  将数据写入到 .doc .pos .pay 3个索引文件中
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
     *
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
     *
     * @param fieldInfo
     */
    @Override
    public void setField(FieldInfo fieldInfo) {
        // 获取当前field下 哪些信息需要写入到索引文件中
        super.setField(fieldInfo);
        // 设置 跳跃表内部的属性
        skipWriter.setField(writePositions, writeOffsets, writePayloads);
        // 因为开始处理一个新的 field 所以将用于描述存储上个field下term的 blockState对象重置
        lastState = emptyState;
        // 当前field是否会生成标准因子
        fieldHasNorms = fieldInfo.hasNorms();
    }

    /**
     * 在处理某个 term之前 需要将相关数据重置
     *
     * @param norms 该term关联的 field 的标准因子迭代器
     */
    @Override
    public void startTerm(NumericDocValues norms) {
        // 获取此时 .doc文件的偏移量
        docStartFP = docOut.getFilePointer();
        // 如果同时还需要写入 其他信息 就获取此时的文件偏移量
        if (writePositions) {
            posStartFP = posOut.getFilePointer();
            if (writePayloads || writeOffsets) {
                payStartFP = payOut.getFilePointer();
            }
        }
        // 重置 lastDocId   lastBlockDocId
        lastDocID = 0;
        lastBlockDocID = -1;
        // 将跳跃表内相关信息也进行更新
        skipWriter.resetSkip();
        // 设置标准因子  以及 清空累加器的数据
        this.norms = norms;
        competitiveFreqNormAccumulator.clear();
    }

    /**
     * 代表term出现在某个doc下   这里只要记录freq 和 norm docID 信息
     *
     * @param docID       记录该term在 docId 对应的doc时的数据
     * @param termDocFreq 该term在doc中出现的次数
     * @throws IOException
     */
    @Override
    public void startDoc(int docID, int termDocFreq) throws IOException {
        // Have collected a block of docs, and get a new doc.
        // Should write skip data as well as postings list for
        // current block.
        // 每当处理的doc 数量满足一个 block时  将数据写入到 跳跃表中    这样就可以利用跳跃表结构快速检索某个doc相关的数据
        if (lastBlockDocID != -1 && docBufferUpto == 0) {
            skipWriter.bufferSkip(lastBlockDocID, competitiveFreqNormAccumulator, docCount,
                    lastBlockPosFP, lastBlockPayFP, lastBlockPosBufferUpto, lastBlockPayloadByteUpto);
            // 因为累加的数据已经转移到  跳跃表结构了 所以可以清空该累加器
            competitiveFreqNormAccumulator.clear();
        }

        // 获取当前 doc 与上一个doc 的差值
        final int docDelta = docID - lastDocID;

        if (docID < 0 || (docCount > 0 && docDelta <= 0)) {
            throw new CorruptIndexException("docs out of order (" + docID + " <= " + lastDocID + " )", docOut);
        }

        // docBufferUpto 初始状态为 0     这里记录 docId 差值 以及频率信息
        docDeltaBuffer[docBufferUpto] = docDelta;
        if (writeFreqs) {
            freqBuffer[docBufferUpto] = termDocFreq;
        }

        // 增加当前处理的 doc数量
        docBufferUpto++;
        // 增加总的 doc 处理数
        docCount++;

        // 代表此时大小已经满足一个block 需要将数据持久化
        if (docBufferUpto == BLOCK_SIZE) {
            // 这个util 的 读写就不细看了 涉及的位运算比较恶心
            forDeltaUtil.encodeDeltas(docDeltaBuffer, docOut);
            if (writeFreqs) {
                // 将频率信息写入 out
                pforUtil.encode(freqBuffer, docOut);
            }
            // NOTE: don't set docBufferUpto back to 0 here;
            // finishDoc will do so (because it needs to see that
            // the block was filled so it can save skip data)
        }


        // 记录上次处理的 doc  同时由于切换了文档 将之前在同一doc内使用差值存储的基准值重置
        lastDocID = docID;
        lastPosition = 0;
        lastStartOffset = 0;

        long norm;
        // 如果该field 携带了标准因子
        if (fieldHasNorms) {
            // 检测是否有针对该 doc 生成的标准因子
            boolean found = norms.advanceExact(docID);
            if (found == false) {
                // This can happen if indexing hits a problem after adding a doc to the
                // postings but before buffering the norm. Such documents are written
                // deleted and will go away on the first merge.
                // 标准因子默认为 1
                norm = 1L;
            } else {
                // 获取该文档对应的标准因子
                norm = norms.longValue();
                assert norm != 0 : docID;
            }
        } else {
            // 未设置标准因子时 也使用1
            norm = 1L;
        }

        // 累加 doc出现频率 以及携带的标准因子
        competitiveFreqNormAccumulator.add(writeFreqs ? termDocFreq : 1, norm);
    }

    /**
     * 将描述 term位置信息的数据写入
     *
     * @param position    该term 在 field.value(doc) 中的逻辑位置
     * @param payload     携带的payload
     * @param startOffset 该term在 doc的起始偏移量
     * @param endOffset   该term在 doc的终止偏移量
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

        // 只存储增量信息
        posDeltaBuffer[posBufferUpto] = position - lastPosition;
        // 如果有携带 负载信息
        if (writePayloads) {
            // 传入的负载为null
            if (payload == null || payload.length == 0) {
                // no payload
                payloadLengthBuffer[posBufferUpto] = 0;
            } else {
                // 记录长度信息  以及写下 payload
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

        // 使用 特殊的util写入数据
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
     *
     * @throws IOException
     */
    @Override
    public void finishDoc() throws IOException {
        // Since we don't know df for current term, we had to buffer
        // those skip data for each block, and when a new doc comes,
        // write them to skip file.
        // 如果此时刚好填满了一个block   更新lastXXX 信息 并重置 docBufferUpto
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
     * 代表某个term相关的所有doc的位置信息已经写完了  这时将一些描述结果的属性设置到 state 中
     */
    @Override
    public void finishTerm(BlockTermState _state) throws IOException {
        IntBlockTermState state = (IntBlockTermState) _state;
        assert state.docFreq > 0;

        // TODO: wasteful we are counting this (counting # docs
        // for this term) in two places?
        assert state.docFreq == docCount : state.docFreq + " vs " + docCount;

        // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to it.
        // 如果该term只出现在一个doc中 那么只存在一个docId
        final int singletonDocID;
        // 代表该term只在一个doc中出现过
        if (state.docFreq == 1) {
            // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
            // 读取docId
            singletonDocID = (int) docDeltaBuffer[0];
        } else {
            singletonDocID = -1;
            // vInt encode the remaining doc deltas and freqs:
            // 注意docDeltaBuffer 存储的是据上一个doc的差值信息
            for (int i = 0; i < docBufferUpto; i++) {
                final int docDelta = (int) docDeltaBuffer[i];
                // 对应的值是 该term 在该doc中的freq
                final int freq = (int) freqBuffer[i];
                if (!writeFreqs) {
                    docOut.writeVInt(docDelta);
                } else if (freq == 1) {
                    // 最低位为1 代表该doc下term出现的频率为1
                    docOut.writeVInt((docDelta << 1) | 1);
                } else {
                    // 最低位为0 代表还需要额外读取一个频率值
                    docOut.writeVInt(docDelta << 1);
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
            // 每个term每次出现都对应一个位置   所以计算位置信息的 block数量 就是将 totalTermFreq 与 blockSize 做比较
            if (state.totalTermFreq > BLOCK_SIZE) {
                // record file offset for last pos in last block
                // 只有当position信息 提前写入到 索引文件中 才会有该值
                lastPosBlockOffset = posOut.getFilePointer() - posStartFP;
            } else {
                lastPosBlockOffset = -1;
            }
            // 代表还有部分数据没有写入到磁盘  将这些数据单独写入  注意这里不会生成 lastPosBlockOffset
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
                for (int i = 0; i < posBufferUpto; i++) {
                    // 这里记录的是位置的增量值
                    final int posDelta = (int) posDeltaBuffer[i];
                    // 如果携带了 payload
                    if (writePayloads) {
                        // 记录负载信息的长度
                        final int payloadLength = (int) payloadLengthBuffer[i];
                        if (payloadLength != lastPayloadLength) {
                            lastPayloadLength = payloadLength;
                            // 最低位为1 代表存储了负载长度
                            posOut.writeVInt((posDelta << 1) | 1);
                            posOut.writeVInt(payloadLength);
                        } else {
                            // 最低位为1 代表该position对应的负载数据长度与上一个一致
                            posOut.writeVInt(posDelta << 1);
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
        // 代表有数据已经写入到跳跃表中了  需要将数据持久化到索引文件中    跳跃表应该只是检索用的 就像 disi中的rank[] 实际数据 docId 和 freq已经通过util写入到docOut了
        if (docCount > BLOCK_SIZE) {
            // 差值就是  在跳跃表的数据还未写入前的索引文件偏移量
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

    /**
     * 将描述 term的一些相关信息 比如出现在哪些doc下 position等信息 写入到传入的 out中
     *
     * @param out       存储生成的位置信息
     * @param fieldInfo 该term 关联的 field
     * @param _state    某个term之前通过positionWriter写入位置信息后返回的 各种 filePoint 等信息
     * @param absolute  本次写入的是绝对数值 还是相对数值  在BlockTreeTermsWriter中 当某些term需要合并成block时 首个term需要写入绝对数据 之后的数据就可以以之前的数据为参照 写入相对数据
     * @throws IOException
     */
    @Override
    public void encodeTerm(DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute) throws IOException {
        IntBlockTermState state = (IntBlockTermState) _state;
        // 如果基于绝对值进行处理 那么无法借助上次的 state信息
        if (absolute) {
            lastState = emptyState;
            assert lastState.docStartFP == 0;
        }

        // lastState.singletonDocID != -1  代表上一个term只出现在一个doc中  (无关出现了多少次)
        // 如果本次的term 刚好也只出现在一个doc中 就可以直接存储他们的差值
        // 当term只出现在 一个doc时  docId 信息是不会写入到 .doc文件中的  所以 .doc.filePoint 是一致的
        if (lastState.singletonDocID != -1 && state.singletonDocID != -1 && state.docStartFP == lastState.docStartFP) {
            // With runs of rare values such as ID fields, the increment of pointers in the docs file is often 0.
            // Furthermore some ID schemes like auto-increment IDs or Flake IDs are monotonic, so we encode the delta
            // between consecutive doc IDs to save space.
            // 记录doc的增值
            final long delta = (long) state.singletonDocID - lastState.singletonDocID;
            // zigzag 解决了负数占用位数过多的问题 提供了压缩的可能
            out.writeVLong((BitUtil.zigZagEncode(delta) << 1) | 0x01);
        } else {
            // 写入前后2个term在处理前 .doc文件的偏移量
            out.writeVLong((state.docStartFP - lastState.docStartFP) << 1);
            // 如果本次term只出现在一个doc中 直接写入docId的值
            if (state.singletonDocID != -1) {
                out.writeVInt(state.singletonDocID);
            }
        }

        // 记录位置信息
        if (writePositions) {
            // 写入pos文件的偏移量差值
            out.writeVLong(state.posStartFP - lastState.posStartFP);
            // 如果还携带了 payload 或者 offset信息 也将文件的偏移量差值写入
            if (writePayloads || writeOffsets) {
                out.writeVLong(state.payStartFP - lastState.payStartFP);
            }
        }
        if (writePositions) {
            // 每当position信息 超过一个block时 会将信息写入到索引文件中 最终lastPosBlockOffset 就等于以整数块写入索引文件后的偏移量 - 起始偏移量的差值
            // 当term写完时 最后不足一个block的位置信息会单独写入
            if (state.lastPosBlockOffset != -1) {
                out.writeVLong(state.lastPosBlockOffset);
            }
        }

        // skipOffset是 在跳跃表的数据还未写入前的索引文件偏移量 (此时doc索引文件中仅记录了docID信息)
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
