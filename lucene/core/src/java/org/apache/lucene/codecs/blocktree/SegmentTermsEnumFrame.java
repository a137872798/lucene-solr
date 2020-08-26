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
package org.apache.lucene.codecs.blocktree;


import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;
import java.util.Arrays;

/**
 * 一个框架对象   包含在 segmentTermsEnum中
 */
final class SegmentTermsEnumFrame {
    // Our index in stack[]:
    // 对应 frame数组的下标
    final int ord;

    /**
     * 某次 BlockTreeTermsWriter生成的block 对应的某批node 是否包含termNode
     */
    boolean hasTerms;
    /**
     * 该值初始状态就是 hasTerms
     */
    boolean hasTermsOrig;
    /**
     * 如果本次 writeBlocks 仅产生了一个block 该属性为false  否则为true
     */
    boolean isFloor;

    // 这里涉及到 FST
    FST.Arc<BytesRef> arc;

    //static boolean DEBUG = BlockTreeTermsWriter.DEBUG;

    // File pointer where this block was loaded from
    long fp;
    /**
     * 对应 termOut的起始偏移量
     */
    long fpOrig;
    /**
     * 记录当前 block末尾对应的文件偏移量
     */
    long fpEnd;
    long totalSuffixBytes; // for stats

    /**
     * 存储某个block下所有 term后缀的容器
     */
    byte[] suffixBytes = new byte[128];
    final ByteArrayDataInput suffixesReader = new ByteArrayDataInput();

    /**
     * 存储某个block 下所有 term后缀长度的容器
     */
    byte[] suffixLengthBytes;
    final ByteArrayDataInput suffixLengthsReader;

    /**
     * 存储某个block下所有term对应stat信息的容器
     */
    byte[] statBytes = new byte[64];
    int statsSingletonRunLength = 0;
    final ByteArrayDataInput statsReader = new ByteArrayDataInput();

    byte[] floorData = new byte[32];
    final ByteArrayDataInput floorDataReader = new ByteArrayDataInput();

    // Length of prefix shared by all terms in this block
    // 是由 SegmentTermsEnum 从外部设置的
    int prefix;

    // Number of entries (term or sub-block) in this block
    int entCount;

    // Which term we will next read, or -1 if the block
    // isn't loaded yet
    // 下一个将要读取的term 在block的下标  从0开始
    int nextEnt;

    // True if this block is either not a floor block,
    // or, it's the last sub-block of a floor block
    boolean isLastInFloor;

    // True if all entries are terms
    boolean isLeafBlock;

    /**
     * 生成当前正在处理的blockNode 的那批node数据写入前的文件偏移量
     */
    long lastSubFP;

    int nextFloorLabel;
    int numFollowFloorBlocks;

    // Next term to decode metaData; we decode metaData
    // lazily so that scanning to find the matching term is
    // fast and only if you find a match and app wants the
    // stats or docs/positions enums, will we decode the
    // metaData
    int metaDataUpto;

    /**
     * 记录某个term position等信息偏移量 出现在多少doc 等相关信息
     */
    final BlockTermState state;

    // metadata buffer
    // 存储元数据相关的容器
    byte[] bytes = new byte[32];
    final ByteArrayDataInput bytesReader = new ByteArrayDataInput();

    private final SegmentTermsEnum ste;
    private final int version;

    /**
     * @param ste
     * @param ord
     * @throws IOException
     */
    public SegmentTermsEnumFrame(SegmentTermsEnum ste, int ord) throws IOException {
        this.ste = ste;
        this.ord = ord;
        // parent 就是 BlockTreeTermsReader
        // postingsReader 是 BlockTreeTermsReader 关联的描述term position信息的reader
        // newTermState 创建一个空的用于记录 term 信息的临时对象
        this.state = ste.fr.parent.postingsReader.newTermState();
        this.state.totalTermFreq = -1;
        this.version = ste.fr.parent.version;
        // 支持压缩模式
        if (version >= BlockTreeTermsReader.VERSION_COMPRESSED_SUFFIXES) {
            suffixLengthBytes = new byte[32];
            suffixLengthsReader = new ByteArrayDataInput();
        } else {
            suffixLengthBytes = null;
            suffixLengthsReader = suffixesReader;
        }
    }

    /**
     * 如果某次 writeBlocks 产生了多个block  那么将产生的block数量 前缀 等信息填充到属性中
     *
     * @param in
     * @param source
     */
    public void setFloorData(ByteArrayDataInput in, BytesRef source) {
        // 获取剩余的长度
        final int numBytes = source.length - (in.getPosition() - source.offset);
        // 进行扩容
        if (numBytes > floorData.length) {
            floorData = new byte[ArrayUtil.oversize(numBytes, 1)];
        }
        // 将剩余的数据拷贝到floorData中
        System.arraycopy(source.bytes, source.offset + in.getPosition(), floorData, 0, numBytes);
        floorDataReader.reset(floorData, 0, numBytes);
        // 本批数据中总计生成了多少block  除去 label为-1的block  详见 BlockTreeTermsWriter
        numFollowFloorBlocks = floorDataReader.readVInt();
        // 读取 大前缀后 首个label有效的小前缀的值   比如 a* ab*  a称为大前缀 ab 称为小前缀
        nextFloorLabel = floorDataReader.readByte() & 0xff;
        //if (DEBUG) {
        //System.out.println("    setFloorData fpOrig=" + fpOrig + " bytes=" + new BytesRef(source.bytes, source.offset + in.getPosition(), numBytes) + " numFollowFloorBlocks=" + numFollowFloorBlocks + " nextFloorLabel=" + toHex(nextFloorLabel));
        //}
    }

    public int getTermBlockOrd() {
        return isLeafBlock ? nextEnt : state.termBlockOrd;
    }

    /**
     * 切换到 某次 writeBlocks 中生成的多个block的下一个   又或者是直接切换到下一个block   这里能够确保不会发生回退
     * 比如   当前存在  a*   ab*  ac*  实际写入索引文件的顺序应该是  ab* ac* a* 因为a* 的共享前缀是最小的 它最后被冻结   在外层next的判断中已经过滤掉 ac* -> a*的情况了 这里直接往下读取只可能出现 ab* -> ac* 的情况
     * @throws IOException
     */
    void loadNextFloorBlock() throws IOException {
        //if (DEBUG) {
        //System.out.println("    loadNextFloorBlock fp=" + fp + " fpEnd=" + fpEnd);
        //}
        assert arc == null || isFloor : "arc=" + arc + " isFloor=" + isFloor;
        fp = fpEnd;
        nextEnt = -1;
        loadBlock();
    }

    /* Does initial decode of next block of terms; this
       doesn't actually decode the docFreq, totalTermFreq,
       postings details (frq/prx offset, etc.) metadata;
       it just loads them as byte[] blobs which are then
       decoded on-demand if the metadata is ever requested
       for any term in this block.  This enables terms-only
       intensive consumes (eg certain MTQs, respelling) to
       not pay the price of decoding metadata they won't
       use.
       这里大体上还是还原写入到 termOut中的数据  以一个block为单位
       */
    void loadBlock() throws IOException {

        // Clone the IndexInput lazily, so that consumers
        // that just pull a TermsEnum to
        // seekExact(TermState) don't pay this cost:
        // 延迟初始化输入流  也就是写入term的索引文件输入流
        ste.initIndexInput();

        // -1 代表未加载
        if (nextEnt != -1) {
            // Already loaded
            return;
        }
        //System.out.println("blc=" + blockLoadCount);

        // 跳转到 预备写入 term的偏移量   以下对应 BlockTreeTermsWriter.writeBlock 的逻辑
        ste.in.seek(fp);
        int code = ste.in.readVInt();
        // 代表当前block下 node 的数量
        entCount = code >>> 1;
        assert entCount > 0;
        // 代表此时处理到最后的node了 也就是本次 writeBlocks 正在处理最后一个block
        isLastInFloor = (code & 1) != 0;

        assert arc == null || (isLastInFloor || isFloor) : "fp=" + fp + " arc=" + arc + " isFloor=" + isFloor + " isLastInFloor=" + isLastInFloor;

        // TODO: if suffixes were stored in random-access
        // array structure, then we could do binary search
        // instead of linear scan to find target term; eg
        // we could have simple array of offsets

        final long startSuffixFP = ste.in.getFilePointer();
        // term suffixes:
        // 是否支持压缩  当版本号以5开始后 就开始支持term前缀以压缩方式写入了
        if (version >= BlockTreeTermsReader.VERSION_COMPRESSED_SUFFIXES) {
            // 获取 code信息
            final long codeL = ste.in.readVLong();
            // 代表生成该block的所有node 都是 termNode
            isLeafBlock = (codeL & 0x04) != 0;
            // 所有后缀长度的和  如果是 termNode 就是除去大前缀后的部分 的总和 如果包含 blockNode 那么仅仅写入 大前缀与小前缀的差值部分 因为子blockNode的后缀在之前就已经写好并存储了
            final int numSuffixBytes = (int) (codeL >>> 3);
            if (suffixBytes.length < numSuffixBytes) {
                suffixBytes = new byte[ArrayUtil.oversize(numSuffixBytes, 1)];
            }
            try {
                // 获取压缩类型
                compressionAlg = CompressionAlgorithm.byCode((int) codeL & 0x03);
            } catch (IllegalArgumentException e) {
                throw new CorruptIndexException(e.getMessage(), ste.in, e);
            }
            // 将数据读取到 数组中
            compressionAlg.read(ste.in, suffixBytes, numSuffixBytes);
            // 初始化 reader对象
            suffixesReader.reset(suffixBytes, 0, numSuffixBytes);

            // 后缀数量 也就是 term的数量
            int numSuffixLengthBytes = ste.in.readVInt();
            // 检测是否所有的term 后缀长度相同
            final boolean allEqual = (numSuffixLengthBytes & 0x01) != 0;
            numSuffixLengthBytes >>>= 1;
            // 为存储后缀长度的数组扩容
            if (suffixLengthBytes.length < numSuffixLengthBytes) {
                suffixLengthBytes = new byte[ArrayUtil.oversize(numSuffixLengthBytes, 1)];
            }
            if (allEqual) {
                Arrays.fill(suffixLengthBytes, 0, numSuffixLengthBytes, ste.in.readByte());
            } else {
                ste.in.readBytes(suffixLengthBytes, 0, numSuffixLengthBytes);
            }
            suffixLengthsReader.reset(suffixLengthBytes, 0, numSuffixLengthBytes);
            // TODO 忽略不支持压缩的逻辑
        } else {
            code = ste.in.readVInt();
            isLeafBlock = (code & 1) != 0;
            int numBytes = code >>> 1;
            if (suffixBytes.length < numBytes) {
                suffixBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
            }
            ste.in.readBytes(suffixBytes, 0, numBytes);
            suffixesReader.reset(suffixBytes, 0, numBytes);
        }

        // 记录有关后缀信息总计占有多少byte
        totalSuffixBytes = ste.in.getFilePointer() - startSuffixFP;

    /*if (DEBUG) {
      if (arc == null) {
      System.out.println("    loadBlock (next) fp=" + fp + " entCount=" + entCount + " prefixLen=" + prefix + " isLastInFloor=" + isLastInFloor + " leaf?=" + isLeafBlock);
      } else {
      System.out.println("    loadBlock (seek) fp=" + fp + " entCount=" + entCount + " prefixLen=" + prefix + " hasTerms?=" + hasTerms + " isFloor?=" + isFloor + " isLastInFloor=" + isLastInFloor + " leaf?=" + isLeafBlock);
      }
      }*/
        // 接着是统计信息  统计信息占用多少byte
        // stats
        int numBytes = ste.in.readVInt();
        // 对统计容器扩容
        if (statBytes.length < numBytes) {
            statBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
        }
        // 将数据搬到  statBytes
        ste.in.readBytes(statBytes, 0, numBytes);
        statsReader.reset(statBytes, 0, numBytes);
        statsSingletonRunLength = 0;
        metaDataUpto = 0;

        state.termBlockOrd = 0;
        nextEnt = 0;
        lastSubFP = -1;

        // TODO: we could skip this if !hasTerms; but
        // that's rare so won't help much
        // metadata
        // 元数据  通过这些元数据可以定位到 positionWriter中写入的 3个索引文件 .pay .pos .doc
        numBytes = ste.in.readVInt();
        if (bytes.length < numBytes) {
            bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
        }
        ste.in.readBytes(bytes, 0, numBytes);
        bytesReader.reset(bytes, 0, numBytes);

        // Sub-blocks of a single floor block are always
        // written one after another -- tail recurse:
        fpEnd = ste.in.getFilePointer();
        // if (DEBUG) {
        //   System.out.println("      fpEnd=" + fpEnd);
        // }
    }

    /**
     * 复位
     */
    void rewind() {

        // Force reload:
        fp = fpOrig;
        nextEnt = -1;
        hasTerms = hasTermsOrig;
        if (isFloor) {
            floorDataReader.rewind();
            numFollowFloorBlocks = floorDataReader.readVInt();
            assert numFollowFloorBlocks > 0;
            nextFloorLabel = floorDataReader.readByte() & 0xff;
        }

    /*
    //System.out.println("rewind");
    // Keeps the block loaded, but rewinds its state:
    if (nextEnt > 0 || fp != fpOrig) {
    if (DEBUG) {
    System.out.println("      rewind frame ord=" + ord + " fpOrig=" + fpOrig + " fp=" + fp + " hasTerms?=" + hasTerms + " isFloor?=" + isFloor + " nextEnt=" + nextEnt + " prefixLen=" + prefix);
    }
    if (fp != fpOrig) {
    fp = fpOrig;
    nextEnt = -1;
    } else {
    nextEnt = 0;
    }
    hasTerms = hasTermsOrig;
    if (isFloor) {
    floorDataReader.rewind();
    numFollowFloorBlocks = floorDataReader.readVInt();
    nextFloorLabel = floorDataReader.readByte() & 0xff;
    }
    assert suffixBytes != null;
    suffixesReader.rewind();
    assert statBytes != null;
    statsReader.rewind();
    metaDataUpto = 0;
    state.termBlockOrd = 0;
    // TODO: skip this if !hasTerms?  Then postings
    // impl wouldn't have to write useless 0 byte
    postingsReader.resetTermsBlock(fieldInfo, state);
    lastSubFP = -1;
    } else if (DEBUG) {
    System.out.println("      skip rewind fp=" + fp + " fpOrig=" + fpOrig + " nextEnt=" + nextEnt + " ord=" + ord);
    }
    */
    }

    // Decodes next entry; returns true if it's a sub-block
    // 读取某个term的数据
    public boolean next() throws IOException {
        // 代表该block下所有node 都是termNode
        // 一开始从后往前读(最后一次通过一个空字符串将之前的数据全部写入fst)如果此时发现全部都是termNode 就代表在这途中没有发生任何一次block的生成 也就没有任何公共前缀 因为伴随着前缀被共享 必然会产生一个blockNode
        if (isLeafBlock) {
            // 这里会将读取出来的term信息回填到 SegmentTermsEnum中
            nextLeaf();
            return false;
        } else {
            // 代表本次block下部分节点是 blockNode
            return nextNonLeaf();
        }
    }

    /**
     * 读取某个数据页    看来所有node 都为term的block被称作leaf
     */
    public void nextLeaf() {
        //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
        assert nextEnt != -1 && nextEnt < entCount : "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
        // 切换到下一个 node
        nextEnt++;
        // 读取后缀长度
        suffix = suffixLengthsReader.readVInt();
        startBytePos = suffixesReader.getPosition();
        // 在当前ste.term的基础上增加后缀
        ste.term.setLength(prefix + suffix);
        ste.term.grow(ste.term.length());
        suffixesReader.readBytes(ste.term.bytes(), prefix, suffix);
        ste.termExists = true;
    }

    /**
     * 当前不是完全的叶节点 也就是必然包含blockNode
     *
     * @return
     * @throws IOException
     */
    public boolean nextNonLeaf() throws IOException {
        //if (DEBUG) System.out.println("  stef.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + suffixesReader.getPosition());
        while (true) {
            // 代表此时已经读取到当前block的最后一个节点了
            if (nextEnt == entCount) {
                assert arc == null || (isFloor && isLastInFloor == false) : "isFloor=" + isFloor + " isLastInFloor=" + isLastInFloor;
                // 进入到这里时已经确保此 isFloor && isLastInFloor == false  否则会被外层的 next() 逻辑拦截
                loadNextFloorBlock();
                if (isLeafBlock) {
                    nextLeaf();
                    return false;
                } else {
                    continue;
                }
            }

            assert nextEnt != -1 && nextEnt < entCount : "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
            // 记录下次要读取的node
            nextEnt++;
            final int code = suffixLengthsReader.readVInt();
            suffix = code >>> 1;
            // 这个是后缀长度
            startBytePos = suffixesReader.getPosition();
            ste.term.setLength(prefix + suffix);
            ste.term.grow(ste.term.length());
            // 仅追加后缀 实际上当遇到blockNode  它的含义就变成了 先将该blockNode 的前缀设置进去 比如下次遇到了termNode 就可以将后缀追加上去 进而生成了一个完整的term
            suffixesReader.readBytes(ste.term.bytes(), prefix, suffix);
            // 代表本次处理的是一个 termNode
            if ((code & 1) == 0) {
                // A normal term
                ste.termExists = true;
                subCode = 0;
                state.termBlockOrd++;
                return false;
                // 代表此时处理的是一个 blockNode  并且此时需要将前缀读取出来
                // 每次blockNode 存储的后缀信息 都是下层共享前缀长度 - 上层共享前缀长度  因为最后写入的是 "" 所以反推回来 第一次遇到的blockNode 的后缀长度实际上就是 共享前缀长度
            } else {
                // A sub-block; make sub-FP absolute:
                ste.termExists = false;
                // 当前block数据写入前文件偏移量 对应 生成该blockNode 的那批node开始写入的文件偏移量
                subCode = suffixLengthsReader.readVLong();
                // 获取子block的fp
                lastSubFP = fp - subCode;
                //if (DEBUG) {
                //System.out.println("    lastSubFP=" + lastSubFP);
                //}
                return true;
            }
        }
    }

    // TODO: make this array'd so we can do bin search?
    // likely not worth it?  need to measure how many
    // floor blocks we "typically" get
    // 当 writeBlocks下有多个block时 定位一个更精确的block
    public void scanToFloorFrame(BytesRef target) {

        // !isFloor 代表当前block 对应某次 writerBlocks下所有的node  所以应该是不用再缩小范围了
        // 第二种情况就是没有匹配的必要了
        if (!isFloor || target.length <= prefix) {
            // if (DEBUG) {
            //   System.out.println("    scanToFloorFrame skip: isFloor=" + isFloor + " target.length=" + target.length + " vs prefix=" + prefix);
            // }
            return;
        }

        // 下个待匹配的 label
        final int targetLabel = target.bytes[target.offset + prefix] & 0xFF;

        // if (DEBUG) {
        //   System.out.println("    scanToFloorFrame fpOrig=" + fpOrig + " targetLabel=" + toHex(targetLabel) + " vs nextFloorLabel=" + toHex(nextFloorLabel) + " numFollowFloorBlocks=" + numFollowFloorBlocks);
        // }

        // 第一个floor的label就比目标值大 就不需要再缩小范围了
        if (targetLabel < nextFloorLabel) {
            // if (DEBUG) {
            //   System.out.println("      already on correct block");
            // }
            return;
        }

        assert numFollowFloorBlocks != 0;

        // 以下就是开始进一步缩小范围   能进入到这里的前提就是 targetLabel >= nextFloorLabel
        // 每次就是比较 nextFloorLabel 与 目标值
        long newFP = fpOrig;
        while (true) {
            // 对应的是 scratchBytes.writeVLong((sub.fp - fp) << 1 | (sub.hasTerms ? 1 : 0));
            final long code = floorDataReader.readVLong();
            // 获取本次 writeBlocks 包含小前缀的 blockNode 对应的term索引文件的起始偏移量
            newFP = fpOrig + (code >>> 1);
            // 包含小前缀的 blockNode 中是否包含 termNode
            hasTerms = (code & 1) != 0;
            // if (DEBUG) {
            //   System.out.println("      label=" + toHex(nextFloorLabel) + " fp=" + newFP + " hasTerms?=" + hasTerms + " numFollowFloor=" + numFollowFloorBlocks);
            // }

            // 当前轮到的这个小前缀 是否是本block下最后一个   比如 a* ab* ac* ad*  就是说在a*这个block生成的同时还生成了3个附带小前缀的block ad* 就是最后一个block
            isLastInFloor = numFollowFloorBlocks == 1;
            numFollowFloorBlocks--;

            if (isLastInFloor) {
                nextFloorLabel = 256;
                // if (DEBUG) {
                //   System.out.println("        stop!  last block nextFloorLabel=" + toHex(nextFloorLabel));
                // }
                break;
            } else {
                // 获取下一个label 并进行匹配
                nextFloorLabel = floorDataReader.readByte() & 0xff;
                if (targetLabel < nextFloorLabel) {
                    // if (DEBUG) {
                    //   System.out.println("        stop!  nextFloorLabel=" + toHex(nextFloorLabel));
                    // }
                    break;
                }
            }
        }

        // 此时已经确定了term所在block的最小范围 (仅这层frame 如果小前缀对应的block内部还是 isFloor 也就是还存在多个小前缀block就要继续匹配)
        if (newFP != fp) {
            // Force re-load of the block:
            // if (DEBUG) {
            //   System.out.println("      force switch to fp=" + newFP + " oldFP=" + fp);
            // }
            // 更新索引文件偏移量
            nextEnt = -1;
            fp = newFP;
        } else {
            // if (DEBUG) {
            //   System.out.println("      stay on same fp=" + newFP);
            // }
        }
    }

    /**
     * 解析元数据信息
     *
     * @throws IOException
     */
    public void decodeMetaData() throws IOException {

        //if (DEBUG) System.out.println("\nBTTR.decodeMetadata seg=" + segment + " mdUpto=" + metaDataUpto + " vs termBlockOrd=" + state.termBlockOrd);

        // lazily catch up on metadata decode:
        // 检查当前block下 已经遍历到第几个 termNode了 (不包含 blockNode )
        final int limit = getTermBlockOrd();
        boolean absolute = metaDataUpto == 0;
        assert limit > 0;

        // TODO: better API would be "jump straight to term=N"???
        // 这里比较笨 只能挨个解析 而不能跳跃直接开始解析目标term对应的元数据
        while (metaDataUpto < limit) {

            // TODO: we could make "tiers" of metadata, ie,
            // decode docFreq/totalTF but don't decode postings
            // metadata; this way caller could get
            // docFreq/totalTF w/o paying decode cost for
            // postings

            // TODO: if docFreq were bulk decoded we could
            // just skipN here:

            // 代表支持压缩
            if (version >= BlockTreeTermsReader.VERSION_COMPRESSED_SUFFIXES) {
                // 默认是0
                if (statsSingletonRunLength > 0) {
                    state.docFreq = 1;
                    state.totalTermFreq = 1;
                    statsSingletonRunLength--;
                } else {
                    int token = statsReader.readVInt();
                    // 最低位为1 代表开始检测到某个term 在所有doc下仅出现一次
                    if ((token & 1) == 1) {
                        state.docFreq = 1;
                        state.totalTermFreq = 1;
                        // 存在多少个连续的term
                        statsSingletonRunLength = token >>> 1;
                    } else {
                        // 该term 总计出现在多少doc中
                        state.docFreq = token >>> 1;
                        if (ste.fr.fieldInfo.getIndexOptions() == IndexOptions.DOCS) {
                            state.totalTermFreq = state.docFreq;
                        } else {
                            // 代表还存储了频率信息 基于差值存储 还原该term在所有doc下总计出现多少次
                            state.totalTermFreq = state.docFreq + statsReader.readVLong();
                        }
                    }
                }
            // TODO 忽略非压缩的版本
            } else {
                assert statsSingletonRunLength == 0;
                state.docFreq = statsReader.readVInt();
                //if (DEBUG) System.out.println("    dF=" + state.docFreq);
                if (ste.fr.fieldInfo.getIndexOptions() == IndexOptions.DOCS) {
                    state.totalTermFreq = state.docFreq; // all postings have freq=1
                } else {
                    state.totalTermFreq = state.docFreq + statsReader.readVLong();
                    //if (DEBUG) System.out.println("    totTF=" + state.totalTermFreq);
                }
            }

            // metadata
            // 还原了存储term doc.pos.pay 等信息的元数据  并将结果回填到 state中
            ste.fr.parent.postingsReader.decodeTerm(bytesReader, ste.fr.fieldInfo, state, absolute);

            metaDataUpto++;
            absolute = false;
        }
        state.termBlockOrd = metaDataUpto;
    }

    // Used only by assert
    private boolean prefixMatches(BytesRef target) {
        for (int bytePos = 0; bytePos < prefix; bytePos++) {
            if (target.bytes[target.offset + bytePos] != ste.term.byteAt(bytePos)) {
                return false;
            }
        }

        return true;
    }

    // Scans to sub-block that has this target fp; only
    // called by next(); NOTE: does not set
    // startBytePos/suffix as a side effect
    // 确保此时扫描的 sub-block 就是给定偏移量对应的这个   sub-block的定义就是 blockNode
    public void scanToSubBlock(long subFP) {
        assert !isLeafBlock;
        //if (DEBUG) System.out.println("  scanToSubBlock fp=" + fp + " subFP=" + subFP + " entCount=" + entCount + " lastSubFP=" + lastSubFP);
        //assert nextEnt == 0;
        if (lastSubFP == subFP) {
            //if (DEBUG) System.out.println("    already positioned");
            return;
        }
        assert subFP < fp : "fp=" + fp + " subFP=" + subFP;
        // 倒推出 code值
        final long targetSubCode = fp - subFP;
        //if (DEBUG) System.out.println("    targetSubCode=" + targetSubCode);
        // 遍历所有 sub-block 直到找到符合条件的位置
        while (true) {
            assert nextEnt < entCount;
            nextEnt++;
            // 这里开始跳过之前处理过的前缀
            final int code = suffixLengthsReader.readVInt();
            suffixesReader.skipBytes(code >>> 1);
            if ((code & 1) != 0) {
                final long subCode = suffixLengthsReader.readVLong();
                if (targetSubCode == subCode) {
                    //if (DEBUG) System.out.println("        match!");
                    lastSubFP = subFP;
                    return;
                }
            } else {
                state.termBlockOrd++;
            }
        }
    }

    // NOTE: sets startBytePos/suffix as a side effect
    /**
     *
     * 从当前block下查找目标term
     * @param target
     * @param exactOnly  是否精确查找
     * @return
     * @throws IOException
     */
    public SeekStatus scanToTerm(BytesRef target, boolean exactOnly) throws IOException {
        return isLeafBlock ? scanToTermLeaf(target, exactOnly) : scanToTermNonLeaf(target, exactOnly);
    }

    /**
     * 当前 suffixBytes偏移量
     */
    private int startBytePos;
    /**
     * 当前正在使用的这个term 的后缀长度
     */
    private int suffix;
    /**
     * 当前block数据写入前文件偏移量 对应 生成该blockNode 的那批node开始写入的文件偏移量
     */
    private long subCode;
    CompressionAlgorithm compressionAlg = CompressionAlgorithm.NO_COMPRESSION;

    // for debugging
  /*
  @SuppressWarnings("unused")
  static String brToString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      // If BytesRef isn't actually UTF8, or it's eg a
      // prefix of UTF8 that ends mid-unicode-char, we
      // fallback to hex:
      return b.toString();
    }
  }
  */


    /**
     * Target's prefix matches this block's prefix; we
     * scan the entries check if the suffix matches.
     * 代表当前block下都是 termNode
     * @param target  需要查找的term
     * @param exactOnly   是否精确匹配
     * @return
     * @throws IOException
     */
    public SeekStatus scanToTermLeaf(BytesRef target, boolean exactOnly) throws IOException {

        // if (DEBUG) System.out.println("    scanToTermLeaf: block fp=" + fp + " prefix=" + prefix + " nextEnt=" + nextEnt + " (of " + entCount + ") target=" + brToString(target) + " term=" + brToString(term));

        assert nextEnt != -1;

        ste.termExists = true;
        subCode = 0;

        // 已经读取到末尾了
        if (nextEnt == entCount) {
            if (exactOnly) {
                // 将此时读取到的最后一个term 回填到 SegmentTermsEnum.term上
                fillTerm();
            }
            // 并返回已经读取到末尾
            return SeekStatus.END;
        }

        assert prefixMatches(target);

        // TODO: binary search when all terms have the same length, which is common for ID fields,
        // which are also the most sensitive to lookup performance?
        // Loop over each entry (term or sub-block) in this block:
        do {
            // 不断遍历 termNode尝试匹配
            nextEnt++;

            suffix = suffixLengthsReader.readVInt();

            // if (DEBUG) {
            //   BytesRef suffixBytesRef = new BytesRef();
            //   suffixBytesRef.bytes = suffixBytes;
            //   suffixBytesRef.offset = suffixesReader.getPosition();
            //   suffixBytesRef.length = suffix;
            //   System.out.println("      cycle: term " + (nextEnt-1) + " (of " + entCount + ") suffix=" + brToString(suffixBytesRef));
            // }

            startBytePos = suffixesReader.getPosition();
            suffixesReader.skipBytes(suffix);

            // Loop over bytes in the suffix, comparing to the target
            final int cmp = Arrays.compareUnsigned(
                    suffixBytes, startBytePos, startBytePos + suffix,
                    target.bytes, target.offset + prefix, target.offset + target.length);

            if (cmp < 0) {
                // Current entry is still before the target;
                // keep scanning
            } else if (cmp > 0) {
                // Done!  Current entry is after target --
                // return NOT_FOUND:
                // 找到的termNode 比目标值大 也就代表没有找到匹配的 term
                fillTerm();

                //if (DEBUG) System.out.println("        not found");
                return SeekStatus.NOT_FOUND;
            } else {
                // Exact match!

                // This cannot be a sub-block because we
                // would have followed the index to this
                // sub-block from the start:

                assert ste.termExists;
                fillTerm();
                //if (DEBUG) System.out.println("        found!");
                return SeekStatus.FOUND;
            }
        } while (nextEnt < entCount);

        // It is possible (and OK) that terms index pointed us
        // at this block, but, we scanned the entire block and
        // did not find the term to position to.  This happens
        // when the target is after the last term in the block
        // (but, before the next term in the index).  EG
        // target could be foozzz, and terms index pointed us
        // to the foo* block, but the last term in this block
        // was fooz (and, eg, first term in the next block will
        // bee fop).
        //if (DEBUG) System.out.println("      block end");
        if (exactOnly) {
            fillTerm();
        }

        // TODO: not consistent that in the
        // not-exact case we don't next() into the next
        // frame here
        return SeekStatus.END;
    }

    // Target's prefix matches this block's prefix; we
    // scan the entries check if the suffix matches.
    // 在混合了blockNode 和 termNode 中找到目标term
    public SeekStatus scanToTermNonLeaf(BytesRef target, boolean exactOnly) throws IOException {

        //if (DEBUG) System.out.println("    scanToTermNonLeaf: block fp=" + fp + " prefix=" + prefix + " nextEnt=" + nextEnt + " (of " + entCount + ") target=" + brToString(target) + " term=" + brToString(target));

        assert nextEnt != -1;

        if (nextEnt == entCount) {
            if (exactOnly) {
                fillTerm();
                // subCode == 0 代表此时是 termNode
                ste.termExists = subCode == 0;
            }
            return SeekStatus.END;
        }

        assert prefixMatches(target);

        // Loop over each entry (term or sub-block) in this block:
        while (nextEnt < entCount) {

            nextEnt++;

            final int code = suffixLengthsReader.readVInt();
            suffix = code >>> 1;

            //if (DEBUG) {
            //  BytesRef suffixBytesRef = new BytesRef();
            //  suffixBytesRef.bytes = suffixBytes;
            //  suffixBytesRef.offset = suffixesReader.getPosition();
            //  suffixBytesRef.length = suffix;
            //  System.out.println("      cycle: " + ((code&1)==1 ? "sub-block" : "term") + " " + (nextEnt-1) + " (of " + entCount + ") suffix=" + brToString(suffixBytesRef));
            //}

            // 因为在外层 target 没有匹配到 blockNode 所以在这里只可能以termNode 的形式匹配  又或者没匹配成功

            // 获取此时term的长度
            final int termLen = prefix + suffix;
            startBytePos = suffixesReader.getPosition();
            suffixesReader.skipBytes(suffix);
            ste.termExists = (code & 1) == 0;
            // 代表本次读到的是 termNode
            if (ste.termExists) {
                state.termBlockOrd++;
                subCode = 0;
            } else {
                // 代表本次读到的是 blockNode
                subCode = suffixLengthsReader.readVLong();
                lastSubFP = fp - subCode;
            }

            final int cmp = Arrays.compareUnsigned(
                    suffixBytes, startBytePos, startBytePos + suffix,
                    target.bytes, target.offset + prefix, target.offset + target.length);

            if (cmp < 0) {
                // Current entry is still before the target;
                // keep scanning
            } else if (cmp > 0) {
                // Done!  Current entry is after target --
                // return NOT_FOUND:
                fillTerm();

                //if (DEBUG) System.out.println("        maybe done exactOnly=" + exactOnly + " ste.termExists=" + ste.termExists);

                // 非精确模式下会返回略大于 target的值 在SegmentTermsEnum中 通过fst无法查找到数据时 已经代表不可能会匹配上blockNode了
                if (!exactOnly && !ste.termExists) {
                    //System.out.println("  now pushFrame");
                    // TODO this
                    // We are on a sub-block, and caller wants
                    // us to position to the next term after
                    // the target, so we must recurse into the
                    // sub-frame(s):
                    ste.currentFrame = ste.pushFrame(null, ste.currentFrame.lastSubFP, termLen);
                    ste.currentFrame.loadBlock();
                    while (ste.currentFrame.next()) {
                        ste.currentFrame = ste.pushFrame(null, ste.currentFrame.lastSubFP, ste.term.length());
                        ste.currentFrame.loadBlock();
                    }
                }

                //if (DEBUG) System.out.println("        not found");
                return SeekStatus.NOT_FOUND;
            } else {
                // Exact match!

                // This cannot be a sub-block because we
                // would have followed the index to this
                // sub-block from the start:

                assert ste.termExists;
                fillTerm();
                //if (DEBUG) System.out.println("        found!");
                return SeekStatus.FOUND;
            }
        }

        // It is possible (and OK) that terms index pointed us
        // at this block, but, we scanned the entire block and
        // did not find the term to position to.  This happens
        // when the target is after the last term in the block
        // (but, before the next term in the index).  EG
        // target could be foozzz, and terms index pointed us
        // to the foo* block, but the last term in this block
        // was fooz (and, eg, first term in the next block will
        // bee fop).
        //if (DEBUG) System.out.println("      block end");
        if (exactOnly) {
            fillTerm();
        }

        // TODO: not consistent that in the
        // not-exact case we don't next() into the next
        // frame here
        return SeekStatus.END;
    }

    private void fillTerm() {
        final int termLength = prefix + suffix;
        ste.term.setLength(termLength);
        ste.term.grow(termLength);
        System.arraycopy(suffixBytes, startBytePos, ste.term.bytes(), prefix, suffix);
    }
}
