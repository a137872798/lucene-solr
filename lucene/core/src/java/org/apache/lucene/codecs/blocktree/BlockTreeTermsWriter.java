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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.compress.LowercaseAsciiCompression;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/*
  TODO:
  
    - Currently there is a one-to-one mapping of indexed
      term to term block, but we could decouple the two, ie,
      put more terms into the index than there are blocks.
      The index would take up more RAM but then it'd be able
      to avoid seeking more often and could make PK/FuzzyQ
      faster if the additional indexed terms could store
      the offset into the terms block.

    - The blocks are not written in true depth-first
      order, meaning if you just next() the file pointer will
      sometimes jump backwards.  For example, block foo* will
      be written before block f* because it finished before.
      This could possibly hurt performance if the terms dict is
      not hot, since OSs anticipate sequential file access.  We
      could fix the writer to re-order the blocks as a 2nd
      pass.

    - Each block encodes the term suffixes packed
      sequentially using a separate vInt per term, which is
      1) wasteful and 2) slow (must linear scan to find a
      particular suffix).  We should instead 1) make
      random-access array so we can directly access the Nth
      suffix, and 2) bulk-encode this array using bulk int[]
      codecs; then at search time we can binary search when
      we seek a particular term.
*/

/**
 * Block-based terms index and dictionary writer.
 * <p>
 * Writes terms dict and index, block-encoding (column
 * stride) each term's metadata for each set of terms
 * between two index terms.
 * <p>
 * <p>
 * Files:
 * <ul>
 * <li><code>.tim</code>: <a href="#Termdictionary">Term Dictionary</a></li>
 * <li><code>.tip</code>: <a href="#Termindex">Term Index</a></li>
 * </ul>
 * <p>
 * <a id="Termdictionary"></a>
 * <h2>Term Dictionary</h2>
 *
 * <p>The .tim file contains the list of terms in each
 * field along with per-term statistics (such as docfreq)
 * and per-term metadata (typically pointers to the postings list
 * for that term in the inverted index).
 * </p>
 *
 * <p>The .tim is arranged in blocks: with blocks containing
 * a variable number of entries (by default 25-48), where
 * each entry is either a term or a reference to a
 * sub-block.</p>
 *
 * <p>NOTE: The term dictionary can plug into different postings implementations:
 * the postings writer/reader are actually responsible for encoding
 * and decoding the Postings Metadata and Term Metadata sections.</p>
 *
 * <ul>
 * <li>TermsDict (.tim) --&gt; Header, <i>PostingsHeader</i>, NodeBlock<sup>NumBlocks</sup>,
 * FieldSummary, DirOffset, Footer</li>
 * <li>NodeBlock --&gt; (OuterNode | InnerNode)</li>
 * <li>OuterNode --&gt; EntryCount, SuffixLength, Byte<sup>SuffixLength</sup>, StatsLength, &lt; TermStats &gt;<sup>EntryCount</sup>, MetaLength, &lt;<i>TermMetadata</i>&gt;<sup>EntryCount</sup></li>
 * <li>InnerNode --&gt; EntryCount, SuffixLength[,Sub?], Byte<sup>SuffixLength</sup>, StatsLength, &lt; TermStats ? &gt;<sup>EntryCount</sup>, MetaLength, &lt;<i>TermMetadata ? </i>&gt;<sup>EntryCount</sup></li>
 * <li>TermStats --&gt; DocFreq, TotalTermFreq </li>
 * <li>FieldSummary --&gt; NumFields, &lt;FieldNumber, NumTerms, RootCodeLength, Byte<sup>RootCodeLength</sup>,
 * SumTotalTermFreq?, SumDocFreq, DocCount, LongsSize, MinTerm, MaxTerm&gt;<sup>NumFields</sup></li>
 * <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 * <li>DirOffset --&gt; {@link DataOutput#writeLong Uint64}</li>
 * <li>MinTerm,MaxTerm --&gt; {@link DataOutput#writeVInt VInt} length followed by the byte[]</li>
 * <li>EntryCount,SuffixLength,StatsLength,DocFreq,MetaLength,NumFields,
 * FieldNumber,RootCodeLength,DocCount,LongsSize --&gt; {@link DataOutput#writeVInt VInt}</li>
 * <li>TotalTermFreq,NumTerms,SumTotalTermFreq,SumDocFreq --&gt;
 * {@link DataOutput#writeVLong VLong}</li>
 * <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 * <li>Header is a {@link CodecUtil#writeHeader CodecHeader} storing the version information
 * for the BlockTree implementation.</li>
 * <li>DirOffset is a pointer to the FieldSummary section.</li>
 * <li>DocFreq is the count of documents which contain the term.</li>
 * <li>TotalTermFreq is the total number of occurrences of the term. This is encoded
 * as the difference between the total number of occurrences and the DocFreq.</li>
 * <li>FieldNumber is the fields number from {@link FieldInfos}. (.fnm)</li>
 * <li>NumTerms is the number of unique terms for the field.</li>
 * <li>RootCode points to the root block for the field.</li>
 * <li>SumDocFreq is the total number of postings, the number of term-document pairs across
 * the entire field.</li>
 * <li>DocCount is the number of documents that have at least one posting for this field.</li>
 * <li>LongsSize records how many long values the postings writer/reader record per term
 * (e.g., to hold freq/prox/doc file offsets).
 * <li>MinTerm, MaxTerm are the lowest and highest term in this field.</li>
 * <li>PostingsHeader and TermMetadata are plugged into by the specific postings implementation:
 * these contain arbitrary per-file data (such as parameters or versioning information)
 * and per-term data (such as pointers to inverted files).</li>
 * <li>For inner nodes of the tree, every entry will steal one bit to mark whether it points
 * to child nodes(sub-block). If so, the corresponding TermStats and TermMetaData are omitted </li>
 * </ul>
 * <a id="Termindex"></a>
 * <h2>Term Index</h2>
 * <p>The .tip file contains an index into the term dictionary, so that it can be
 * accessed randomly.  The index is also used to determine
 * when a given term cannot exist on disk (in the .tim file), saving a disk seek.</p>
 * <ul>
 * <li>TermsIndex (.tip) --&gt; Header, FSTIndex<sup>NumFields</sup>
 * &lt;IndexStartFP&gt;<sup>NumFields</sup>, DirOffset, Footer</li>
 * <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}</li>
 * <li>DirOffset --&gt; {@link DataOutput#writeLong Uint64}</li>
 * <li>IndexStartFP --&gt; {@link DataOutput#writeVLong VLong}</li>
 * <!-- TODO: better describe FST output here -->
 * <li>FSTIndex --&gt; {@link FST FST&lt;byte[]&gt;}</li>
 * <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:</p>
 * <ul>
 * <li>The .tip file contains a separate FST for each
 * field.  The FST maps a term prefix to the on-disk
 * block that holds all terms starting with that
 * prefix.  Each field's IndexStartFP points to its
 * FST.</li>
 * <li>DirOffset is a pointer to the start of the IndexStartFPs
 * for all fields</li>
 * <li>It's possible that an on-disk block would contain
 * too many terms (more than the allowed maximum
 * (default: 48)).  When this happens, the block is
 * sub-divided into new blocks (called "floor
 * blocks"), and then the output in the FST for the
 * block's prefix encodes the leading byte of each
 * sub-block, and its file pointer.
 * </ul>
 *
 * @lucene.experimental 该对象负责 将fields信息持久化到索引文件中  同时利用了 FST
 * @see BlockTreeTermsReader
 */
public final class BlockTreeTermsWriter extends FieldsConsumer {

    /**
     * Suggested default value for the {@code
     * minItemsInBlock} parameter to {@link
     * #BlockTreeTermsWriter(SegmentWriteState, PostingsWriterBase, int, int)}.
     */
    public final static int DEFAULT_MIN_BLOCK_SIZE = 25;

    /**
     * Suggested default value for the {@code
     * maxItemsInBlock} parameter to {@link
     * #BlockTreeTermsWriter(SegmentWriteState, PostingsWriterBase, int, int)}.
     */
    public final static int DEFAULT_MAX_BLOCK_SIZE = 48;

    //public static boolean DEBUG = false;
    //public static boolean DEBUG2 = false;

    //private final static boolean SAVE_DOT_FILES = false;

    /**
     * 写入term数据的 输出流
     */
    private final IndexOutput termsOut;
    private final IndexOutput indexOut;

    /**
     * 写入的目标段此时有多少doc
     */
    final int maxDoc;
    /**
     * 要求至少要有min个item项才允许写入到block中
     */
    final int minItemsInBlock;
    final int maxItemsInBlock;

    /**
     * 该对象负责写入 有关 position相关的信息
     */
    final PostingsWriterBase postingsWriter;
    final FieldInfos fieldInfos;

    /**
     * 当某个field下所有term信息都处理完后 将相关信息抽取出来生成 metaData
     */
    private static class FieldMetaData {

        /**
         * 该field的描述信息
         */
        public final FieldInfo fieldInfo;
        /**
         * 所有term信息抽取整合成一个fst对象后 (作为term词典) 该fst下 emptyString 对应的权重
         */
        public final BytesRef rootCode;
        /**
         * 该field下有多少term
         */
        public final long numTerms;
        /**
         * 在写入fst之前  tim文件的偏移量
         */
        public final long indexStartFP;
        /**
         * 该field 下所有term的 频率总和
         */
        public final long sumTotalTermFreq;
        /**
         * field下所有term 对应相关的doc数量总和
         */
        public final long sumDocFreq;
        /**
         * 该field 总计出现多少个doc下
         */
        public final int docCount;

        // 这个field下最大的 term 和 最小的 term
        public final BytesRef minTerm;
        public final BytesRef maxTerm;

        public FieldMetaData(FieldInfo fieldInfo, BytesRef rootCode, long numTerms, long indexStartFP, long sumTotalTermFreq, long sumDocFreq, int docCount,
                             BytesRef minTerm, BytesRef maxTerm) {
            assert numTerms > 0;
            this.fieldInfo = fieldInfo;
            assert rootCode != null : "field=" + fieldInfo.name + " numTerms=" + numTerms;
            this.rootCode = rootCode;
            this.indexStartFP = indexStartFP;
            this.numTerms = numTerms;
            this.sumTotalTermFreq = sumTotalTermFreq;
            this.sumDocFreq = sumDocFreq;
            this.docCount = docCount;
            this.minTerm = minTerm;
            this.maxTerm = maxTerm;
        }
    }

    /**
     * 每当某个field的数据处理完成时  将相关信息抽取出来生成 metaData对象 并添加到该list中
     */
    private final List<FieldMetaData> fields = new ArrayList<>();

    /**
     * Create a new writer.  The number of items (terms or
     * sub-blocks) per block will aim to be between
     * minItemsPerBlock and maxItemsPerBlock, though in some
     * cases the blocks may be smaller than the min.
     *
     * @param state           描述本次刷盘相关的段信息
     * @param postingsWriter  该对象负责写入 term的位置信息
     * @param minItemsInBlock 默认25
     * @param maxItemsInBlock 默认48
     *                        初始化 term写入对象
     */
    public BlockTreeTermsWriter(SegmentWriteState state,
                                PostingsWriterBase postingsWriter,   // 对应 Lucene84PostingsWriter  也就是有关term的position信息 由该对象保存
                                int minItemsInBlock,
                                int maxItemsInBlock)
            throws IOException {
        validateSettings(minItemsInBlock,
                maxItemsInBlock);

        this.minItemsInBlock = minItemsInBlock;
        this.maxItemsInBlock = maxItemsInBlock;

        // 本次段要刷盘的最大doc数量
        this.maxDoc = state.segmentInfo.maxDoc();
        this.fieldInfos = state.fieldInfos;
        // 该对象负责存储 term的position信息 内部使用了一个跳跃表结构
        this.postingsWriter = postingsWriter;

        // 创建存储 term的索引文件  .tim
        final String termsName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockTreeTermsReader.TERMS_EXTENSION);
        termsOut = state.directory.createOutput(termsName, state.context);
        boolean success = false;
        IndexOutput indexOut = null;
        try {
            // 写入文件头
            CodecUtil.writeIndexHeader(termsOut, BlockTreeTermsReader.TERMS_CODEC_NAME, BlockTreeTermsReader.VERSION_CURRENT,
                    state.segmentInfo.getId(), state.segmentSuffix);

            // 创建  .tip 文件
            final String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockTreeTermsReader.TERMS_INDEX_EXTENSION);
            indexOut = state.directory.createOutput(indexName, state.context);
            CodecUtil.writeIndexHeader(indexOut, BlockTreeTermsReader.TERMS_INDEX_CODEC_NAME, BlockTreeTermsReader.VERSION_CURRENT,
                    state.segmentInfo.getId(), state.segmentSuffix);
            //segment = state.segmentInfo.name;

            // 这里就是为 termsOut写入了一个文件头  以及写入了 postingsWriter的 blockSize
            postingsWriter.init(termsOut, state);                          // have consumer write its format/header

            this.indexOut = indexOut;
            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(termsOut, indexOut);
            }
        }
    }

    /**
     * Writes the terms file trailer.
     */
    private void writeTrailer(IndexOutput out, long dirStart) throws IOException {
        out.writeLong(dirStart);
    }

    /**
     * Writes the index file trailer.
     */
    private void writeIndexTrailer(IndexOutput indexOut, long dirStart) throws IOException {
        indexOut.writeLong(dirStart);
    }

    /**
     * Throws {@code IllegalArgumentException} if any of these settings
     * is invalid.
     */
    public static void validateSettings(int minItemsInBlock, int maxItemsInBlock) {
        if (minItemsInBlock <= 1) {
            throw new IllegalArgumentException("minItemsInBlock must be >= 2; got " + minItemsInBlock);
        }
        if (minItemsInBlock > maxItemsInBlock) {
            throw new IllegalArgumentException("maxItemsInBlock must be >= minItemsInBlock; got maxItemsInBlock=" + maxItemsInBlock + " minItemsInBlock=" + minItemsInBlock);
        }
        if (2 * (minItemsInBlock - 1) > maxItemsInBlock) {
            throw new IllegalArgumentException("maxItemsInBlock must be at least 2*(minItemsInBlock-1); got maxItemsInBlock=" + maxItemsInBlock + " minItemsInBlock=" + minItemsInBlock);
        }
    }

    /**
     * 将一组 field 信息 以及标准因子信息写入到 索引文件中
     *
     * @param fields 该对象一般情况下是 FreqProxFields 对象
     * @param norms  该对象负责提供标准因子信息
     * @throws IOException
     */
    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
        //if (DEBUG) System.out.println("\nBTTW.write seg=" + segment);

        String lastField = null;
        // 遍历本次涉及到的所有 field
        // fields 在外层做过包装    (外层对field 按照写入格式进行了分组  并且使用一个装饰器确保只能读取到该组下的 field)
        for (String field : fields) {
            // 确保 field是 递增的     在 FreqProxTermsWriter中 为field 按照fieldName 排序过
            assert lastField == null || lastField.compareTo(field) < 0;
            lastField = field;

            //if (DEBUG) System.out.println("\nBTTW.write seg=" + segment + " field=" + field);
            // 获取一个遍历该field下所有term的 迭代器  实际上就是  FreqProxTerms
            // 因为在解析doc时  term 都会存入到一个 termHash结构中  通过包装持有termHash的 perField对象 就可以遍历之前写入的所有term了
            Terms terms = fields.terms(field);
            if (terms == null) {
                continue;
            }

            TermsEnum termsEnum = terms.iterator();
            // 将 term相关信息通过 TermsWriter写入  注意在之前的环节中已经将term按照字面量进行排序了
            TermsWriter termsWriter = new TermsWriter(fieldInfos.fieldInfo(field));
            while (true) {
                // 从 termHash中 挨个取出 term 并插入到 termWriter中
                BytesRef term = termsEnum.next();
                //if (DEBUG) System.out.println("BTTW: next term " + term);

                if (term == null) {
                    break;
                }

                //if (DEBUG) System.out.println("write field=" + fieldInfo.name + " term=" + brToString(term));
                // 挨个将 term信息写入到 索引文件中
                termsWriter.write(term, termsEnum, norms);
            }

            // 每当某个field下所有term写完后 需要将剩余不足block的部分也写入到索引文件中
            termsWriter.finish();

            //if (DEBUG) System.out.println("\nBTTW.write done seg=" + segment + " field=" + field);
        }
    }

    /**
     * 对输出进行编码  也就是为 fileOffset 的最低2位设置标识
     *
     * @param fp
     * @param hasTerms 上一次处理的entry是term还是block
     * @param isFloor  本次处理的所有term 是否在一个block内
     * @return
     */
    static long encodeOutput(long fp, boolean hasTerms, boolean isFloor) {
        assert fp < (1L << 62);
        return (fp << 2) | (hasTerms ? BlockTreeTermsReader.OUTPUT_FLAG_HAS_TERMS : 0) | (isFloor ? BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR : 0);
    }

    private static class PendingEntry {
        public final boolean isTerm;

        protected PendingEntry(boolean isTerm) {
            this.isTerm = isTerm;
        }
    }

    /**
     * 该对象作为一个存储term数据的临时对象
     */
    private static final class PendingTerm extends PendingEntry {
        /**
         * 采用拷贝的方式存储 term的数据
         */
        public final byte[] termBytes;
        // stats + metadata
        // 存储该term下 doc数据后得到的结果
        public final BlockTermState state;

        public PendingTerm(BytesRef term, BlockTermState state) {
            // 代表该对象存储的是 term
            super(true);
            this.termBytes = new byte[term.length];
            System.arraycopy(term.bytes, term.offset, termBytes, 0, term.length);
            this.state = state;
        }

        @Override
        public String toString() {
            return "TERM: " + brToString(termBytes);
        }
    }

    // for debugging
    @SuppressWarnings("unused")
    static String brToString(BytesRef b) {
        if (b == null) {
            return "(null)";
        } else {
            try {
                return b.utf8ToString() + " " + b;
            } catch (Throwable t) {
                // If BytesRef isn't actually UTF8, or it's eg a
                // prefix of UTF8 that ends mid-unicode-char, we
                // fallback to hex:
                return b.toString();
            }
        }
    }

    // for debugging
    @SuppressWarnings("unused")
    static String brToString(byte[] b) {
        return brToString(new BytesRef(b));
    }

    /**
     * entry 有2种类型 除了 term 还有block 类型  该对象包含了与  FST 交互的逻辑
     */
    private static final class PendingBlock extends PendingEntry {
        public final BytesRef prefix;
        public final long fp;
        public FST<BytesRef> index;
        public List<FST<BytesRef>> subIndices;
        public final boolean hasTerms;
        public final boolean isFloor;
        public final int floorLeadByte;

        /**
         * @param prefix        该block携带的前缀信息  本次处理的所有entry中都包含相同的前缀
         *                      同时该数据中除了前缀外 如果 ifFloor为 true 那么最后一位会存储 floorLeadLabel
         * @param fp            写入该term相关信息前 termWriter的文件偏移量
         * @param hasTerms      代表处理的上一个entry是 term 还是 block
         * @param isFloor       代表处理本次 entry 划分成了多个块
         * @param floorLeadByte 处理上一个entry时 灵活byte对应的ascii
         * @param subIndices    当被处理的entry是 term时 该list为null
         */
        public PendingBlock(BytesRef prefix, long fp, boolean hasTerms, boolean isFloor, int floorLeadByte, List<FST<BytesRef>> subIndices) {
            super(false);
            this.prefix = prefix;
            this.fp = fp;
            this.hasTerms = hasTerms;
            this.isFloor = isFloor;
            this.floorLeadByte = floorLeadByte;
            this.subIndices = subIndices;
        }

        @Override
        public String toString() {
            return "BLOCK: prefix=" + brToString(prefix);
        }

        /**
         * 每当某次处理 writeBlocks结束后 可能会有多个block写入到 一个列表中 在处理结束后会调用该方法  这里所有 PendingBlock的前缀是一样的
         *
         * @param blocks         此时所有待处理的block对象  包含自身
         * @param scratchBytes
         * @param scratchIntsRef
         * @throws IOException
         */
        public void compileIndex(List<PendingBlock> blocks, ByteBuffersDataOutput scratchBytes, IntsRefBuilder scratchIntsRef) throws IOException {

            assert (isFloor && blocks.size() > 1) || (isFloor == false && blocks.size() == 1) : "isFloor=" + isFloor + " blocks=" + blocks;
            assert this == blocks.get(0);

            assert scratchBytes.size() == 0;

            // TODO: try writing the leading vLong in MSB order
            // (opposite of what Lucene does today), for better
            // outputs sharing in the FST
            // encodeOutput 就是在fileOffset的最后2位 追加一些描述信息
            scratchBytes.writeVLong(encodeOutput(fp, hasTerms, isFloor));

            // 因为本次处理 拆分成了多个block
            if (isFloor) {
                // 写入block的数量
                scratchBytes.writeVInt(blocks.size() - 1);
                // 注意 不会写入自己  并且 root block的 floorLeadByte 是-1
                for (int i = 1; i < blocks.size(); i++) {
                    PendingBlock sub = blocks.get(i);
                    assert sub.floorLeadByte != -1;
                    //if (DEBUG) {
                    //  System.out.println("    write floorLeadByte=" + Integer.toHexString(sub.floorLeadByte&0xff));
                    //}
                    // 写入灵活变动的byte
                    scratchBytes.writeByte((byte) sub.floorLeadByte);
                    assert sub.fp > fp;
                    // 将term的偏移量 写入到scratchBytes 中  并且最低位存储的是 该block的上一个entry是否是否是 term
                    scratchBytes.writeVLong((sub.fp - fp) << 1 | (sub.hasTerms ? 1 : 0));
                }
            }

            // 该对象内部包含一些构建 FST的方法
            final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
            final FSTCompiler<BytesRef> fstCompiler = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs).shouldShareNonSingletonNodes(false).build();
            //if (DEBUG) {
            //  System.out.println("  compile index for prefix=" + prefix);
            //}
            //indexBuilder.DEBUG = false;
            final byte[] bytes = scratchBytes.toArrayCopy();
            assert bytes.length > 0;
            // Util.toIntsRef(prefix, scratchIntsRef)   将 前缀数据写入到scratchIntsRef 中
            fstCompiler.add(Util.toIntsRef(prefix, scratchIntsRef), new BytesRef(bytes, 0, bytes.length));
            scratchBytes.reset();


            // 这里以fst作为一个索引 有点像disi中rank的定位 实际上在 termOut中已经包含了所有term的信息了  这里只要存储前缀信息就可以 便于快速检测定位term的位置
            // Copy over index for all sub-blocks
            // 将所有子block 整合到 fst中
            for (PendingBlock block : blocks) {
                // 如果blocks只包含一个block
                if (block.subIndices != null) {
                    for (FST<BytesRef> subIndex : block.subIndices) {
                        append(fstCompiler, subIndex, scratchIntsRef);
                    }
                    block.subIndices = null;
                }
            }

            // 将所有子block的数据 抽取到本block上
            index = fstCompiler.compile();

            assert subIndices == null;

      /*
      Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
      Util.toDot(index, w, false, false);
      System.out.println("SAVED to out.dot");
      w.close();
      */
        }

        /**
         * TODO: maybe we could add bulk-add method to
         *       Builder?  Takes FST and unions it w/ current
         *       FST.
         * @param fstCompiler   构建fst的对象
         * @param subIndex      本次要加入到fst的数据
         * @param scratchIntsRef      临时容器
         * @throws IOException
         */
        private void append(FSTCompiler<BytesRef> fstCompiler, FST<BytesRef> subIndex, IntsRefBuilder scratchIntsRef) throws IOException {
            final BytesRefFSTEnum<BytesRef> subIndexEnum = new BytesRefFSTEnum<>(subIndex);
            BytesRefFSTEnum.InputOutput<BytesRef> indexEnt;
            // 通过迭代器读取数据 并将数据体抽取出来追加到 fst中
            while ((indexEnt = subIndexEnum.next()) != null) {
                //if (DEBUG) {
                //  System.out.println("      add sub=" + indexEnt.input + " " + indexEnt.input + " output=" + indexEnt.output);
                //}
                fstCompiler.add(Util.toIntsRef(indexEnt.input, scratchIntsRef), indexEnt.output);
            }
        }
    }

    private final ByteBuffersDataOutput scratchBytes = ByteBuffersDataOutput.newResettableInstance();
    private final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();

    static final BytesRef EMPTY_BYTES_REF = new BytesRef();

    /**
     * 该对象负责记录统计信息
     */
    private static class StatsWriter {

        private final DataOutput out;
        /**
         * 是否包含了 频率信息
         */
        private final boolean hasFreqs;
        private int singletonCount;

        StatsWriter(DataOutput out, boolean hasFreqs) {
            this.out = out;
            this.hasFreqs = hasFreqs;
        }

        /**
         * @param df  docFreq
         * @param ttf totalTermFreq
         * @throws IOException
         */
        void add(int df, long ttf) throws IOException {
            // Singletons (DF==1, TTF==1) are run-length encoded
            // 代表该term只出现在一个doc中   且未包含频率信息 或者 频率为1 那么记录一个 singleton
            if (df == 1 && (hasFreqs == false || ttf == 1)) {
                singletonCount++;
            } else {
                finish();
                // 最低位如果是0 代表不是 singleton
                out.writeVInt(df << 1);
                if (hasFreqs) {
                    // 这个差值写进去有个屁用啊
                    out.writeVLong(ttf - df);
                }
            }
        }

        void finish() throws IOException {
            if (singletonCount > 0) {
                // 什么骚操作...
                out.writeVInt(((singletonCount - 1) << 1) | 1);
                singletonCount = 0;
            }
        }

    }

    /**
     * 该对象专门负责写入 terms
     */
    class TermsWriter {


        private final FieldInfo fieldInfo;
        /**
         * 代表此时已经处理了多少term
         */
        private long numTerms;
        /**
         * 在写入term的过程中 会使用遍历到的doc设置该位图
         */
        final FixedBitSet docsSeen;
        /**
         * 某个field下所有的term  每个各出现在多少doc中的 freq总和
         */
        long sumTotalTermFreq;
        /**
         * 某个field下所有term 每个各出现在多少doc中的数量总和
         */
        long sumDocFreq;
        long indexStartFP;

        // Records index into pending where the current prefix at that
        // length "started"; for example, if current term starts with 't',
        // startsByPrefix[0] is the index into pending for the first
        // term/sub-block starting with 't'.  We use this to figure out when
        // to write a new block:
        // 记录上一个写入的term
        private final BytesRefBuilder lastTerm = new BytesRefBuilder();
        /**
         * 该数组存储相同的前缀  长度会根据term的长度进行扩容
         */
        private int[] prefixStarts = new int[8];

        // Pending stack of terms and blocks.  As terms arrive (in sorted order)
        // we append to this stack, and once the top of the stack has enough
        // terms starting with a common prefix, we write a new block with
        // those terms and replace those terms in the stack with a new block:
        // 作为一个存储 terms/blocks 的栈
        private final List<PendingEntry> pending = new ArrayList<>();

        // Reused in writeBlocks:
        // 当前待处理的 block  每当后缀不同触发 writeBlocks时 每满足一定的量，将多个term写入到output中 以及包装成 PendingBlock对象 加入到该列表
        private final List<PendingBlock> newBlocks = new ArrayList<>();

        /**
         * 记录首个处理的 term
         */
        private PendingTerm firstPendingTerm;
        /**
         * 对应最后一个处理的term
         */
        private PendingTerm lastPendingTerm;

        /**
         * Writes the top count entries in pending, using prevTerm to compute the prefix.
         * 尝试将 pending中的元素 拆解成多个 block
         * @param prefixLength 从后往前 直到第一个不共享的位置开始
         * @param count        之前该位置对应的值 有多少term共享了
         */
        void writeBlocks(int prefixLength, int count) throws IOException {

            assert count > 0;

            //if (DEBUG2) {
            //  BytesRef br = new BytesRef(lastTerm.bytes());
            //  br.length = prefixLength;
            //  System.out.println("writeBlocks: seg=" + segment + " prefix=" + brToString(br) + " count=" + count);
            //}

            // Root block better write all remaining pending entries:
            assert prefixLength > 0 || count == pending.size();

            int lastSuffixLeadLabel = -1;

            // True if we saw at least one term in this block (we record if a block
            // only points to sub-blocks in the terms index so we can avoid seeking
            // to it when we are looking for a term):
            boolean hasTerms = false;
            boolean hasSubBlocks = false;

            // 该 被共享的位置是从第几个 term开始输入的
            int start = pending.size() - count;
            // 当前是第几个term
            int end = pending.size();

            int nextBlockStart = start;
            int nextFloorLeadLabel = -1;

            // start 最小从0开始
            // 将start 到 end 之间的数据 拆分成多个block
            for (int i = start; i < end; i++) {
                // 获取这段时间内 写入的term
                PendingEntry ent = pending.get(i);

                int suffixLeadLabel;

                // 找到之前暂存的 term节点
                if (ent.isTerm) {
                    PendingTerm term = (PendingTerm) ent;
                    // 代表某次 term 的全部byte刚好对应这个前缀  那么要存储的后缀就为-1 代表没有后缀需要存储
                    // 假设  aaa aaab aaac aaad aaae  aab 这时进入这个方法 prefixLength 长度就是3 意味着从长度为3的部分出现不同 然后这里开始遍历 aaa -> aaae 的entry
                    // 这种情况对应 aaa 和 aab

                    // 首先要明确能进入到这里 start.length-...>end.length 长度只可能 >= prefixLength
                    // 因为count代表该前缀开始被共享的地方 通过count计算出的start 能确保都包含这个前缀
                    // 如果刚好前缀 就是该term本身 那么这个 leadLabel 就是-1
                    if (term.termBytes.length == prefixLength) {
                        // Suffix is 0, i.e. prefix 'foo' and term is
                        // 'foo' so the term has empty string suffix
                        // in this block
                        assert lastSuffixLeadLabel == -1 : "i=" + i + " lastSuffixLeadLabel=" + lastSuffixLeadLabel;
                        suffixLeadLabel = -1;
                    } else {
                        // 比如  aaaa 和 aab  这时传入的prefixLength = 3   也就是会取aaaa的最后一个a   这里是取最后一个值的 ascii码么 ???
                        suffixLeadLabel = term.termBytes[prefixLength] & 0xff;
                    }
                    // TODO 这种情况先忽略
                } else {
                    PendingBlock block = (PendingBlock) ent;
                    assert block.prefix.length > prefixLength;
                    suffixLeadLabel = block.prefix.bytes[block.prefix.offset + prefixLength] & 0xff;
                }
                // if (DEBUG) System.out.println("  i=" + i + " ent=" + ent + " suffixLeadLabel=" + suffixLeadLabel);

                // 对应每次灵活变动的 最后一个值
                if (suffixLeadLabel != lastSuffixLeadLabel) {
                    int itemsInBlock = i - nextBlockStart;
                    // 当遍历多个值后 发现
                    if (itemsInBlock >= minItemsInBlock && end - nextBlockStart > maxItemsInBlock) {
                        // The count is too large for one block, so we must break it into "floor" blocks, where we record
                        // the leading label of the suffix of the first term in each floor block, so at search time we can
                        // jump to the right floor block.  We just use a naive greedy segmenter here: make a new floor
                        // block as soon as we have at least minItemsInBlock.  This is not always best: it often produces
                        // a too-small block as the final block:
                        // 如果按一次写入 那么数量过多 所以选择打散
                        // 只要不是一次写入了 所有数据 就认为是打散的
                        boolean isFloor = itemsInBlock < count;
                        // writeBlock 就是将当前后缀信息写入到output中 以及一些元数据 统计信息等
                        // 返回的是一个 PendingBlock对象 该对象内部包含了本次共享的前缀   (此时前缀还没有写入到output中)
                        newBlocks.add(writeBlock(prefixLength, isFloor, nextFloorLeadLabel, nextBlockStart, i, hasTerms, hasSubBlocks));

                        // 每当写入一个block的数据后 对相关标识进行重置
                        hasTerms = false;
                        hasSubBlocks = false;
                        // 每次操作完记录上次写入block时对应的 suffixLeadLabel信息
                        nextFloorLeadLabel = suffixLeadLabel;
                        // pending中的entry 会被拆解成多个block写入  这里代表已经写完了一个block 所以更新下一个block的起点
                        nextBlockStart = i;
                    }

                    lastSuffixLeadLabel = suffixLeadLabel;
                }

                // 根据entry的类型 设置标识
                if (ent.isTerm) {
                    hasTerms = true;
                } else {
                    hasSubBlocks = true;
                }
            }

            // Write last block, if any:
            // 每当触发了一次 writeBlock 就会生成一个 PendingBlock 这里应该是强制将最后的数据 以一个块的单位写入
            if (nextBlockStart < end) {
                int itemsInBlock = end - nextBlockStart;
                boolean isFloor = itemsInBlock < count;
                newBlocks.add(writeBlock(prefixLength, isFloor, nextFloorLeadLabel, nextBlockStart, end, hasTerms, hasSubBlocks));
            }

            assert newBlocks.isEmpty() == false;

            // 此时所有term 都已经将后缀数据写入到输出流了 同时它们存储的前缀数据都是相同的  以block为单位生成了多个 pendingBlock对象
            PendingBlock firstBlock = newBlocks.get(0);

            assert firstBlock.isFloor || newBlocks.size() == 1;

            // scratchBytes，scratchIntsRef 应该只是2个临时对象
            firstBlock.compileIndex(newBlocks, scratchBytes, scratchIntsRef);

            // Remove slice from the top of the pending stack, that we just wrote:
            pending.subList(pending.size() - count, pending.size()).clear();

            // Append new block
            // 每当处理完一批term后 就会产生一个block 并填装到pending中(他代表之前处理的多个term的总集)
            pending.add(firstBlock);

            newBlocks.clear();
        }

        private boolean allEqual(byte[] b, int startOffset, int endOffset, byte value) {
            Objects.checkFromToIndex(startOffset, endOffset, b.length);
            for (int i = startOffset; i < endOffset; ++i) {
                if (b[i] != value) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Writes the specified slice (start is inclusive, end is exclusive)
         * from pending stack as a new block.  If isFloor is true, there
         * were too many (more than maxItemsInBlock) entries sharing the
         * same prefix, and so we broke it into multiple floor blocks where
         * we record the starting label of the suffix of each floor block.
         *
         * @param prefixLength   代表本次处理的term与之后的term 不同byte对应的位置 不是数组下标
         * @param start          首先该对象会将所有处理的term存储到一个 pending中  并且每次按照相同前缀存储数据时 是有一个最小的阈值的 只有当前缀不再相同
         *                       且之前很多term都使用相同前缀时 才触发 writerBlocks  这些term又需要拆开来处理   而start都是当中某批数据的首个term
         * @param end            对应某批数据的最后一个term
         *                       <p>
         * @param isFloor        代表没有将所有数据一次性写入
         * @param floorLeadLabel 处理上一个entry时 灵活byte对应的ascii码
         * @param hasTerms       代表上一个entry是term
         * @param hasSubBlocks   代表上一个entry是 block
         */
        private PendingBlock writeBlock(int prefixLength, boolean isFloor, int floorLeadLabel, int start, int end,
                                        boolean hasTerms, boolean hasSubBlocks) throws IOException {

            assert end > start;

            // 获取此时输出流的偏移量
            long startFP = termsOut.getFilePointer();

            // 先假设为true吧
            boolean hasFloorLeadLabel = isFloor && floorLeadLabel != -1;

            // 从这里可以看出 多个block 他们的前缀是一样的

            // 这里为前缀额外拓展了一个槽
            final BytesRef prefix = new BytesRef(prefixLength + (hasFloorLeadLabel ? 1 : 0));
            // 注意这里只存储了前缀  此时count个entry都是共享这个前缀的
            System.arraycopy(lastTerm.get().bytes, 0, prefix.bytes, 0, prefixLength);
            prefix.length = prefixLength;

            //if (DEBUG2) System.out.println("    writeBlock field=" + fieldInfo.name + " prefix=" + brToString(prefix) + " fp=" + startFP + " isFloor=" + isFloor + " isLastInFloor=" + (end == pending.size()) + " floorLeadLabel=" + floorLeadLabel + " start=" + start + " end=" + end + " hasTerms=" + hasTerms + " hasSubBlocks=" + hasSubBlocks);

            // Write block header:
            // 记录了有多少个entry共享这些前缀
            int numEntries = end - start;
            // 类似于token 携带特殊信息
            int code = numEntries << 1;
            // 代表此时已经处理最后一批数据了 最低位设置成1  如果最低位为0 代表还有block    当前term还没有写入到pending中 所以lastTerm就是pending.size()对应的term
            if (end == pending.size()) {
                // Last block:
                code |= 1;
            }
            termsOut.writeVInt(code);

      /*
      if (DEBUG) {
        System.out.println("  writeBlock " + (isFloor ? "(floor) " : "") + "seg=" + segment + " pending.size()=" + pending.size() + " prefixLength=" + prefixLength + " indexPrefix=" + brToString(prefix) + " entCount=" + (end-start+1) + " startFP=" + startFP + (isFloor ? (" floorLeadLabel=" + Integer.toHexString(floorLeadLabel)) : ""));
      }
      */

            // 1st pass: pack term suffix bytes into byte[] blob
            // TODO: cutover to bulk int codec... simple64?

            // We optimize the leaf block case (block has only terms), writing a more
            // compact format in this case:
            // 代表上次处理的是 term
            boolean isLeafBlock = hasSubBlocks == false;

            //System.out.println("  isLeaf=" + isLeafBlock);

            // 这里存储的是block数据
            final List<FST<BytesRef>> subIndices;

            // first数据是基于绝对值存储的
            boolean absolute = true;

            // 代表没有blockEntry
            if (isLeafBlock) {
                // Block contains only ordinary terms:  因为不包含block数据 所以该属性可以为null
                subIndices = null;
                StatsWriter statsWriter = new StatsWriter(this.statsWriter, fieldInfo.getIndexOptions() != IndexOptions.DOCS);
                for (int i = start; i < end; i++) {
                    PendingEntry ent = pending.get(i);
                    assert ent.isTerm : "i=" + i;

                    PendingTerm term = (PendingTerm) ent;

                    assert StringHelper.startsWith(term.termBytes, prefix) : "term.term=" + term.termBytes + " prefix=" + prefix;
                    BlockTermState state = term.state;
                    final int suffix = term.termBytes.length - prefixLength;
                    //if (DEBUG2) {
                    //  BytesRef suffixBytes = new BytesRef(suffix);
                    //  System.arraycopy(term.termBytes, prefixLength, suffixBytes.bytes, 0, suffix);
                    //  suffixBytes.length = suffix;
                    //  System.out.println("    write term suffix=" + brToString(suffixBytes));
                    //}

                    // For leaf block we write suffix straight
                    suffixLengthsWriter.writeVInt(suffix);
                    suffixWriter.append(term.termBytes, prefixLength, suffix);
                    assert floorLeadLabel == -1 || (term.termBytes[prefixLength] & 0xff) >= floorLeadLabel;

                    // Write term stats, to separate byte[] blob:
                    statsWriter.add(state.docFreq, state.totalTermFreq);

                    // Write term meta data
                    postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);
                    absolute = false;
                }
                statsWriter.finish();
            } else {
                // Block has at least one prefix term or a sub block:
                subIndices = new ArrayList<>();
                // 这里应该是已经确保 fieldInfo.getIndexOptions() ！= NONE 了吧 所以这里只要不是 DOCS 剩下的几种情况都携带了频率信息
                StatsWriter statsWriter = new StatsWriter(this.statsWriter, fieldInfo.getIndexOptions() != IndexOptions.DOCS);
                for (int i = start; i < end; i++) {
                    PendingEntry ent = pending.get(i);
                    // 如果该entry 是 term的情况
                    if (ent.isTerm) {
                        PendingTerm term = (PendingTerm) ent;

                        assert StringHelper.startsWith(term.termBytes, prefix) : "term.term=" + term.termBytes + " prefix=" + prefix;
                        // 代表该term 写入完成时携带的一些信息
                        BlockTermState state = term.state;
                        // 代表还剩余多少后缀
                        final int suffix = term.termBytes.length - prefixLength;
                        //if (DEBUG2) {
                        //  BytesRef suffixBytes = new BytesRef(suffix);
                        //  System.arraycopy(term.termBytes, prefixLength, suffixBytes.bytes, 0, suffix);
                        //  suffixBytes.length = suffix;
                        //  System.out.println("      write term suffix=" + brToString(suffixBytes));
                        //}

                        // For non-leaf block we borrow 1 bit to record
                        // if entry is term or sub-block, and 1 bit to record if
                        // it's a prefix term.  Terms cannot be larger than ~32 KB
                        // so we won't run out of bits:

                        // 将后缀信息写入到 相关输出流中   在一连串的输入后 相当于本批所有term的后缀信息(数据,长度)都被写入到容器中了
                        // ！！！注意在 non-leaf 的情况下 额外占用了一位
                        suffixLengthsWriter.writeVInt(suffix << 1);
                        suffixWriter.append(term.termBytes, prefixLength, suffix);

                        // Write term stats, to separate byte[] blob:
                        // 将包含该term的doc数量 以及该term出现的总次数 记录到 stats对象中
                        statsWriter.add(state.docFreq, state.totalTermFreq);

                        // TODO: now that terms dict "sees" these longs,
                        // we can explore better column-stride encodings
                        // to encode all long[0]s for this block at
                        // once, all long[1]s, etc., e.g. using
                        // Simple64.  Alternatively, we could interleave
                        // stats + meta ... no reason to have them
                        // separate anymore:

                        // Write term meta data
                        // 将该term的元数据信息写入到索引文件中
                        postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);
                        // 写入一个数据后 立即标记成 基于差值存储
                        absolute = false;
                    } else {
                        // 如果是 blockEntry  TODO 先忽略
                        PendingBlock block = (PendingBlock) ent;
                        assert StringHelper.startsWith(block.prefix, prefix);
                        final int suffix = block.prefix.length - prefixLength;
                        assert StringHelper.startsWith(block.prefix, prefix);

                        assert suffix > 0;

                        // For non-leaf block we borrow 1 bit to record
                        // if entry is term or sub-block:f
                        // non-leaf 下长度要多占用一位
                        suffixLengthsWriter.writeVInt((suffix << 1) | 1);
                        suffixWriter.append(block.prefix.bytes, prefixLength, suffix);

                        //if (DEBUG2) {
                        //  BytesRef suffixBytes = new BytesRef(suffix);
                        //  System.arraycopy(block.prefix.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
                        //  suffixBytes.length = suffix;
                        //  System.out.println("      write sub-block suffix=" + brToString(suffixBytes) + " subFP=" + block.fp + " subCode=" + (startFP-block.fp) + " floor=" + block.isFloor);
                        //}

                        assert floorLeadLabel == -1 || (block.prefix.bytes[prefixLength] & 0xff) >= floorLeadLabel : "floorLeadLabel=" + floorLeadLabel + " suffixLead=" + (block.prefix.bytes[prefixLength] & 0xff);
                        assert block.fp < startFP;

                        suffixLengthsWriter.writeVLong(startFP - block.fp);
                        subIndices.add(block.index);
                    }
                }
                // 当本次涉及到的所有term信息处理完毕后 触发finish做收尾处理
                statsWriter.finish();

                assert subIndices.size() != 0;
            }

            // Write suffixes byte[] blob to terms dict output, either uncompressed, compressed with LZ4 or with LowercaseAsciiCompression.
            // 默认情况下不进行压缩
            CompressionAlgorithm compressionAlg = CompressionAlgorithm.NO_COMPRESSION;
            // If there are 2 suffix bytes or less per term, then we don't bother compressing as suffix are unlikely what
            // makes the terms dictionary large, and it also tends to be frequently the case for dense IDs like
            // auto-increment IDs, so not compressing in that case helps not hurt ID lookups by too much.
            // We also only start compressing when the prefix length is greater than 2 since blocks whose prefix length is
            // 1 or 2 always all get visited when running a fuzzy query whose max number of edits is 2.
            // 这个应该是经过各种实际考量过的   首先前缀小于2的话 会经常命中模糊查询 所以就不进行压缩了
            // 并且后缀的平均长度要达到2以上 才有压缩的必要
            if (suffixWriter.length() > 2L * numEntries && prefixLength > 2) {
                // LZ4 inserts references whenever it sees duplicate strings of 4 chars or more, so only try it out if the
                // average suffix length is greater than 6.
                // 因为LZ4 压缩有一个基础长度 当相同的字符串长度小于该值时 是无法进行压缩的 所以这里增加了6倍的限制
                if (suffixWriter.length() > 6L * numEntries) {
                    LZ4.compress(suffixWriter.bytes(), 0, suffixWriter.length(), spareWriter, compressionHashTable);
                    if (spareWriter.size() < suffixWriter.length() - (suffixWriter.length() >>> 2)) {
                        // LZ4 saved more than 25%, go for it
                        // 当成功压缩到原来的 25% 就采用这种压缩方式
                        compressionAlg = CompressionAlgorithm.LZ4;
                    }
                }
                // 代表采用LZ4 进行压缩时 压缩率不满意
                if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) {
                    // 先释放掉之前存储的数据
                    spareWriter.reset();
                    if (spareBytes.length < suffixWriter.length()) {
                        spareBytes = new byte[ArrayUtil.oversize(suffixWriter.length(), 1)];
                    }
                    // 这个压缩算法就不看了
                    if (LowercaseAsciiCompression.compress(suffixWriter.bytes(), suffixWriter.length(), spareBytes, spareWriter)) {
                        compressionAlg = CompressionAlgorithm.LOWERCASE_ASCII;
                    }
                }
            }
            // 生成token信息 首先最高3位存储的是 多个entry的后缀总长度
            long token = ((long) suffixWriter.length()) << 3;
            // 4 需要3个位表示 代表是叶子block??? 从上下文判断应该就是仅存在 termEntry
            if (isLeafBlock) {
                token |= 0x04;
            }
            // 低2位标明压缩方式
            token |= compressionAlg.code;
            // 从这里可以看出 存储的都是后缀数据
            termsOut.writeVLong(token);
            if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) {
                termsOut.writeBytes(suffixWriter.bytes(), suffixWriter.length());
            } else {
                spareWriter.copyTo(termsOut);
            }
            suffixWriter.setLength(0);
            spareWriter.reset();

            // Write suffix lengths
            final int numSuffixBytes = Math.toIntExact(suffixLengthsWriter.size());
            spareBytes = ArrayUtil.grow(spareBytes, numSuffixBytes);
            // 把长度信息存储到 spareBytes 中
            suffixLengthsWriter.copyTo(new ByteArrayDataOutput(spareBytes));
            suffixLengthsWriter.reset();

            // 代表所有后缀的长度信息都相同
            if (allEqual(spareBytes, 1, numSuffixBytes, spareBytes[0])) {
                // Structured fields like IDs often have most values of the same length
                // 最低位是1 代表所有长度一致  之后只要读取一个byte就可以获取所有的长度信息
                termsOut.writeVInt((numSuffixBytes << 1) | 1);
                termsOut.writeByte(spareBytes[0]);
            } else {
                // 最低位是0 将所有长度信息写入到 输出流中
                termsOut.writeVInt(numSuffixBytes << 1);
                termsOut.writeBytes(spareBytes, numSuffixBytes);
            }

            // Stats
            final int numStatsBytes = Math.toIntExact(statsWriter.size());
            // 当一组term信息都写入完毕后 将统计信息写入到 terms索引文件中
            termsOut.writeVInt(numStatsBytes);
            statsWriter.copyTo(termsOut);
            statsWriter.reset();

            // Write term meta data byte[] blob
            // 写入元数据
            termsOut.writeVInt((int) metaWriter.size());
            metaWriter.copyTo(termsOut);
            metaWriter.reset();

            // if (DEBUG) {
            //   System.out.println("      fpEnd=" + out.getFilePointer());
            // }

            if (hasFloorLeadLabel) {
                // We already allocated to length+1 above:
                // 之前预留一个空间是为了写入特殊标识
                prefix.bytes[prefix.length++] = (byte) floorLeadLabel;
            }

            // 将相关信息包装成一个block  如果该block 下没有subIndices 那么应该就是将他当作一个 subBlock
            // 注意前缀还没有写入到 output中
            return new PendingBlock(prefix, startFP, hasTerms, isFloor, floorLeadLabel, subIndices);
        }

        /**
         * 代表该对象维护的是哪个 field下的term
         *
         * @param fieldInfo
         */
        TermsWriter(FieldInfo fieldInfo) {
            this.fieldInfo = fieldInfo;
            assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
            // 创建doc位图对象 推测每当处理某个term时  会将该term出现过的所有doc记录下来
            docsSeen = new FixedBitSet(maxDoc);
            // 切换此时正在处理的 field
            postingsWriter.setField(fieldInfo);
        }

        /**
         * Writes one term's worth of postings.
         *
         * @param text      当前 term对应的文本信息
         * @param termsEnum 用于遍历 term的迭代器  (实际上内部存储了一个 termHash)
         * @param norms     标准因子
         */
        public void write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {
            /*
            if (DEBUG) {
                int[] tmp = new int[lastTerm.length];
                System.arraycopy(prefixStarts, 0, tmp, 0, tmp.length);
                System.out.println("BTTW: write term=" + brToString(text) + " prefixStarts=" + Arrays.toString(tmp) + " pending.size()=" + pending.size());
            }
            */

            // 将 term的相关信息先写入到  负责存储 term position信息的  positionWriter中   该对象内部还使用了跳跃表  返回的结果是表述存储信息的
            BlockTermState state = postingsWriter.writeTerm(text, termsEnum, docsSeen, norms);
            // 代表数据有效   当该term没有写入到任何doc时 返回null
            if (state != null) {

                assert state.docFreq != 0;
                assert fieldInfo.getIndexOptions() == IndexOptions.DOCS || state.totalTermFreq >= state.docFreq : "postingsWriter=" + postingsWriter;

                // 存储term字面量信息   内部每次以block为单位 写入索引文件中 并且block对应的前缀信息 (也许还会携带label标签)  会存入到fst结构中 便于快速查询
                pushTerm(text);

                // 将term 包装成一个entry对象  并存储到一个栈结构中
                PendingTerm term = new PendingTerm(text, state);
                pending.add(term);
                //if (DEBUG) System.out.println("    add pending term = " + text + " pending.size()=" + pending.size());

                // 代表该term出现在多少个doc中
                sumDocFreq += state.docFreq;
                // term出现在所有相关的doc的freq总和
                sumTotalTermFreq += state.totalTermFreq;
                numTerms++;
                // 如果首个 term节点还未设置  就先设置
                if (firstPendingTerm == null) {
                    firstPendingTerm = term;
                }
                // 更新上一个写入的 term节点
                lastPendingTerm = term;
            }
        }

        /**
         * Pushes the new term to the top of the stack, and writes new blocks.
         * 将 term字面值存储到 term
         */
        private void pushTerm(BytesRef text) throws IOException {
            // Find common prefix between last term and current term:
            // 返回相同的前缀长度 ！！！注意不是数组下标
            int prefixLength = Arrays.mismatch(lastTerm.bytes(), 0, lastTerm.length(), text.bytes, text.offset, text.offset + text.length);
            // -1代表2个term完全相同    这种情况应该不会出现 因为term已经去重过了 在 termHash的 newTerm addTerm中 相同的term只会累加值 而不会重复存储
            if (prefixLength == -1) { // Only happens for the first term, if it is empty
                assert lastTerm.length() == 0;
                prefixLength = 0;
            }

            // if (DEBUG) System.out.println("  shared=" + pos + "  lastTerm.length=" + lastTerm.length);

            // Close the "abandoned" suffix now:
            // 从后往前 确保公共前缀长的先写入block
            for (int i = lastTerm.length() - 1; i >= prefixLength; i--) {

                // How many items on top of the stack share the current suffix
                // we are closing:
                // 当发生变化时  本次已经囤积的值 与不同后缀的位置开始出现的时间 必须相距minItemsInBlock个term  才会写入到 block中
                // 也就是小幅度的变化并不会使得数据被转移到 block中 而是继续存储在 pending列表中
                int prefixTopSize = pending.size() - prefixStarts[i];
                // minItemsInBlock  代表之前共享该byte的 term至少要有多少个才能写入到 block 中
                if (prefixTopSize >= minItemsInBlock) {
                    // if (DEBUG) System.out.println("pushTerm i=" + i + " prefixTopSize=" + prefixTopSize + " minItemsInBlock=" + minItemsInBlock);
                    // 代表写入的起始位置  直到第一个非共享的位置结束
                    writeBlocks(i + 1, prefixTopSize);
                    // 这一步???  下面会重新设置啊 这一步设置的结果并不重要
                    prefixStarts[i] -= prefixTopSize - 1;
                }
            }

            // prefixStarts 记录的是该前缀首次出现是什么时候
            if (prefixStarts.length < text.length) {
                prefixStarts = ArrayUtil.grow(prefixStarts, text.length);
            }

            // Init new tail:
            // 从非共享的部分开始 更新这部分出现的时间点   注意 超过text的部分 还是续用之前的值
            for (int i = prefixLength; i < text.length; i++) {
                prefixStarts[i] = pending.size();
            }

            // 更新上一次写入的值
            lastTerm.copyBytes(text);
        }

        /**
         * Finishes all terms in this field
         * 当某个field下所有的term写入完成时  要将剩余的部分写入到索引文件中  (之前的term数据以block为单位写入)
         * @throws IOException
         */
        public void finish() throws IOException {
            if (numTerms > 0) {
                // if (DEBUG) System.out.println("BTTW: finish prefixStarts=" + Arrays.toString(prefixStarts));

                // Add empty term to force closing of all final blocks:
                // 通过写入空的 term 使得本次共享前缀长度为0 促使之前所有的term 都写入到索引文件中   但是如果写入的term总数就比较少的话 不满足一个block的大小还是不会写入
                pushTerm(new BytesRef());

                // TODO: if pending.size() is already 1 with a non-zero prefix length
                // we can save writing a "degenerate" root block, but we have to
                // fix all the places that assume the root block's prefix is the empty string:
                pushTerm(new BytesRef());
                // 这里强制将栈中待处理的所有term 都写入到索引文件中
                writeBlocks(0, pending.size());

                // We better have one final "root" block:
                assert pending.size() == 1 && !pending.get(0).isTerm : "pending.size()=" + pending.size() + " pending=" + pending;
                // 这时所有term 都合成为一个blockNode  并且每个被共享次数超过 block的 前缀 都会加入到fst结构中形成一个索引   (这个索引应该就是term的词典)
                final PendingBlock root = (PendingBlock) pending.get(0);
                assert root.prefix.length == 0;
                assert root.index.getEmptyOutput() != null;

                // Write FST to index
                // 获取此时termIndex的起始偏移量 （FST就是构成term词典的核心结构）
                indexStartFP = indexOut.getFilePointer();
                // 将fst的数据持久化到索引文件中
                root.index.save(indexOut);
                //System.out.println("  write FST " + indexStartFP + " field=" + fieldInfo.name);

        /*
        if (DEBUG) {
          final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
          Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
          Util.toDot(root.index, w, false, false);
          System.out.println("SAVED to " + dotFileName);
          w.close();
        }
        */
        // 将相关信息包装成一个 metaData对象 并保存在全局 field容器中
                assert firstPendingTerm != null;
                BytesRef minTerm = new BytesRef(firstPendingTerm.termBytes);

                assert lastPendingTerm != null;
                BytesRef maxTerm = new BytesRef(lastPendingTerm.termBytes);

                fields.add(new FieldMetaData(fieldInfo,
                        ((PendingBlock) pending.get(0)).index.getEmptyOutput(),
                        numTerms,
                        indexStartFP,
                        sumTotalTermFreq,
                        sumDocFreq,
                        docsSeen.cardinality(),
                        minTerm, maxTerm));
            } else {
                assert sumTotalTermFreq == 0 || fieldInfo.getIndexOptions() == IndexOptions.DOCS && sumTotalTermFreq == -1;
                assert sumDocFreq == 0;
                assert docsSeen.cardinality() == 0;
            }
        }

        /**
         * 这个是记录多个 term后缀不同时 后缀长度的容器
         */
        private final ByteBuffersDataOutput suffixLengthsWriter = ByteBuffersDataOutput.newResettableInstance();
        /**
         * 这里是存放后缀数据的容器
         */
        private final BytesRefBuilder suffixWriter = new BytesRefBuilder();
        private final ByteBuffersDataOutput statsWriter = ByteBuffersDataOutput.newResettableInstance();
        private final ByteBuffersDataOutput metaWriter = ByteBuffersDataOutput.newResettableInstance();

        /**
         * 该对象存储压缩后的 后缀数据
         */
        private final ByteBuffersDataOutput spareWriter = ByteBuffersDataOutput.newResettableInstance();
        private byte[] spareBytes = BytesRef.EMPTY_BYTES;

        /**
         * 采用高压缩率LZ4 存储时使用的临时容器对象
         */
        private final LZ4.HighCompressionHashTable compressionHashTable = new LZ4.HighCompressionHashTable();
    }

    private boolean closed;

    /**
     * 当所有field信息写完后 触发close
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        boolean success = false;
        try {

            final long dirStart = termsOut.getFilePointer();
            final long indexDirStart = indexOut.getFilePointer();

            termsOut.writeVInt(fields.size());

            // 就是把 metaData 数据写入到索引文件中
            for (FieldMetaData field : fields) {
                //System.out.println("  field " + field.fieldInfo.name + " " + field.numTerms + " terms");
                termsOut.writeVInt(field.fieldInfo.number);
                assert field.numTerms > 0;
                termsOut.writeVLong(field.numTerms);
                termsOut.writeVInt(field.rootCode.length);
                termsOut.writeBytes(field.rootCode.bytes, field.rootCode.offset, field.rootCode.length);
                assert field.fieldInfo.getIndexOptions() != IndexOptions.NONE;
                if (field.fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
                    termsOut.writeVLong(field.sumTotalTermFreq);
                }
                termsOut.writeVLong(field.sumDocFreq);
                termsOut.writeVInt(field.docCount);
                indexOut.writeVLong(field.indexStartFP);
                writeBytesRef(termsOut, field.minTerm);
                writeBytesRef(termsOut, field.maxTerm);
            }
            writeTrailer(termsOut, dirStart);
            CodecUtil.writeFooter(termsOut);
            writeIndexTrailer(indexOut, indexDirStart);
            CodecUtil.writeFooter(indexOut);
            success = true;
        } finally {
            if (success) {
                IOUtils.close(termsOut, indexOut, postingsWriter);
            } else {
                IOUtils.closeWhileHandlingException(termsOut, indexOut, postingsWriter);
            }
        }
    }

    private static void writeBytesRef(IndexOutput out, BytesRef bytes) throws IOException {
        out.writeVInt(bytes.length);
        out.writeBytes(bytes.bytes, bytes.offset, bytes.length);
    }
}
