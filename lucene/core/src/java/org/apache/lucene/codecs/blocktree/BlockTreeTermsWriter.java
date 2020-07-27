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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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
     * field 相关的元数据信息
     */
    private static class FieldMetaData {

        /**
         * 该field的描述信息
         */
        public final FieldInfo fieldInfo;
        public final BytesRef rootCode;
        public final long numTerms;
        public final long indexStartFP;
        public final long sumTotalTermFreq;
        public final long sumDocFreq;
        /**
         *
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

    private final List<FieldMetaData> fields = new ArrayList<>();

    /**
     * Create a new writer.  The number of items (terms or
     * sub-blocks) per block will aim to be between
     * minItemsPerBlock and maxItemsPerBlock, though in some
     * cases the blocks may be smaller than the min.
     * 初始化 term写入对象
     */
    public BlockTreeTermsWriter(SegmentWriteState state,
                                PostingsWriterBase postingsWriter,   // 对应 Lucene84PostingsWriter
                                int minItemsInBlock,
                                int maxItemsInBlock)
            throws IOException {
        validateSettings(minItemsInBlock,
                maxItemsInBlock);

        this.minItemsInBlock = minItemsInBlock;
        this.maxItemsInBlock = maxItemsInBlock;

        this.maxDoc = state.segmentInfo.maxDoc();
        this.fieldInfos = state.fieldInfos;
        this.postingsWriter = postingsWriter;

        // 生成存储 term的索引文件  后缀名为 tim
        final String termsName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockTreeTermsReader.TERMS_EXTENSION);
        termsOut = state.directory.createOutput(termsName, state.context);
        boolean success = false;
        IndexOutput indexOut = null;
        try {
            // 写入文件头
            CodecUtil.writeIndexHeader(termsOut, BlockTreeTermsReader.TERMS_CODEC_NAME, BlockTreeTermsReader.VERSION_CURRENT,
                    state.segmentInfo.getId(), state.segmentSuffix);

            // 创建 tim 对应的索引文件 tip
            final String indexName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BlockTreeTermsReader.TERMS_INDEX_EXTENSION);
            indexOut = state.directory.createOutput(indexName, state.context);
            CodecUtil.writeIndexHeader(indexOut, BlockTreeTermsReader.TERMS_INDEX_CODEC_NAME, BlockTreeTermsReader.VERSION_CURRENT,
                    state.segmentInfo.getId(), state.segmentSuffix);
            //segment = state.segmentInfo.name;

            // 为termsOut 写入文件头
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
     * @param fields
     * @param norms
     * @throws IOException
     */
    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
        //if (DEBUG) System.out.println("\nBTTW.write seg=" + segment);

        String lastField = null;
        // 遍历本次涉及到的所有 field
        for (String field : fields) {
            // 确保 field是 递增的
            assert lastField == null || lastField.compareTo(field) < 0;
            lastField = field;

            //if (DEBUG) System.out.println("\nBTTW.write seg=" + segment + " field=" + field);
            // 找到某个field下关联的所有  term
            Terms terms = fields.terms(field);
            if (terms == null) {
                continue;
            }

            TermsEnum termsEnum = terms.iterator();
            // term 通过该对象写入
            TermsWriter termsWriter = new TermsWriter(fieldInfos.fieldInfo(field));
            while (true) {
                BytesRef term = termsEnum.next();
                //if (DEBUG) System.out.println("BTTW: next term " + term);

                if (term == null) {
                    break;
                }

                //if (DEBUG) System.out.println("write field=" + fieldInfo.name + " term=" + brToString(term));
                // 挨个将 term信息写入到 索引文件中
                termsWriter.write(term, termsEnum, norms);
            }

            termsWriter.finish();

            //if (DEBUG) System.out.println("\nBTTW.write done seg=" + segment + " field=" + field);
        }
    }

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

    private static final class PendingBlock extends PendingEntry {
        public final BytesRef prefix;
        public final long fp;
        public FST<BytesRef> index;
        public List<FST<BytesRef>> subIndices;
        public final boolean hasTerms;
        public final boolean isFloor;
        public final int floorLeadByte;

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

        public void compileIndex(List<PendingBlock> blocks, ByteBuffersDataOutput scratchBytes, IntsRefBuilder scratchIntsRef) throws IOException {

            assert (isFloor && blocks.size() > 1) || (isFloor == false && blocks.size() == 1) : "isFloor=" + isFloor + " blocks=" + blocks;
            assert this == blocks.get(0);

            assert scratchBytes.size() == 0;

            // TODO: try writing the leading vLong in MSB order
            // (opposite of what Lucene does today), for better
            // outputs sharing in the FST
            scratchBytes.writeVLong(encodeOutput(fp, hasTerms, isFloor));
            if (isFloor) {
                scratchBytes.writeVInt(blocks.size() - 1);
                for (int i = 1; i < blocks.size(); i++) {
                    PendingBlock sub = blocks.get(i);
                    assert sub.floorLeadByte != -1;
                    //if (DEBUG) {
                    //  System.out.println("    write floorLeadByte=" + Integer.toHexString(sub.floorLeadByte&0xff));
                    //}
                    scratchBytes.writeByte((byte) sub.floorLeadByte);
                    assert sub.fp > fp;
                    scratchBytes.writeVLong((sub.fp - fp) << 1 | (sub.hasTerms ? 1 : 0));
                }
            }

            final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
            final FSTCompiler<BytesRef> fstCompiler = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs).shouldShareNonSingletonNodes(false).build();
            //if (DEBUG) {
            //  System.out.println("  compile index for prefix=" + prefix);
            //}
            //indexBuilder.DEBUG = false;
            final byte[] bytes = scratchBytes.toArrayCopy();
            assert bytes.length > 0;
            fstCompiler.add(Util.toIntsRef(prefix, scratchIntsRef), new BytesRef(bytes, 0, bytes.length));
            scratchBytes.reset();

            // Copy over index for all sub-blocks
            for (PendingBlock block : blocks) {
                if (block.subIndices != null) {
                    for (FST<BytesRef> subIndex : block.subIndices) {
                        append(fstCompiler, subIndex, scratchIntsRef);
                    }
                    block.subIndices = null;
                }
            }

            index = fstCompiler.compile();

            assert subIndices == null;

      /*
      Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
      Util.toDot(index, w, false, false);
      System.out.println("SAVED to out.dot");
      w.close();
      */
        }

        // TODO: maybe we could add bulk-add method to
        // Builder?  Takes FST and unions it w/ current
        // FST.
        private void append(FSTCompiler<BytesRef> fstCompiler, FST<BytesRef> subIndex, IntsRefBuilder scratchIntsRef) throws IOException {
            final BytesRefFSTEnum<BytesRef> subIndexEnum = new BytesRefFSTEnum<>(subIndex);
            BytesRefFSTEnum.InputOutput<BytesRef> indexEnt;
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
         *
         * @param df  docFreq
         * @param ttf  totalTermFreq
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
         * 类似于 sumDocFreq 但是是以doc下出现了多少次为维度
         */
        long sumTotalTermFreq;
        /**
         * 该对象本身是以 field为维度创建的  这里就是记录该field下所有term出现在了多少个doc中 (以doc为单位)
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
        // 该对象内部存储的是一个个块
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
         *
         * @param prefixLength 写入的起始位置 非数组下标
         * @param count        代表该位置此时已经有多少term共享了
         *                     将后缀写入到 block中
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

            // 计算该前缀首次被共享时 此时该对象已经处理了多少term
            // (只有首次 每个值被分配为0  之后当某次没有任何前缀相同时 产生了新的前缀 此时它们按照pending进行初始化 就不是0了)
            int start = pending.size() - count;
            // 此时该对象已经写入了多少term
            int end = pending.size();

            // 代表某个block的开头  每当写入一个block的数据时 该值会更新
            int nextBlockStart = start;
            int nextFloorLeadLabel = -1;

            for (int i = start; i < end; i++) {
                // 获取这段时间内 写入的term
                PendingEntry ent = pending.get(i);

                int suffixLeadLabel;

                if (ent.isTerm) {
                    PendingTerm term = (PendingTerm) ent;
                    // 代表某次 term 的全部byte刚好对应这个前缀  那么针对它 要存储的后缀就为-1 代表没有后缀需要存储
                    if (term.termBytes.length == prefixLength) {
                        // Suffix is 0, i.e. prefix 'foo' and term is
                        // 'foo' so the term has empty string suffix
                        // in this block
                        assert lastSuffixLeadLabel == -1 : "i=" + i + " lastSuffixLeadLabel=" + lastSuffixLeadLabel;
                        suffixLeadLabel = -1;
                    } else {
                        // 注意[prefixLength] 实际上是获取首个不同的byte
                        suffixLeadLabel = term.termBytes[prefixLength] & 0xff;
                    }
                    // TODO 这种情况先忽略
                } else {
                    PendingBlock block = (PendingBlock) ent;
                    assert block.prefix.length > prefixLength;
                    suffixLeadLabel = block.prefix.bytes[block.prefix.offset + prefixLength] & 0xff;
                }
                // if (DEBUG) System.out.println("  i=" + i + " ent=" + ent + " suffixLeadLabel=" + suffixLeadLabel);

                // suffixLeadLabel 是后缀的首个byte (第一个不相同的byte)
                // lastSuffixLeadLabel 记录上一次的后缀首个byte
                if (suffixLeadLabel != lastSuffixLeadLabel) {
                    // 此时处理的term 距离上次写入block的term的差值  第一次为0
                    int itemsInBlock = i - nextBlockStart;
                    // 当差值超过一定值时 将数据写入到block 中
                    // TODO end - nextBlockStart > maxItemsInBlock 啥意思???
                    if (itemsInBlock >= minItemsInBlock && end - nextBlockStart > maxItemsInBlock) {
                        // The count is too large for one block, so we must break it into "floor" blocks, where we record
                        // the leading label of the suffix of the first term in each floor block, so at search time we can
                        // jump to the right floor block.  We just use a naive greedy segmenter here: make a new floor
                        // block as soon as we have at least minItemsInBlock.  This is not always best: it often produces
                        // a too-small block as the final block:
                        boolean isFloor = itemsInBlock < count;
                        // 写入block
                        newBlocks.add(writeBlock(prefixLength, isFloor, nextFloorLeadLabel, nextBlockStart, i, hasTerms, hasSubBlocks));

                        hasTerms = false;
                        hasSubBlocks = false;
                        nextFloorLeadLabel = suffixLeadLabel;
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
            if (nextBlockStart < end) {
                int itemsInBlock = end - nextBlockStart;
                boolean isFloor = itemsInBlock < count;
                newBlocks.add(writeBlock(prefixLength, isFloor, nextFloorLeadLabel, nextBlockStart, end, hasTerms, hasSubBlocks));
            }

            assert newBlocks.isEmpty() == false;

            PendingBlock firstBlock = newBlocks.get(0);

            assert firstBlock.isFloor || newBlocks.size() == 1;

            firstBlock.compileIndex(newBlocks, scratchBytes, scratchIntsRef);

            // Remove slice from the top of the pending stack, that we just wrote:
            pending.subList(pending.size() - count, pending.size()).clear();

            // Append new block
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
         * @param start  首先该对象会将所有处理的term存储到一个 pending中  并且每次按照相同前缀存储数据时 是有一个最小的阈值的 只有当前缀不再相同
         *               且之前很多term都使用相同前缀时 才触发 writerBlocks  这些term又需要拆开来处理   而start都是当中某批数据的首个term
         * @param end 对应某批数据的最后一个term
         *
         * 如果isFloor为true 代表此时有太多entry共享了相同的前缀 需要打散成多个 floor blocks 啥玩意???
         */
        private PendingBlock writeBlock(int prefixLength, boolean isFloor, int floorLeadLabel, int start, int end,
                                        boolean hasTerms, boolean hasSubBlocks) throws IOException {

            assert end > start;

            // 获取此时输出流的偏移量
            long startFP = termsOut.getFilePointer();

            // 先假设为true吧
            boolean hasFloorLeadLabel = isFloor && floorLeadLabel != -1;

            // 这里为前缀额外拓展了一个槽
            final BytesRef prefix = new BytesRef(prefixLength + (hasFloorLeadLabel ? 1 : 0));
            // 将上一个term的数据拷贝到新申请的数据块中
            System.arraycopy(lastTerm.get().bytes, 0, prefix.bytes, 0, prefixLength);
            prefix.length = prefixLength;

            //if (DEBUG2) System.out.println("    writeBlock field=" + fieldInfo.name + " prefix=" + brToString(prefix) + " fp=" + startFP + " isFloor=" + isFloor + " isLastInFloor=" + (end == pending.size()) + " floorLeadLabel=" + floorLeadLabel + " start=" + start + " end=" + end + " hasTerms=" + hasTerms + " hasSubBlocks=" + hasSubBlocks);

            // Write block header:
            // 代表本次要处理的 term（每个term被包装成对应entry） 数量
            int numEntries = end - start;
            // 类似于token 携带特殊信息
            int code = numEntries << 1;
            // 代表此时已经处理最后一批数据了 最低位设置成1  如果最低位为0 代表还有block
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
            // compact format in this case:   啥玩意  块只有term的情况下 将更多数据以压缩格式写入???
            boolean isLeafBlock = hasSubBlocks == false;

            //System.out.println("  isLeaf=" + isLeafBlock);

            final List<FST<BytesRef>> subIndices;

            // 首次数据是基于绝对值存储的
            boolean absolute = true;

            // TODO 先假设  hasSubBlocks == true 吧 忽略这里
            if (isLeafBlock) {
                // Block contains only ordinary terms:
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

                        // 将后缀信息写入到 相关输出流中
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
            // 满足这个条件才会进行压缩
            if (suffixWriter.length() > 2L * numEntries && prefixLength > 2) {
                // LZ4 inserts references whenever it sees duplicate strings of 4 chars or more, so only try it out if the
                // average suffix length is greater than 6.
                if (suffixWriter.length() > 6L * numEntries) {
                    LZ4.compress(suffixWriter.bytes(), 0, suffixWriter.length(), spareWriter, compressionHashTable);
                    if (spareWriter.size() < suffixWriter.length() - (suffixWriter.length() >>> 2)) {
                        // LZ4 saved more than 25%, go for it
                        compressionAlg = CompressionAlgorithm.LZ4;
                    }
                }
                if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) {
                    spareWriter.reset();
                    if (spareBytes.length < suffixWriter.length()) {
                        spareBytes = new byte[ArrayUtil.oversize(suffixWriter.length(), 1)];
                    }
                    if (LowercaseAsciiCompression.compress(suffixWriter.bytes(), suffixWriter.length(), spareBytes, spareWriter)) {
                        compressionAlg = CompressionAlgorithm.LOWERCASE_ASCII;
                    }
                }
            }
            long token = ((long) suffixWriter.length()) << 3;
            if (isLeafBlock) {
                token |= 0x04;
            }
            token |= compressionAlg.code;
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
            suffixLengthsWriter.copyTo(new ByteArrayDataOutput(spareBytes));
            suffixLengthsWriter.reset();
            if (allEqual(spareBytes, 1, numSuffixBytes, spareBytes[0])) {
                // Structured fields like IDs often have most values of the same length
                termsOut.writeVInt((numSuffixBytes << 1) | 1);
                termsOut.writeByte(spareBytes[0]);
            } else {
                termsOut.writeVInt(numSuffixBytes << 1);
                termsOut.writeBytes(spareBytes, numSuffixBytes);
            }

            // Stats
            final int numStatsBytes = Math.toIntExact(statsWriter.size());
            termsOut.writeVInt(numStatsBytes);
            statsWriter.copyTo(termsOut);
            statsWriter.reset();

            // Write term meta data byte[] blob
            termsOut.writeVInt((int) metaWriter.size());
            metaWriter.copyTo(termsOut);
            metaWriter.reset();

            // if (DEBUG) {
            //   System.out.println("      fpEnd=" + out.getFilePointer());
            // }

            if (hasFloorLeadLabel) {
                // We already allocated to length+1 above:
                prefix.bytes[prefix.length++] = (byte) floorLeadLabel;
            }

            return new PendingBlock(prefix, startFP, hasTerms, isFloor, floorLeadLabel, subIndices);
        }

        /**
         * 通过某个 field的信息来初始化 term写入对象
         *
         * @param fieldInfo
         */
        TermsWriter(FieldInfo fieldInfo) {
            this.fieldInfo = fieldInfo;
            assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
            docsSeen = new FixedBitSet(maxDoc);
            postingsWriter.setField(fieldInfo);
        }

        /**
         * Writes one term's worth of postings.
         *
         * @param norms 标准因子
         *              将该field下 某个term 写入到该对象中
         */
        public void write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {
      /*
      if (DEBUG) {
        int[] tmp = new int[lastTerm.length];
        System.arraycopy(prefixStarts, 0, tmp, 0, tmp.length);
        System.out.println("BTTW: write term=" + brToString(text) + " prefixStarts=" + Arrays.toString(tmp) + " pending.size()=" + pending.size());
      }
      */
            // 将该term的倒排索引信息写入到 doc文件中  (该term出现在哪些doc上 出现多少次 位置信息 payload 等等)
            BlockTermState state = postingsWriter.writeTerm(text, termsEnum, docsSeen, norms);
            // 代表数据有效   当该term没有写入到任何doc时 返回null
            if (state != null) {

                assert state.docFreq != 0;
                assert fieldInfo.getIndexOptions() == IndexOptions.DOCS || state.totalTermFreq >= state.docFreq : "postingsWriter=" + postingsWriter;

                pushTerm(text);

                // 将term 包装成一个 hold对象  并存储到一个栈结构中
                PendingTerm term = new PendingTerm(text, state);
                pending.add(term);
                //if (DEBUG) System.out.println("    add pending term = " + text + " pending.size()=" + pending.size());

                // 累加到全局数据
                sumDocFreq += state.docFreq;
                sumTotalTermFreq += state.totalTermFreq;
                numTerms++;
                if (firstPendingTerm == null) {
                    firstPendingTerm = term;
                }
                lastPendingTerm = term;
            }
        }

        /**
         * Pushes the new term to the top of the stack, and writes new blocks.
         */
        // 将term数据写入
        private void pushTerm(BytesRef text) throws IOException {
            // Find common prefix between last term and current term:
            // 返回相同的前缀长度
            int prefixLength = Arrays.mismatch(lastTerm.bytes(), 0, lastTerm.length(), text.bytes, text.offset, text.offset + text.length);
            // -1代表2个term完全相同  然而在同一个field下 是不可能出现2个相同的term的 所以这里重置成0
            if (prefixLength == -1) { // Only happens for the first term, if it is empty
                assert lastTerm.length() == 0;
                prefixLength = 0;
            }

            // if (DEBUG) System.out.println("  shared=" + pos + "  lastTerm.length=" + lastTerm.length);

            // Close the "abandoned" suffix now:
            // 假设上一个term是本次term的子集  那么不会进入该循环
            for (int i = lastTerm.length() - 1; i >= prefixLength; i--) {

                // How many items on top of the stack share the current suffix
                // we are closing:
                // 这个差值就可以理解为此时不在相同的某个byte在之前已经被共享的次数
                // 比如 bbb bbc bbd bbe bbf bc  这样之前的 bb的第二个b是一直被共享着的  所以 prefixStarts[i] 一直为0
                // 而这次第二个b 不再被共享 此时发现他与pending的差值比较大 就写入到block中
                // 而 bbb bbc bbd bbe bbf 的情况是 最后一个元素 一直在跟pending同步 prefixTopSize值反而就小
                // 当然也会出现连续2个值都需要被写入的情况   比如  bbb bbc bbd bbe bbf c 这样 以bb为前缀的数据都满足要求
                int prefixTopSize = pending.size() - prefixStarts[i];
                if (prefixTopSize >= minItemsInBlock) {
                    // if (DEBUG) System.out.println("pushTerm i=" + i + " prefixTopSize=" + prefixTopSize + " minItemsInBlock=" + minItemsInBlock);
                    // 代表写入的起始位置
                    writeBlocks(i + 1, prefixTopSize);
                    prefixStarts[i] -= prefixTopSize - 1;
                }
            }

            // 对前缀数组进行扩容
            if (prefixStarts.length < text.length) {
                prefixStarts = ArrayUtil.grow(prefixStarts, text.length);
            }

            // Init new tail:
            // 这里更新的是尾部   从非共享的部分开始设置
            for (int i = prefixLength; i < text.length; i++) {
                prefixStarts[i] = pending.size();
            }

            lastTerm.copyBytes(text);
        }

        // Finishes all terms in this field
        public void finish() throws IOException {
            if (numTerms > 0) {
                // if (DEBUG) System.out.println("BTTW: finish prefixStarts=" + Arrays.toString(prefixStarts));

                // Add empty term to force closing of all final blocks:
                pushTerm(new BytesRef());

                // TODO: if pending.size() is already 1 with a non-zero prefix length
                // we can save writing a "degenerate" root block, but we have to
                // fix all the places that assume the root block's prefix is the empty string:
                pushTerm(new BytesRef());
                writeBlocks(0, pending.size());

                // We better have one final "root" block:
                assert pending.size() == 1 && !pending.get(0).isTerm : "pending.size()=" + pending.size() + " pending=" + pending;
                final PendingBlock root = (PendingBlock) pending.get(0);
                assert root.prefix.length == 0;
                assert root.index.getEmptyOutput() != null;

                // Write FST to index
                indexStartFP = indexOut.getFilePointer();
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
        private final ByteBuffersDataOutput spareWriter = ByteBuffersDataOutput.newResettableInstance();
        private byte[] spareBytes = BytesRef.EMPTY_BYTES;
        private final LZ4.HighCompressionHashTable compressionHashTable = new LZ4.HighCompressionHashTable();
    }

    private boolean closed;

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
