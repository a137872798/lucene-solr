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
package org.apache.lucene.codecs.compressing;


import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsReader.SerializedDocument;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link StoredFieldsWriter} impl for {@link CompressingStoredFieldsFormat}.
 *
 * @lucene.experimental
 * 该对象才是将field信息存储到索引文件的工作对象
 */
public final class CompressingStoredFieldsWriter extends StoredFieldsWriter {

    /**
     * Extension of stored fields file
     */
    // 存储数据的索引文件拓展名
    public static final String FIELDS_EXTENSION = "fdt";
    /**
     * Extension of stored fields index
     */
    // 存储index 的索引文件拓展名
    public static final String INDEX_EXTENSION_PREFIX = "fd";
    /**
     * Codec name for the index.
     */
    public static final String INDEX_CODEC_NAME = "Lucene85FieldsIndex";

    // 代表field 的数据类型是 string
    static final int STRING = 0x00;
    // 代表field 的数据类型是二进制流
    static final int BYTE_ARR = 0x01;

    // 代表field的数据类型是 Number
    static final int NUMERIC_INT = 0x02;
    static final int NUMERIC_FLOAT = 0x03;
    static final int NUMERIC_LONG = 0x04;
    static final int NUMERIC_DOUBLE = 0x05;

    static final int TYPE_BITS = PackedInts.bitsRequired(NUMERIC_DOUBLE);
    static final int TYPE_MASK = (int) PackedInts.maxValue(TYPE_BITS);

    static final int VERSION_START = 1;
    static final int VERSION_OFFHEAP_INDEX = 2;
    static final int VERSION_CURRENT = VERSION_OFFHEAP_INDEX;

    private final String segment;
    private FieldsIndexWriter indexWriter;
    /**
     * 对应索引文件
     */
    private IndexOutput fieldsStream;

    /**
     * 执行压缩工作的 压缩器  基于LZ4算法
     */
    private Compressor compressor;
    /**
     * 这里指定了压缩格式
     */
    private final CompressionMode compressionMode;
    /**
     * 代表每写入多少数据触发一次刷盘
     */
    private final int chunkSize;
    /**
     * 每写入多少 doc 触发一次刷盘
     */
    private final int maxDocsPerChunk;

    /**
     * field相关信息写入到该容器
     */
    private final ByteBuffersDataOutput bufferedDocs;

    /**
     * 记录每个doc下存储了多少field
     */
    private int[] numStoredFields; // number of stored fields
    /**
     * 记录每当写入一个doc下所有field数据后 该输出流的最终偏移量
     */
    private int[] endOffsets; // end offsets in bufferedDocs
    private int docBase; // doc ID at the beginning of the chunk

    /**
     * 每个值对应一个处理完并缓存在内存中的doc  当刷盘后 该值会清空
     */
    private int numBufferedDocs; // docBase + numBufferedDocs == current doc ID

    private long numChunks; // number of compressed blocks written
    private long numDirtyChunks; // number of incomplete compressed blocks written

    /**
     * Sole constructor.
     *
     * @param directory
     * @param si
     * @param segmentSuffix
     * @param context
     * @param formatName
     * @param compressionMode
     * @param chunkSize
     * @param maxDocsPerChunk
     * @param blockShift
     * @throws IOException
     */
    CompressingStoredFieldsWriter(Directory directory, SegmentInfo si, String segmentSuffix, IOContext context,
                                  String formatName, CompressionMode compressionMode, int chunkSize, int maxDocsPerChunk, int blockShift) throws IOException {
        assert directory != null;
        this.segment = si.name;
        this.compressionMode = compressionMode;
        // 获取 LZ4 压缩对象
        this.compressor = compressionMode.newCompressor();
        this.chunkSize = chunkSize;
        this.maxDocsPerChunk = maxDocsPerChunk;
        this.docBase = 0;
        // 这里创建了一个  可重复利用 BB对象的 output
        this.bufferedDocs = ByteBuffersDataOutput.newResettableInstance();
        // 每个doc 下存储了多少field
        this.numStoredFields = new int[16];
        this.endOffsets = new int[16];
        this.numBufferedDocs = 0;

        boolean success = false;
        try {
            // 创建文件输出流  后缀名 fdt 用于存储field信息
            fieldsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION), context);
            // 写入文件头
            CodecUtil.writeIndexHeader(fieldsStream, formatName, VERSION_CURRENT, si.getId(), segmentSuffix);
            assert CodecUtil.indexHeaderLength(formatName, segmentSuffix) == fieldsStream.getFilePointer();

            // 针对field信息的索引文件
            indexWriter = new FieldsIndexWriter(directory, segment, segmentSuffix, INDEX_EXTENSION_PREFIX, INDEX_CODEC_NAME, si.getId(), blockShift, context);

            fieldsStream.writeVInt(chunkSize);
            fieldsStream.writeVInt(PackedInts.VERSION_CURRENT);

            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(fieldsStream, indexWriter);
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.close(fieldsStream, indexWriter, compressor);
        } finally {
            fieldsStream = null;
            indexWriter = null;
            compressor = null;
        }
    }

    /**
     * 此时写入到doc中第几个field  每当一个doc写入完成时 就可以将该值置零
     */
    private int numStoredFieldsInDoc;

    /**
     * 代表此时开始处理一个新的 doc   这里没有任何操作
     *
     * @throws IOException
     */
    @Override
    public void startDocument() throws IOException {
    }

    /**
     * 当某个document下所有field的处理完后触发  只涉及到 field.value 类型等
     *
     * @throws IOException
     */
    @Override
    public void finishDocument() throws IOException {
        // 每当处理完一个doc 时 numBufferedDocs 就会加1 代表此时内存中缓存的doc数量
        // 当doc数量超过了 numStoredFields 时 进行扩容
        if (numBufferedDocs == this.numStoredFields.length) {
            final int newLength = ArrayUtil.oversize(numBufferedDocs + 1, 4);
            this.numStoredFields = ArrayUtil.growExact(this.numStoredFields, newLength);
            endOffsets = ArrayUtil.growExact(endOffsets, newLength);
        }

        // 记录当前doc下存储了多少field
        this.numStoredFields[numBufferedDocs] = numStoredFieldsInDoc;
        numStoredFieldsInDoc = 0;

        // 记录当前 doc 的 末尾偏移量
        endOffsets[numBufferedDocs] = Math.toIntExact(bufferedDocs.size());
        ++numBufferedDocs;
        // 检测是否满足刷盘条件
        if (triggerFlush()) {
            flush();
        }
    }

    /**
     * 存储int[] 数据
     *
     * @param values
     * @param length
     * @param out
     * @throws IOException
     */
    private static void saveInts(int[] values, int length, DataOutput out) throws IOException {
        assert length > 0;
        // 当长度为1时 直接写入数据
        if (length == 1) {
            out.writeVInt(values[0]);
        } else {
            boolean allEqual = true;
            for (int i = 1; i < length; ++i) {
                if (values[i] != values[0]) {
                    allEqual = false;
                    break;
                }
            }
            // 当全部相同时 存入一个特殊标识
            if (allEqual) {
                out.writeVInt(0);
                out.writeVInt(values[0]);
            } else {
                long max = 0;
                for (int i = 0; i < length; ++i) {
                    max |= values[i];
                }
                // 通过 |= 计算 获取最大值需要多少位表示
                final int bitsRequired = PackedInts.bitsRequired(max);
                // 记录这个值 就代表在解析时  每多少位就是一个值
                out.writeVInt(bitsRequired);
                // 在写入的时候 还没有按位转换
                final PackedInts.Writer w = PackedInts.getWriterNoHeader(out, PackedInts.Format.PACKED, length, bitsRequired, 1);
                // 按位存储数据
                for (int i = 0; i < length; ++i) {
                    w.add(values[i]);
                }
                // 调用该方法时 才是将long 值转化成按位存储 并转存
                w.finish();
            }
        }
    }

    /**
     * @param docBase         本次 numStoredFields lengths 内部数据的 在哪个doc之上
     * @param numBufferedDocs 本次处理了多少doc (+docBase 才是绝对值 )
     * @param numStoredFields 本次处理的每个doc 各有多少field
     * @param lengths         每个doc写入的数据总长度
     * @param sliced
     * @throws IOException
     */
    private void writeHeader(int docBase, int numBufferedDocs, int[] numStoredFields, int[] lengths, boolean sliced) throws IOException {
        final int slicedBit = sliced ? 1 : 0;

        // save docBase and numBufferedDocs
        fieldsStream.writeVInt(docBase);
        // 这里相当于已经存入了长度信息  TODO slicedBit这个标识怎么用???
        fieldsStream.writeVInt((numBufferedDocs) << 1 | slicedBit);

        // save numStoredFields
        saveInts(numStoredFields, numBufferedDocs, fieldsStream);

        // save lengths
        saveInts(lengths, numBufferedDocs, fieldsStream);
    }

    /**
     * 每当缓存的数据量过大 或者缓存的doc数量过多就会触发刷盘
     *
     * @return
     */
    private boolean triggerFlush() {
        return bufferedDocs.size() >= chunkSize || // chunks of at least chunkSize bytes
                numBufferedDocs >= maxDocsPerChunk;
    }

    /**
     * 满足刷盘条件时 将BB 内部的数据持久化到索引文件
     *
     * @throws IOException
     */
    private void flush() throws IOException {
        // 写入本次刷盘多少doc 以及刷盘前索引文件的偏移量
        indexWriter.writeIndex(numBufferedDocs, fieldsStream.getFilePointer());

        // transform end offsets into lengths
        final int[] lengths = endOffsets;
        for (int i = numBufferedDocs - 1; i > 0; --i) {
            // 差值就是某个doc 往BB总计写入的 数据长度
            lengths[i] = endOffsets[i] - endOffsets[i - 1];
            assert lengths[i] >= 0;
        }
        // 代表此时BB内部的数据已经比较多了
        final boolean sliced = bufferedDocs.size() >= 2 * chunkSize;
        // 往索引文件中写入相关信息
        writeHeader(docBase, numBufferedDocs, numStoredFields, lengths, sliced);

        // compress stored fields to fieldsStream.
        //
        // TODO: do we need to slice it since we already have the slices in the buffer? Perhaps
        // we should use max-block-bits restriction on the buffer itself, then we won't have to check it here.
        // 这个对象内部存储的是 termValue
        byte[] content = bufferedDocs.toArrayCopy();
        bufferedDocs.reset();

        // 以chunk为单位进行压缩  并将结果写入到 索引文件中
        if (sliced) {
            // big chunk, slice it
            for (int compressed = 0; compressed < content.length; compressed += chunkSize) {
                compressor.compress(content, compressed, Math.min(chunkSize, content.length - compressed), fieldsStream);
            }
        } else {
            compressor.compress(content, 0, content.length, fieldsStream);
        }

        // reset
        docBase += numBufferedDocs;
        numBufferedDocs = 0;
        bufferedDocs.reset();
        numChunks++;
    }

    /**
     * 将某个域的信息 先写入到内存中
     *
     * @param info  这个是描述 field的元数据信息
     * @param field
     * @throws IOException
     */
    @Override
    public void writeField(FieldInfo info, IndexableField field)
            throws IOException {

        // 代表在当前文档中又存储了一个 field
        ++numStoredFieldsInDoc;

        int bits = 0;
        final BytesRef bytes;
        final String string;

        // 尝试以 num的形式读取 field的值    也就是docValue的类型是通过检测 field.value的类型来判别的
        Number number = field.numericValue();
        if (number != null) {
            if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
                bits = NUMERIC_INT;
            } else if (number instanceof Long) {
                bits = NUMERIC_LONG;
            } else if (number instanceof Float) {
                bits = NUMERIC_FLOAT;
            } else if (number instanceof Double) {
                bits = NUMERIC_DOUBLE;
            } else {
                throw new IllegalArgumentException("cannot store numeric type " + number.getClass());
            }
            string = null;
            bytes = null;
        } else {
            // 代表数据是二进制流类型
            bytes = field.binaryValue();
            if (bytes != null) {
                bits = BYTE_ARR;
                string = null;
            } else {
                // 代表是文本型数据
                bits = STRING;
                string = field.stringValue();
                if (string == null) {
                    throw new IllegalArgumentException("field " + field.name() + " is stored but does not have binaryValue, stringValue nor numericValue");
                }
            }
        }

        // 存储的第一个值 代表 field的编号以及它的数据类型
        final long infoAndBits = (((long) info.number) << TYPE_BITS) | bits;
        bufferedDocs.writeVLong(infoAndBits);

        if (bytes != null) {
            // 先写入 二进制数据的长度 之后写入数据体
            bufferedDocs.writeVInt(bytes.length);
            bufferedDocs.writeBytes(bytes.bytes, bytes.offset, bytes.length);
        } else if (string != null) {
            // 写入string 实际上就会拆解成上面2步
            bufferedDocs.writeString(string);
        } else {
            // 数字类型会采用 锯齿写入法 也就是避免了 负数占用的位数过多的问题
            if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
                bufferedDocs.writeZInt(number.intValue());
            } else if (number instanceof Long) {
                writeTLong(bufferedDocs, number.longValue());
            } else if (number instanceof Float) {
                writeZFloat(bufferedDocs, number.floatValue());
            } else if (number instanceof Double) {
                writeZDouble(bufferedDocs, number.doubleValue());
            } else {
                throw new AssertionError("Cannot get here");
            }
        }
    }

    // -0 isn't compressed.
    static final int NEGATIVE_ZERO_FLOAT = Float.floatToIntBits(-0f);
    static final long NEGATIVE_ZERO_DOUBLE = Double.doubleToLongBits(-0d);

    // for compression of timestamps
    static final long SECOND = 1000L;
    static final long HOUR = 60 * 60 * SECOND;
    static final long DAY = 24 * HOUR;
    static final int SECOND_ENCODING = 0x40;
    static final int HOUR_ENCODING = 0x80;
    static final int DAY_ENCODING = 0xC0;

    /**
     * Writes a float in a variable-length format.  Writes between one and
     * five bytes. Small integral values typically take fewer bytes.
     * <p>
     * ZFloat --&gt; Header, Bytes*?
     * <ul>
     * <li>Header --&gt; {@link DataOutput#writeByte Uint8}. When it is
     * equal to 0xFF then the value is negative and stored in the next
     * 4 bytes. Otherwise if the first bit is set then the other bits
     * in the header encode the value plus one and no other
     * bytes are read. Otherwise, the value is a positive float value
     * whose first byte is the header, and 3 bytes need to be read to
     * complete it.
     * <li>Bytes --&gt; Potential additional bytes to read depending on the
     * header.
     * </ul>
     */
    static void writeZFloat(DataOutput out, float f) throws IOException {
        int intVal = (int) f;
        final int floatBits = Float.floatToIntBits(f);

        if (f == intVal
                && intVal >= -1
                && intVal <= 0x7D
                && floatBits != NEGATIVE_ZERO_FLOAT) {
            // small integer value [-1..125]: single byte
            out.writeByte((byte) (0x80 | (1 + intVal)));
        } else if ((floatBits >>> 31) == 0) {
            // other positive floats: 4 bytes
            out.writeInt(floatBits);
        } else {
            // other negative float: 5 bytes
            out.writeByte((byte) 0xFF);
            out.writeInt(floatBits);
        }
    }

    /**
     * Writes a float in a variable-length format.  Writes between one and
     * five bytes. Small integral values typically take fewer bytes.
     * <p>
     * ZFloat --&gt; Header, Bytes*?
     * <ul>
     * <li>Header --&gt; {@link DataOutput#writeByte Uint8}. When it is
     * equal to 0xFF then the value is negative and stored in the next
     * 8 bytes. When it is equal to 0xFE then the value is stored as a
     * float in the next 4 bytes. Otherwise if the first bit is set
     * then the other bits in the header encode the value plus one and
     * no other bytes are read. Otherwise, the value is a positive float
     * value whose first byte is the header, and 7 bytes need to be read
     * to complete it.
     * <li>Bytes --&gt; Potential additional bytes to read depending on the
     * header.
     * </ul>
     */
    static void writeZDouble(DataOutput out, double d) throws IOException {
        int intVal = (int) d;
        final long doubleBits = Double.doubleToLongBits(d);

        if (d == intVal &&
                intVal >= -1 &&
                intVal <= 0x7C &&
                doubleBits != NEGATIVE_ZERO_DOUBLE) {
            // small integer value [-1..124]: single byte
            out.writeByte((byte) (0x80 | (intVal + 1)));
            return;
        } else if (d == (float) d) {
            // d has an accurate float representation: 5 bytes
            out.writeByte((byte) 0xFE);
            out.writeInt(Float.floatToIntBits((float) d));
        } else if ((doubleBits >>> 63) == 0) {
            // other positive doubles: 8 bytes
            out.writeLong(doubleBits);
        } else {
            // other negative doubles: 9 bytes
            out.writeByte((byte) 0xFF);
            out.writeLong(doubleBits);
        }
    }

    /**
     * Writes a long in a variable-length format.  Writes between one and
     * ten bytes. Small values or values representing timestamps with day,
     * hour or second precision typically require fewer bytes.
     * <p>
     * ZLong --&gt; Header, Bytes*?
     * <ul>
     * <li>Header --&gt; The first two bits indicate the compression scheme:
     * <ul>
     * <li>00 - uncompressed
     * <li>01 - multiple of 1000 (second)
     * <li>10 - multiple of 3600000 (hour)
     * <li>11 - multiple of 86400000 (day)
     * </ul>
     * Then the next bit is a continuation bit, indicating whether more
     * bytes need to be read, and the last 5 bits are the lower bits of
     * the encoded value. In order to reconstruct the value, you need to
     * combine the 5 lower bits of the header with a vLong in the next
     * bytes (if the continuation bit is set to 1). Then
     * {@link BitUtil#zigZagDecode(int) zigzag-decode} it and finally
     * multiply by the multiple corresponding to the compression scheme.
     * <li>Bytes --&gt; Potential additional bytes to read depending on the
     * header.
     * </ul>
     */
    // T for "timestamp"
    static void writeTLong(DataOutput out, long l) throws IOException {
        int header;
        if (l % SECOND != 0) {
            header = 0;
        } else if (l % DAY == 0) {
            // timestamp with day precision
            header = DAY_ENCODING;
            l /= DAY;
        } else if (l % HOUR == 0) {
            // timestamp with hour precision, or day precision with a timezone
            header = HOUR_ENCODING;
            l /= HOUR;
        } else {
            // timestamp with second precision
            header = SECOND_ENCODING;
            l /= SECOND;
        }

        final long zigZagL = BitUtil.zigZagEncode(l);
        header |= (zigZagL & 0x1F); // last 5 bits
        final long upperBits = zigZagL >>> 5;
        if (upperBits != 0) {
            header |= 0x20;
        }
        out.writeByte((byte) header);
        if (upperBits != 0) {
            out.writeVLong(upperBits);
        }
    }

    /**
     * 代表此时已经处理完所有数据
     *
     * @param fis
     * @param numDocs
     * @throws IOException
     */
    @Override
    public void finish(FieldInfos fis, int numDocs) throws IOException {
        // 此时还有未刷盘的数据
        if (numBufferedDocs > 0) {
            flush();
            numDirtyChunks++; // incomplete: we had to force this flush
        } else {
            assert bufferedDocs.size() == 0;
        }
        if (docBase != numDocs) {
            throw new RuntimeException("Wrote " + docBase + " docs, finish called with numDocs=" + numDocs);
        }
        // 这里会生成元数据文件 并且删除掉2个临时文件
        indexWriter.finish(numDocs, fieldsStream.getFilePointer());
        fieldsStream.writeVLong(numChunks);
        fieldsStream.writeVLong(numDirtyChunks);
        CodecUtil.writeFooter(fieldsStream);
        assert bufferedDocs.size() == 0;
    }

    // bulk merge is scary: its caused corruption bugs in the past.
    // we try to be extra safe with this impl, but add an escape hatch to
    // have a workaround for undiscovered bugs.
    static final String BULK_MERGE_ENABLED_SYSPROP = CompressingStoredFieldsWriter.class.getName() + ".enableBulkMerge";
    static final boolean BULK_MERGE_ENABLED;

    static {
        boolean v = true;
        try {
            v = Boolean.parseBoolean(System.getProperty(BULK_MERGE_ENABLED_SYSPROP, "true"));
        } catch (SecurityException ignored) {
        }
        BULK_MERGE_ENABLED = v;
    }

    /**
     * 在对多个 segment 进行数据合并时调用该方法
     *
     * @param mergeState
     * @return
     * @throws IOException
     */
    @Override
    public int merge(MergeState mergeState) throws IOException {

        // merge总计写入了多少doc
        int docCount = 0;
        int numReaders = mergeState.maxDocs.length;

        MatchingReaders matching = new MatchingReaders(mergeState);
        // 代表需要做排序处理
        if (mergeState.needsIndexSort) {
            /**
             * If all readers are compressed and they have the same fieldinfos then we can merge the serialized document
             * directly.
             */
            List<CompressingStoredFieldsMergeSub> subs = new ArrayList<>();
            for (int i = 0; i < mergeState.storedFieldsReaders.length; i++) {
                // 一般情况下都是满足的
                if (matching.matchingReaders[i] &&
                        mergeState.storedFieldsReaders[i] instanceof CompressingStoredFieldsReader) {
                    CompressingStoredFieldsReader storedFieldsReader = (CompressingStoredFieldsReader) mergeState.storedFieldsReaders[i];
                    // 忽略校验的逻辑
                    storedFieldsReader.checkIntegrity();
                    // 将相关信息抽取出来包装成一个 Sub对象
                    subs.add(new CompressingStoredFieldsMergeSub(storedFieldsReader, mergeState.docMaps[i], mergeState.maxDocs[i]));
                } else {
                    // 也就是只要有一个reader对应的field 没有存入到 mergeFieldInfos中 就降级采用父类的merge方法
                    // 这个可能是兼容老代码 先忽略
                    return super.merge(mergeState);
                }
            }

            // 该对象负责遍历所有reader的 docId   indexIsSorted为true 会通过优先队列确保读取顺序按照 globalDocId 从小到大
            final DocIDMerger<CompressingStoredFieldsMergeSub> docIDMerger =
                    DocIDMerger.of(subs, true);

            // 这里将所有doc读取出来后写入到索引文件
            while (true) {
                // 先找到此时最小的 sub 对象
                CompressingStoredFieldsMergeSub sub = docIDMerger.next();
                if (sub == null) {
                    break;
                }
                assert sub.mappedDocID == docCount;
                // 通过原始docId 找到此时存储的数据
                SerializedDocument doc = sub.reader.document(sub.docID);
                // 对子类开放的前置钩子
                startDocument();
                // 将数据转移到 输出流中
                bufferedDocs.copyBytes(doc.in, doc.length);
                numStoredFieldsInDoc = doc.numStoredFields;
                finishDocument();
                ++docCount;
            }
            // 将剩余未持久化的数据 持久化  并写入一些文件尾数据
            finish(mergeState.mergeFieldInfos, docCount);
            return docCount;
        }


        // 代表数据一开始就是有序的 不需要上面创建sub这种复杂的做法
        for (int readerIndex = 0; readerIndex < numReaders; readerIndex++) {

            // visitor 只是开放了一些读取内部属性的api
            MergeVisitor visitor = new MergeVisitor(mergeState, readerIndex);
            CompressingStoredFieldsReader matchingFieldsReader = null;

            // 代表该 segment对应的所有field 都已经合并到新的 segment
            if (matching.matchingReaders[readerIndex]) {
                final StoredFieldsReader fieldsReader = mergeState.storedFieldsReaders[readerIndex];
                // we can only bulk-copy if the matching reader is also a CompressingStoredFieldsReader
                if (fieldsReader != null && fieldsReader instanceof CompressingStoredFieldsReader) {
                    matchingFieldsReader = (CompressingStoredFieldsReader) fieldsReader;
                }
            }

            // 读取该segment对应的 maxDoc 和 liveDocs
            final int maxDoc = mergeState.maxDocs[readerIndex];
            final Bits liveDocs = mergeState.liveDocs[readerIndex];

            // if its some other format, or an older version of this format, or safety switch:
            // 跳过兼容老代码的部分
            if (matchingFieldsReader == null || matchingFieldsReader.getVersion() != VERSION_CURRENT || BULK_MERGE_ENABLED == false) {
                // naive merge...
                StoredFieldsReader storedFieldsReader = mergeState.storedFieldsReaders[readerIndex];
                if (storedFieldsReader != null) {
                    storedFieldsReader.checkIntegrity();
                }
                for (int docID = 0; docID < maxDoc; docID++) {
                    if (liveDocs != null && liveDocs.get(docID) == false) {
                        continue;
                    }
                    startDocument();
                    storedFieldsReader.visitDocument(docID, visitor);
                    finishDocument();
                    ++docCount;
                }
                // 如果此时segment下的doc都还存在
            } else if (matchingFieldsReader.getCompressionMode() == compressionMode &&
                    matchingFieldsReader.getChunkSize() == chunkSize &&
                    matchingFieldsReader.getPackedIntsVersion() == PackedInts.VERSION_CURRENT &&
                    liveDocs == null &&
                    !tooDirty(matchingFieldsReader)) {
                // optimized merge, raw byte copy
                // its not worth fine-graining this if there are deletions.

                // if the format is older, its always handled by the naive merge case above
                assert matchingFieldsReader.getVersion() == VERSION_CURRENT;
                matchingFieldsReader.checkIntegrity();

                // flush any pending chunks
                // 将上一个reader存储在 内存中的数据刷盘
                if (numBufferedDocs > 0) {
                    flush();
                    numDirtyChunks++; // incomplete: we had to force this flush
                }

                // iterate over each chunk. we use the stored fields index to find chunk boundaries,
                // read the docstart + doccount from the chunk header (we write a new header, since doc numbers will change),
                // and just copy the bytes directly.
                // 该对象负责读取 field 数据   一个doc 内部也就是由多个field组成的
                IndexInput rawDocs = matchingFieldsReader.getFieldsStream();
                // 该对象负责读取元数据信息
                FieldsIndex index = matchingFieldsReader.getIndexReader();
                rawDocs.seek(index.getStartPointer(0));
                int docID = 0;
                // 根据文档号 将之前存储的所有数据读取出来   doc的数据以field的形式存储在 rawDocs 中
                while (docID < maxDoc) {
                    // read header
                    // 这里是读取block下首个 doc的id
                    int base = rawDocs.readVInt();
                    if (base != docID) {
                        throw new CorruptIndexException("invalid state: base=" + base + ", docID=" + docID, rawDocs);
                    }

                    // TODO 最低位存的是什么
                    int code = rawDocs.readVInt();

                    // write a new index entry and new header for this chunk.
                    // 该block下一共存储了多少doc
                    int bufferedDocs = code >>> 1;
                    // indexWriter是写数据文件的索引信息的   这里记录本次block的相关信息
                    indexWriter.writeIndex(bufferedDocs, fieldsStream.getFilePointer());

                    // 看来是要将 某个segment的 block 原封不动的转移到新的 segment中
                    fieldsStream.writeVInt(docBase); // rebase
                    fieldsStream.writeVInt(code);
                    docID += bufferedDocs;
                    docBase += bufferedDocs;
                    docCount += bufferedDocs;

                    if (docID > maxDoc) {
                        throw new CorruptIndexException("invalid state: base=" + base + ", count=" + bufferedDocs + ", maxDoc=" + maxDoc, rawDocs);
                    }

                    // copy bytes until the next chunk boundary (or end of chunk data).
                    // using the stored fields index for this isn't the most efficient, but fast enough
                    // and is a source of redundancy for detecting bad things.
                    // 这个时候 docId 已经变成了本次 copy的终点
                    final long end;
                    if (docID == maxDoc) {
                        end = matchingFieldsReader.getMaxPointer();
                    } else {
                        // 找到终点对应的偏移量
                        end = index.getStartPointer(docID);
                    }
                    // 这里没有用0拷贝技术 应该是 FileChannel#transferTo
                    fieldsStream.copyBytes(rawDocs, end - rawDocs.getFilePointer());
                }

                // 代表某个 segment的数据已经读取完了
                if (rawDocs.getFilePointer() != matchingFieldsReader.getMaxPointer()) {
                    throw new CorruptIndexException("invalid state: pos=" + rawDocs.getFilePointer() + ", max=" + matchingFieldsReader.getMaxPointer(), rawDocs);
                }

                // since we bulk merged all chunks, we inherit any dirty ones from this segment.
                // 记录该reader下的chunk数量 (chunk也就是block )
                numChunks += matchingFieldsReader.getNumChunks();
                numDirtyChunks += matchingFieldsReader.getNumDirtyChunks();
            } else {
                // 代表该 segment下部分doc 已经被删除了 无法直接拷贝所有数据

                // optimized merge, we copy serialized (but decompressed) bytes directly
                // even on simple docs (1 stored field), it seems to help by about 20%

                // if the format is older, its always handled by the naive merge case above
                assert matchingFieldsReader.getVersion() == VERSION_CURRENT;
                matchingFieldsReader.checkIntegrity();

                // 这里以doc为单位 挨个拷贝
                for (int docID = 0; docID < maxDoc; docID++) {
                    if (liveDocs != null && liveDocs.get(docID) == false) {
                        continue;
                    }
                    SerializedDocument doc = matchingFieldsReader.document(docID);
                    startDocument();
                    bufferedDocs.copyBytes(doc.in, doc.length);
                    numStoredFieldsInDoc = doc.numStoredFields;
                    finishDocument();
                    ++docCount;
                }
            }
        }
        finish(mergeState.mergeFieldInfos, docCount);
        return docCount;
    }

    /**
     * Returns true if we should recompress this reader, even though we could bulk merge compressed data
     * <p>
     * The last chunk written for a segment is typically incomplete, so without recompressing,
     * in some worst-case situations (e.g. frequent reopen with tiny flushes), over time the
     * compression ratio can degrade. This is a safety switch.
     * 在什么情况下 reader 会被认为是 dirty呢???
     */
    boolean tooDirty(CompressingStoredFieldsReader candidate) {
        // more than 1% dirty, or more than hard limit of 1024 dirty chunks
        return candidate.getNumDirtyChunks() > 1024 ||
                candidate.getNumDirtyChunks() * 100 > candidate.getNumChunks();
    }

    /**
     *
     */
    private static class CompressingStoredFieldsMergeSub extends DocIDMerger.Sub {

        /**
         * 对应某个要被合并的段的reader
         */
        private final CompressingStoredFieldsReader reader;
        /**
         * 该段对应的  maxDoc
         */
        private final int maxDoc;
        int docID = -1;

        public CompressingStoredFieldsMergeSub(CompressingStoredFieldsReader reader, MergeState.DocMap docMap, int maxDoc) {
            super(docMap);
            this.maxDoc = maxDoc;
            this.reader = reader;
        }

        @Override
        public int nextDoc() {
            docID++;
            if (docID == maxDoc) {
                return NO_MORE_DOCS;
            } else {
                return docID;
            }
        }
    }
}
