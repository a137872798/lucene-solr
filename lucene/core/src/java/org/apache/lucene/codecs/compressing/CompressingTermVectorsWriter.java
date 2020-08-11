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


import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * {@link TermVectorsWriter} for {@link CompressingTermVectorsFormat}.
 * @lucene.experimental
 * 存储词向量信息的对象
 */
public final class CompressingTermVectorsWriter extends TermVectorsWriter {

  // hard limit on the maximum number of documents per chunk
  static final int MAX_DOCUMENTS_PER_CHUNK = 128;

  static final String VECTORS_EXTENSION = "tvd";
  static final String VECTORS_INDEX_EXTENSION_PREFIX = "tv";
  static final String VECTORS_INDEX_CODEC_NAME = "Lucene85TermVectorsIndex";

  static final int VERSION_START = 1;
  static final int VERSION_OFFHEAP_INDEX = 2;
  static final int VERSION_CURRENT = VERSION_OFFHEAP_INDEX;

  static final int PACKED_BLOCK_SIZE = 64;

  // 用于标记是否记录 term的3种属性
  static final int POSITIONS = 0x01;
  static final int   OFFSETS = 0x02;
  static final int  PAYLOADS = 0x04;
  static final int FLAGS_BITS = PackedInts.bitsRequired(POSITIONS | OFFSETS | PAYLOADS);

  private final String segment;


  private FieldsIndexWriter indexWriter;
  private IndexOutput vectorsStream;

  private final CompressionMode compressionMode;
  private final Compressor compressor;
  private final int chunkSize;

  /**
   * 记录flush了几次
   */
  private long numChunks; // number of compressed blocks written
  /**
   * 由于调用了 finish 导致强制刷盘的docData  此时他们的数据不一定是完整的
   */
  private long numDirtyChunks; // number of incomplete compressed blocks written

  /** a pending doc */
  // 记录了有关doc的数据
  private class DocData {
    /**
     * 该doc 内部有多少field
     */
    final int numFields;
    /**
     * 本次doc下存储的 field相关信息
     */
    final Deque<FieldData> fields;
    final int posStart, offStart, payStart;

    /**
     *
     * @param numFields  标明该 doc内部有多少个 field
     * @param posStart   相关向量值的基础值  本次docData的数据都会以该值作为基础 往上累加
     * @param offStart
     * @param payStart
     */
    DocData(int numFields, int posStart, int offStart, int payStart) {
      this.numFields = numFields;
      this.fields = new ArrayDeque<>(numFields);
      this.posStart = posStart;
      this.offStart = offStart;
      this.payStart = payStart;
    }

    /**
     * 为当前 docData 对象 添加一个 fieldData
     * @param fieldNum  当前 field的编号
     * @param numTerms   该对象内部有多少 term
     * 下面代表是否要记录这3种属性
     * @param positions
     * @param offsets
     * @param payloads
     * @return
     */
    FieldData addField(int fieldNum, int numTerms, boolean positions, boolean offsets, boolean payloads) {
      final FieldData field;
      if (fields.isEmpty()) {
        // 第一个field 的 pos off 等属性是从上个doc默认获取的     也就是在逻辑上将他们的偏移量等信息连起来了
        field = new FieldData(fieldNum, numTerms, positions, offsets, payloads, posStart, offStart, payStart);
      } else {
        final FieldData last = fields.getLast();
        final int posStart = last.posStart + (last.hasPositions ? last.totalPositions : 0);
        final int offStart = last.offStart + (last.hasOffsets ? last.totalPositions : 0);
        final int payStart = last.payStart + (last.hasPayloads ? last.totalPositions : 0);
        field = new FieldData(fieldNum, numTerms, positions, offsets, payloads, posStart, offStart, payStart);
      }
      fields.add(field);
      return field;
    }
  }

  /**
   * 代表当前doc 要存储多少个 field的数据  构建结构体对象
   * @param numVectorFields
   * @return
   */
  private DocData addDocData(int numVectorFields) {
    FieldData last = null;
    // 反向遍历 DocData 因为每次TermVector的数据都是增量数据 所以需要借助上一次的数据
    for (Iterator<DocData> it = pendingDocs.descendingIterator(); it.hasNext(); ) {
      final DocData doc = it.next();
      // 只要某个 doc的 field 列表不为空 并且这里会返回最后一个
      if (!doc.fields.isEmpty()) {
        last = doc.fields.getLast();
        break;
      }
    }
    // 如果此时还没有创建 docData 此时创建一个新对象
    final DocData doc;
    if (last == null) {
      doc = new DocData(numVectorFields, 0, 0, 0);
    } else {
      // 如果存在上一个doc  以上一个文档的数据作为起点 生成新的 docData
      final int posStart = last.posStart + (last.hasPositions ? last.totalPositions : 0);
      final int offStart = last.offStart + (last.hasOffsets ? last.totalPositions : 0);
      final int payStart = last.payStart + (last.hasPayloads ? last.totalPositions : 0);
      doc = new DocData(numVectorFields, posStart, offStart, payStart);
    }
    pendingDocs.add(doc);
    return doc;
  }

  /** a pending field */
  // 以每个doc为 单位处理写入逻辑  每个doc内部的field 会被包装成 fieldData 对象
  private class FieldData {
    final boolean hasPositions, hasOffsets, hasPayloads;
    final int fieldNum, flags, numTerms;
    /**
     * 分别存储 某term出现的频率  与前一个term的公共前缀长度 后缀长度
     */
    final int[] freqs, prefixLengths, suffixLengths;
    final int posStart, offStart, payStart;
    int totalPositions;
    /**
     * 记录写入到第几个term
     */
    int ord;

    /**
     *
     * @param fieldNum  field 编号
     * @param numTerms   内部有多少 term
     * @param positions     是否记录这3种属性
     * @param offsets
     * @param payloads
     * @param posStart   上个field 的值
     * @param offStart
     * @param payStart
     */
    FieldData(int fieldNum, int numTerms, boolean positions, boolean offsets, boolean payloads,
        int posStart, int offStart, int payStart) {
      this.fieldNum = fieldNum;
      this.numTerms = numTerms;
      this.hasPositions = positions;
      this.hasOffsets = offsets;
      this.hasPayloads = payloads;
      this.flags = (positions ? POSITIONS : 0) | (offsets ? OFFSETS : 0) | (payloads ? PAYLOADS : 0);
      // 为每个term 记录存储相关信息的数据
      this.freqs = new int[numTerms];
      this.prefixLengths = new int[numTerms];
      this.suffixLengths = new int[numTerms];
      this.posStart = posStart;
      this.offStart = offStart;
      this.payStart = payStart;
      totalPositions = 0;
      ord = 0;
    }

    /**
     * 往该 field 中追加某个term
     * @param freq   该term出现的频率
     * @param prefixLength   与上个词相同前缀的长度
     * @param suffixLength   剩余的部分就是后缀长度
     */
    void addTerm(int freq, int prefixLength, int suffixLength) {
      freqs[ord] = freq;
      prefixLengths[ord] = prefixLength;
      suffixLengths[ord] = suffixLength;
      ++ord;
    }
    void addPosition(int position, int startOffset, int length, int payloadLength) {
      if (hasPositions) {
        if (posStart + totalPositions == positionsBuf.length) {
          positionsBuf = ArrayUtil.grow(positionsBuf);
        }
        positionsBuf[posStart + totalPositions] = position;
      }
      if (hasOffsets) {
        if (offStart + totalPositions == startOffsetsBuf.length) {
          final int newLength = ArrayUtil.oversize(offStart + totalPositions, 4);
          startOffsetsBuf = ArrayUtil.growExact(startOffsetsBuf, newLength);
          lengthsBuf = ArrayUtil.growExact(lengthsBuf, newLength);
        }
        startOffsetsBuf[offStart + totalPositions] = startOffset;
        lengthsBuf[offStart + totalPositions] = length;
      }
      if (hasPayloads) {
        if (payStart + totalPositions == payloadLengthsBuf.length) {
          payloadLengthsBuf = ArrayUtil.grow(payloadLengthsBuf);
        }
        payloadLengthsBuf[payStart + totalPositions] = payloadLength;
      }
      ++totalPositions;
    }
  }

  /**
   * 记录写入的doc总数  注意这里还没有持久化
   */
  private int numDocs; // total number of docs seen
  /**
   * 每个尚未刷盘的 数据块 以docData为单位存储
   */
  private final Deque<DocData> pendingDocs; // pending docs
  /**
   * 对应处理本次doc生成的结果
   */
  private DocData curDoc; // current document
  /**
   * 对应当前正在采集的field
   */
  private FieldData curField; // current field

  /**
   * 存储最近一个写入的term  当插入一个新的field时 重置该字段
   */
  private final BytesRef lastTerm;

  /**
   * 每当出现一个 term 都会有自己的 position startOff 等信息 (包括那些重复的term)‘
   * startOffsetsBuf 记录每个term的起始偏移量
   * lengthsBuf 记录 startOffset 到 endOffset 的长度
   */
  private int[] positionsBuf, startOffsetsBuf, lengthsBuf, payloadLengthsBuf;

  /**
   * 这里负责存储 term 当term与上个term有公共前缀的时候 只写入后缀
   */
  private final ByteBuffersDataOutput termSuffixes; // buffered term suffixes
  private final ByteBuffersDataOutput payloadBytes; // buffered term payloads
  private final BlockPackedWriter writer;

  /**
   * 初始化写入词向量信息的 writer
   * @param directory  本次文件会写入到哪个目录
   * @param si   本次段信息  与 IndexWriter 一一对应的关系
   * @param segmentSuffix
   * @param context
   * @param formatName
   * @param compressionMode
   * @param chunkSize
   * @param blockShift
   * @throws IOException
   */
  public CompressingTermVectorsWriter(Directory directory, SegmentInfo si, String segmentSuffix, IOContext context,
      String formatName, CompressionMode compressionMode, int chunkSize, int blockShift) throws IOException {
    assert directory != null;
    this.segment = si.name;
    this.compressionMode = compressionMode;
    this.compressor = compressionMode.newCompressor();
    this.chunkSize = chunkSize;

    numDocs = 0;
    pendingDocs = new ArrayDeque<>();

    termSuffixes = ByteBuffersDataOutput.newResettableInstance();
    payloadBytes = ByteBuffersDataOutput.newResettableInstance();
    lastTerm = new BytesRef(ArrayUtil.oversize(30, 1));

    boolean success = false;
    try {
      // 这里会创建 segmentName_suffix.tvd 的索引文件
      vectorsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, segmentSuffix, VECTORS_EXTENSION),
                                                     context);
      // 写入文件头
      CodecUtil.writeIndexHeader(vectorsStream, formatName, VERSION_CURRENT, si.getId(), segmentSuffix);
      assert CodecUtil.indexHeaderLength(formatName, segmentSuffix) == vectorsStream.getFilePointer();

      // 这个索引文件相当于是 tvd的索引
      indexWriter = new FieldsIndexWriter(directory, segment, segmentSuffix, VECTORS_INDEX_EXTENSION_PREFIX, VECTORS_INDEX_CODEC_NAME, si.getId(), blockShift, context);

      vectorsStream.writeVInt(PackedInts.VERSION_CURRENT);
      vectorsStream.writeVInt(chunkSize);
      // 这里创建一个采用差值存储的对象
      writer = new BlockPackedWriter(vectorsStream, PACKED_BLOCK_SIZE);

      positionsBuf = new int[1024];
      startOffsetsBuf = new int[1024];
      lengthsBuf = new int[1024];
      payloadLengthsBuf = new int[1024];

      success = true;
    } finally {
      // 文件创建失败时 抛出异常
      if (!success) {
        IOUtils.closeWhileHandlingException(vectorsStream, indexWriter, indexWriter);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(vectorsStream, indexWriter);
    } finally {
      vectorsStream = null;
      indexWriter = null;
    }
  }

  /**
   * 代表预备向 writer中写入数据  在writer中初始化一些结构对象
   * @param numVectorFields   代表该doc下有多少field
   * @throws IOException
   */
  @Override
  public void startDocument(int numVectorFields) throws IOException {
    curDoc = addDocData(numVectorFields);
  }

  /**
   * 当某个 doc内部所有的数据 都写完的时候 触发该方法
   * @throws IOException
   */
  @Override
  public void finishDocument() throws IOException {
    // append the payload bytes of the doc after its terms
    // 将 term携带的 payload信息写入到 termSuffixes中
    payloadBytes.copyTo(termSuffixes);
    // 每写完一个 payload 就重置
    payloadBytes.reset();
    ++numDocs;
    // 检测是否满足刷盘条件了
    if (triggerFlush()) {
      // 执行刷盘操作
      flush();
    }
    // 将此时正在处理的 docData 置空
    curDoc = null;
  }

  /**
   * 开始往DocData中写入某个fieldData的信息
   * @param info
   * @param numTerms  该field解析出了多少 term
   *                  下面3个变量代表需要存储term的哪些相关信息
   * @param positions
   * @param offsets
   * @param payloads
   * @throws IOException
   */
  @Override
  public void startField(FieldInfo info, int numTerms, boolean positions,
      boolean offsets, boolean payloads) throws IOException {
    curField = curDoc.addField(info.number, numTerms, positions, offsets, payloads);
    lastTerm.length = 0;
  }

  /**
   * 代表当前 field 的数据采集完了
   * @throws IOException
   */
  @Override
  public void finishField() throws IOException {
    curField = null;
  }

  /**
   * @param term  对应term内的数据体
   * @param freq  该term在field中出现的次数
   * @throws IOException
   */
  @Override
  public void startTerm(BytesRef term, int freq) throws IOException {
    assert freq >= 1;
    final int prefix;
    // 因为存入term时 在外层已经做过排序了 所以可以节省前缀的方式写入
    // 这里想要存储相同的前缀 以便节省空间
    if (lastTerm.length == 0) {
      // no previous term: no bytes to write
      prefix = 0;
    } else {
      prefix = StringHelper.bytesDifference(lastTerm, term);
    }
    curField.addTerm(freq, prefix, term.length - prefix);

    // 注意这里只写入后缀
    termSuffixes.writeBytes(term.bytes, term.offset + prefix, term.length - prefix);
    // copy last term
    if (lastTerm.bytes.length < term.length) {
      lastTerm.bytes = new byte[ArrayUtil.oversize(term.length, 1)];
    }
    lastTerm.offset = 0;
    lastTerm.length = term.length;
    // 更新 lastTerm的值
    System.arraycopy(term.bytes, term.offset, lastTerm.bytes, 0, term.length);
  }

  @Override
  public void addPosition(int position, int startOffset, int endOffset,
      BytesRef payload) throws IOException {
    assert curField.flags != 0;
    curField.addPosition(position, startOffset, endOffset - startOffset, payload == null ? 0 : payload.length);
    if (curField.hasPayloads && payload != null) {
      payloadBytes.writeBytes(payload.bytes, payload.offset, payload.length);
    }
  }

  /**
   * 检测此时是否满足刷盘条件
   * @return
   */
  private boolean triggerFlush() {
    // 此时存储的 后缀内容超过一定数量
    return termSuffixes.size() >= chunkSize
            // 代表悬置了太多doc
        || pendingDocs.size() >= MAX_DOCUMENTS_PER_CHUNK;
  }

  /**
   * 将格式化好的 数据写入到索引文件中
   * @throws IOException
   */
  private void flush() throws IOException {
    final int chunkDocs = pendingDocs.size();
    assert chunkDocs > 0 : chunkDocs;

    // write the index file
    // 这里记录了一些临时信息
    indexWriter.writeIndex(chunkDocs, vectorsStream.getFilePointer());

    // 代表之前已经有多少 doc flush了
    final int docBase = numDocs - chunkDocs;
    // 这里将数据写入到索引文件  先存储 之前有多少doc 写入 之后存储 本次要写入的doc
    vectorsStream.writeVInt(docBase);
    vectorsStream.writeVInt(chunkDocs);

    // total number of fields of the chunk
    // 读取本次要写入的所有 doc 对应的 fieldNum
    final int totalFields = flushNumFields(chunkDocs);

    // 开始写入域有关的信息
    if (totalFields > 0) {
      // unique field numbers (sorted)
      // 将所有field 按照 fieldNum 去重并排序后 写入到 vectorsStream  并返回
      final int[] fieldNums = flushFieldNums();
      // offsets in the array of unique field numbers
      // 这里写入 通过 field号码 找到 field数量的数组的下标
      flushFields(totalFields, fieldNums);
      // flags (does the field have positions, offsets, payloads?)
      // 写入 flag 信息
      flushFlags(totalFields, fieldNums);
      // number of terms of each field
      // 写入每个  termNum 注意跟上面的 flushFieldNums 是不同的
      flushNumTerms(totalFields);
      // prefix and suffix lengths for each field
      // 记录每个 term的长度
      flushTermLengths();
      // term freqs - 1 (because termFreq is always >=1) for each term


      // 存储 term相关的信息 freq/position/offset/payload
      flushTermFreqs();
      // positions for all terms, when enabled
      flushPositions();
      // offsets for all terms, when enabled
      flushOffsets(fieldNums);
      // payload lengths for all terms, when enabled
      // 写入payload的长度
      flushPayloadLengths();

      // compress terms and payloads and write them to the output
      //
      // TODO: We could compress in the slices we already have in the buffer (min/max slice
      // can be set on the buffer itself).
      // 取出后缀数据
      byte[] content = termSuffixes.toArrayCopy();
      // 将后缀压缩后写入到 output中
      compressor.compress(content, 0, content.length, vectorsStream);
    }

    // reset
    pendingDocs.clear();
    curDoc = null;
    curField = null;
    termSuffixes.reset();
    numChunks++;
  }

  /**
   * 写入 field 总数
   * @param chunkDocs
   * @return
   * @throws IOException
   */
  private int flushNumFields(int chunkDocs) throws IOException {
    if (chunkDocs == 1) {
      final int numFields = pendingDocs.getFirst().numFields;
      vectorsStream.writeVInt(numFields);
      return numFields;
    } else {
      writer.reset(vectorsStream);
      int totalFields = 0;
      for (DocData dd : pendingDocs) {
        writer.add(dd.numFields);
        totalFields += dd.numFields;
      }
      writer.finish();
      return totalFields;
    }
  }

  /** Returns a sorted array containing unique field numbers */
  /**
   * 将 fieldNum 按从小到大的顺序存储到 int[] 中
   * @return
   * @throws IOException
   */
  private int[] flushFieldNums() throws IOException {
    // 这里要去重 因为 多个doc 可以存储 相同fieldNum 的 field
    SortedSet<Integer> fieldNums = new TreeSet<>();
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        fieldNums.add(fd.fieldNum);
      }
    }

    final int numDistinctFields = fieldNums.size();
    assert numDistinctFields > 0;
    // 最大的数占用的位肯定最多
    final int bitsRequired = PackedInts.bitsRequired(fieldNums.last());

    // 这里是这样 首先 bitsRequired 最大为 31 也就是2的5次  如果field 本身的值比较小 那么可以将他们压缩成一个byte   如果field的值 超过了剩余的3位能表示的值 8
    // 那么只能额外使用一个VInt  来记录 field真正的数量
    // token值的解析 看 CompressingTermVectorsReader       final int token = vectorsStream.readByte() & 0xFF;   的逻辑
    // 7 需要3位 加上这里的  << 5 代表这个token 最多只占 8 位  并且最低位代表 每个数据使用多少 bit 来存储
    final int token = (Math.min(numDistinctFields - 1, 0x07) << 5) | bitsRequired;
    vectorsStream.writeByte((byte) token);

    if (numDistinctFields - 1 >= 0x07) {
      vectorsStream.writeVInt(numDistinctFields - 1 - 0x07);
    }
    // 这里将 int 值转换成 byte 值存储到vectorsStream中
    final PackedInts.Writer writer = PackedInts.getWriterNoHeader(vectorsStream, PackedInts.Format.PACKED, fieldNums.size(), bitsRequired, 1);
    for (Integer fieldNum : fieldNums) {
      // 这里存储的是 去重后 且  已经排序后的 field 号码
      writer.add(fieldNum);
    }
    writer.finish();

    int[] fns = new int[fieldNums.size()];
    int i = 0;
    for (Integer key : fieldNums) {
      fns[i++] = key;
    }
    return fns;
  }

  /**
   * @param totalFields
   * @param fieldNums  每次写入索引文件都是多个 doc    该数组是按照 每个 field的数量排序的  然后存储的是 fieldNum
   * @throws IOException
   */
  private void flushFields(int totalFields, int[] fieldNums) throws IOException {
    final PackedInts.Writer writer = PackedInts.getWriterNoHeader(vectorsStream, PackedInts.Format.PACKED, totalFields, PackedInts.bitsRequired(fieldNums.length - 1), 1);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        //  返回所在的下标 并写入
        final int fieldNumIndex = Arrays.binarySearch(fieldNums, fd.fieldNum);
        assert fieldNumIndex >= 0;
        writer.add(fieldNumIndex);
      }
    }
    writer.finish();
  }

  /**
   * 这里写入 存储的索引数据携带了  选项
   * @param totalFields
   * @param fieldNums  按号码大小排序的数组
   * @throws IOException
   */
  private void flushFlags(int totalFields, int[] fieldNums) throws IOException {
    // check if fields always have the same flags
    boolean nonChangingFlags = true;
    // 按照 fieldNums 的顺序 记录 flag 信息
    int[] fieldFlags = new int[fieldNums.length];
    Arrays.fill(fieldFlags, -1);
    outer:
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        final int fieldNumOff = Arrays.binarySearch(fieldNums, fd.fieldNum);
        assert fieldNumOff >= 0;
        // 代表还未设置  进行设置
        if (fieldFlags[fieldNumOff] == -1) {
          fieldFlags[fieldNumOff] = fd.flags;
          // 代表在多个文档中可能会出现相同的 field  并且他们的 flag发生了变化  应该是不允许的
        } else if (fieldFlags[fieldNumOff] != fd.flags) {
          nonChangingFlags = false;
          break outer;
        }
      }
    }

    if (nonChangingFlags) {
      // write one flag per field num
      // 写入0 代表 相同号码的field 的 flag 是相同的
      vectorsStream.writeVInt(0);
      final PackedInts.Writer writer = PackedInts.getWriterNoHeader(vectorsStream, PackedInts.Format.PACKED, fieldFlags.length, FLAGS_BITS, 1);
      for (int flags : fieldFlags) {
        assert flags >= 0;
        writer.add(flags);
      }
      assert writer.ord() == fieldFlags.length - 1;
      writer.finish();
    } else {
      // write one flag for every field instance
      // 代表 相同field的 flag 出现了不同的情况  所以还是将每个flag 单独写入
      vectorsStream.writeVInt(1);
      final PackedInts.Writer writer = PackedInts.getWriterNoHeader(vectorsStream, PackedInts.Format.PACKED, totalFields, FLAGS_BITS, 1);
      for (DocData dd : pendingDocs) {
        for (FieldData fd : dd.fields) {
          writer.add(fd.flags);
        }
      }
      assert writer.ord() == totalFields - 1;
      writer.finish();
    }
  }

  /**
   * 写入 termNum
   * @param totalFields
   * @throws IOException
   */
  private void flushNumTerms(int totalFields) throws IOException {
    int maxNumTerms = 0;
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        maxNumTerms |= fd.numTerms;
      }
    }
    final int bitsRequired = PackedInts.bitsRequired(maxNumTerms);
    // 标记之后每多少位 是一个数值
    vectorsStream.writeVInt(bitsRequired);
    final PackedInts.Writer writer = PackedInts.getWriterNoHeader(
        vectorsStream, PackedInts.Format.PACKED, totalFields, bitsRequired, 1);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        writer.add(fd.numTerms);
      }
    }
    assert writer.ord() == totalFields - 1;
    writer.finish();
  }

  /**
   * 写入每个 term 与上个 term的相同前缀长度 以及后缀长度
   * @throws IOException
   */
  private void flushTermLengths() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        for (int i = 0; i < fd.numTerms; ++i) {
          writer.add(fd.prefixLengths[i]);
        }
      }
    }
    writer.finish();
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        for (int i = 0; i < fd.numTerms; ++i) {
          writer.add(fd.suffixLengths[i]);
        }
      }
    }
    writer.finish();
  }

  private void flushTermFreqs() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        for (int i = 0; i < fd.numTerms; ++i) {
          // TODO 这里 频率减了 1
          writer.add(fd.freqs[i] - 1);
        }
      }
    }
    writer.finish();
  }

  /**
   * 写入 position 信息
   * @throws IOException
   */
  private void flushPositions() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        if (fd.hasPositions) {
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) {
            int previousPosition = 0;
            // 当某个词 出现了多次时   果然同一个词的position存放是连续的
            for (int j = 0; j < fd.freqs[i]; ++j) {
              final int position = positionsBuf[fd .posStart + pos++];
              // 差值存储
              writer.add(position - previousPosition);
              previousPosition = position;
            }
          }
          assert pos == fd.totalPositions;
        }
      }
    }
    writer.finish();
  }

  /**
   * 写入每个词的偏移量信息
   * @param fieldNums
   * @throws IOException
   */
  private void flushOffsets(int[] fieldNums) throws IOException {
    boolean hasOffsets = false;
    long[] sumPos = new long[fieldNums.length];
    long[] sumOffsets = new long[fieldNums.length];
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        // 只要有一个 field 包含 offset 信息 设置该标识为true
        hasOffsets |= fd.hasOffsets;
        if (fd.hasOffsets && fd.hasPositions) {
          final int fieldNumOff = Arrays.binarySearch(fieldNums, fd.fieldNum);
          int pos = 0;
          // TODO 需要之后再捋一下
          for (int i = 0; i < fd.numTerms; ++i) {
            // 这里只取 某个词最后一次出现的post 的总和
            sumPos[fieldNumOff] += positionsBuf[fd.posStart + fd.freqs[i]-1 + pos];
            sumOffsets[fieldNumOff] += startOffsetsBuf[fd.offStart + fd.freqs[i]-1 + pos];
            pos += fd.freqs[i];
          }
          assert pos == fd.totalPositions;
        }
      }
    }

    // 所有field 都没有存储 offset 信息 直接返回
    if (!hasOffsets) {
      // nothing to do
      return;
    }

    // 这里存储的是比率  md我到现在还不知道 啥是 position 啥是 offset
    final float[] charsPerTerm = new float[fieldNums.length];
    for (int i = 0; i < fieldNums.length; ++i) {
      charsPerTerm[i] = (sumPos[i] <= 0 || sumOffsets[i] <= 0) ? 0 : (float) ((double) sumOffsets[i] / sumPos[i]);
    }

    // start offsets
    for (int i = 0; i < fieldNums.length; ++i) {
      vectorsStream.writeInt(Float.floatToRawIntBits(charsPerTerm[i]));
    }

    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        // 如果包含了 offset信息  这里准备存储 offset 信息
        if ((fd.flags & OFFSETS) != 0) {
          final int fieldNumOff = Arrays.binarySearch(fieldNums, fd.fieldNum);
          final float cpt = charsPerTerm[fieldNumOff];
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) {
            int previousPos = 0;
            int previousOff = 0;
            for (int j = 0; j < fd.freqs[i]; ++j) {
              final int position = fd.hasPositions ? positionsBuf[fd.posStart + pos] : 0;
              final int startOffset = startOffsetsBuf[fd.offStart + pos];
              // TODO 这是神马 ???
              writer.add(startOffset - previousOff - (int) (cpt * (position - previousPos)));
              previousPos = position;
              previousOff = startOffset;
              ++pos;
            }
          }
        }
      }
    }
    writer.finish();

    // lengths
    // 这里写入长度
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        if ((fd.flags & OFFSETS) != 0) {
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) {
            for (int j = 0; j < fd.freqs[i]; ++j) {
              writer.add(lengthsBuf[fd.offStart + pos++] - fd.prefixLengths[i] - fd.suffixLengths[i]);
            }
          }
          assert pos == fd.totalPositions;
        }
      }
    }
    writer.finish();
  }

  /**
   * 写入 payload 的 长度
   * @throws IOException
   */
  private void flushPayloadLengths() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        if (fd.hasPayloads) {
          for (int i = 0; i < fd.totalPositions; ++i) {
            writer.add(payloadLengthsBuf[fd.payStart + i]);
          }
        }
      }
    }
    writer.finish();
  }

  /**
   * 代表本次的 doc 解析工作已结束   记录本次总计处理了多少doc
   * @param fis
   * @param numDocs
   * @throws IOException
   */
  @Override
  public void finish(FieldInfos fis, int numDocs) throws IOException {
    if (!pendingDocs.isEmpty()) {
      // 如果此时还有未刷盘的 docData 那么先处理
      flush();
      numDirtyChunks++; // incomplete: we had to force this flush
    }
    // 代表出现了异常
    if (numDocs != this.numDocs) {
      throw new RuntimeException("Wrote " + this.numDocs + " docs, finish called with numDocs=" + numDocs);
    }
    // 生成元数据文件
    indexWriter.finish(numDocs, vectorsStream.getFilePointer());
    // 记录最后的信息 并且写入校验和
    vectorsStream.writeVLong(numChunks);
    vectorsStream.writeVLong(numDirtyChunks);
    CodecUtil.writeFooter(vectorsStream);
  }

  /**
   * 写入term相关的向量信息
   * @param numProx
   * @param positions   存储了 position信息
   * @param offsets   存储了offset信息
   * @throws IOException
   */
  @Override
  public void addProx(int numProx, DataInput positions, DataInput offsets)
      throws IOException {
    assert (curField.hasPositions) == (positions != null);
    assert (curField.hasOffsets) == (offsets != null);

    // 如果field 要求记录 position信息
    if (curField.hasPositions) {
      final int posStart = curField.posStart + curField.totalPositions;
      // 扩容
      if (posStart + numProx > positionsBuf.length) {
        positionsBuf = ArrayUtil.grow(positionsBuf, posStart + numProx);
      }
      int position = 0;
      if (curField.hasPayloads) {
        final int payStart = curField.payStart + curField.totalPositions;
        if (payStart + numProx > payloadLengthsBuf.length) {
          payloadLengthsBuf = ArrayUtil.grow(payloadLengthsBuf, payStart + numProx);
        }
        // 先写入 payload 信息
        // 同一个 term 会按照出现的频率 连续存储
        for (int i = 0; i < numProx; ++i) {
          final int code = positions.readVInt();
          // 最低位不为0 代表包含 payload 信息
          if ((code & 1) != 0) {
            // This position has a payload
            // 下一个值就是长度  这里的逻辑完全对应 TermVectorsConsumerPerField.writeProx
            final int payloadLength = positions.readVInt();
            payloadLengthsBuf[payStart + i] = payloadLength;
            payloadBytes.copyBytes(positions, payloadLength);
          } else {
            // 这里代表payload 为空
            payloadLengthsBuf[payStart + i] = 0;
          }
          // 真正的位置信息 实际上要 右移一位  注意这个position 可能就是差值存储的  所以这里开始叠加
          position += code >>> 1;
          positionsBuf[posStart + i] = position;
        }
      } else {
        for (int i = 0; i < numProx; ++i) {
          position += (positions.readVInt() >>> 1);
          positionsBuf[posStart + i] = position;
        }
      }
    }

    // 这里是存储 offset 的信息
    if (curField.hasOffsets) {
      // 应该也是差值存储  所以 需要 += 进行还原
      final int offStart = curField.offStart + curField.totalPositions;
      if (offStart + numProx > startOffsetsBuf.length) {
        final int newLength = ArrayUtil.oversize(offStart + numProx, 4);
        startOffsetsBuf = ArrayUtil.growExact(startOffsetsBuf, newLength);
        lengthsBuf = ArrayUtil.growExact(lengthsBuf, newLength);
      }
      int lastOffset = 0, startOffset, endOffset;
      for (int i = 0; i < numProx; ++i) {
        startOffset = lastOffset + offsets.readVInt();
        endOffset = startOffset + offsets.readVInt();
        lastOffset = endOffset;
        startOffsetsBuf[offStart + i] = startOffset;
        lengthsBuf[offStart + i] = endOffset - startOffset;
      }
    }

    curField.totalPositions += numProx;
  }
  
  // bulk merge is scary: its caused corruption bugs in the past.
  // we try to be extra safe with this impl, but add an escape hatch to
  // have a workaround for undiscovered bugs.
  static final String BULK_MERGE_ENABLED_SYSPROP = CompressingTermVectorsWriter.class.getName() + ".enableBulkMerge";
  static final boolean BULK_MERGE_ENABLED;
  static {
    boolean v = true;
    try {
      v = Boolean.parseBoolean(System.getProperty(BULK_MERGE_ENABLED_SYSPROP, "true"));
    } catch (SecurityException ignored) {}
    BULK_MERGE_ENABLED = v;
  }

  @Override
  public int merge(MergeState mergeState) throws IOException {
    if (mergeState.needsIndexSort) {
      // TODO: can we gain back some optos even if index is sorted?  E.g. if sort results in large chunks of contiguous docs from one sub
      // being copied over...?
      return super.merge(mergeState);
    }
    int docCount = 0;
    int numReaders = mergeState.maxDocs.length;

    MatchingReaders matching = new MatchingReaders(mergeState);
    
    for (int readerIndex=0;readerIndex<numReaders;readerIndex++) {
      CompressingTermVectorsReader matchingVectorsReader = null;
      final TermVectorsReader vectorsReader = mergeState.termVectorsReaders[readerIndex];
      if (matching.matchingReaders[readerIndex]) {
        // we can only bulk-copy if the matching reader is also a CompressingTermVectorsReader
        if (vectorsReader != null && vectorsReader instanceof CompressingTermVectorsReader) {
          matchingVectorsReader = (CompressingTermVectorsReader) vectorsReader;
        }
      }

      final int maxDoc = mergeState.maxDocs[readerIndex];
      final Bits liveDocs = mergeState.liveDocs[readerIndex];
      
      if (matchingVectorsReader != null &&
          matchingVectorsReader.getCompressionMode() == compressionMode &&
          matchingVectorsReader.getChunkSize() == chunkSize &&
          matchingVectorsReader.getVersion() == VERSION_CURRENT && 
          matchingVectorsReader.getPackedIntsVersion() == PackedInts.VERSION_CURRENT &&
          BULK_MERGE_ENABLED &&
          liveDocs == null &&
          !tooDirty(matchingVectorsReader)) {
        // optimized merge, raw byte copy
        // its not worth fine-graining this if there are deletions.
        
        matchingVectorsReader.checkIntegrity();
        
        // flush any pending chunks
        if (!pendingDocs.isEmpty()) {
          flush();
          numDirtyChunks++; // incomplete: we had to force this flush
        }
        
        // iterate over each chunk. we use the vectors index to find chunk boundaries,
        // read the docstart + doccount from the chunk header (we write a new header, since doc numbers will change),
        // and just copy the bytes directly.
        IndexInput rawDocs = matchingVectorsReader.getVectorsStream();
        FieldsIndex index = matchingVectorsReader.getIndexReader();
        rawDocs.seek(index.getStartPointer(0));
        int docID = 0;
        while (docID < maxDoc) {
          // read header
          int base = rawDocs.readVInt();
          if (base != docID) {
            throw new CorruptIndexException("invalid state: base=" + base + ", docID=" + docID, rawDocs);
          }
          int bufferedDocs = rawDocs.readVInt();
          
          // write a new index entry and new header for this chunk.
          indexWriter.writeIndex(bufferedDocs, vectorsStream.getFilePointer());
          vectorsStream.writeVInt(docCount); // rebase
          vectorsStream.writeVInt(bufferedDocs);
          docID += bufferedDocs;
          docCount += bufferedDocs;
          numDocs += bufferedDocs;
          
          if (docID > maxDoc) {
            throw new CorruptIndexException("invalid state: base=" + base + ", count=" + bufferedDocs + ", maxDoc=" + maxDoc, rawDocs);
          }
          
          // copy bytes until the next chunk boundary (or end of chunk data).
          // using the stored fields index for this isn't the most efficient, but fast enough
          // and is a source of redundancy for detecting bad things.
          final long end;
          if (docID == maxDoc) {
            end = matchingVectorsReader.getMaxPointer();
          } else {
            end = index.getStartPointer(docID);
          }
          vectorsStream.copyBytes(rawDocs, end - rawDocs.getFilePointer());
        }
               
        if (rawDocs.getFilePointer() != matchingVectorsReader.getMaxPointer()) {
          throw new CorruptIndexException("invalid state: pos=" + rawDocs.getFilePointer() + ", max=" + matchingVectorsReader.getMaxPointer(), rawDocs);
        }
        
        // since we bulk merged all chunks, we inherit any dirty ones from this segment.
        numChunks += matchingVectorsReader.getNumChunks();
        numDirtyChunks += matchingVectorsReader.getNumDirtyChunks();
      } else {        
        // naive merge...
        if (vectorsReader != null) {
          vectorsReader.checkIntegrity();
        }
        for (int i = 0; i < maxDoc; i++) {
          if (liveDocs != null && liveDocs.get(i) == false) {
            continue;
          }
          Fields vectors;
          if (vectorsReader == null) {
            vectors = null;
          } else {
            vectors = vectorsReader.get(i);
          }
          addAllDocVectors(vectors, mergeState);
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
   */
  boolean tooDirty(CompressingTermVectorsReader candidate) {
    // more than 1% dirty, or more than hard limit of 1024 dirty chunks
    return candidate.getNumDirtyChunks() > 1024 || 
           candidate.getNumDirtyChunks() * 100 > candidate.getNumChunks();
  }
}
