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


import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.packed.BlockPackedReaderIterator;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.VERSION_OFFHEAP_INDEX;
import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.FLAGS_BITS;
import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.OFFSETS;
import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.PACKED_BLOCK_SIZE;
import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.PAYLOADS;
import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.POSITIONS;
import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.VECTORS_EXTENSION;
import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.VECTORS_INDEX_CODEC_NAME;
import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.VECTORS_INDEX_EXTENSION_PREFIX;
import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.VERSION_CURRENT;
import static org.apache.lucene.codecs.compressing.CompressingTermVectorsWriter.VERSION_START;

/**
 * {@link TermVectorsReader} for {@link CompressingTermVectorsFormat}.
 * @lucene.experimental
 * 该对象 对应CompressingTermVectorsWriter
 */
public final class CompressingTermVectorsReader extends TermVectorsReader implements Closeable {

  private final FieldInfos fieldInfos;
  /**
   * 对应元数据存储文件
   */
  final FieldsIndex indexReader;
  /**
   * 对应 存储词向量的索引文件
   */
  final IndexInput vectorsStream;
  private final int version;
  private final int packedIntsVersion;
  private final CompressionMode compressionMode;
  private final Decompressor decompressor;
  private final int chunkSize;
  private final int numDocs;
  private boolean closed;
  private final BlockPackedReaderIterator reader;
  private final long numChunks; // number of compressed blocks written
  private final long numDirtyChunks; // number of incomplete compressed blocks written
  private final long maxPointer; // end of the data section

  // used by clone
  private CompressingTermVectorsReader(CompressingTermVectorsReader reader) {
    this.fieldInfos = reader.fieldInfos;
    this.vectorsStream = reader.vectorsStream.clone();
    this.indexReader = reader.indexReader.clone();
    this.packedIntsVersion = reader.packedIntsVersion;
    this.compressionMode = reader.compressionMode;
    this.decompressor = reader.decompressor.clone();
    this.chunkSize = reader.chunkSize;
    this.numDocs = reader.numDocs;
    this.reader = new BlockPackedReaderIterator(vectorsStream, packedIntsVersion, PACKED_BLOCK_SIZE, 0);
    this.version = reader.version;
    this.numChunks = reader.numChunks;
    this.numDirtyChunks = reader.numDirtyChunks;
    this.maxPointer = reader.maxPointer;
    this.closed = false;
  }

  /**
   * Sole constructor
   * 先通过 元数据文件 读取对应索引文件相关数据的偏移量 以及 doc数据 等等信息
   */
  public CompressingTermVectorsReader(Directory d, SegmentInfo si, String segmentSuffix, FieldInfos fn,
      IOContext context, String formatName, CompressionMode compressionMode) throws IOException {
    this.compressionMode = compressionMode;
    final String segment = si.name;
    boolean success = false;
    fieldInfos = fn;
    numDocs = si.maxDoc();

    try {
      // Open the data file and read metadata
      final String vectorsStreamFN = IndexFileNames.segmentFileName(segment, segmentSuffix, VECTORS_EXTENSION);
      vectorsStream = d.openInput(vectorsStreamFN, context);
      version = CodecUtil.checkIndexHeader(vectorsStream, formatName, VERSION_START, VERSION_CURRENT, si.getId(), segmentSuffix);
      assert CodecUtil.indexHeaderLength(formatName, segmentSuffix) == vectorsStream.getFilePointer();

      FieldsIndex indexReader = null;
      long maxPointer = -1;

      if (version < VERSION_OFFHEAP_INDEX) {
        // Load the index into memory
        // 兼容旧版本的 先忽略
        final String indexName = IndexFileNames.segmentFileName(segment, segmentSuffix, "tvx");
        try (ChecksumIndexInput indexStream = d.openChecksumInput(indexName, context)) {
          Throwable priorE = null;
          try {
            assert formatName.endsWith("Data");
            final String codecNameIdx = formatName.substring(0, formatName.length() - "Data".length()) + "Index";
            final int version2 = CodecUtil.checkIndexHeader(indexStream, codecNameIdx, VERSION_START, VERSION_CURRENT, si.getId(), segmentSuffix);
            if (version != version2) {
              throw new CorruptIndexException("Version mismatch between stored fields index and data: " + version + " != " + version2, indexStream);
            }
            assert CodecUtil.indexHeaderLength(codecNameIdx, segmentSuffix) == indexStream.getFilePointer();
            indexReader = new LegacyFieldsIndexReader(indexStream, si);
            maxPointer = indexStream.readVLong(); // the end of the data section
          } catch (Throwable exception) {
            priorE = exception;
          } finally {
            CodecUtil.checkFooter(indexStream, priorE);
          }
        }
      } else {
        FieldsIndexReader fieldsIndexReader = new FieldsIndexReader(d, si.name, segmentSuffix, VECTORS_INDEX_EXTENSION_PREFIX, VECTORS_INDEX_CODEC_NAME, si.getId());
        indexReader = fieldsIndexReader;
        maxPointer = fieldsIndexReader.getMaxPointer();
      }
      // 该对象在初始化的时候 会根据拓展名前缀 和 segmentName 找到 m 文件 和 x 文件
      this.indexReader = indexReader;
      // 索引文件最后的偏移量
      this.maxPointer = maxPointer;

      // 获取索引文件当前的偏移量   主要用于复位
      long pos = vectorsStream.getFilePointer();
      // 这是当前元数据文件记录的索引文件最后的偏移量    因为在写入元数据文件之后 还会对索引文件追加几个数据
      vectorsStream.seek(maxPointer);
      numChunks = vectorsStream.readVLong();
      numDirtyChunks = vectorsStream.readVLong();
      if (numDirtyChunks > numChunks) {
        throw new CorruptIndexException("invalid chunk counts: dirty=" + numDirtyChunks + ", total=" + numChunks, vectorsStream);
      }

      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(vectorsStream);
      vectorsStream.seek(pos);

      packedIntsVersion = vectorsStream.readVInt();
      chunkSize = vectorsStream.readVInt();
      decompressor = compressionMode.newDecompressor();
      // 词向量对象内部的数据 没有按位存储 只是使用了 VInt 之类的自定义变量
      this.reader = new BlockPackedReaderIterator(vectorsStream, packedIntsVersion, PACKED_BLOCK_SIZE, 0);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  CompressionMode getCompressionMode() {
    return compressionMode;
  }

  int getChunkSize() {
    return chunkSize;
  }

  int getPackedIntsVersion() {
    return packedIntsVersion;
  }
  
  int getVersion() {
    return version;
  }

  FieldsIndex getIndexReader() {
    return indexReader;
  }

  IndexInput getVectorsStream() {
    return vectorsStream;
  }
  
  long getMaxPointer() {
    return maxPointer;
  }
  
  long getNumChunks() {
    return numChunks;
  }
  
  long getNumDirtyChunks() {
    return numDirtyChunks;
  }

  /**
   * @throws AlreadyClosedException if this TermVectorsReader is closed
   */
  private void ensureOpen() throws AlreadyClosedException {
    if (closed) {
      throw new AlreadyClosedException("this FieldsReader is closed");
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      IOUtils.close(indexReader, vectorsStream);
      closed = true;
    }
  }

  @Override
  public TermVectorsReader clone() {
    return new CompressingTermVectorsReader(this);
  }

  /**
   * 通过传入一个 docId  将doc 内部的词向量信息读取到内存中
   * @param doc
   * @return
   * @throws IOException
   */
  @Override
  public Fields get(int doc) throws IOException {
    ensureOpen();

    // seek to the right place
    {
      // 找到doc 对应的起始偏移量
      final long startPointer = indexReader.getStartPointer(doc);
      // 定位索引文件的光标
      vectorsStream.seek(startPointer);
    }

    // decode
    // - docBase: first doc ID of the chunk
    // - chunkDocs: number of docs of the chunk
    // 记录本批doc 是从哪个docId 开始的
    final int docBase = vectorsStream.readVInt();
    // 本批数据有多少 doc
    final int chunkDocs = vectorsStream.readVInt();
    if (doc < docBase || doc >= docBase + chunkDocs || docBase + chunkDocs > numDocs) {
      throw new CorruptIndexException("docBase=" + docBase + ",chunkDocs=" + chunkDocs + ",doc=" + doc, vectorsStream);
    }

    // 第一个要解析的数据是 当前doc 下有多少 field
    final int skip; // number of fields to skip
    final int numFields; // number of fields of the document we're looking for
    final int totalFields; // total number of fields of the chunk (sum for all docs)
    // 如果当前doc 只有一个 那么直接读取一个Vint就好 它就代表有多少field
    if (chunkDocs == 1) {
      skip = 0;
      numFields = totalFields = vectorsStream.readVInt();
    } else {
      // 这里的数据 都是按位写入  (内部还会涉及到期望值 和 min的计算)
      reader.reset(vectorsStream, chunkDocs);
      int sum = 0;
      // 这里分3次读取 第一次读取的是本批 docId 小于 查询的目标id 的数据
      // 第二次刚好查询目标id 对应的数据
      // 第三次查询的数据是 超过id 的数据
      for (int i = docBase; i < doc; ++i) {
        // 读取出来的每个值 都是某个doc 下包含的field数量
        sum += reader.next();
      }
      skip = sum;
      numFields = (int) reader.next();
      sum += numFields;
      for (int i = doc + 1; i < docBase + chunkDocs; ++i) {
        sum += reader.next();
      }
      totalFields = sum;
    }

    // 代表该doc下刚好没有任何field 数据 那么就没有词向量数据
    if (numFields == 0) {
      // no vectors
      return null;
    }

    // read field numbers that have term vectors
    // 这段逻辑存储的是本批doc 下所有的field 在去重后的 fieldNum信息
    final int[] fieldNums;
    {
      // 从 writer的逻辑中可以看到  token 最多只占 8 位
      final int token = vectorsStream.readByte() & 0xFF;
      assert token != 0; // means no term vectors, cannot happen since we checked for numFields == 0
      // 最低位 记录这些 fieldNum 最多需要多少位来存储
      final int bitsPerFieldNum = token & 0x1F;
      int totalDistinctFields = token >>> 5;
      // 代表fieldNum 的长度超过了 7 就需要额外读取一个VInt值 并将这个值 + 7
      if (totalDistinctFields == 0x07) {
        totalDistinctFields += vectorsStream.readVInt();
      }
      ++totalDistinctFields;
      // 这里将值读取出来  并存储到 对应的数组中
      final PackedInts.ReaderIterator it = PackedInts.getReaderIteratorNoHeader(vectorsStream, PackedInts.Format.PACKED, packedIntsVersion, totalDistinctFields, bitsPerFieldNum, 1);
      fieldNums = new int[totalDistinctFields];
      for (int i = 0; i < totalDistinctFields; ++i) {
        fieldNums[i] = (int) it.next();
      }
    }

    // read field numbers and flags
    final int[] fieldNumOffs = new int[numFields];
    final PackedInts.Reader flags;
    {
      // 这里是下标 占用多少位
      final int bitsPerOff = PackedInts.bitsRequired(fieldNums.length - 1);

      // 上面相当于存储的是唯一的 field数据体  这里按照doc 下所有携带的 fieldNum 找到上面对应数据体所在的下标  这样就可以节省空间了
      // 这里只占当前这个 doc 下所有 field 在 fieldNums[] 中对应的下标
      // PackedInts.getReaderNoHeader 这个对象是先从 input 中读取一块数据到内存 之后根据 下标 通过每个值需要的bit 推算出起点 然后读取值
      final PackedInts.Reader allFieldNumOffs = PackedInts.getReaderNoHeader(vectorsStream, PackedInts.Format.PACKED, packedIntsVersion, totalFields, bitsPerOff);
      // 这里是判断  相同fieldNum 对应的 field的 flag 是否完全相同
      switch (vectorsStream.readVInt()) {
        case 0:
          // 代表所有 fieldNum 相同的 field  flag 完全相同  那么写入flag 的顺序与 fieldNums[] 一样
          final PackedInts.Reader fieldFlags = PackedInts.getReaderNoHeader(vectorsStream, PackedInts.Format.PACKED, packedIntsVersion, fieldNums.length, FLAGS_BITS);
          PackedInts.Mutable f = PackedInts.getMutable(totalFields, FLAGS_BITS, PackedInts.COMPACT);
          for (int i = 0; i < totalFields; ++i) {
            // 找到对应的field 在 fieldNums[] 的下标
            final int fieldNumOff = (int) allFieldNumOffs.get(i);
            assert fieldNumOff >= 0 && fieldNumOff < fieldNums.length;
            // 这里就读取对应的值
            final int fgs = (int) fieldFlags.get(fieldNumOff);
            // 将结果又写入压缩的结构中
            f.set(i, fgs);
          }
          flags = f;
          break;
        case 1:
          // 这种情况就是 每个field (不去重) 对应一个  flag
          flags = PackedInts.getReaderNoHeader(vectorsStream, PackedInts.Format.PACKED, packedIntsVersion, totalFields, FLAGS_BITS);
          break;
        default:
          throw new AssertionError();
      }
      // 开始读取 该doc 下所有 field 对应的 fieldNum 在 fieldNums[] 的下标
      for (int i = 0; i < numFields; ++i) {
        fieldNumOffs[i] = (int) allFieldNumOffs.get(skip + i);
      }
    }

    // number of terms per field for all fields
    // 这里读取 term 相关的信息  每个field 下term数量
    final PackedInts.Reader numTerms;
    final int totalTerms;
    {
      final int bitsRequired = vectorsStream.readVInt();
      numTerms = PackedInts.getReaderNoHeader(vectorsStream, PackedInts.Format.PACKED, packedIntsVersion, totalFields, bitsRequired);
      int sum = 0;
      for (int i = 0; i < totalFields; ++i) {
        sum += numTerms.get(i);
      }
      totalTerms = sum;
    }

    // term lengths
    int docOff = 0, docLen = 0, totalLen;
    final int[] fieldLengths = new int[numFields];
    final int[][] prefixLengths = new int[numFields][];
    final int[][] suffixLengths = new int[numFields][];
    {
      // 代表 重新从input 中读取对应的长度 用于填充内存的数据
      reader.reset(vectorsStream, totalTerms);
      // skip
      // 读取每个term的前缀长度
      int toSkip = 0;
      for (int i = 0; i < skip; ++i) {
        toSkip += numTerms.get(i);
      }
      reader.skip(toSkip);
      // read prefix lengths
      // 通过读取前面的数据 已经知道了某个 term下有多少field 这里读取对应数量的值 每个值内存储了 前缀长度和后缀长度
      // numField 代表 doc下有多少 field   numTerms 代表 field 下有多少 term
      for (int i = 0; i < numFields; ++i) {
        // 直接定位到目标 field 下有多少term
        final int termCount = (int) numTerms.get(skip + i);
        final int[] fieldPrefixLengths = new int[termCount];
        prefixLengths[i] = fieldPrefixLengths;
        for (int j = 0; j < termCount; ) {
          final LongsRef next = reader.next(termCount - j);
          for (int k = 0; k < next.length; ++k) {
            fieldPrefixLengths[j++] = (int) next.longs[next.offset + k];
          }
        }
      }
      // TODO  不细看了  核心就是将写入到索引文件的数据 重新读取到内存中
      reader.skip(totalTerms - reader.ord());

      reader.reset(vectorsStream, totalTerms);
      // skip
      toSkip = 0;
      for (int i = 0; i < skip; ++i) {
        for (int j = 0; j < numTerms.get(i); ++j) {
          docOff += reader.next();
        }
      }
      for (int i = 0; i < numFields; ++i) {
        final int termCount = (int) numTerms.get(skip + i);
        final int[] fieldSuffixLengths = new int[termCount];
        suffixLengths[i] = fieldSuffixLengths;
        for (int j = 0; j < termCount; ) {
          final LongsRef next = reader.next(termCount - j);
          for (int k = 0; k < next.length; ++k) {
            fieldSuffixLengths[j++] = (int) next.longs[next.offset + k];
          }
        }
        fieldLengths[i] = sum(suffixLengths[i]);
        docLen += fieldLengths[i];
      }
      totalLen = docOff + docLen;
      for (int i = skip + numFields; i < totalFields; ++i) {
        for (int j = 0; j < numTerms.get(i); ++j) {
          totalLen += reader.next();
        }
      }
    }

    // term freqs
    final int[] termFreqs = new int[totalTerms];
    {
      reader.reset(vectorsStream, totalTerms);
      for (int i = 0; i < totalTerms; ) {
        final LongsRef next = reader.next(totalTerms - i);
        for (int k = 0; k < next.length; ++k) {
          termFreqs[i++] = 1 + (int) next.longs[next.offset + k];
        }
      }
    }

    // total number of positions, offsets and payloads
    int totalPositions = 0, totalOffsets = 0, totalPayloads = 0;
    for (int i = 0, termIndex = 0; i < totalFields; ++i) {
      final int f = (int) flags.get(i);
      final int termCount = (int) numTerms.get(i);
      for (int j = 0; j < termCount; ++j) {
        final int freq = termFreqs[termIndex++];
        if ((f & POSITIONS) != 0) {
          totalPositions += freq;
        }
        if ((f & OFFSETS) != 0) {
          totalOffsets += freq;
        }
        if ((f & PAYLOADS) != 0) {
          totalPayloads += freq;
        }
      }
      assert i != totalFields - 1 || termIndex == totalTerms : termIndex + " " + totalTerms;
    }

    final int[][] positionIndex = positionIndex(skip, numFields, numTerms, termFreqs);
    final int[][] positions, startOffsets, lengths;
    if (totalPositions > 0) {
      positions = readPositions(skip, numFields, flags, numTerms, termFreqs, POSITIONS, totalPositions, positionIndex);
    } else {
      positions = new int[numFields][];
    }

    if (totalOffsets > 0) {
      // average number of chars per term
      final float[] charsPerTerm = new float[fieldNums.length];
      for (int i = 0; i < charsPerTerm.length; ++i) {
        charsPerTerm[i] = Float.intBitsToFloat(vectorsStream.readInt());
      }
      startOffsets = readPositions(skip, numFields, flags, numTerms, termFreqs, OFFSETS, totalOffsets, positionIndex);
      lengths = readPositions(skip, numFields, flags, numTerms, termFreqs, OFFSETS, totalOffsets, positionIndex);

      for (int i = 0; i < numFields; ++i) {
        final int[] fStartOffsets = startOffsets[i];
        final int[] fPositions = positions[i];
        // patch offsets from positions
        if (fStartOffsets != null && fPositions != null) {
          final float fieldCharsPerTerm = charsPerTerm[fieldNumOffs[i]];
          for (int j = 0; j < startOffsets[i].length; ++j) {
            fStartOffsets[j] += (int) (fieldCharsPerTerm * fPositions[j]);
          }
        }
        if (fStartOffsets != null) {
          final int[] fPrefixLengths = prefixLengths[i];
          final int[] fSuffixLengths = suffixLengths[i];
          final int[] fLengths = lengths[i];
          for (int j = 0, end = (int) numTerms.get(skip + i); j < end; ++j) {
            // delta-decode start offsets and  patch lengths using term lengths
            final int termLength = fPrefixLengths[j] + fSuffixLengths[j];
            lengths[i][positionIndex[i][j]] += termLength;
            for (int k = positionIndex[i][j] + 1; k < positionIndex[i][j + 1]; ++k) {
              fStartOffsets[k] += fStartOffsets[k - 1];
              fLengths[k] += termLength;
            }
          }
        }
      }
    } else {
      startOffsets = lengths = new int[numFields][];
    }
    if (totalPositions > 0) {
      // delta-decode positions
      for (int i = 0; i < numFields; ++i) {
        final int[] fPositions = positions[i];
        final int[] fpositionIndex = positionIndex[i];
        if (fPositions != null) {
          for (int j = 0, end = (int) numTerms.get(skip + i); j < end; ++j) {
            // delta-decode start offsets
            for (int k = fpositionIndex[j] + 1; k < fpositionIndex[j + 1]; ++k) {
              fPositions[k] += fPositions[k - 1];
            }
          }
        }
      }
    }

    // payload lengths
    final int[][] payloadIndex = new int[numFields][];
    int totalPayloadLength = 0;
    int payloadOff = 0;
    int payloadLen = 0;
    if (totalPayloads > 0) {
      reader.reset(vectorsStream, totalPayloads);
      // skip
      int termIndex = 0;
      for (int i = 0; i < skip; ++i) {
        final int f = (int) flags.get(i);
        final int termCount = (int) numTerms.get(i);
        if ((f & PAYLOADS) != 0) {
          for (int j = 0; j < termCount; ++j) {
            final int freq = termFreqs[termIndex + j];
            for (int k = 0; k < freq; ++k) {
              final int l = (int) reader.next();
              payloadOff += l;
            }
          }
        }
        termIndex += termCount;
      }
      totalPayloadLength = payloadOff;
      // read doc payload lengths
      for (int i = 0; i < numFields; ++i) {
        final int f = (int) flags.get(skip + i);
        final int termCount = (int) numTerms.get(skip + i);
        if ((f & PAYLOADS) != 0) {
          final int totalFreq = positionIndex[i][termCount];
          payloadIndex[i] = new int[totalFreq + 1];
          int posIdx = 0;
          payloadIndex[i][posIdx] = payloadLen;
          for (int j = 0; j < termCount; ++j) {
            final int freq = termFreqs[termIndex + j];
            for (int k = 0; k < freq; ++k) {
              final int payloadLength = (int) reader.next();
              payloadLen += payloadLength;
              payloadIndex[i][posIdx+1] = payloadLen;
              ++posIdx;
            }
          }
          assert posIdx == totalFreq;
        }
        termIndex += termCount;
      }
      totalPayloadLength += payloadLen;
      for (int i = skip + numFields; i < totalFields; ++i) {
        final int f = (int) flags.get(i);
        final int termCount = (int) numTerms.get(i);
        if ((f & PAYLOADS) != 0) {
          for (int j = 0; j < termCount; ++j) {
            final int freq = termFreqs[termIndex + j];
            for (int k = 0; k < freq; ++k) {
              totalPayloadLength += reader.next();
            }
          }
        }
        termIndex += termCount;
      }
      assert termIndex == totalTerms : termIndex + " " + totalTerms;
    }

    // decompress data
    final BytesRef suffixBytes = new BytesRef();
    decompressor.decompress(vectorsStream, totalLen + totalPayloadLength, docOff + payloadOff, docLen + payloadLen, suffixBytes);
    suffixBytes.length = docLen;
    final BytesRef payloadBytes = new BytesRef(suffixBytes.bytes, suffixBytes.offset + docLen, payloadLen);

    final int[] fieldFlags = new int[numFields];
    for (int i = 0; i < numFields; ++i) {
      fieldFlags[i] = (int) flags.get(skip + i);
    }

    final int[] fieldNumTerms = new int[numFields];
    for (int i = 0; i < numFields; ++i) {
      fieldNumTerms[i] = (int) numTerms.get(skip + i);
    }

    final int[][] fieldTermFreqs = new int[numFields][];
    {
      int termIdx = 0;
      for (int i = 0; i < skip; ++i) {
        termIdx += numTerms.get(i);
      }
      for (int i = 0; i < numFields; ++i) {
        final int termCount = (int) numTerms.get(skip + i);
        fieldTermFreqs[i] = new int[termCount];
        for (int j = 0; j < termCount; ++j) {
          fieldTermFreqs[i][j] = termFreqs[termIdx++];
        }
      }
    }

    assert sum(fieldLengths) == docLen : sum(fieldLengths) + " != " + docLen;

    return new TVFields(fieldNums, fieldFlags, fieldNumOffs, fieldNumTerms, fieldLengths,
        prefixLengths, suffixLengths, fieldTermFreqs,
        positionIndex, positions, startOffsets, lengths,
        payloadBytes, payloadIndex,
        suffixBytes);
  }

  // field -> term index -> position index
  private int[][] positionIndex(int skip, int numFields, PackedInts.Reader numTerms, int[] termFreqs) {
    final int[][] positionIndex = new int[numFields][];
    int termIndex = 0;
    for (int i = 0; i < skip; ++i) {
      final int termCount = (int) numTerms.get(i);
      termIndex += termCount;
    }
    for (int i = 0; i < numFields; ++i) {
      final int termCount = (int) numTerms.get(skip + i);
      positionIndex[i] = new int[termCount + 1];
      for (int j = 0; j < termCount; ++j) {
        final int freq = termFreqs[termIndex+j];
        positionIndex[i][j + 1] = positionIndex[i][j] + freq;
      }
      termIndex += termCount;
    }
    return positionIndex;
  }

  private int[][] readPositions(int skip, int numFields, PackedInts.Reader flags, PackedInts.Reader numTerms, int[] termFreqs, int flag, final int totalPositions, int[][] positionIndex) throws IOException {
    final int[][] positions = new int[numFields][];
    reader.reset(vectorsStream, totalPositions);
    // skip
    int toSkip = 0;
    int termIndex = 0;
    for (int i = 0; i < skip; ++i) {
      final int f = (int) flags.get(i);
      final int termCount = (int) numTerms.get(i);
      if ((f & flag) != 0) {
        for (int j = 0; j < termCount; ++j) {
          final int freq = termFreqs[termIndex+j];
          toSkip += freq;
        }
      }
      termIndex += termCount;
    }
    reader.skip(toSkip);
    // read doc positions
    for (int i = 0; i < numFields; ++i) {
      final int f = (int) flags.get(skip + i);
      final int termCount = (int) numTerms.get(skip + i);
      if ((f & flag) != 0) {
        final int totalFreq = positionIndex[i][termCount];
        final int[] fieldPositions = new int[totalFreq];
        positions[i] = fieldPositions;
        for (int j = 0; j < totalFreq; ) {
          final LongsRef nextPositions = reader.next(totalFreq - j);
          for (int k = 0; k < nextPositions.length; ++k) {
            fieldPositions[j++] = (int) nextPositions.longs[nextPositions.offset + k];
          }
        }
      }
      termIndex += termCount;
    }
    reader.skip(totalPositions - reader.ord());
    return positions;
  }

  /**
   * 这个对象就是将 索引文件解析后 生成了 存储了某个doc数据的对象
   */
  private class TVFields extends Fields {

    private final int[] fieldNums, fieldFlags, fieldNumOffs, numTerms, fieldLengths;
    private final int[][] prefixLengths, suffixLengths, termFreqs, positionIndex, positions, startOffsets, lengths, payloadIndex;
    private final BytesRef suffixBytes, payloadBytes;

    /**
     *
     * @param fieldNums  存储某次刷盘所有doc的fieldNum  的数组  fieldNum 就像一个id   与field数量无必然关系
     * @param fieldFlags
     * @param fieldNumOffs   对应 fieldNums的偏移量  长度对应field的数量
     * @param numTerms
     * @param fieldLengths
     * @param prefixLengths
     * @param suffixLengths
     * @param termFreqs
     * @param positionIndex
     * @param positions
     * @param startOffsets
     * @param lengths
     * @param payloadBytes
     * @param payloadIndex
     * @param suffixBytes
     */
    public TVFields(int[] fieldNums, int[] fieldFlags, int[] fieldNumOffs, int[] numTerms, int[] fieldLengths,
        int[][] prefixLengths, int[][] suffixLengths, int[][] termFreqs,
        int[][] positionIndex, int[][] positions, int[][] startOffsets, int[][] lengths,
        BytesRef payloadBytes, int[][] payloadIndex,
        BytesRef suffixBytes) {
      this.fieldNums = fieldNums;
      this.fieldFlags = fieldFlags;
      this.fieldNumOffs = fieldNumOffs;
      this.numTerms = numTerms;
      this.fieldLengths = fieldLengths;
      this.prefixLengths = prefixLengths;
      this.suffixLengths = suffixLengths;
      this.termFreqs = termFreqs;
      this.positionIndex = positionIndex;
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.lengths = lengths;
      this.payloadBytes = payloadBytes;
      this.payloadIndex = payloadIndex;
      this.suffixBytes = suffixBytes;
    }

    /**
     * 返回具备遍历内部元素的迭代器
     * @return
     */
    @Override
    public Iterator<String> iterator() {
      return new Iterator<String>() {
        int i = 0;
        @Override
        public boolean hasNext() {
          return i < fieldNumOffs.length;
        }
        @Override
        public String next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          final int fieldNum = fieldNums[fieldNumOffs[i++]];
          return fieldInfos.fieldInfo(fieldNum).name;
        }
        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    /**
     * 返回某个域下所有的 term
     * @param field
     * @return
     * @throws IOException
     */
    @Override
    public Terms terms(String field) throws IOException {
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      if (fieldInfo == null) {
        return null;
      }
      int idx = -1;
      for (int i = 0; i < fieldNumOffs.length; ++i) {
        if (fieldNums[fieldNumOffs[i]] == fieldInfo.number) {
          idx = i;
          break;
        }
      }

      if (idx == -1 || numTerms[idx] == 0) {
        // no term
        return null;
      }
      int fieldOff = 0, fieldLen = -1;
      for (int i = 0; i < fieldNumOffs.length; ++i) {
        if (i < idx) {
          fieldOff += fieldLengths[i];
        } else {
          fieldLen = fieldLengths[i];
          break;
        }
      }
      assert fieldLen >= 0;
      return new TVTerms(numTerms[idx], fieldFlags[idx],
          prefixLengths[idx], suffixLengths[idx], termFreqs[idx],
          positionIndex[idx], positions[idx], startOffsets[idx], lengths[idx],
          payloadIndex[idx], payloadBytes,
          new BytesRef(suffixBytes.bytes, suffixBytes.offset + fieldOff, fieldLen));
    }

    @Override
    public int size() {
      return fieldNumOffs.length;
    }

  }

  /**
   * 描述某个 field 下所有的 term 信息
   */
  private static class TVTerms extends Terms {

    private final int numTerms, flags;
    private final long totalTermFreq;
    private final int[] prefixLengths, suffixLengths, termFreqs, positionIndex, positions, startOffsets, lengths, payloadIndex;
    private final BytesRef termBytes, payloadBytes;

    TVTerms(int numTerms, int flags, int[] prefixLengths, int[] suffixLengths, int[] termFreqs,
        int[] positionIndex, int[] positions, int[] startOffsets, int[] lengths,
        int[] payloadIndex, BytesRef payloadBytes,
        BytesRef termBytes) {
      this.numTerms = numTerms;
      this.flags = flags;
      this.prefixLengths = prefixLengths;
      this.suffixLengths = suffixLengths;
      this.termFreqs = termFreqs;
      this.positionIndex = positionIndex;
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.lengths = lengths;
      this.payloadIndex = payloadIndex;
      this.payloadBytes = payloadBytes;
      this.termBytes = termBytes;
      long ttf = 0;
      for (int tf : termFreqs) {
        ttf += tf;
      }
      this.totalTermFreq = ttf;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      TVTermsEnum termsEnum = new TVTermsEnum();
      termsEnum.reset(numTerms, flags, prefixLengths, suffixLengths, termFreqs, positionIndex, positions, startOffsets, lengths,
          payloadIndex, payloadBytes,
          new ByteArrayDataInput(termBytes.bytes, termBytes.offset, termBytes.length));
      return termsEnum;
    }

    @Override
    public long size() throws IOException {
      return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      return totalTermFreq;
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return numTerms;
    }

    @Override
    public int getDocCount() throws IOException {
      return 1;
    }

    @Override
    public boolean hasFreqs() {
      return true;
    }

    @Override
    public boolean hasOffsets() {
      return (flags & OFFSETS) != 0;
    }

    @Override
    public boolean hasPositions() {
      return (flags & POSITIONS) != 0;
    }

    @Override
    public boolean hasPayloads() {
      return (flags & PAYLOADS) != 0;
    }

  }

  private static class TVTermsEnum extends BaseTermsEnum {

    private int numTerms, startPos, ord;
    private int[] prefixLengths, suffixLengths, termFreqs, positionIndex, positions, startOffsets, lengths, payloadIndex;
    private ByteArrayDataInput in;
    private BytesRef payloads;
    private final BytesRef term;

    private TVTermsEnum() {
      term = new BytesRef(16);
    }

    void reset(int numTerms, int flags, int[] prefixLengths, int[] suffixLengths, int[] termFreqs, int[] positionIndex, int[] positions, int[] startOffsets, int[] lengths,
        int[] payloadIndex, BytesRef payloads, ByteArrayDataInput in) {
      this.numTerms = numTerms;
      this.prefixLengths = prefixLengths;
      this.suffixLengths = suffixLengths;
      this.termFreqs = termFreqs;
      this.positionIndex = positionIndex;
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.lengths = lengths;
      this.payloadIndex = payloadIndex;
      this.payloads = payloads;
      this.in = in;
      startPos = in.getPosition();
      reset();
    }

    void reset() {
      term.length = 0;
      in.setPosition(startPos);
      ord = -1;
    }

    @Override
    public BytesRef next() throws IOException {
      if (ord == numTerms - 1) {
        return null;
      } else {
        assert ord < numTerms;
        ++ord;
      }

      // read term
      term.offset = 0;
      term.length = prefixLengths[ord] + suffixLengths[ord];
      if (term.length > term.bytes.length) {
        term.bytes = ArrayUtil.grow(term.bytes, term.length);
      }
      in.readBytes(term.bytes, prefixLengths[ord], suffixLengths[ord]);

      return term;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text)
        throws IOException {
      if (ord < numTerms && ord >= 0) {
        final int cmp = term().compareTo(text);
        if (cmp == 0) {
          return SeekStatus.FOUND;
        } else if (cmp > 0) {
          reset();
        }
      }
      // linear scan
      while (true) {
        final BytesRef term = next();
        if (term == null) {
          return SeekStatus.END;
        }
        final int cmp = term.compareTo(text);
        if (cmp > 0) {
          return SeekStatus.NOT_FOUND;
        } else if (cmp == 0) {
          return SeekStatus.FOUND;
        }
      }
    }

    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef term() throws IOException {
      return term;
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      return 1;
    }

    @Override
    public long totalTermFreq() throws IOException {
      return termFreqs[ord];
    }

    @Override
    public final PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      final TVPostingsEnum docsEnum;
      if (reuse != null && reuse instanceof TVPostingsEnum) {
        docsEnum = (TVPostingsEnum) reuse;
      } else {
        docsEnum = new TVPostingsEnum();
      }

      docsEnum.reset(termFreqs[ord], positionIndex[ord], positions, startOffsets, lengths, payloads, payloadIndex);
      return docsEnum;
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      final PostingsEnum delegate = postings(null, PostingsEnum.FREQS);
      return new SlowImpactsEnum(delegate);
    }

  }

  private static class TVPostingsEnum extends PostingsEnum {

    private int doc = -1;
    private int termFreq;
    private int positionIndex;
    private int[] positions;
    private int[] startOffsets;
    private int[] lengths;
    private final BytesRef payload;
    private int[] payloadIndex;
    private int basePayloadOffset;
    private int i;

    TVPostingsEnum() {
      payload = new BytesRef();
    }

    public void reset(int freq, int positionIndex, int[] positions,
        int[] startOffsets, int[] lengths, BytesRef payloads,
        int[] payloadIndex) {
      this.termFreq = freq;
      this.positionIndex = positionIndex;
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.lengths = lengths;
      this.basePayloadOffset = payloads.offset;
      this.payload.bytes = payloads.bytes;
      payload.offset = payload.length = 0;
      this.payloadIndex = payloadIndex;

      doc = i = -1;
    }

    private void checkDoc() {
      if (doc == NO_MORE_DOCS) {
        throw new IllegalStateException("DocsEnum exhausted");
      } else if (doc == -1) {
        throw new IllegalStateException("DocsEnum not started");
      }
    }

    private void checkPosition() {
      checkDoc();
      if (i < 0) {
        throw new IllegalStateException("Position enum not started");
      } else if (i >= termFreq) {
        throw new IllegalStateException("Read past last position");
      }
    }

    @Override
    public int nextPosition() throws IOException {
      if (doc != 0) {
        throw new IllegalStateException();
      } else if (i >= termFreq - 1) {
        throw new IllegalStateException("Read past last position");
      }

      ++i;

      if (payloadIndex != null) {
        payload.offset = basePayloadOffset + payloadIndex[positionIndex + i];
        payload.length = payloadIndex[positionIndex + i + 1] - payloadIndex[positionIndex + i];
      }

      if (positions == null) {
        return -1;
      } else {
        return positions[positionIndex + i];
      }
    }

    @Override
    public int startOffset() throws IOException {
      checkPosition();
      if (startOffsets == null) {
        return -1;
      } else {
        return startOffsets[positionIndex + i];
      }
    }

    @Override
    public int endOffset() throws IOException {
      checkPosition();
      if (startOffsets == null) {
        return -1;
      } else {
        return startOffsets[positionIndex + i] + lengths[positionIndex + i];
      }
    }

    @Override
    public BytesRef getPayload() throws IOException {
      checkPosition();
      if (payloadIndex == null || payload.length == 0) {
        return null;
      } else {
        return payload;
      }
    }

    @Override
    public int freq() throws IOException {
      checkDoc();
      return termFreq;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      if (doc == -1) {
        return (doc = 0);
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return 1;
    }
  }

  private static int sum(int[] arr) {
    int sum = 0;
    for (int el : arr) {
      sum += el;
    }
    return sum;
  }

  @Override
  public long ramBytesUsed() {
    return indexReader.ramBytesUsed();
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.singleton(Accountables.namedAccountable("term vector index", indexReader));
  }
  
  @Override
  public void checkIntegrity() throws IOException {
    indexReader.checkIntegrity();
    CodecUtil.checksumEntireFile(vectorsStream);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(mode=" + compressionMode + ",chunksize=" + chunkSize + ")";
  }
}
