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
package org.apache.lucene.codecs.lucene80;


import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.compress.LZ4.FastCompressionHashTable;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat.NUMERIC_BLOCK_SIZE;

/** writer for {@link Lucene80DocValuesFormat}
 * 以field为单位 存储 docValue 信息
 */
final class Lucene80DocValuesConsumer extends DocValuesConsumer implements Closeable {

  IndexOutput data, meta;
  final int maxDoc;
  private final SegmentWriteState state;

  /** expert: Creates a new writer */
  public Lucene80DocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    boolean success = false;
    try {
      this.state = state;
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeIndexHeader(data, dataCodec, Lucene80DocValuesFormat.VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(meta, metaCodec, Lucene80DocValuesFormat.VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      maxDoc = state.segmentInfo.maxDoc();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeInt(-1); // write EOF marker
        CodecUtil.writeFooter(meta); // write checksum
      }
      if (data != null) {
        CodecUtil.writeFooter(data); // write checksum
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      meta = data = null;
    }
  }

  /**
   * 存储某个field在所有doc中的  数字类型的值
   * @param field field information
   * @param valuesProducer Numeric values to write.
   * @throws IOException
   */
  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    // 写入 fieldNum 以及 docValue 类型
    meta.writeInt(field.number);
    meta.writeByte(Lucene80DocValuesFormat.NUMERIC);

    writeValues(field, new EmptyDocValuesProducer() {
      // 这里虽然又包装了一层 不过可以看作是普通的 NumericDocValues   并且在没有设置 sort时 内部的docValue也没有排序 还是按照docId的顺序
      @Override
      public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return DocValues.singleton(valuesProducer.getNumeric(field));
      }
    });
  }

  /**
   * 该对象记录了所有写入的值中的最大值 最小值 等
   */
  private static class MinMaxTracker {
    long min, max, numValues, spaceInBits;

    MinMaxTracker() {
      reset();
      spaceInBits = 0;
    }

    private void reset() {
      min = Long.MAX_VALUE;
      max = Long.MIN_VALUE;
      numValues = 0;
    }

    /** Accumulate a new value. */
    // 尝试更新 min/max
    void update(long v) {
      min = Math.min(min, v);
      max = Math.max(max, v);
      ++numValues;
    }

    /** Update the required space. */
    void finish() {
      if (max > min) {
        // 计算当前block下按照差值存储需要多少额外的空间
        spaceInBits += DirectWriter.unsignedBitsRequired(max - min) * numValues;
      }
    }

    /** Update space usage and get ready for accumulating values for the next block. */
    void nextBlock() {
      finish();
      reset();
    }
  }

  /**
   * 将 docValue的值写入到索引文件中
   * @param field
   * @param valuesProducer
   * @return
   * @throws IOException
   */
  private long[] writeValues(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    // 获取已经完成排序的 所有docValue
    SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
    int numDocsWithValue = 0;
    MinMaxTracker minMax = new MinMaxTracker();
    MinMaxTracker blockMinMax = new MinMaxTracker();
    long gcd = 0;
    Set<Long> uniqueValues = new HashSet<>();
    // 挨个处理每个 docValue
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      // 代表SortedNumericDocValues 内部包含了多少 NumericDocValues 因为它是一个组合对象
      // 实际上一般传入的是 SingletonSortedNumericDocValues  也就是count总是1
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        long v = values.nextValue();

        if (gcd != 1) {
          // 这种时候 计算容易发生溢出  所以不采用 gcd计算
          if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) {
            // in that case v - minValue might overflow and make the GCD computation return
            // wrong results. Since these extreme values are unlikely, we just discard
            // GCD computation for them
            gcd = 1;
            // 要确保此前至少有一个数据
          } else if (minMax.numValues != 0) { // minValue needs to be set first
            // 计算最大公约数
            gcd = MathUtil.gcd(gcd, v - minMax.min);
          }
        }

        // 记录所有docValue中的 min/max
        minMax.update(v);
        // 记录每个block下的 min/max
        blockMinMax.update(v);
        // 每当写入的值超过了 一个block的大小 就切换成下一个block
        if (blockMinMax.numValues == NUMERIC_BLOCK_SIZE) {
          blockMinMax.nextBlock();
        }

        // 当不同的值超过 256个时 清除set
        if (uniqueValues != null
            && uniqueValues.add(v)
            && uniqueValues.size() > 256) {
          uniqueValues = null;
        }
      }

      // 累加出现的总数
      numDocsWithValue++;
    }

    // 计算 存储差值需要的空间
    minMax.finish();
    blockMinMax.finish();

    final long numValues = minMax.numValues;
    long min = minMax.min;
    final long max = minMax.max;
    assert blockMinMax.spaceInBits <= minMax.spaceInBits;

    // 代表该field 没有出现在任何doc中
    if (numDocsWithValue == 0) {              // meta[-2, 0]: No documents with values
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1);   // denseRankPower
    // 代表所有doc都有值
    } else if (numDocsWithValue == maxDoc) {  // meta[-1, 0]: All documents has values
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1);   // denseRankPower
    } else {                                  // meta[data.offset, data.length]: IndexedDISI structure for documents with values
      // 部分doc有值 使用disi结构存储离散的 docId
      long offset = data.getFilePointer();
      meta.writeLong(offset);// docsWithFieldOffset
      values = valuesProducer.getSortedNumeric(field);
      final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    // 写入docValue总数
    meta.writeLong(numValues);
    final int numBitsPerValue;
    boolean doBlocks = false;
    Map<Long, Integer> encode = null;
    // 代表所有值都是一样的
    if (min >= max) {                         // meta[-1]: All values are 0
      numBitsPerValue = 0;
      meta.writeInt(-1); // tablesize
    } else {
      // 代表一共出现的 不重复的 docValue值数量不超过 256
      if (uniqueValues != null
          && uniqueValues.size() > 1
          && DirectWriter.unsignedBitsRequired(uniqueValues.size() - 1) < DirectWriter.unsignedBitsRequired((max - min) / gcd)) {
        numBitsPerValue = DirectWriter.unsignedBitsRequired(uniqueValues.size() - 1);
        final Long[] sortedUniqueValues = uniqueValues.toArray(new Long[0]);
        Arrays.sort(sortedUniqueValues);
        meta.writeInt(sortedUniqueValues.length); // tablesize
        for (Long v : sortedUniqueValues) {
          meta.writeLong(v); // table[] entry
        }
        encode = new HashMap<>();
        for (int i = 0; i < sortedUniqueValues.length; ++i) {
          encode.put(sortedUniqueValues[i], i);
        }
        min = 0;
        gcd = 1;
      } else {
        uniqueValues = null;
        // we do blocks if that appears to save 10+% storage
        doBlocks = minMax.spaceInBits > 0 && (double) blockMinMax.spaceInBits / minMax.spaceInBits <= 0.9;
        if (doBlocks) {
          numBitsPerValue = 0xFF;
          meta.writeInt(-2 - NUMERIC_BLOCK_SHIFT); // tablesize
        } else {
          numBitsPerValue = DirectWriter.unsignedBitsRequired((max - min) / gcd);
          if (gcd == 1 && min > 0
              && DirectWriter.unsignedBitsRequired(max) == DirectWriter.unsignedBitsRequired(max - min)) {
            min = 0;
          }
          meta.writeInt(-1); // tablesize
        }
      }
    }

    meta.writeByte((byte) numBitsPerValue);
    meta.writeLong(min);
    meta.writeLong(gcd);
    long startOffset = data.getFilePointer();
    meta.writeLong(startOffset); // valueOffset
    long jumpTableOffset = -1;
    if (doBlocks) {
      jumpTableOffset = writeValuesMultipleBlocks(valuesProducer.getSortedNumeric(field), gcd);
    } else if (numBitsPerValue != 0) {
      writeValuesSingleBlock(valuesProducer.getSortedNumeric(field), numValues, numBitsPerValue, min, gcd, encode);
    }
    meta.writeLong(data.getFilePointer() - startOffset); // valuesLength
    meta.writeLong(jumpTableOffset);
    return new long[] {numDocsWithValue, numValues};
  }

  private void writeValuesSingleBlock(SortedNumericDocValues values, long numValues, int numBitsPerValue,
      long min, long gcd, Map<Long, Integer> encode) throws IOException {
    DirectWriter writer = DirectWriter.getInstance(data, numValues, numBitsPerValue);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        long v = values.nextValue();
        if (encode == null) {
          writer.add((v - min) / gcd);
        } else {
          writer.add(encode.get(v));
        }
      }
    }
    writer.finish();
  }

  // Returns the offset to the jump-table for vBPV
  private long writeValuesMultipleBlocks(SortedNumericDocValues values, long gcd) throws IOException {
    long[] offsets = new long[ArrayUtil.oversize(1, Long.BYTES)];
    int offsetsIndex = 0;
    final long[] buffer = new long[NUMERIC_BLOCK_SIZE];
    final ByteBuffersDataOutput encodeBuffer = ByteBuffersDataOutput.newResettableInstance();
    int upTo = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        buffer[upTo++] = values.nextValue();
        if (upTo == NUMERIC_BLOCK_SIZE) {
          offsets = ArrayUtil.grow(offsets, offsetsIndex+1);
          offsets[offsetsIndex++] = data.getFilePointer();
          writeBlock(buffer, NUMERIC_BLOCK_SIZE, gcd, encodeBuffer);
          upTo = 0;
        }
      }
    }
    if (upTo > 0) {
      offsets = ArrayUtil.grow(offsets, offsetsIndex+1);
      offsets[offsetsIndex++] = data.getFilePointer();
      writeBlock(buffer, upTo, gcd, encodeBuffer);
    }

    // All blocks has been written. Flush the offset jump-table
    final long offsetsOrigo = data.getFilePointer();
    for (int i = 0 ; i < offsetsIndex ; i++) {
      data.writeLong(offsets[i]);
    }
    data.writeLong(offsetsOrigo);
    return offsetsOrigo;
  }

  private void writeBlock(long[] values, int length, long gcd, ByteBuffersDataOutput buffer) throws IOException {
    assert length > 0;
    long min = values[0];
    long max = values[0];
    for (int i = 1; i < length; ++i) {
      final long v = values[i];
      assert Math.floorMod(values[i] - min, gcd) == 0;
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    if (min == max) {
      data.writeByte((byte) 0);
      data.writeLong(min);
    } else {
      final int bitsPerValue = DirectWriter.unsignedBitsRequired(max - min);
      buffer.reset();
      assert buffer.size() == 0;
      final DirectWriter w = DirectWriter.getInstance(buffer, length, bitsPerValue);
      for (int i = 0; i < length; ++i) {
        w.add((values[i] - min) / gcd);
      }
      w.finish();
      data.writeByte((byte) bitsPerValue);
      data.writeLong(min);
      data.writeInt(Math.toIntExact(buffer.size()));
      buffer.copyTo(data);
    }
  }

  /**
   * 该对象采用 LZ4 压缩算法写入数据
   */
  class CompressedBinaryBlockWriter implements Closeable {
    final FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();    
    int uncompressedBlockLength = 0;

    /**
     * 记录每次 flush时 压缩前的最大长度
     */
    int maxUncompressedBlockLength = 0;
    /**
     * 记录当前 block 已经写入了多少doc
     */
    int numDocsInCurrentBlock = 0;
    /**
     * 对应本次尚未刷盘的 doc 所关联的 docValue长度
     */
    final int[] docLengths = new int[Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK]; 
    byte[] block = BytesRef.EMPTY_BYTES;
    /**
     * 记录总计触发了多少次 flushData   或者说某次大流程中总共刷盘了多少次  又或者代表总计写入了多少block
     */
    int totalChunks = 0;
    /**
     * 记录此时 data文件的最大偏移量
     */
    long maxPointer = 0;

    /**
     * 记录数据文件的起点
     */
    final long blockAddressesStart;

    /**
     * 这个临时文件存储的是 有关每次 data文件写入的 长度 (flush后长度 - flush前长度)
     */
    private final IndexOutput tempBinaryOffsets;
    
    
    public CompressedBinaryBlockWriter() throws IOException {
      // 创建临时文件  每次都记录 2个block之间写入了多少数据
      tempBinaryOffsets = state.directory.createTempOutput(state.segmentInfo.name, "binary_pointers", state.context);
      boolean success = false;
      try {
        CodecUtil.writeHeader(tempBinaryOffsets, Lucene80DocValuesFormat.META_CODEC + "FilePointers", Lucene80DocValuesFormat.VERSION_CURRENT);
        blockAddressesStart = data.getFilePointer();
        success = true;
      } finally {
        if (success == false) {
          IOUtils.closeWhileHandlingException(this); //self-close because constructor caller can't 
        }
      }
    }

    /**
     * 写入 docId 与 docValue
     * @param doc
     * @param v
     * @throws IOException
     */
    void addDoc(int doc, BytesRef v) throws IOException {
      // 设置对应的长度
      docLengths[numDocsInCurrentBlock] = v.length;
      // 存储当前block下所有docValue的容器
      block = ArrayUtil.grow(block, uncompressedBlockLength + v.length);
      System.arraycopy(v.bytes, v.offset, block, uncompressedBlockLength, v.length);
      uncompressedBlockLength += v.length;
      // 记录当前block下已经存储了多少 doc
      numDocsInCurrentBlock++;
      // 当满足一个刷盘大小时 才进行刷盘 (类似批处理的思路)  这里是每当添加32个docValue时 强制刷盘
      if (numDocsInCurrentBlock == Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK) {
        flushData();
      }      
    }

    /**
     * 当某个block 被填满时 将之前的数据采用压缩算法处理后写入到 out中
     * @throws IOException
     */
    private void flushData() throws IOException {
      if (numDocsInCurrentBlock > 0) {
        // Write offset to this block to temporary offsets file
        totalChunks++;

        // 代表上一次刷盘后的数据起点
        long thisBlockStartPointer = data.getFilePointer();
        
        // Optimisation - check if all lengths are same
        boolean allLengthsSame = true;
        // 先检测所有长度是否一致
        for (int i = 1; i < Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK; i++) {
          if (docLengths[i] != docLengths[i-1]) {
            allLengthsSame = false;
            break;
          }
        }
        // 的代表所有的长度都是一致的 那么只需要写入一次长度即可  加上一个特殊标识 标明 allLengthsSame
        if (allLengthsSame) {
            // Only write one value shifted. Steal a bit to indicate all other lengths are the same
            // 最低位为 1 代表所有长度一致
            int onlyOneLength = (docLengths[0] <<1) | 1;
            data.writeVInt(onlyOneLength);
        } else {
          for (int i = 0; i < Lucene80DocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK; i++) {
            if (i == 0) {
              // Write first value shifted and steal a bit to indicate other lengths are to follow
              // 最低位为0 代表长度不一致
              int multipleLengths = (docLengths[0] <<1);
              data.writeVInt(multipleLengths);              
            } else {
              // 之后挨个写入长度
              data.writeVInt(docLengths[i]);
            }
          }
        }
        maxUncompressedBlockLength = Math.max(maxUncompressedBlockLength, uncompressedBlockLength);
        // 将数据压缩后写入到 data 中
        LZ4.compress(block, 0, uncompressedBlockLength, data, ht);
        numDocsInCurrentBlock = 0;
        // Ensure initialized with zeroes because full array is always written
        Arrays.fill(docLengths, 0);
        uncompressedBlockLength = 0;
        // 获取此时data写入到的位置
        maxPointer = data.getFilePointer();
        // 在临时文件中记录一个总长度
        tempBinaryOffsets.writeVLong(maxPointer - thisBlockStartPointer);
      }
    }

    /**
     * 代表有关 docValue的数据以及写入到 meta/data文件中了
     * @throws IOException
     */
    void writeMetaData() throws IOException {
      if (totalChunks == 0) {
        return;
      }

      // 记录此时data文件写入docValue后的位置
      long startDMW = data.getFilePointer();
      // 这里是记录 每次flush的详情的

      // 记录data 文件的结尾偏移量
      meta.writeLong(startDMW);
      // 记录总计生成了多少 block
      meta.writeVInt(totalChunks);
      // 每个block 对应的 docValue 数量
      meta.writeVInt(Lucene80DocValuesFormat.BINARY_BLOCK_SHIFT);
      // 记录未压缩前的block块的   主要是用来还原的
      meta.writeVInt(maxUncompressedBlockLength);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

      // 为临时文件写入文件尾   这个临时文件记录了每次flush写入的长度
      CodecUtil.writeFooter(tempBinaryOffsets);
      IOUtils.close(tempBinaryOffsets);             
      // write the compressed block offsets info to the meta file by reading from temp file
      // 读取临时文件的数据 并写入到 meta中
      try (ChecksumIndexInput filePointersIn = state.directory.openChecksumInput(tempBinaryOffsets.getName(), IOContext.READONCE)) {
        CodecUtil.checkHeader(filePointersIn, Lucene80DocValuesFormat.META_CODEC + "FilePointers", Lucene80DocValuesFormat.VERSION_CURRENT,
          Lucene80DocValuesFormat.VERSION_CURRENT);
        Throwable priorE = null;
        try {
          // 内部数据通过 期望/min 进行压缩处理 减小实际写入的开销  每个block的min等元数据信息写入到meta 实际数据写入到data
          final DirectMonotonicWriter filePointers = DirectMonotonicWriter.getInstance(meta, data, totalChunks, DIRECT_MONOTONIC_BLOCK_SHIFT);
          long fp = blockAddressesStart;
          for (int i = 0; i < totalChunks; ++i) {
            // 将每次 flush 时 data的起始偏移量写入到索引文件
            filePointers.add(fp);
            fp += filePointersIn.readVLong();
          }
          if (maxPointer < fp) {
            throw new CorruptIndexException("File pointers don't add up ("+fp+" vs expected "+maxPointer+")", filePointersIn);
          }
          filePointers.finish();
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(filePointersIn, priorE);
        }
      }
      // Write the length of the DMW block in the data
      // 记录 当将数据写入到 DMW 后 偏移量变化了多少
      meta.writeLong(data.getFilePointer() - startDMW);
    }

    /**
     * 临时文件就是为了记录 处理每个block时 data文件的起始偏移量的 所以当写入完整时 临时文件就可以删除了
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
      if (tempBinaryOffsets != null) {
        IOUtils.close(tempBinaryOffsets);             
        state.directory.deleteFile(tempBinaryOffsets.getName());
      }
    }
    
  }

  /**
   * 存储某个 field 下的docValue  (类型为 二进制数据)
   * @param field field information
   * @param valuesProducer Binary values to write.   对应哪些doc发生了变化
   * @throws IOException
   */
  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    // 往元数据索引文件中写入 fieldNum 和 docValue 类型
    // 标明 此时存储的 docValue是属于哪个 field的
    meta.writeInt(field.number);
    // 代表 docValue 的值类型是 二进制类型
    meta.writeByte(Lucene80DocValuesFormat.BINARY);

    // 该 writer对象采用LZ4 压缩算法存储数据
    try (CompressedBinaryBlockWriter blockWriter = new CompressedBinaryBlockWriter()){
      // BinaryDocValues 的意思是在迭代docId的同时能获取到对应的二进制值 这个值就是field.value
      BinaryDocValues values = valuesProducer.getBinary(field);
      // 记录此时偏移量的起点
      long start = data.getFilePointer();
      meta.writeLong(start); // dataOffset
      // 因为不是所有的doc 都包含该field 所以需要记录 该field总计关联了多少doc  以及生成索引失败的doc 也不会算在内
      int numDocsWithField = 0;
      int minLength = Integer.MAX_VALUE;
      int maxLength = 0;
      // 将所有相关的doc 以及对应的value 写入到数据索引文件中
      for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
        // 随着每次读取到一个doc 增加该值
        numDocsWithField++;
        // 对应docValue
        BytesRef v = values.binaryValue();
        // 将数据写入到 基于 LZ4 的writer中  这样每当写满32个docValue时 会自动将数据压缩 后存入out中
        blockWriter.addDoc(doc, v);      
        int length = v.length;      
        minLength = Math.min(length, minLength);
        maxLength = Math.max(length, maxLength);
      }
      // 将最后不足32个docValue 强制写入到out中
      blockWriter.flushData();

      assert numDocsWithField <= maxDoc;
      // 写入总长度
      meta.writeLong(data.getFilePointer() - start); // dataLength

      // 这个套路跟 norm 一样
      if (numDocsWithField == 0) {
        meta.writeLong(-2); // docsWithFieldOffset     -2代表没有写入任何数据
        meta.writeLong(0L); // docsWithFieldLength
        meta.writeShort((short) -1); // jumpTableEntryCount    大量的 docValue直接读取会比较慢 这里通过jumpTable 作为一种索引结构 快速的定位到某个docValue的位置
        meta.writeByte((byte) -1);   // denseRankPower
      } else if (numDocsWithField == maxDoc) {
        meta.writeLong(-1); // docsWithFieldOffset     -1代表每个doc都有对应的值 没有空缺的情况
        meta.writeLong(0L); // docsWithFieldLength
        meta.writeShort((short) -1); // jumpTableEntryCount     这种情况应该是 docValue本身排序就会存在规则 (连续的就容易寻找) 所以也不需要借助 jumpTable
        meta.writeByte((byte) -1);   // denseRankPower
      } else {
        // data记录docValue   而 meta 负责记录docId 信息  当docId 不连续时 根据情况使用 DISI结构存储
        long offset = data.getFilePointer();
        meta.writeLong(offset); // docsWithFieldOffset      写入data文件的偏移量
        values = valuesProducer.getBinary(field);
        final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);  //  写入 jumpTable 相关数据   默认的 rankPower 为9
        // 记录 disi数据的长度
        meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
        // 记录总计有多少个jump 块
        meta.writeShort(jumpTableEntryCount);
        // 记录 power的值
        meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      }

      // 分别记录 该field 下有多少docValue  最大值的长度 最小值的长度
      meta.writeInt(numDocsWithField);
      meta.writeInt(minLength);
      meta.writeInt(maxLength);    

      // 这里还会在 meta 中追加一些信息
      blockWriter.writeMetaData();
      
    }

  }

  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene80DocValuesFormat.SORTED);
    doAddSortedField(field, valuesProducer);
  }

  /**
   * 写入 SortedField信息时 需要额外写入termDict
   * @param field
   * @param valuesProducer
   * @throws IOException
   */
  private void doAddSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    SortedDocValues values = valuesProducer.getSorted(field);
    int numDocsWithField = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithField++;
    }

    if (numDocsWithField == 0) {
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1);   // denseRankPower
    } else if (numDocsWithField == maxDoc) {
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1);   // denseRankPower
    } else {
      long offset = data.getFilePointer();
      meta.writeLong(offset); // docsWithFieldOffset
      values = valuesProducer.getSorted(field);
      final short jumpTableentryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableentryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    meta.writeInt(numDocsWithField);
    if (values.getValueCount() <= 1) {
      meta.writeByte((byte) 0); // bitsPerValue
      meta.writeLong(0L); // ordsOffset
      meta.writeLong(0L); // ordsLength
    } else {
      int numberOfBitsPerOrd = DirectWriter.unsignedBitsRequired(values.getValueCount() - 1);
      meta.writeByte((byte) numberOfBitsPerOrd); // bitsPerValue
      long start = data.getFilePointer();
      meta.writeLong(start); // ordsOffset
      DirectWriter writer = DirectWriter.getInstance(data, numDocsWithField, numberOfBitsPerOrd);
      values = valuesProducer.getSorted(field);
      for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
        writer.add(values.ordValue());
      }
      writer.finish();
      meta.writeLong(data.getFilePointer() - start); // ordsLength
    }

    // 将SortedDocValues 包装成SortedSetDocValues
    addTermsDict(DocValues.singleton(valuesProducer.getSorted(field)));
  }

  /**
   * 在SortedSetDocValues 中  field.value 就是存储在termHash中的
   * @param values
   * @throws IOException
   */
  private void addTermsDict(SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount();
    meta.writeVLong(size);
    meta.writeInt(Lucene80DocValuesFormat.TERMS_DICT_BLOCK_SHIFT);

    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    ByteBuffersIndexOutput addressOutput = new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
    meta.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
    long numBlocks = (size + Lucene80DocValuesFormat.TERMS_DICT_BLOCK_MASK) >>> Lucene80DocValuesFormat.TERMS_DICT_BLOCK_SHIFT;
    DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT);

    BytesRefBuilder previous = new BytesRefBuilder();
    long ord = 0;
    long start = data.getFilePointer();
    int maxLength = 0;
    // 因为SortedSetDocValues 下field.value 是byteRef类型 同时使用termHash存储 所以实际上它也可以作为 termEnum
    // 在遍历时 是按照term.ord进行遍历的 也就是从小到大的顺序   （利用termHash做的排序）
    TermsEnum iterator = values.termsEnum();
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      // 每15个term 生成一个词典
      if ((ord & Lucene80DocValuesFormat.TERMS_DICT_BLOCK_MASK) == 0) {
        writer.add(data.getFilePointer() - start);
        // 将term信息写入
        data.writeVInt(term.length);
        data.writeBytes(term.bytes, term.offset, term.length);
      } else {
        // 仅写入后缀信息
        final int prefixLength = StringHelper.bytesDifference(previous.get(), term);
        final int suffixLength = term.length - prefixLength;
        assert suffixLength > 0; // terms are unique

        data.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));
        if (prefixLength >= 15) {
          data.writeVInt(prefixLength - 15);
        }
        if (suffixLength >= 16) {
          data.writeVInt(suffixLength - 16);
        }
        data.writeBytes(term.bytes, term.offset + prefixLength, term.length - prefixLength);
      }
      maxLength = Math.max(maxLength, term.length);
      previous.copyBytes(term);
      ++ord;
    }
    writer.finish();
    meta.writeInt(maxLength);
    meta.writeLong(start);
    meta.writeLong(data.getFilePointer() - start);
    start = data.getFilePointer();
    addressBuffer.copyTo(data);
    meta.writeLong(start);
    meta.writeLong(data.getFilePointer() - start);

    // Now write the reverse terms index
    writeTermsIndex(values);
  }

  private void writeTermsIndex(SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount();
    meta.writeInt(Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
    long start = data.getFilePointer();

    long numBlocks = 1L + ((size + Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) >>> Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    DirectMonotonicWriter writer;
    try (ByteBuffersIndexOutput addressOutput = new ByteBuffersIndexOutput(addressBuffer, "temp", "temp")) {
      writer = DirectMonotonicWriter.getInstance(meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT);
      TermsEnum iterator = values.termsEnum();
      BytesRefBuilder previous = new BytesRefBuilder();
      long offset = 0;
      long ord = 0;
      for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
        if ((ord & Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == 0) {
          writer.add(offset);
          final int sortKeyLength;
          if (ord == 0) {
            // no previous term: no bytes to write
            sortKeyLength = 0;
          } else {
            sortKeyLength = StringHelper.sortKeyLength(previous.get(), term);
          }
          offset += sortKeyLength;
          data.writeBytes(term.bytes, term.offset, sortKeyLength);
        } else if ((ord & Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == Lucene80DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) {
          previous.copyBytes(term);
        }
        ++ord;
      }
      writer.add(offset);
      writer.finish();
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
      start = data.getFilePointer();
      addressBuffer.copyTo(data);
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
    }
  }

  /**
   * 这个写入相对比较简单 不需要写入 termDict信息
   * @param field field information
   * @param valuesProducer produces the values to write
   * @throws IOException
   */
  @Override
  public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene80DocValuesFormat.SORTED_NUMERIC);

    long[] stats = writeValues(field, valuesProducer);
    int numDocsWithField = Math.toIntExact(stats[0]);
    long numValues = stats[1];
    assert numValues >= numDocsWithField;

    meta.writeInt(numDocsWithField);
    if (numValues > numDocsWithField) {
      long start = data.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

      final DirectMonotonicWriter addressesWriter = DirectMonotonicWriter.getInstance(meta, data, numDocsWithField + 1L, DIRECT_MONOTONIC_BLOCK_SHIFT);
      long addr = 0;
      addressesWriter.add(addr);
      SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
      for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
        addr += values.docValueCount();
        addressesWriter.add(addr);
      }
      addressesWriter.finish();
      meta.writeLong(data.getFilePointer() - start);
    }
  }

  @Override
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene80DocValuesFormat.SORTED_SET);

    SortedSetDocValues values = valuesProducer.getSortedSet(field);
    int numDocsWithField = 0;
    long numOrds = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithField++;
      for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
        numOrds++;
      }
    }

    // 代表每个doc下都只写入了一个field 这样就退化成和 addSortedField 逻辑一样了
    if (numDocsWithField == numOrds) {
      meta.writeByte((byte) 0); // multiValued (0 = singleValued)
      doAddSortedField(field, new EmptyDocValuesProducer() {
        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
          return SortedSetSelector.wrap(valuesProducer.getSortedSet(field), SortedSetSelector.Type.MIN);
        }
      });
      return;
    }
    meta.writeByte((byte) 1);  // multiValued (1 = multiValued)

    assert numDocsWithField != 0;
    if (numDocsWithField == maxDoc) {
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else {
      long offset = data.getFilePointer();
      meta.writeLong(offset);  // docsWithFieldOffset
      values = valuesProducer.getSortedSet(field);
      final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    int numberOfBitsPerOrd = DirectWriter.unsignedBitsRequired(values.getValueCount() - 1);
    meta.writeByte((byte) numberOfBitsPerOrd); // bitsPerValue
    long start = data.getFilePointer();
    meta.writeLong(start); // ordsOffset
    DirectWriter writer = DirectWriter.getInstance(data, numOrds, numberOfBitsPerOrd);
    values = valuesProducer.getSortedSet(field);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
        writer.add(ord);
      }
    }
    writer.finish();
    meta.writeLong(data.getFilePointer() - start); // ordsLength

    meta.writeInt(numDocsWithField);
    start = data.getFilePointer();
    meta.writeLong(start); // addressesOffset
    meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

    final DirectMonotonicWriter addressesWriter = DirectMonotonicWriter.getInstance(meta, data, numDocsWithField + 1, DIRECT_MONOTONIC_BLOCK_SHIFT);
    long addr = 0;
    addressesWriter.add(addr);
    values = valuesProducer.getSortedSet(field);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      values.nextOrd();
      addr++;
      while (values.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
        addr++;
      }
      addressesWriter.add(addr);
    }
    addressesWriter.finish();
    meta.writeLong(data.getFilePointer() - start); // addressesLength

    addTermsDict(values);
  }
}
