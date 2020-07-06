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

import static org.apache.lucene.codecs.compressing.FieldsIndexWriter.FIELDS_INDEX_EXTENSION_SUFFIX;
import static org.apache.lucene.codecs.compressing.FieldsIndexWriter.FIELDS_META_EXTENSION_SUFFIX;
import static org.apache.lucene.codecs.compressing.FieldsIndexWriter.VERSION_CURRENT;
import static org.apache.lucene.codecs.compressing.FieldsIndexWriter.VERSION_START;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * 这个对象 就是负责读取 FieldsIndexWriter 写入的数据
 */
final class FieldsIndexReader extends FieldsIndex {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FieldsIndexReader.class);

  private final int maxDoc;
  private final int blockShift;
  /**
   * 代表对应的 元数据写入了多少数据块 (每触发一个FieldIndexWriter.flush 写入一次)
   */
  private final int numChunks;
  private final DirectMonotonicReader.Meta docsMeta;
  private final DirectMonotonicReader.Meta startPointersMeta;
  private final IndexInput indexInput;
  private final long docsStartPointer, docsEndPointer, startPointersStartPointer, startPointersEndPointer;
  /**
   * docs  记录每次刷盘写入的 doc 数量
   * startPointers 记录每次刷盘时 索引文件的偏移量
   */
  private final DirectMonotonicReader docs, startPointers;
  private final long maxPointer;

  /**
   * 通过指定 目录 以及segmentName 和 拓展名
   * @param dir
   * @param name
   * @param suffix
   * @param extensionPrefix
   * @param codecName
   * @param id
   * @throws IOException
   */
  FieldsIndexReader(Directory dir, String name, String suffix, String extensionPrefix, String codecName, byte[] id) throws IOException {
    try (ChecksumIndexInput metaIn = dir.openChecksumInput(IndexFileNames.segmentFileName(name, suffix, extensionPrefix + FIELDS_META_EXTENSION_SUFFIX), IOContext.READONCE)) {
      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(metaIn, codecName + "Meta", VERSION_START, VERSION_CURRENT, id, suffix);

        /**
         * 对应这里
         *  // 为元数据文件写入数据
         *       metaOut.writeInt(numDocs);
         *       metaOut.writeInt(blockShift);
         *       metaOut.writeInt(totalChunks + 1); // 总计刷盘多少次
         *       metaOut.writeLong(dataOut.getFilePointer());  // dataOut文件上次的偏移量
         */
        maxDoc = metaIn.readInt();
        blockShift = metaIn.readInt();
        numChunks = metaIn.readInt();
        // 标记从哪里开始读取dataOut的数据
        docsStartPointer = metaIn.readLong();
        // 创建 DirectMonotonicWriter 的对应对象  该对象只允许写入单调递增的数据  先将每个数据减去平均数 然后再减去差值 后按位存储
        // 这里读取出了 有关写入 docsNum 的信息
        docsMeta = DirectMonotonicReader.loadMeta(metaIn, numChunks, blockShift);
        // dataOut 末尾
        docsEndPointer = startPointersStartPointer = metaIn.readLong();
        // 这里读取出了 有关写入 filePosition 的信息
        // 注意数据是写入到一个 data文件中的
        startPointersMeta = DirectMonotonicReader.loadMeta(metaIn, numChunks, blockShift);
        // 这个就是data文件的偏移量
        startPointersEndPointer = metaIn.readLong();
        // 这个是索引文件最后的偏移量  上面读取的是元数据文件的偏移量
        maxPointer = metaIn.readLong();
      } finally {
        CodecUtil.checkFooter(metaIn, priorE);
      }
    }

    // 开始读取 data 元数据文件
    indexInput = dir.openInput(IndexFileNames.segmentFileName(name, suffix, extensionPrefix + FIELDS_INDEX_EXTENSION_SUFFIX), IOContext.READ);
    boolean success = false;
    try {
      CodecUtil.checkIndexHeader(indexInput, codecName + "Idx", VERSION_START, VERSION_CURRENT, id, suffix);
      CodecUtil.retrieveChecksum(indexInput);
      success = true;
    } finally {
      if (success == false) {
        indexInput.close();
      }
    }
    // 创建了2个文件分片对象 支持随机读取
    final RandomAccessInput docsSlice = indexInput.randomAccessSlice(docsStartPointer, docsEndPointer - docsStartPointer);
    final RandomAccessInput startPointersSlice = indexInput.randomAccessSlice(startPointersStartPointer, startPointersEndPointer - startPointersStartPointer);
    // 生成reader 对象 该对象在读取时会自动将处理过的数据还原
    docs = DirectMonotonicReader.getInstance(docsMeta, docsSlice);
    startPointers = DirectMonotonicReader.getInstance(startPointersMeta, startPointersSlice);
  }

  /**
   * 通过另一个对象进行初始化
   * @param other
   * @throws IOException
   */
  private FieldsIndexReader(FieldsIndexReader other) throws IOException {
    maxDoc = other.maxDoc;
    numChunks = other.numChunks;
    blockShift = other.blockShift;
    docsMeta = other.docsMeta;
    startPointersMeta = other.startPointersMeta;
    indexInput = other.indexInput.clone();
    docsStartPointer = other.docsStartPointer;
    docsEndPointer = other.docsEndPointer;
    startPointersStartPointer = other.startPointersStartPointer;
    startPointersEndPointer = other.startPointersEndPointer;
    maxPointer = other.maxPointer;
    final RandomAccessInput docsSlice = indexInput.randomAccessSlice(docsStartPointer, docsEndPointer - docsStartPointer);
    final RandomAccessInput startPointersSlice = indexInput.randomAccessSlice(startPointersStartPointer, startPointersEndPointer - startPointersStartPointer);
    docs = DirectMonotonicReader.getInstance(docsMeta, docsSlice);
    startPointers = DirectMonotonicReader.getInstance(startPointersMeta, startPointersSlice);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + docsMeta.ramBytesUsed() + startPointersMeta.ramBytesUsed() +
        docs.ramBytesUsed() + startPointers.ramBytesUsed();
  }

  @Override
  public void close() throws IOException {
    indexInput.close();
  }

  /**
   * 获取某个doc 在索引文件中的 起点   TODO 存储的数据 不是 每次flush写入几个doc吗 数据好像对不上???
   * @param docID
   * @return
   */
  @Override
  long getStartPointer(int docID) {
    Objects.checkIndex(docID, maxDoc);
    long blockIndex = docs.binarySearch(0, numChunks, docID);
    if (blockIndex < 0) {
      blockIndex = -2 - blockIndex;
    }
    return startPointers.get(blockIndex);
  }

  @Override
  public FieldsIndex clone() {
    try {
      return new FieldsIndexReader(this);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public long getMaxPointer() {
    return maxPointer;
  }

  @Override
  void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(indexInput);
  }
}
