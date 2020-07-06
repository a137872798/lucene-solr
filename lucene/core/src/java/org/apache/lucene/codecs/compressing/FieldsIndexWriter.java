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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Efficient index format for block-based {@link Codec}s.
 * <p>For each block of compressed stored fields, this stores the first document
 * of the block and the start pointer of the block in a
 * {@link DirectMonotonicWriter}. At read time, the docID is binary-searched in
 * the {@link DirectMonotonicReader} that records doc IDS, and the returned
 * index is used to look up the start pointer in the
 * {@link DirectMonotonicReader} that records start pointers.
 * @lucene.internal
 * 该对象 负责将 域信息写入到索引文件中
 * 每当创建一个  索引文件时 就需要该对象写入数据
 */
public final class FieldsIndexWriter implements Closeable {

  /** Extension of stored fields index file. */
  public static final String FIELDS_INDEX_EXTENSION_SUFFIX = "x";

  /** Extension of stored fields meta file. */
  public static final String FIELDS_META_EXTENSION_SUFFIX = "m";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = 0;

  /**
   * 代表数据会写入到哪个目录
    */
  private final Directory dir;
  /**
   * 段名
   */
  private final String name;
  /**
   * 段后缀
   */
  private final String suffix;
  /**
   * 文件拓展名
   */
  private final String extension;
  /**
   * "Lucene85FieldsIndex"
   */
  private final String codecName;
  /**
   * 段id
   */
  private final byte[] id;
  private final int blockShift;
  private final IOContext ioContext;
  // 这里2个输出流 对应2个临时文件
  private IndexOutput docsOut;

  /**
   * 每当 某个索引writer 对象 finish 之前 都会先将一些信息记录在该对象中 比如 filePointersOut 就是记录当前 索引文件正要写入的偏移量
   */
  private IndexOutput filePointersOut;
  /**
   * 记录总计写入了多少 doc
   */
  private int totalDocs;
  /**
   * 代表该对象对应的 索引写入对象发起了几次刷盘操作
   */
  private int totalChunks;
  /**
   * 上一次 filePointer的位置 主要用于差值存储
   */
  private long previousFP;

  /**
   *
   * @param dir  写入目标
   * @param name  segmentName
   * @param suffix  segmentSuffix
   * @param extension  拓展名 由索引文件格式决定
   * @param codecName   编码方式名
   * @param id   segmentId
   * @param blockShift
   * @param ioContext
   * @throws IOException
   */
  FieldsIndexWriter(Directory dir, String name, String suffix, String extension,
      String codecName, byte[] id, int blockShift, IOContext ioContext) throws IOException {
    this.dir = dir;
    this.name = name;
    this.suffix = suffix;
    this.extension = extension;
    this.codecName = codecName;
    this.id = id;
    this.blockShift = blockShift;
    this.ioContext = ioContext;

    // 每当要写入索引文件时 会伴随着2个临时文件  -doc_ids 和 file_pointers
    // 这里创建临时文件
    this.docsOut = dir.createTempOutput(name, codecName + "-doc_ids", ioContext);
    boolean success = false;
    try {
      // 写入文件头
      CodecUtil.writeHeader(docsOut, codecName + "Docs", VERSION_CURRENT);
      // 创建临时文件 + 写入文件头
      filePointersOut = dir.createTempOutput(name, codecName + "file_pointers", ioContext);
      CodecUtil.writeHeader(filePointersOut, codecName + "FilePointers", VERSION_CURRENT);
      success = true;
    } finally {
      if (success == false) {
        // 创建失败时  释放文件句柄 删除临时文件
        close();
      }
    }
  }

  /**
   * 将信息写入到索引中
   * @param numDocs   代表本次要写入多少doc
   * @param startPointer  这个是 某个索引文件当前写入到的偏移量
   * @throws IOException
   */
  void writeIndex(int numDocs, long startPointer) throws IOException {
    assert startPointer >= previousFP;
    docsOut.writeVInt(numDocs);
    // 这里每次存储的都是一个差值
    filePointersOut.writeVLong(startPointer - previousFP);
    previousFP = startPointer;
    totalDocs += numDocs;
    // 每次写入索引对象的 flush 都对应一个chunk
    totalChunks++;
  }

  /**
   * 代表所有的 doc已经处理完毕了
   * @param numDocs  总计写入了多少doc
   * @param maxPointer  此时索引文件最后的偏移量
   * @throws IOException
   */
  void finish(int numDocs, long maxPointer) throws IOException {
    // 此时要求的 doc数量必须是之前所有 writerIndex 写入的数量总和
    if (numDocs != totalDocs) {
      throw new IllegalStateException("Expected " + numDocs + " docs, but got " + totalDocs);
    }
    // 写入结尾标记  以及校验和
    CodecUtil.writeFooter(docsOut);
    CodecUtil.writeFooter(filePointersOut);
    // 关闭文件流  但是此时的文件后缀名还是 tmp (临时文件)
    IOUtils.close(docsOut, filePointersOut);

    // 创建2个元数据文件   tvm   tvx
    try (IndexOutput metaOut = dir.createOutput(IndexFileNames.segmentFileName(name, suffix, extension + FIELDS_META_EXTENSION_SUFFIX), ioContext);
        IndexOutput dataOut = dir.createOutput(IndexFileNames.segmentFileName(name, suffix, extension + FIELDS_INDEX_EXTENSION_SUFFIX), ioContext)) {

      CodecUtil.writeIndexHeader(metaOut, codecName + "Meta", VERSION_CURRENT, id, suffix);
      CodecUtil.writeIndexHeader(dataOut, codecName + "Idx", VERSION_CURRENT, id, suffix);

      // 为元数据文件写入数据
      metaOut.writeInt(numDocs);
      metaOut.writeInt(blockShift);
      metaOut.writeInt(totalChunks + 1); // 总计刷盘多少次
      metaOut.writeLong(dataOut.getFilePointer());  // dataOut文件上次的偏移量

      // docsOut 记录每次flush 多少文档
      try (ChecksumIndexInput docsIn = dir.openChecksumInput(docsOut.getName(), IOContext.READONCE)) {
        // 检查头部与之前写入的是否一致
        CodecUtil.checkHeader(docsIn, codecName + "Docs", VERSION_CURRENT, VERSION_CURRENT);
        Throwable priorE = null;
        try {
          // 通过2个输出流对象 创建 writer   因为本次是最后一次刷盘 所以totalChunk + 1
          final DirectMonotonicWriter docs = DirectMonotonicWriter.getInstance(metaOut, dataOut, totalChunks + 1, blockShift);
          long doc = 0;
          // 往 dataOut中写入 每次flush的doc数量   先写入基数0
          docs.add(doc);
          // 以单调递增的方式 写入
          for (int i = 0; i < totalChunks; ++i) {
            doc += docsIn.readVInt();
            docs.add(doc);
          }
          docs.finish();
          if (doc != totalDocs) {
            throw new CorruptIndexException("Docs don't add up", docsIn);
          }
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(docsIn, priorE);
        }
      }
      // 删除临时文件
      dir.deleteFile(docsOut.getName());
      docsOut = null;

      // 写入最后的data文件偏移量
      metaOut.writeLong(dataOut.getFilePointer());
      // 这里将文件的偏移量数据 也写入到 metaOut中
      try (ChecksumIndexInput filePointersIn = dir.openChecksumInput(filePointersOut.getName(), IOContext.READONCE)) {
        CodecUtil.checkHeader(filePointersIn, codecName + "FilePointers", VERSION_CURRENT, VERSION_CURRENT);
        Throwable priorE = null;
        try {
          final DirectMonotonicWriter filePointers = DirectMonotonicWriter.getInstance(metaOut, dataOut, totalChunks + 1, blockShift);
          long fp = 0;
          // 写入每次文件的偏移量
          for (int i = 0; i < totalChunks; ++i) {
            fp += filePointersIn.readVLong();
            filePointers.add(fp);
          }
          if (maxPointer < fp) {
            throw new CorruptIndexException("File pointers don't add up", filePointersIn);
          }
          filePointers.add(maxPointer);
          filePointers.finish();
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(filePointersIn, priorE);
        }
      }
      // 删除临时文件
      dir.deleteFile(filePointersOut.getName());
      filePointersOut = null;

      metaOut.writeLong(dataOut.getFilePointer());
      metaOut.writeLong(maxPointer);

      CodecUtil.writeFooter(metaOut);
      CodecUtil.writeFooter(dataOut);
    }
  }

  /**
   * 当初始化过程失败时 释放资源 并删除临时文件
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(docsOut, filePointersOut);
    } finally {
      List<String> fileNames = new ArrayList<>();
      if (docsOut != null) {
        fileNames.add(docsOut.getName());
      }
      if (filePointersOut != null) {
        fileNames.add(filePointersOut.getName());
      }
      try {
        IOUtils.deleteFiles(dir, fileNames);
      } finally {
        docsOut = filePointersOut = null;
      }
    }
  }
}
