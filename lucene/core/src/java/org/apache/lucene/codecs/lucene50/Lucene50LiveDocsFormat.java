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
package org.apache.lucene.codecs.lucene50;


import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/** 
 * Lucene 5.0 live docs format 
 * <p>The .liv file is optional, and only exists when a segment contains
 * deletions.
 * <p>Although per-segment, this file is maintained exterior to compound segment
 * files.
 * <p>Deletions (.liv) --&gt; IndexHeader,Generation,Bits
 * <ul>
 *   <li>SegmentHeader --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *   <li>Bits --&gt; &lt;{@link DataOutput#writeLong Int64}&gt; <sup>LongCount</sup></li>
 * </ul>
 * 对应的索引文件中记录了 哪些doc还存活 以及当前有多少待删除的doc
 */
public final class Lucene50LiveDocsFormat extends LiveDocsFormat {
  
  /** Sole constructor. */
  public Lucene50LiveDocsFormat() {
  }
  
  /** extension of live docs */
  private static final String EXTENSION = "liv";
  
  /** codec of live docs */
  private static final String CODEC_NAME = "Lucene50LiveDocs";
  
  /** supported version range */
  private static final int VERSION_START = 0;
  private static final int VERSION_CURRENT = VERSION_START;

  /**
   * 获取当前还有效的 doc位图
   * @param dir
   * @param info
   * @param context
   * @return
   * @throws IOException
   */
  @Override
  public Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context) throws IOException {
    long gen = info.getDelGen();
    // 每一个 delGen 代表一次删除动作 同时也就产生了 本次还live的doc
    String name = IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, gen);
    // 记录该段下最大的doc 编号
    final int length = info.info.maxDoc();
    try (ChecksumIndexInput input = dir.openChecksumInput(name, context)) {
      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(input, CODEC_NAME, VERSION_START, VERSION_CURRENT, 
                                     info.info.getId(), Long.toString(gen, Character.MAX_RADIX));
        long data[] = new long[FixedBitSet.bits2words(length)];
        for (int i = 0; i < data.length; i++) {
          // 看来 liv 文件是直接将还存活的doc 生成位图 然后按long 存储
          data[i] = input.readLong();
        }
        FixedBitSet fbs = new FixedBitSet(data, length);
        if (fbs.length() - fbs.cardinality() != info.getDelCount()) {
          throw new CorruptIndexException("bits.deleted=" + (fbs.length() - fbs.cardinality()) + 
                                          " info.delcount=" + info.getDelCount(), input);
        }
        return fbs.asReadOnlyBits();
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(input, priorE);
      }
    }
    throw new AssertionError();
  }

  /**
   * 该方法一般是在 flush() 结束后 将此时还存活的doc标记出来   因为在解析doc时 虽然有些doc被标记成删除 但是它们还是写入到索引文件中了
   * @param bits
   * @param dir    索引文件写入的目标目录
   * @param info
   * @param newDelCount   本次新删除了多少数量
   * @param context
   * @throws IOException
   */
  @Override
  public void writeLiveDocs(Bits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context) throws IOException {

    // 每次针对同一个段 发生一次 更新liveDoc的动作 就会使用新的gen作为文件名后缀
    long gen = info.getNextDelGen();
    // 索引文件是以段为单位的  记录某个段 的哪个 gen 对应的liveDoc 和 delCount
    String name = IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, gen);

    // 每当某个doc不存在时 +1
    int delCount = 0;
    try (IndexOutput output = dir.createOutput(name, context)) {
      CodecUtil.writeIndexHeader(output, CODEC_NAME, VERSION_CURRENT, info.info.getId(), Long.toString(gen, Character.MAX_RADIX));
      // 位图总共有多少个long
      final int longCount = FixedBitSet.bits2words(bits.length());
      for (int i = 0; i < longCount; ++i) {
        long currentBits = 0;
        // 在遍历long的每个位
        for (int j = i << 6, end = Math.min(j + 63, bits.length() - 1); j <= end; ++j) {
          if (bits.get(j)) {
            currentBits |= 1L << j; // mod 64
          } else {
            delCount += 1;
          }
        }
        // 这里直接写入 long值  对应的就是位图中某个 block  此时long中已经记录了哪些doc是存活的
        output.writeLong(currentBits);
      }
      CodecUtil.writeFooter(output);
    }
    // info.getDelCount() 代表此前记录的删除的数量 是一个过期的值   通过新增本次删除的量 需要与 delCount相等
    if (delCount != info.getDelCount() + newDelCount) {
      throw new CorruptIndexException("bits.deleted=" + delCount + 
          " info.delcount=" + info.getDelCount() + " newdelcount=" + newDelCount, name);
    }
  }

  @Override
  public void files(SegmentCommitInfo info, Collection<String> files) throws IOException {
    // 代表这个段对象 至少触发过一次 删除操作
    if (info.hasDeletions()) {
      // 追加一个描述 alive状态的文件
      files.add(IndexFileNames.fileNameFromGeneration(info.info.name, EXTENSION, info.getDelGen()));
    }
  }
}
