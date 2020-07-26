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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.codecs.lucene80.Lucene80NormsFormat.VERSION_CURRENT;
import static org.apache.lucene.codecs.lucene80.Lucene80NormsFormat.VERSION_START;

/**
 * Reader for {@link Lucene80NormsFormat}
 * 该对象负责读取 按特定格式存储的 标准因子信息
 */
final class Lucene80NormsProducer extends NormsProducer implements Cloneable {
  // metadata maps (just file pointers and minimal stuff)
  private final Map<Integer,NormsEntry> norms = new HashMap<>();
  private final int maxDoc;
  private IndexInput data;
  /**
   * 该对象是为了merge而创建的副本对象
   */
  private boolean merging;
  private Map<Integer, IndexInput> disiInputs;
  private Map<Integer, RandomAccessInput> disiJumpTables;
  private Map<Integer, RandomAccessInput> dataInputs;

  Lucene80NormsProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    maxDoc = state.segmentInfo.maxDoc();
    String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    int version = -1;

    // read in the entries from the metadata file.
    try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
      Throwable priorE = null;
      try {
        version = CodecUtil.checkIndexHeader(in, metaCodec, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
        // 读取元数据信息  并生成entry
        readFields(in, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(in, priorE);
      }
    }

    String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    data = state.directory.openInput(dataName, state.context);
    boolean success = false;
    try {
      final int version2 = CodecUtil.checkIndexHeader(data, dataCodec, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      if (version != version2) {
        throw new CorruptIndexException("Format versions mismatch: meta=" + version + ",data=" + version2, data);
      }

      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(data);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this.data);
      }
    }
  }

  /**
   * 在merge过程中 生成一个reader对象的副本
   * @return
   */
  @Override
  public NormsProducer getMergeInstance() {
    Lucene80NormsProducer clone;
    try {
      clone = (Lucene80NormsProducer) super.clone();
    } catch (CloneNotSupportedException e) {
      // cannot happen
      throw new RuntimeException(e);
    }
    clone.data = data.clone();
    clone.disiInputs = new HashMap<>();
    clone.disiJumpTables = new HashMap<>();
    clone.dataInputs = new HashMap<>();
    clone.merging = true;
    return clone;
  }

  static class NormsEntry {
    byte denseRankPower;
    byte bytesPerNorm;
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    int numDocsWithField;
    /**
     * 记录某个field对应的标准因子存储的起始偏移量
     */
    long normsOffset;
  }

  /**
   * 代表doc连续存储
   */
  static abstract class DenseNormsIterator extends NumericDocValues {

    final int maxDoc;
    int doc = -1;

    DenseNormsIterator(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      this.doc = target;
      return true;
    }

    @Override
    public long cost() {
      return maxDoc;
    }

  }

  /**
   * 稀疏对象 唯一的区别就是 获取docId 和 迭代docId 委托给 disi对象
   */
  static abstract class SparseNormsIterator extends NumericDocValues {

    final IndexedDISI disi;

    SparseNormsIterator(IndexedDISI disi) {
      this.disi = disi;
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return disi.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return disi.advanceExact(target);
    }

    @Override
    public long cost() {
      return disi.cost();
    }
  }

  private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      } else if (!info.hasNorms()) {
        throw new CorruptIndexException("Invalid field: " + info.name, meta);
      }
      NormsEntry entry = new NormsEntry();
      entry.docsWithFieldOffset = meta.readLong();
      entry.docsWithFieldLength = meta.readLong();
      entry.jumpTableEntryCount = meta.readShort();
      entry.denseRankPower = meta.readByte();
      entry.numDocsWithField = meta.readInt();
      entry.bytesPerNorm = meta.readByte();
      switch (entry.bytesPerNorm) {
        case 0: case 1: case 2: case 4: case 8:
          break;
        default:
          throw new CorruptIndexException("Invalid bytesPerValue: " + entry.bytesPerNorm + ", field: " + info.name, meta);
      }
      entry.normsOffset = meta.readLong();
      norms.put(info.number, entry);
    }
  }

  /**
   * 为某个field 的标准因子创建数据分片
   * @param field
   * @param entry
   * @return
   * @throws IOException
   */
  private RandomAccessInput getDataInput(FieldInfo field, NormsEntry entry) throws IOException {
    RandomAccessInput slice = null;
    // 代表本次获取的数据是为了 merge使用
    if (merging) {
      slice = dataInputs.get(field.number);
    }
    if (slice == null) {
      // normsOffset 对应该field下记录的标准因子起始偏移量   每个doc都有一个标准因子值 numDocsWithField 代表该field下有多少doc
      slice = data.randomAccessSlice(entry.normsOffset, entry.numDocsWithField * (long) entry.bytesPerNorm);
      if (merging) {
        dataInputs.put(field.number, slice);
      }
    }
    return slice;
  }

  private IndexInput getDisiInput(FieldInfo field, NormsEntry entry) throws IOException {
    if (merging == false) {
      return IndexedDISI.createBlockSlice(
          data, "docs", entry.docsWithFieldOffset, entry.docsWithFieldLength, entry.jumpTableEntryCount);
    }

    IndexInput in = disiInputs.get(field.number);
    if (in == null) {
      in = IndexedDISI.createBlockSlice(
          data, "docs", entry.docsWithFieldOffset, entry.docsWithFieldLength, entry.jumpTableEntryCount);
      disiInputs.put(field.number, in);
    }

    final IndexInput inF = in; // same as in but final

    // Wrap so that reads can be interleaved from the same thread if two
    // norms instances are pulled and consumed in parallel. Merging usually
    // doesn't need this feature but CheckIndex might, plus we need merge
    // instances to behave well and not be trappy.
    return new IndexInput("docs") {

      long offset = 0;

      @Override
      public void readBytes(byte[] b, int off, int len) throws IOException {
        inF.seek(offset);
        offset += len;
        inF.readBytes(b, off, len);
      }

      @Override
      public byte readByte() throws IOException {
        throw new UnsupportedOperationException("Unused by IndexedDISI");
      }

      @Override
      public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        throw new UnsupportedOperationException("Unused by IndexedDISI");
      }

      @Override
      public short readShort() throws IOException {
        inF.seek(offset);
        offset += Short.BYTES;
        return inF.readShort();
      }

      @Override
      public long readLong() throws IOException {
        inF.seek(offset);
        offset += Long.BYTES;
        return inF.readLong();
      }

      @Override
      public void seek(long pos) throws IOException {
        offset = pos;
      }

      @Override
      public long length() {
        throw new UnsupportedOperationException("Unused by IndexedDISI");
      }

      @Override
      public long getFilePointer() {
        return offset;
      }

      @Override
      public void close() throws IOException {
        throw new UnsupportedOperationException("Unused by IndexedDISI");
      }
    };
  }

  private RandomAccessInput getDisiJumpTable(FieldInfo field, NormsEntry entry) throws IOException {
    RandomAccessInput jumpTable = null;
    if (merging) {
      jumpTable = disiJumpTables.get(field.number);
    }
    if (jumpTable == null) {
      jumpTable = IndexedDISI.createJumpTable(
          data, entry.docsWithFieldOffset, entry.docsWithFieldLength, entry.jumpTableEntryCount);
      if (merging) {
        disiJumpTables.put(field.number, jumpTable);
      }
    }
    return jumpTable;
 }


  /**
   * 获取某个field 对应的标准因子
   * @param field
   * @return
   * @throws IOException
   */
  @Override
  public NumericDocValues getNorms(FieldInfo field) throws IOException {
    // 先找到记录相关信息的元数据对象
    final NormsEntry entry = norms.get(field.number);
    // 同样存在3种情况 此时代表没有数据
    if (entry.docsWithFieldOffset == -2) {
      // empty
      return DocValues.emptyNumeric();
    } else if (entry.docsWithFieldOffset == -1) {
      // dense
      // 代表所有docId 对应的标准因子是一样的  这时normsOffset 本身就是标准因子 (对应 producer的存储逻辑)
      if (entry.bytesPerNorm == 0) {
        return new DenseNormsIterator(maxDoc) {
          @Override
          public long longValue() throws IOException {
            return entry.normsOffset;
          }
        };
      }
      // 将该field 对应的所有doc 的标准因子取出来组成一个 NumDocValue
      final RandomAccessInput slice = getDataInput(field, entry);
      // 从下面的逻辑可以看出标准因子的值是连续存储的
      switch (entry.bytesPerNorm) {
        case 1:
          return new DenseNormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readByte(doc);
            }
          };
        case 2:
          return new DenseNormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              // 因为每2个byte存储一个值 所以每次 pos也是移动2byte
              return slice.readShort(((long) doc) << 1);
            }
          };
        case 4:
          return new DenseNormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readInt(((long) doc) << 2);
            }
          };
        case 8:
          return new DenseNormsIterator(maxDoc) {
            @Override
            public long longValue() throws IOException {
              return slice.readLong(((long) doc) << 3);
            }
          };
        default:
          // should not happen, we already validate bytesPerNorm in readFields
          throw new AssertionError();
      }
    } else {
      // 唯一的区别就是迭代docId委托给 disi 对象
      // sparse
      final IndexInput disiInput = getDisiInput(field, entry);
      final RandomAccessInput disiJumpTable = getDisiJumpTable(field, entry);
      final IndexedDISI disi = new IndexedDISI(disiInput, disiJumpTable, entry.jumpTableEntryCount, entry.denseRankPower, entry.numDocsWithField);

      if (entry.bytesPerNorm == 0) {
        return new SparseNormsIterator(disi) {
          @Override
          public long longValue() throws IOException {
            return entry.normsOffset;
          }
        };
      }
      final RandomAccessInput slice = getDataInput(field, entry);
      switch (entry.bytesPerNorm) {
        case 1:
          return new SparseNormsIterator(disi) {
            @Override
            public long longValue() throws IOException {
              return slice.readByte(disi.index());
            }
          };
        case 2:
          return new SparseNormsIterator(disi) {
            @Override
            public long longValue() throws IOException {
              return slice.readShort(((long) disi.index()) << 1);
            }
          };
        case 4:
          return new SparseNormsIterator(disi) {
            @Override
            public long longValue() throws IOException {
              return slice.readInt(((long) disi.index()) << 2);
            }
          };
        case 8:
          return new SparseNormsIterator(disi) {
            @Override
            public long longValue() throws IOException {
              return slice.readLong(((long) disi.index()) << 3);
            }
          };
        default:
          // should not happen, we already validate bytesPerNorm in readFields
          throw new AssertionError();
      }
    }
  }

  @Override
  public void close() throws IOException {
    data.close();
  }

  @Override
  public long ramBytesUsed() {
    return 64L * norms.size(); // good enough
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + norms.size() + ")";
  }
}
