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
package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

/** Buffers up pending byte[][] value(s) per doc, then flushes when segment flushes. */
// 负责写入 多维度数据
class PointValuesWriter {
  private final FieldInfo fieldInfo;
  private final ByteBlockPool bytes;
  private final Counter iwBytesUsed;


  /**
   * 可能多个 points 对应的是同一个docID
   */
  private int[] docIDs;

  /**
   * 总计写入多少次
   */
  private int numPoints;
  /**
   * 总计写入到多少 doc中
   */
  private int numDocs;
  private int lastDocID = -1;
  /**
   * 总长度 通过 field下相关的维度数*每个维度占用的bytes 计算获得
   */
  private final int packedBytesLength;

  /**
   *
   * @param docWriter  关联的doc写入对象
   * @param fieldInfo
   */
  public PointValuesWriter(DocumentsWriterPerThread docWriter, FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = docWriter.bytesUsed;
    this.bytes = new ByteBlockPool(docWriter.byteBlockAllocator);
    docIDs = new int[16];
    iwBytesUsed.addAndGet(16 * Integer.BYTES);
    packedBytesLength = fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes();
  }

  // TODO: if exactly the same value is added to exactly the same doc, should we dedup?
  // 写入多维度信息
  public void addPackedValue(int docID, BytesRef value) {
    if (value == null) {
      throw new IllegalArgumentException("field=" + fieldInfo.name + ": point value must not be null");
    }
    // 那么 value 应该就是联合了多个维度的数据
    if (value.length != packedBytesLength) {
      throw new IllegalArgumentException("field=" + fieldInfo.name + ": this field's value has length=" + value.length + " but should be " + (fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes()));
    }

    // 对doc数组扩容 不影响别的
    if (docIDs.length == numPoints) {
      docIDs = ArrayUtil.grow(docIDs, numPoints+1);
      iwBytesUsed.addAndGet((docIDs.length - numPoints) * Integer.BYTES);
    }
    bytes.append(value);
    // 记录当前第几维度 对应的docId   也就是多次写入的数据可以在同一doc中   但是有些 DocValueWriter 严格要求 doc下同一field 只能出现一次
    docIDs[numPoints] = docID;
    // 当切换doc时 更新lastDocID
    if (docID != lastDocID) {
      numDocs++;
      lastDocID = docID;
    }

    numPoints++;
  }

  /**
   * 将之前存储的多维度信息写入到 writer中
   * @param state
   * @param sortMap
   * @param writer
   * @throws IOException
   */
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, PointsWriter writer) throws IOException {

    //
    PointValues points = new MutablePointValues() {
      // numPoints 记录总计写入了多少值   根据写入的值生成顺序数组
      final int[] ords = new int[numPoints];
      {
        for (int i = 0; i < numPoints; ++i) {
          ords[i] = i;
        }
      }

      @Override
      public void intersect(IntersectVisitor visitor) throws IOException {
        final BytesRef scratch = new BytesRef();
        // 用于填装多个维度的数据 的数组
        final byte[] packedValue = new byte[packedBytesLength];
        for(int i=0;i<numPoints;i++) {
          getValue(i, scratch);
          assert scratch.length == packedValue.length;
          System.arraycopy(scratch.bytes, scratch.offset, packedValue, 0, packedBytesLength);
          // 通过visitor来解析同时存储多个维度数据的 packedValue
          visitor.visit(getDocID(i), packedValue);
        }
      }

      @Override
      public long estimatePointCount(IntersectVisitor visitor) {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getMinPackedValue() {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getMaxPackedValue() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumDimensions() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumIndexDimensions() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getBytesPerDimension() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long size() {
        return numPoints;
      }

      @Override
      public int getDocCount() {
        return numDocs;
      }

      @Override
      public void swap(int i, int j) {
        int tmp = ords[i];
        ords[i] = ords[j];
        ords[j] = tmp;
      }

      @Override
      public int getDocID(int i) {
        return docIDs[ords[i]];
      }

      @Override
      public void getValue(int i, BytesRef packedValue) {
        final long offset = (long) packedBytesLength * ords[i];
        packedValue.length = packedBytesLength;
        bytes.setRawBytesRef(packedValue, offset);
      }

      @Override
      public byte getByteAt(int i, int k) {
        final long offset = (long) packedBytesLength * ords[i] + k;
        return bytes.readByte(offset);
      }
    };

    final PointValues values;
    if (sortMap == null) {
      values = points;
    } else {
      // TODO 先忽略排序的情况
      values = new MutableSortingPointValues((MutablePointValues) points, sortMap);
    }

    // 该对象负责读取多维度数据
    PointsReader reader = new PointsReader() {
      @Override
      public PointValues getValues(String fieldName) {
        if (fieldName.equals(fieldInfo.name) == false) {
          throw new IllegalArgumentException("fieldName must be the same");
        }
        // 返回的就是 MutablePointValues  它需要借助 PointVisitor 来将多维度数据解开 之前写入的 field.value 实际上是多个维度的数据整合起来
        return values;
      }

      @Override
      public void checkIntegrity() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ramBytesUsed() {
        return 0L;
      }

      @Override
      public void close() {
      }
    };

    // TODO 这里不看bkd树的实现了 所以就当作写入成功
    writer.writeField(fieldInfo, reader);
  }

  static final class MutableSortingPointValues extends MutablePointValues {

    private final MutablePointValues in;
    private final Sorter.DocMap docMap;

    public MutableSortingPointValues(final MutablePointValues in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException {
      in.intersect(new IntersectVisitor() {
        @Override
        public void visit(int docID) throws IOException {
          visitor.visit(docMap.oldToNew(docID));
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
          visitor.visit(docMap.oldToNew(docID), packedValue);
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
          return visitor.compare(minPackedValue, maxPackedValue);
        }
      });
    }

    @Override
    public long estimatePointCount(IntersectVisitor visitor) {
      return in.estimatePointCount(visitor);
    }

    @Override
    public byte[] getMinPackedValue() throws IOException {
      return in.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() throws IOException {
      return in.getMaxPackedValue();
    }

    @Override
    public int getNumDimensions() throws IOException {
      return in.getNumDimensions();
    }

    @Override
    public int getNumIndexDimensions() throws IOException {
      return in.getNumIndexDimensions();
    }

    @Override
    public int getBytesPerDimension() throws IOException {
      return in.getBytesPerDimension();
    }

    @Override
    public long size() {
      return in.size();
    }

    @Override
    public int getDocCount() {
      return in.getDocCount();
    }

    @Override
    public void getValue(int i, BytesRef packedValue) {
      in.getValue(i, packedValue);
    }

    @Override
    public byte getByteAt(int i, int k) {
      return in.getByteAt(i, k);
    }

    @Override
    public int getDocID(int i) {
      return docMap.oldToNew(in.getDocID(i));
    }

    @Override
    public void swap(int i, int j) {
      in.swap(i, j);
    }
  }
}
