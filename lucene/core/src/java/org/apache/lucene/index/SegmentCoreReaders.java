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


import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.index.IndexReader.ClosedListener;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;

/** Holds core readers that are shared (unchanged) when
 * SegmentReader is cloned or reopened
 * 该对象内部组合了各种输入流  可以以field为单位读取全部相关信息  相当于一个总入口
 */
final class SegmentCoreReaders {

  // Counts how many other readers share the core objects
  // (freqStream, proxStream, tis, etc.) of this reader;
  // when coreRef drops to 0, these core objects may be
  // closed.  A given instance of SegmentReader may be
  // closed, even though it shares core objects with other
  // SegmentReaders:
  private final AtomicInteger ref = new AtomicInteger(1);

  // 下面对应各种索引文件的输入流
  final FieldsProducer fields;
  final NormsProducer normsProducer;

  final StoredFieldsReader fieldsReaderOrig;
  final TermVectorsReader termVectorsReaderOrig;
  final PointsReader pointsReader;
  final CompoundDirectory cfsReader;
  /**
   * 对应该 segment的名称
   */
  final String segment;
  /** 
   * fieldinfos for this core: means gen=-1.
   * this is the exact fieldinfos these codec components saw at write.
   * in the case of DV updates, SR may hold a newer version. */
  // 该段下所有 field 信息
  final FieldInfos coreFieldInfos;

  // TODO: make a single thread local w/ a
  // Thingy class holding fieldsReader, termVectorsReader,
  // normsProducer

  final CloseableThreadLocal<StoredFieldsReader> fieldsReaderLocal = new CloseableThreadLocal<StoredFieldsReader>() {
    @Override
    protected StoredFieldsReader initialValue() {
      return fieldsReaderOrig.clone();
    }
  };
  
  final CloseableThreadLocal<TermVectorsReader> termVectorsLocal = new CloseableThreadLocal<TermVectorsReader>() {
    @Override
    protected TermVectorsReader initialValue() {
      return (termVectorsReaderOrig == null) ? null : termVectorsReaderOrig.clone();
    }
  };

  private final Set<IndexReader.ClosedListener> coreClosedListeners = 
      Collections.synchronizedSet(new LinkedHashSet<IndexReader.ClosedListener>());

  /**
   *
   * @param dir  指定段数据所在的目录
   * @param si   描述某个段的提交信息
   * @param context
   * @throws IOException
   */
  SegmentCoreReaders(Directory dir, SegmentCommitInfo si, IOContext context) throws IOException {

    // 获取该段对应的索引文件结构
    final Codec codec = si.info.getCodec();
    final Directory cfsDir; // confusing name: if (cfs) it's the cfsdir, otherwise it's the segment's directory.
    boolean success = false;
    
    try {
      // 代表生成了 复合模式的文件   TODO 复合模式就不看了 这个不影响主流程
      if (si.info.getUseCompoundFile()) {
        cfsDir = cfsReader = codec.compoundFormat().getCompoundReader(dir, si.info, context);
      } else {
        cfsReader = null;
        cfsDir = dir;
      }

      segment = si.info.name;

      // 先还原该段下所有的 fieldInfo
      coreFieldInfos = codec.fieldInfosFormat().read(cfsDir, si.info, "", context);

      // 生成一个读取时的上下文对象
      final SegmentReadState segmentReadState = new SegmentReadState(cfsDir, si.info, coreFieldInfos, context);


      final PostingsFormat format = codec.postingsFormat();
      // Ask codec for its Fields
      // 该对象可以读取 每个field 下的position/payload 等信息
      fields = format.fieldsProducer(segmentReadState);
      assert fields != null;
      // ask codec for its Norms: 
      // TODO: since we don't write any norms file if there are no norms,
      // kinda jaky to assume the codec handles the case of no norms file at all gracefully?!

      // 如果还包含标准因子 还需要生成对应的 reader 对象
      if (coreFieldInfos.hasNorms()) {
        normsProducer = codec.normsFormat().normsProducer(segmentReadState);
        assert normsProducer != null;
      } else {
        normsProducer = null;
      }

      // 获取存储 field 信息的索引文件输入流
      fieldsReaderOrig = si.info.getCodec().storedFieldsFormat().fieldsReader(cfsDir, si.info, coreFieldInfos, context);

      // 如果包含词向量 还要再生成读取对象
      if (coreFieldInfos.hasVectors()) { // open term vector files only as needed
        termVectorsReaderOrig = si.info.getCodec().termVectorsFormat().vectorsReader(cfsDir, si.info, coreFieldInfos, context);
      } else {
        termVectorsReaderOrig = null;
      }

      if (coreFieldInfos.hasPointValues()) {
        pointsReader = codec.pointsFormat().fieldsReader(segmentReadState);
      } else {
        pointsReader = null;
      }
      success = true;
    } catch (EOFException | FileNotFoundException e) {
      throw new CorruptIndexException("Problem reading index from " + dir, dir.toString(), e);
    } catch (NoSuchFileException e) {
      throw new CorruptIndexException("Problem reading index.", e.getFile(), e);
    } finally {
      if (!success) {
        decRef();
      }
    }
  }
  
  int getRefCount() {
    return ref.get();
  }
  
  void incRef() {
    int count;
    while ((count = ref.get()) > 0) {
      if (ref.compareAndSet(count, count+1)) {
        return;
      }
    }
    throw new AlreadyClosedException("SegmentCoreReaders is already closed");
  }

  @SuppressWarnings("try")
  void decRef() throws IOException {
    if (ref.decrementAndGet() == 0) {
      Throwable th = null;
      try (Closeable finalizer = this::notifyCoreClosedListeners){
        IOUtils.close(termVectorsLocal, fieldsReaderLocal, fields, termVectorsReaderOrig, fieldsReaderOrig,
                      cfsReader, normsProducer, pointsReader);
      }
    }
  }

  private final IndexReader.CacheHelper cacheHelper = new IndexReader.CacheHelper() {
    private final IndexReader.CacheKey cacheKey = new IndexReader.CacheKey();

    @Override
    public CacheKey getKey() {
      return cacheKey;
    }

    @Override
    public void addClosedListener(ClosedListener listener) {
      coreClosedListeners.add(listener);
    }
  };

  IndexReader.CacheHelper getCacheHelper() {
    return cacheHelper;
  }

  private void notifyCoreClosedListeners() throws IOException {
    synchronized(coreClosedListeners) {
      IOUtils.applyToAll(coreClosedListeners, l -> l.onClose(cacheHelper.getKey()));
    }
  }

  @Override
  public String toString() {
    return "SegmentCoreReader(" + segment + ")";
  }
}
