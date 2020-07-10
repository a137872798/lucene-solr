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
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * IndexReader implementation over a single segment. 
 * <p>
 * Instances pointing to the same segment (but with different deletes, etc)
 * may share the same core data.
 * @lucene.experimental
 * 该对象以一个段为单位 内部存储了 获取该段下所有field信息的 reader总控对象 (core)   而其他模块与reader模块的交互是以该类作为入口
 */
public final class SegmentReader extends CodecReader {

  /**
   * 这是一个副本对象
   */
  private final SegmentCommitInfo si;
  // this is the original SI that IW uses internally but it's mutated behind the scenes
  // and we don't want this SI to be used for anything. Yet, IW needs this to do maintainance
  // and lookup pooled readers etc.
  private final SegmentCommitInfo originalSi;
  private final LeafMetaData metaData;
  private final Bits liveDocs;
  private final Bits hardLiveDocs;

  // Normally set to si.maxDoc - si.delDocCount, unless we
  // were created as an NRT reader from IW, in which case IW
  // tells us the number of live docs:
  // 此时segment 下还有多少doc
  private final int numDocs;

  final SegmentCoreReaders core;
  final SegmentDocValues segDocValues;

  /** True if we are holding RAM only liveDocs or DV updates, i.e. the SegmentCommitInfo delGen doesn't match our liveDocs. */
  final boolean isNRT;
  
  final DocValuesProducer docValuesProducer;

  /**
   * 对应最新的 fieldInfo 信息  因为 segment 下fieldInfo 可能会发生变化
   */
  final FieldInfos fieldInfos;

  /**
   * Constructs a new SegmentReader with a new core.
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * 初始化一个用于读取 段信息的对象
   */
  SegmentReader(SegmentCommitInfo si, int createdVersionMajor, IOContext context) throws IOException {
    this.si = si.clone();
    this.originalSi = si;
    // 生成段数据页的元数据
    this.metaData = new LeafMetaData(createdVersionMajor, si.info.getMinVersion(), si.info.getIndexSort());

    // We pull liveDocs/DV updates from disk:
    this.isNRT = false;

    // 生成 segment 读取对象
    core = new SegmentCoreReaders(si.info.dir, si, context);
    segDocValues = new SegmentDocValues();
    
    boolean success = false;
    // 获取编解码格式对象
    final Codec codec = si.info.getCodec();
    try {
      // 代表发生过删除动作
      if (si.hasDeletions()) {
        // NOTE: the bitvector is stored using the regular directory, not cfs
        // 获取记录当前还live的doc位图
        hardLiveDocs = liveDocs = codec.liveDocsFormat().readLiveDocs(directory(), si, IOContext.READONCE);
      } else {
        assert si.getDelCount() == 0;
        // 为null 应该代表所有doc都存活吧
        hardLiveDocs = liveDocs = null;
      }
      // 代表此时该段下还有多少doc
      numDocs = si.info.maxDoc() - si.getDelCount();
      
      fieldInfos = initFieldInfos();
      // 初始化读取 docValue的 reader 对象
      docValuesProducer = initDocValuesProducer();
      assert assertLiveDocs(isNRT, hardLiveDocs, liveDocs);
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above.  In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        doClose();
      }
    }
  }

  /** Create new SegmentReader sharing core from a previous
   *  SegmentReader and using the provided liveDocs, and recording
   *  whether those liveDocs were carried in ram (isNRT=true).
   *
   *  创建一个新的 reader对象 内部大量属性使用传入的 sr
   */
  SegmentReader(SegmentCommitInfo si, SegmentReader sr, Bits liveDocs, Bits hardLiveDocs, int numDocs, boolean isNRT) throws IOException {
    if (numDocs > si.info.maxDoc()) {
      throw new IllegalArgumentException("numDocs=" + numDocs + " but maxDoc=" + si.info.maxDoc());
    }
    if (liveDocs != null && liveDocs.length() != si.info.maxDoc()) {
      throw new IllegalArgumentException("maxDoc=" + si.info.maxDoc() + " but liveDocs.size()=" + liveDocs.length());
    }
    this.si = si.clone();
    this.originalSi = si;
    this.metaData = sr.getMetaData();
    // 这里没有使用 sr的位图对象 而是使用外部传入的
    this.liveDocs = liveDocs;
    this.hardLiveDocs = hardLiveDocs;
    assert assertLiveDocs(isNRT, hardLiveDocs, liveDocs);
    this.isNRT = isNRT;
    this.numDocs = numDocs;
    this.core = sr.core;
    core.incRef();
    this.segDocValues = sr.segDocValues;

    boolean success = false;
    try {
      // 如果fieldInfo 没有变化 使用之前读取到的   否则重新从索引文件中读取
      fieldInfos = initFieldInfos();
      docValuesProducer = initDocValuesProducer();
      success = true;
    } finally {
      if (!success) {
        doClose();
      }
    }
  }

  private static boolean assertLiveDocs(boolean isNRT, Bits hardLiveDocs, Bits liveDocs) {
    if (isNRT) {
      assert hardLiveDocs == null || liveDocs != null : " liveDocs must be non null if hardLiveDocs are non null";
    } else {
      assert hardLiveDocs == liveDocs : "non-nrt case must have identical liveDocs";
    }
    return true;
  }

  /**
   * init most recent DocValues for the current commit
   */
  private DocValuesProducer initDocValuesProducer() throws IOException {

    // 如果所有field 信息都不包含 docValue 就不用生成reader 对象
    if (fieldInfos.hasDocValues() == false) {
      return null;
    } else {
      Directory dir;
      if (core.cfsReader != null) {
        dir = core.cfsReader;
      } else {
        dir = si.info.dir;
      }
      // 如果 fieldInfo 发生了变化
      if (si.hasFieldUpdates()) {
        // 该对象维护了  通过field 查找 对应的docValue reader
        return new SegmentDocValuesProducer(si, dir, core.coreFieldInfos, fieldInfos, segDocValues);
      } else {
        // simple case, no DocValues updates
        return segDocValues.getDocValuesProducer(-1L, si, dir, fieldInfos);
      }
    }
  }
  
  /**
   * init most recent FieldInfos for the current commit
   */
  private FieldInfos initFieldInfos() throws IOException {
    // 如果 fieldInfo 没有发生变化 直接返回一开始读取的fieldInfo
    if (!si.hasFieldUpdates()) {
      return core.coreFieldInfos;
    } else {
      // updates always outside of CFS
      // 代表发生了变化 使用最新的 gen 去寻找索引文件 并读取数据
      FieldInfosFormat fisFormat = si.info.getCodec().fieldInfosFormat();
      final String segmentSuffix = Long.toString(si.getFieldInfosGen(), Character.MAX_RADIX);
      return fisFormat.read(si.info.dir, si.info, segmentSuffix, IOContext.READONCE);
    }
  }
  
  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return liveDocs;
  }

  /**
   * 当该对象被关闭时 会释放所有文件句柄
   * @throws IOException
   */
  @Override
  protected void doClose() throws IOException {
    //System.out.println("SR.close seg=" + si);
    try {
      core.decRef();
    } finally {
      if (docValuesProducer instanceof SegmentDocValuesProducer) {
        segDocValues.decRef(((SegmentDocValuesProducer)docValuesProducer).dvGens);
      } else if (docValuesProducer != null) {
        segDocValues.decRef(Collections.singletonList(-1L));
      }
    }
  }

  @Override
  public FieldInfos getFieldInfos() {
    ensureOpen();
    return fieldInfos;
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return numDocs;
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return si.info.maxDoc();
  }

  @Override
  public TermVectorsReader getTermVectorsReader() {
    ensureOpen();
    return core.termVectorsLocal.get();
  }

  @Override
  public StoredFieldsReader getFieldsReader() {
    ensureOpen();
    return core.fieldsReaderLocal.get();
  }
  
  @Override
  public PointsReader getPointsReader() {
    ensureOpen();
    return core.pointsReader;
  }

  @Override
  public NormsProducer getNormsReader() {
    ensureOpen();
    return core.normsProducer;
  }
  
  @Override
  public DocValuesProducer getDocValuesReader() {
    ensureOpen();
    return docValuesProducer;
  }

  @Override
  public FieldsProducer getPostingsReader() {
    ensureOpen();
    return core.fields;
  }

  @Override
  public String toString() {
    // SegmentInfo.toString takes dir and number of
    // *pending* deletions; so we reverse compute that here:
    return si.toString(si.info.maxDoc() - numDocs - si.getDelCount());
  }
  
  /**
   * Return the name of the segment this reader is reading.
   */
  public String getSegmentName() {
    return si.info.name;
  }
  
  /**
   * Return the SegmentInfoPerCommit of the segment this reader is reading.
   */
  public SegmentCommitInfo getSegmentInfo() {
    return si;
  }

  /** Returns the directory this index resides in. */
  public Directory directory() {
    // Don't ensureOpen here -- in certain cases, when a
    // cloned/reopened reader needs to commit, it may call
    // this method on the closed original reader
    return si.info.dir;
  }

  private final Set<ClosedListener> readerClosedListeners = new CopyOnWriteArraySet<>();

  @Override
  void notifyReaderClosedListeners() throws IOException {
    synchronized(readerClosedListeners) {
      IOUtils.applyToAll(readerClosedListeners, l -> l.onClose(readerCacheHelper.getKey()));
    }
  }

  private final IndexReader.CacheHelper readerCacheHelper = new IndexReader.CacheHelper() {
    private final IndexReader.CacheKey cacheKey = new IndexReader.CacheKey();

    @Override
    public CacheKey getKey() {
      return cacheKey;
    }

    @Override
    public void addClosedListener(ClosedListener listener) {
      ensureOpen();
      readerClosedListeners.add(listener);
    }
  };

  @Override
  public CacheHelper getReaderCacheHelper() {
    return readerCacheHelper;
  }

  /** Wrap the cache helper of the core to add ensureOpen() calls that make
   *  sure users do not register closed listeners on closed indices. */
  private final IndexReader.CacheHelper coreCacheHelper = new IndexReader.CacheHelper() {

    @Override
    public CacheKey getKey() {
      return core.getCacheHelper().getKey();
    }

    @Override
    public void addClosedListener(ClosedListener listener) {
      ensureOpen();
      core.getCacheHelper().addClosedListener(listener);
    }
  };

  @Override
  public CacheHelper getCoreCacheHelper() {
    return coreCacheHelper;
  }

  @Override
  public LeafMetaData getMetaData() {
    return metaData;
  }

  /**
   * Returns the original SegmentInfo passed to the segment reader on creation time.
   * {@link #getSegmentInfo()} returns a clone of this instance.
   */
  SegmentCommitInfo getOriginalSegmentInfo() {
    return originalSi;
  }

  /**
   * Returns the live docs that are not hard-deleted. This is an expert API to be used with
   * soft-deletes to filter out document that hard deleted for instance due to aborted documents or to distinguish
   * soft and hard deleted documents ie. a rolled back tombstone.
   * @lucene.experimental
   */
  public Bits getHardLiveDocs() {
    return hardLiveDocs;
  }

  @Override
  public void checkIntegrity() throws IOException {
    super.checkIntegrity();
    if (core.cfsReader != null) {
      core.cfsReader.checkIntegrity();
    }
  }
}
