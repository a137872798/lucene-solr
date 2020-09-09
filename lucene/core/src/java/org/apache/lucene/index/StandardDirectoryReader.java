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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/** Default implementation of {@link DirectoryReader}.
 * 该对象具备NRT 能力
 * */
public final class StandardDirectoryReader extends DirectoryReader {

  /**
   * 该对象负责往 目录下写入数据
   */
  final IndexWriter writer;
  /**
   * 维护初始化时读取到的段信息 以及 之后刷盘后生成的新segment信息
   */
  final SegmentInfos segmentInfos;
  private final boolean applyAllDeletes;
  private final boolean writeAllDeletes;

  /**
   *
   * @param directory  本次打开的目录
   * @param readers   本次涉及到的所有 reader对象  每个reader  对应一个 segment
   * @param writer    这些segment是由哪个 IndexWriter生成的
   * @param sis       涉及到的segment
   * @param applyAllDeletes    在生成该对象时是否已经将eventQueue中的任务执行完了
   * @param writeAllDeletes    将最新的信息写入到索引文件了
   * @throws IOException
   */
  StandardDirectoryReader(Directory directory, LeafReader[] readers, IndexWriter writer,
                          SegmentInfos sis, boolean applyAllDeletes, boolean writeAllDeletes) throws IOException {
    super(directory, readers);
    this.writer = writer;
    this.segmentInfos = sis;
    this.applyAllDeletes = applyAllDeletes;
    this.writeAllDeletes = writeAllDeletes;
  }

  /**
   * called from DirectoryReader.open(...) methods
   * 基于一个提交点进行初始化  从IndexWriter来看 每次commit 都会产生一个提交点对象  对应一个 segment_N 文件
   */
  static DirectoryReader open(final Directory directory, final IndexCommit commit) throws IOException {
    return new SegmentInfos.FindSegmentsFile<DirectoryReader>(directory) {

      /**
       * 通过指定的 segmentName 读取段文件数据
       * @param segmentFileName
       * @return
       * @throws IOException
       */
      @Override
      protected DirectoryReader doBody(String segmentFileName) throws IOException {
        // 解析 segment_N 文件   相当于在初始化情况下只能读取commit后的segment索引数据   但是一般使用必然会调用 openIfChange来更新对象
        SegmentInfos sis = SegmentInfos.readCommit(directory, segmentFileName);
        final SegmentReader[] readers = new SegmentReader[sis.size()];
        boolean success = false;
        try {
          // 为每个段对象生成读取各种索引文件的reader对象
          for (int i = sis.size()-1; i >= 0; i--) {
            // 每个 SegmentReader 对象是 LeafReader的子类 也就是他们本身可以组成一个树结构
            readers[i] = new SegmentReader(sis.info(i), sis.getIndexCreatedVersionMajor(), IOContext.READ);
          }

          // This may throw CorruptIndexException if there are too many docs, so
          // it must be inside try clause so we close readers in that case:
          DirectoryReader reader = new StandardDirectoryReader(directory, readers, null, sis, false, false);
          success = true;

          return reader;
        } finally {
          if (success == false) {
            IOUtils.closeWhileHandlingException(readers);
          }
        }
      }
    }.run(commit);
  }

  /**
   * Used by near real-time search
   * 因为是手动指定 segmentInfos的 所以如果此时 segmentInfos 包含了最新的索引文件 就可以获取到未commit 但是flush的数据
   * @param writer
   * @param infos  本次涉及到的所有段 信息
   * @param applyAllDeletes  是否已等待任务队列中所有的 delete/update处理完
   * @param writeAllDeletes   是否已将最新信息写入到索引文件 (DocValue  liveDoc ..)
   * @return
   * @throws IOException
   */
  static DirectoryReader open(IndexWriter writer, SegmentInfos infos, boolean applyAllDeletes, boolean writeAllDeletes) throws IOException {
    // IndexWriter synchronizes externally before calling
    // us, which ensures infos will not change; so there's
    // no need to process segments in reverse order
    final int numSegments = infos.size();

    // 存储还有效的reader
    final List<SegmentReader> readers = new ArrayList<>(numSegments);
    final Directory dir = writer.getDirectory();

    // 先读取 segment_N 文件
    final SegmentInfos segmentInfos = infos.clone();
    int infosUpto = 0;
    try {
      for (int i = 0; i < numSegments; i++) {
        // NOTE: important that we use infos not
        // segmentInfos here, so that we are passing the
        // actual instance of SegmentInfoPerCommit in
        // IndexWriter's segmentInfos:
        final SegmentCommitInfo info = infos.info(i);
        assert info.info.dir == dir;
        // 该对象维护了该segment下所有索引文件对应的 reader对象
        // 在调用该方法前 已经将 readerPool 开启池化功能了 这样当引用计数减为1时不会释放reader

        final ReadersAndUpdates rld = writer.getPooledInstance(info, true);
        try {
          // 如果不存在reader对象则初始化 否则更新reader对象的liveDoc
          final SegmentReader reader = rld.getReadOnlyClone(IOContext.READ);
          // 代表reader 还存在 就加入到 readers中
          if (reader.numDocs() > 0 || writer.getConfig().mergePolicy.keepFullyDeletedSegment(() -> reader)) {
            // Steal the ref:
            readers.add(reader);
            infosUpto++;
          // 无效的reader对象被释放
          } else {
            reader.decRef();
            segmentInfos.remove(infosUpto);
          }
        } finally {
          // 对应 getPooledInstance(info, true)   因为该方法只要create传入 true 就会增加引用计数
          writer.release(rld);
        }
      }

      // 为这些段下每个 索引文件增加引用计数
      writer.incRefDeleter(segmentInfos);

      // 将readers 整合成 StandardDirectoryReader 后返回
      StandardDirectoryReader result = new StandardDirectoryReader(dir,
          readers.toArray(new SegmentReader[readers.size()]), writer,
          segmentInfos, applyAllDeletes, writeAllDeletes);
      return result;
    } catch (Throwable t) {
      try {
        IOUtils.applyToAll(readers, SegmentReader::decRef);
      } catch (Throwable t1) {
        t.addSuppressed(t1);
      }
      throw t;
    }
  }

  /**
   * This constructor is only used for {@link #doOpenIfChanged(SegmentInfos)}, as well as NRT replication.
   * 该构造函数会在 doOpenIfChanged 中调用 仅更新发生变化的reader 对象  所以可以最后做merge吗   既然总能感知到最新的索引数据 那么merge的执行顺序就不重要了
   *  @lucene.internal */
  public static DirectoryReader open(Directory directory, SegmentInfos infos, List<? extends LeafReader> oldReaders) throws IOException {

    // we put the old SegmentReaders in a map, that allows us
    // to lookup a reader using its segment name
    final Map<String,Integer> segmentReaders = (oldReaders == null ? Collections.emptyMap() : new HashMap<>(oldReaders.size()));

    // 将旧的reader 按照segmentName 和 下标进行分组
    if (oldReaders != null) {
      // create a Map SegmentName->SegmentReader
      for (int i = 0, c = oldReaders.size(); i < c; i++) {
        final SegmentReader sr = (SegmentReader) oldReaders.get(i);
        segmentReaders.put(sr.getSegmentName(), Integer.valueOf(i));
      }
    }

    // 这是当前最新的segmentInfos
    SegmentReader[] newReaders = new SegmentReader[infos.size()];
    for (int i = infos.size() - 1; i>=0; i--) {
      SegmentCommitInfo commitInfo = infos.info(i);

      // 先找到没有发生变化的segment

      // find SegmentReader for this segment
      Integer oldReaderIndex = segmentReaders.get(commitInfo.info.name);
      SegmentReader oldReader;
      if (oldReaderIndex == null) {
        // this is a new segment, no old SegmentReader can be reused
        oldReader = null;
      } else {
        // there is an old reader for this segment - we'll try to reopen it
        oldReader = (SegmentReader) oldReaders.get(oldReaderIndex.intValue());
      }

      // Make a best effort to detect when the app illegally "rm -rf" their
      // index while a reader was open, and then called openIfChanged:
      if (oldReader != null && Arrays.equals(commitInfo.info.getId(), oldReader.getSegmentInfo().info.getId()) == false) {
        throw new IllegalStateException("same segment " + commitInfo.info.name + " has invalid doc count change; likely you are re-opening a reader after illegally removing index files yourself and building a new index in their place.  Use IndexWriter.deleteAll or open a new IndexWriter using OpenMode.CREATE instead");
      }

      boolean success = false;
      try {
        SegmentReader newReader;
        // oldReader不存在就代表本次生成了一个新的 segment  需要初始化 SegmentReader   TODO 先不看复合文件
        if (oldReader == null || commitInfo.info.getUseCompoundFile() != oldReader.getSegmentInfo().info.getUseCompoundFile()) {
          // this is a new reader; in case we hit an exception we can decRef it safely
          newReader = new SegmentReader(commitInfo, infos.getIndexCreatedVersionMajor(), IOContext.READ);
          newReaders[i] = newReader;
        } else {
          // 针对之前旧的 reader对象 需要检测是否发生了变化

          if (oldReader.isNRT) {
            // We must load liveDocs/DV updates from disk:   当检测到commitInfo记录了有数据被删除 就需要读取最新的 liveDoc信息  通过commitInfo.delGen 定位到最新的 liveDoc
            Bits liveDocs = commitInfo.hasDeletions() ? commitInfo.info.getCodec().liveDocsFormat()
                .readLiveDocs(commitInfo.info.dir, commitInfo, IOContext.READONCE) : null;

            // 使用从磁盘读取的 liveDoc 更新reader对象  同时内部还会检测 fieldInfo 和 docValue是否变化 同时会读取最新的相关索引文件
            newReaders[i] = new SegmentReader(commitInfo, oldReader, liveDocs, liveDocs,
                commitInfo.info.maxDoc() - commitInfo.getDelCount(), false);
          } else {
            // 非 NRT 的场景

            // 因为update/delete 没有作用到segment上 所以本次返回结果与之前一致 同时增加旧reader的引用计数
            if (oldReader.getSegmentInfo().getDelGen() == commitInfo.getDelGen()
                && oldReader.getSegmentInfo().getFieldInfosGen() == commitInfo.getFieldInfosGen()) {
              // No change; this reader will be shared between
              // the old and the new one, so we must incRef
              // it:
              oldReader.incRef();
              newReaders[i] = oldReader;
            } else {
              // Steal the ref returned by SegmentReader ctor:
              assert commitInfo.info.dir == oldReader.getSegmentInfo().info.dir;

              if (oldReader.getSegmentInfo().getDelGen() == commitInfo.getDelGen()) {
                // only DV updates  因为 liveDoc的信息可以复用 只要重新获取fieldInfo和docValueUpdate就好
                newReaders[i] = new SegmentReader(commitInfo, oldReader, oldReader.getLiveDocs(),
                    oldReader.getHardLiveDocs(), oldReader.numDocs(), false); // this is not an NRT reader!
              } else {
                // both DV and liveDocs have changed
                // 数据都需要重新读取
                Bits liveDocs = commitInfo.hasDeletions() ? commitInfo.info.getCodec().liveDocsFormat()
                    .readLiveDocs(commitInfo.info.dir, commitInfo, IOContext.READONCE) : null;
                newReaders[i] = new SegmentReader(commitInfo, oldReader, liveDocs, liveDocs,
                    commitInfo.info.maxDoc() - commitInfo.getDelCount(), false);
              }
            }
          }
        }
        success = true;
      } finally {
        if (!success) {
          decRefWhileHandlingException(newReaders);
        }
      }
    }    
    return new StandardDirectoryReader(directory, newReaders, null, infos, false, false);
  }

  // TODO: move somewhere shared if it's useful elsewhere
  // 减少之前分配的引用计数
  private static void decRefWhileHandlingException(SegmentReader[] readers) {
    for(SegmentReader reader : readers) {
      if (reader != null) {
        try {
          reader.decRef();
        } catch (Throwable t) {
          // Ignore so we keep throwing original exception
        }
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(getClass().getSimpleName());
    buffer.append('(');
    final String segmentsFile = segmentInfos.getSegmentsFileName();
    if (segmentsFile != null) {
      buffer.append(segmentsFile).append(":").append(segmentInfos.getVersion());
    }
    if (writer != null) {
      buffer.append(":nrt");
    }
    for (final LeafReader r : getSequentialSubReaders()) {
      buffer.append(' ');
      buffer.append(r);
    }
    buffer.append(')');
    return buffer.toString();
  }

  /**
   * 默认触发的还是  doOpenIfChanged(final IndexCommit commit)  不过参数传入null
   * @return
   * @throws IOException
   */
  @Override
  protected DirectoryReader doOpenIfChanged() throws IOException {
    return doOpenIfChanged((IndexCommit) null);
  }

  /**
   *
   * @param commit  指定了某次提交点对应的 segmentInfos
   * @return
   * @throws IOException
   */
  @Override
  protected DirectoryReader doOpenIfChanged(final IndexCommit commit) throws IOException {
    ensureOpen();

    // If we were obtained by writer.getReader(), re-ask the
    // writer to get a new reader.
    if (writer != null) {
      return doOpenFromWriter(commit);
    } else {
      return doOpenNoWriter(commit);
    }
  }

  @Override
  protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
    ensureOpen();
    if (writer == this.writer && applyAllDeletes == this.applyAllDeletes) {
      return doOpenFromWriter(null);
    } else {
      return writer.getReader(applyAllDeletes, writeAllDeletes);
    }
  }

  /**
   * 代表从 writer中重新打开reader
   * @param commit
   * @return
   * @throws IOException
   */
  private DirectoryReader doOpenFromWriter(IndexCommit commit) throws IOException {
    if (commit != null) {
      return doOpenFromCommit(commit);
    }

    // 检测segmentInfos 是否发生了变化   如果没有任何变化 返回false
    if (writer.nrtIsCurrent(segmentInfos)) {
      return null;
    }

    // 重新触发一次fullFlush 并阻塞等待所有的 update/delete 处理完
    DirectoryReader reader = writer.getReader(applyAllDeletes, writeAllDeletes);

    // If in fact no changes took place, return null:
    // 如果执行完 fullFlush后 获得的最新segmentInfos 与原来一致 则代表不需要产生新的reader
    if (reader.getVersion() == segmentInfos.getVersion()) {
      // 释放之前创建的各种reader
      reader.decRef();
      return null;
    }

    return reader;
  }

  /**
   * 这种情况只能检测 commit后的数据 也就是必须基于 segment_N 文件进行检测
   * @param commit
   * @return
   * @throws IOException
   */
  private DirectoryReader doOpenNoWriter(IndexCommit commit) throws IOException {

    if (commit == null) {
      if (isCurrent()) {
        return null;
      }
    } else {
      if (directory != commit.getDirectory()) {
        throw new IOException("the specified commit does not match the specified Directory");
      }
      if (segmentInfos != null && commit.getSegmentsFileName().equals(segmentInfos.getSegmentsFileName())) {
        return null;
      }
    }

    return doOpenFromCommit(commit);
  }

  /**
   * 基于一次指定的 segment_N 初始化reader
   * @param commit
   * @return
   * @throws IOException
   */
  private DirectoryReader doOpenFromCommit(IndexCommit commit) throws IOException {
    return new SegmentInfos.FindSegmentsFile<DirectoryReader>(directory) {
      @Override
      protected DirectoryReader doBody(String segmentFileName) throws IOException {
        final SegmentInfos infos = SegmentInfos.readCommit(directory, segmentFileName);
        return doOpenIfChanged(infos);
      }
    }.run(commit);
  }


  /**
   * 创建指定 segment对应的reader对象  并整合成 标准目录读取对象
   * @param infos
   * @return
   * @throws IOException
   */
  DirectoryReader doOpenIfChanged(SegmentInfos infos) throws IOException {
    return StandardDirectoryReader.open(directory, infos, getSequentialSubReaders());
  }

  @Override
  public long getVersion() {
    ensureOpen();
    return segmentInfos.getVersion();
  }

  /** Return the {@link SegmentInfos} for this reader.
   *
   * @lucene.internal */
  public SegmentInfos getSegmentInfos() {
    return segmentInfos;
  }

  @Override
  public boolean isCurrent() throws IOException {
    ensureOpen();
    if (writer == null || writer.isClosed()) {
      // Fully read the segments file: this ensures that it's
      // completely written so that if
      // IndexWriter.prepareCommit has been called (but not
      // yet commit), then the reader will still see itself as
      // current:
      SegmentInfos sis = SegmentInfos.readLatestCommit(directory);

      // we loaded SegmentInfos from the directory
      return sis.getVersion() == segmentInfos.getVersion();
    } else {
      return writer.nrtIsCurrent(segmentInfos);
    }
  }

  @Override
  @SuppressWarnings("try")
  protected void doClose() throws IOException {
    Closeable decRefDeleter = () -> {
      if (writer != null) {
        try {
          writer.decRefDeleter(segmentInfos);
        } catch (AlreadyClosedException ex) {
          // This is OK, it just means our original writer was
          // closed before we were, and this may leave some
          // un-referenced files in the index, which is
          // harmless.  The next time IW is opened on the
          // index, it will delete them.
        }
      }
    };
    try (Closeable finalizer = decRefDeleter) {
      // try to close each reader, even if an exception is thrown
      final List<? extends LeafReader> sequentialSubReaders = getSequentialSubReaders();
      IOUtils.applyToAll(sequentialSubReaders, LeafReader::decRef);
    }
  }

  @Override
  public IndexCommit getIndexCommit() throws IOException {
    ensureOpen();
    return new ReaderCommit(this, segmentInfos, directory);
  }

  static final class ReaderCommit extends IndexCommit {
    private String segmentsFileName;
    Collection<String> files;
    Directory dir;
    long generation;
    final Map<String,String> userData;
    private final int segmentCount;
    private final StandardDirectoryReader reader;

    ReaderCommit(StandardDirectoryReader reader, SegmentInfos infos, Directory dir) throws IOException {
      segmentsFileName = infos.getSegmentsFileName();
      this.dir = dir;
      userData = infos.getUserData();
      files = Collections.unmodifiableCollection(infos.files(true));
      generation = infos.getGeneration();
      segmentCount = infos.size();

      // NOTE: we intentionally do not incRef this!  Else we'd need to make IndexCommit Closeable...
      this.reader = reader;
    }

    @Override
    public String toString() {
      return "StandardDirectoryReader.ReaderCommit(" + segmentsFileName + " files=" + files + ")";
    }

    @Override
    public int getSegmentCount() {
      return segmentCount;
    }

    @Override
    public String getSegmentsFileName() {
      return segmentsFileName;
    }

    @Override
    public Collection<String> getFileNames() {
      return files;
    }

    @Override
    public Directory getDirectory() {
      return dir;
    }

    @Override
    public long getGeneration() {
      return generation;
    }

    @Override
    public boolean isDeleted() {
      return false;
    }

    @Override
    public Map<String,String> getUserData() {
      return userData;
    }

    @Override
    public void delete() {
      throw new UnsupportedOperationException("This IndexCommit does not support deletions");
    }

    @Override
    StandardDirectoryReader getReader() {
      return reader;
    }
  }

  private final Set<ClosedListener> readerClosedListeners = new CopyOnWriteArraySet<>();

  private final CacheHelper cacheHelper = new CacheHelper() {
    private final CacheKey cacheKey = new CacheKey();

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
  void notifyReaderClosedListeners() throws IOException {
    synchronized(readerClosedListeners) {
      IOUtils.applyToAll(readerClosedListeners, l -> l.onClose(cacheHelper.getKey()));
    }
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return cacheHelper;
  }
}
