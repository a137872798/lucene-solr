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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

/*
 * This class keeps track of each SegmentInfos instance that
 * is still "live", either because it corresponds to a
 * segments_N file in the Directory (a "commit", i.e. a
 * committed SegmentInfos) or because it's an in-memory
 * SegmentInfos that a writer is actively updating but has
 * not yet committed.  This class uses simple reference
 * counting to map the live SegmentInfos instances to
 * individual files in the Directory.
 *
 * The same directory file may be referenced by more than
 * one IndexCommit, i.e. more than one SegmentInfos.
 * Therefore we count how many commits reference each file.
 * When all the commits referencing a certain file have been
 * deleted, the refcount for that file becomes zero, and the
 * file is deleted.
 *
 * A separate deletion policy interface
 * (IndexDeletionPolicy) is consulted on creation (onInit)
 * and once per commit (onCommit), to decide when a commit
 * should be removed.
 *
 * It is the business of the IndexDeletionPolicy to choose
 * when to delete commit points.  The actual mechanics of
 * file deletion, retrying, etc, derived from the deletion
 * of commit points is the business of the IndexFileDeleter.
 *
 * The current default deletion policy is {@link
 * KeepOnlyLastCommitDeletionPolicy}, which removes all
 * prior commits when a new commit has completed.  This
 * matches the behavior before 2.2.
 *
 * Note that you must hold the write.lock before
 * instantiating this class.  It opens segments_N file(s)
 * directly with no retry logic.
 * 该对象负责删除索引文件
 */

final class IndexFileDeleter implements Closeable {

  /* Reference count for all files in the index.
   * Counts how many existing commits reference a file.
   * 存放文件名 -> 引用计数的映射关系
   **/
  private Map<String, RefCount> refCounts = new HashMap<>();

  /* Holds all commits (segments_N) currently in the index.
   * This will have just 1 commit if you are using the
   * default delete policy (KeepOnlyLastCommitDeletionPolicy).
   * Other policies may leave commit points live for longer
   * in which case this list would be longer than 1: */
  // 每次的提交信息对应的 索引文件 之类的信息都会保存在该list下
  private List<CommitPoint> commits = new ArrayList<>();

  /* Holds files we had incref'd from the previous
   * non-commit checkpoint: */
  // 上一次提交相关的文件
  private final List<String> lastFiles = new ArrayList<>();

  /* Commits that the IndexDeletionPolicy have decided to delete: */
  // 这里存储了已经确定要删除的 提交点
  private List<CommitPoint> commitsToDelete = new ArrayList<>();

  private final InfoStream infoStream;
  private final Directory directoryOrig; // for commit point metadata
  private final Directory directory;

  /**
   * 默认删除策略就是 KeepOnlyLastCommitDeletionPolicy  也就是如果此时存在多个未提交的提交点时 只保留最后一个
   */
  private final IndexDeletionPolicy policy;

  /**
   * 代表初始的那个段文件是否已经被删除了
   */
  final boolean startingCommitDeleted;
  /**
   * 最新的 segment_N 对应的数据
   */
  private SegmentInfos lastSegmentInfos;

  /** Change to true to see details of reference counts when
   *  infoStream is enabled */
  public static boolean VERBOSE_REF_COUNTS = false;

  private final IndexWriter writer;

  // called only from assert
  private boolean locked() {
    return writer == null || Thread.holdsLock(writer);
  }

  /**
   * Initialize the deleter: find all previous commits in
   * the Directory, incref the files they reference, call
   * the policy to let it delete commits.  This will remove
   * any files not referenced by any of the commits.
   * @throws IOException if there is a low-level IO error
   * @param files  当前 indexWriter 在操作前可观测到的所有文件  也就是之前提交的   如果选择打开一个新的段文件 那么该数组应该为空
   * @param policy 代表使用的删除策略
   * @param segmentInfos 本次打开的段信息 可能是延续上次的 也可能是本次新建的
   * @param initialIndexExists 代表目录下之前就存在 segment_N 文件
   * @param isReaderInit 代表 indexWriter 初始化时的配置对象是否携带 StandardDirectoryReader 属性
   */
  public IndexFileDeleter(String[] files, Directory directoryOrig, Directory directory, IndexDeletionPolicy policy, SegmentInfos segmentInfos,
                          InfoStream infoStream, IndexWriter writer, boolean initialIndexExists,
                          boolean isReaderInit) throws IOException {
    Objects.requireNonNull(writer);
    this.infoStream = infoStream;
    this.writer = writer;

    // 代表本次 段信息会将数据写入到哪个文件
    final String currentSegmentsFile = segmentInfos.getSegmentsFileName();

    if (infoStream.isEnabled("IFD")) {
      infoStream.message("IFD", "init: current segments file is \"" + currentSegmentsFile + "\"; deletionPolicy=" + policy);
    }

    this.policy = policy;
    this.directoryOrig = directoryOrig;
    this.directory = directory;

    // First pass: walk the files and initialize our ref
    // counts:
    // 当前正在使用的 段文件
    CommitPoint currentCommitPoint = null;

    if (currentSegmentsFile != null) {
      Matcher m = IndexFileNames.CODEC_FILE_PATTERN.matcher("");
      for (String fileName : files) {
        m.reset(fileName);
        // 除了锁文件 并且 （命中正则 或者为 segments,pending_segment 文件）
        if (!fileName.endsWith("write.lock") && (m.matches() || fileName.startsWith(IndexFileNames.SEGMENTS) || fileName.startsWith(IndexFileNames.PENDING_SEGMENTS))) {
          
          // Add this file to refCounts with initial count 0:
          // 这里主要是初始化引用计数 此时计数还是0
          getRefCount(fileName);

          if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
            
            // This is a commit (segments or segments_N), and
            // it's valid (<= the max gen).  Load it, then
            // incref all files it refers to:
            if (infoStream.isEnabled("IFD")) {
              infoStream.message("IFD", "init: load commit \"" + fileName + "\"");
            }
            // 读取之前文件的段信息   在 indexWriter中 只会读取最新的段文件
            SegmentInfos sis = SegmentInfos.readCommit(directoryOrig, fileName);

            // TODO 为什么每个段都有一个提交点对象  该对象不是在 commit时才创建的吗
            final CommitPoint commitPoint = new CommitPoint(commitsToDelete, directoryOrig, sis);
            // 代表此时遍历到的是本次正在使用的段文件
            if (sis.getGeneration() == segmentInfos.getGeneration()) {
              currentCommitPoint = commitPoint;
            }
            commits.add(commitPoint);
            // 将所有关联的索引文件下 包括段文件本身 增加引用计数
            incRef(sis, true);
              
            if (lastSegmentInfos == null || sis.getGeneration() > lastSegmentInfos.getGeneration()) {
              // 这里记录的是最新的 segment_N 对应的数据
              lastSegmentInfos = sis;
            }
          }
        }
      }
    }

    // 加载最新的 segment_N 数据
    if (currentCommitPoint == null && currentSegmentsFile != null && initialIndexExists) {
      // We did not in fact see the segments_N file
      // corresponding to the segmentInfos that was passed
      // in.  Yet, it must exist, because our caller holds
      // the write lock.  This can happen when the directory
      // listing was stale (eg when index accessed via NFS
      // client with stale directory listing cache).  So we
      // try now to explicitly open this commit point:
      SegmentInfos sis = null;
      try {
        sis = SegmentInfos.readCommit(directoryOrig, currentSegmentsFile);
      } catch (IOException e) {
        throw new CorruptIndexException("unable to read current segments_N file", currentSegmentsFile, e);
      }
      if (infoStream.isEnabled("IFD")) {
        infoStream.message("IFD", "forced open of current segments file " + segmentInfos.getSegmentsFileName());
      }
      currentCommitPoint = new CommitPoint(commitsToDelete, directoryOrig, sis);
      commits.add(currentCommitPoint);
      incRef(sis, true);
    }

    // TODO 先跳过设置reader的逻辑
    if (isReaderInit) {
      // Incoming SegmentInfos may have NRT changes not yet visible in the latest commit, so we have to protect its files from deletion too:
      checkpoint(segmentInfos, false);
    }

    // We keep commits list in sorted order (oldest to newest):
    // 这里按 gen 从小到大的逻辑排序
    CollectionUtil.timSort(commits);
    Collection<String> relevantFiles = new HashSet<>(refCounts.keySet());
    // 找到该目录下一组待删除的文件
    Set<String> pendingDeletions = directoryOrig.getPendingDeletions();
    if (pendingDeletions.isEmpty() == false) {
      relevantFiles.addAll(pendingDeletions);
    }
    // refCounts only includes "normal" filenames (does not include write.lock)
    // 同步每个segmentInfo对应的gen信息  以及nextGen
    inflateGens(segmentInfos, relevantFiles, infoStream);

    // Now delete anything with ref count at 0.  These are
    // presumably abandoned files eg due to crash of
    // IndexWriter.
    Set<String> toDelete = new HashSet<>();
    for(Map.Entry<String, RefCount> entry : refCounts.entrySet() ) {
      RefCount rc = entry.getValue();
      final String fileName = entry.getKey();
      // 这里是命中正则表达式  但是又不属于 segmentInfo.files() 相关的文件 这些文件应当被直接删除
      if (0 == rc.count) {
        // A segments_N file should never have ref count 0 on init:
        if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
          throw new IllegalStateException("file \"" + fileName + "\" has refCount=0, which should never happen on init");
        }
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "init: removing unreferenced file \"" + fileName + "\"");
        }
        toDelete.add(fileName);
      }
    }

    // 将目录下一些其他文件删除
    deleteFiles(toDelete);

    // Finally, give policy a chance to remove things on
    // startup:
    // 每个commit 对象对应一个 segment_N 文件   默认实现是 KeepOnlyLastCommitDeletionPolicy
    // 实现就是只保留最后一个提交点 将其他提交点 设置到  待删除列表中   TODO 可能相同gen的段相关文件被看作是一个提交点
    policy.onInit(commits);

    // Always protect the incoming segmentInfos since
    // sometime it may not be the most recent commit
    // 将当前段信息存储到 lastFile中
    checkpoint(segmentInfos, false);

    if (currentCommitPoint == null) {
      startingCommitDeleted = false;
    } else {
      startingCommitDeleted = currentCommitPoint.isDeleted();
    }

    // 尝试删除队列中的提交点
    deleteCommits();
  }

  /** Set all gens beyond what we currently see in the directory, to avoid double-write in cases where the previous IndexWriter did not
   *  gracefully close/rollback (e.g. os/machine crashed or lost power).
   * @param infos 本次相关的所有段信息
   * @param files 涉及到的所有文件
   */
  static void inflateGens(SegmentInfos infos, Collection<String> files, InfoStream infoStream) {

    long maxSegmentGen = Long.MIN_VALUE;
    long maxSegmentName = Long.MIN_VALUE;

    // Confusingly, this is the union of liveDocs, field infos, doc values
    // (and maybe others, in the future) gens.  This is somewhat messy,
    // since it means DV updates will suddenly write to the next gen after
    // live docs' gen, for example, but we don't have the APIs to ask the
    // codec which file is which:
    Map<String,Long> maxPerSegmentGen = new HashMap<>();

    // 遍历相关文件获取最大的 gen
    for(String fileName : files) {
      // 文件锁 忽略
      if (fileName.equals(IndexWriter.WRITE_LOCK_NAME)) {
        // do nothing
      } else if (fileName.startsWith(IndexFileNames.SEGMENTS)) {
        try {
          // 解析最大的年代值
          maxSegmentGen = Math.max(SegmentInfos.generationFromSegmentsFileName(fileName), maxSegmentGen);
        } catch (NumberFormatException ignore) {
          // trash file: we have to handle this since we allow anything starting with 'segments' here
        }
        // 这个 pending_segment 文件什么时候 出现 ???
      } else if (fileName.startsWith(IndexFileNames.PENDING_SEGMENTS)) {
        try {
          maxSegmentGen = Math.max(SegmentInfos.generationFromSegmentsFileName(fileName.substring(8)), maxSegmentGen);
        } catch (NumberFormatException ignore) {
          // trash file: we have to handle this since we allow anything starting with 'pending_segments' here
        }
      } else {
        // 剩余文件都是以 _ 开头的
        String segmentName = IndexFileNames.parseSegmentName(fileName);
        assert segmentName.startsWith("_"): "wtf? file=" + fileName;

        if (fileName.toLowerCase(Locale.ROOT).endsWith(".tmp")) {
          // A temp file: don't try to look at its gen
          continue;
        }

        maxSegmentName = Math.max(maxSegmentName, Long.parseLong(segmentName.substring(1), Character.MAX_RADIX));

        Long curGen = maxPerSegmentGen.get(segmentName);
        if (curGen == null) {
          curGen = 0L;
        }

        try {
          curGen = Math.max(curGen, IndexFileNames.parseGeneration(fileName));
        } catch (NumberFormatException ignore) {
          // trash file: we have to handle this since codec regex is only so good
        }
        maxPerSegmentGen.put(segmentName, curGen);
      }
    }

    // Generation is advanced before write:
    // 选择最大的gen 设置到 segmentInfos中
    infos.setNextWriteGeneration(Math.max(infos.getGeneration(), maxSegmentGen));

    // 同步 counter 与_segment文件的编号
    if (infos.counter < 1+maxSegmentName) {
      if (infoStream.isEnabled("IFD")) {
        infoStream.message("IFD", "init: inflate infos.counter to " + (1+maxSegmentName) + " vs current=" + infos.counter);
      }
      infos.counter = 1+maxSegmentName;
    }

    for(SegmentCommitInfo info : infos) {
      // 找到每个段对象的年代信息
      Long gen = maxPerSegmentGen.get(info.info.name);
      assert gen != null;
      long genLong = gen;
      // 更新 下次写入的gen
      if (info.getNextWriteDelGen() < genLong+1) {
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "init: seg=" + info.info.name + " set nextWriteDelGen=" + (genLong+1) + " vs current=" + info.getNextWriteDelGen());
        }
        info.setNextWriteDelGen(genLong+1);
      }
      if (info.getNextWriteFieldInfosGen() < genLong+1) {
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "init: seg=" + info.info.name + " set nextWriteFieldInfosGen=" + (genLong+1) + " vs current=" + info.getNextWriteFieldInfosGen());
        }
        info.setNextWriteFieldInfosGen(genLong+1);
      }
      if (info.getNextWriteDocValuesGen() < genLong+1) {
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "init: seg=" + info.info.name + " set nextWriteDocValuesGen=" + (genLong+1) + " vs current=" + info.getNextWriteDocValuesGen());
        }
        info.setNextWriteDocValuesGen(genLong+1);
      }
    }
  }

  void ensureOpen() throws AlreadyClosedException {
    // 如果 处于closing 或者 closed 抛出异常
    writer.ensureOpen(false);
    // since we allow 'closing' state, we must still check this, we could be closing because we hit e.g. OOM
    // 如果当前writer 已经设置了某种致命异常 抛出异常
    if (writer.getTragicException() != null) {
      throw new AlreadyClosedException("refusing to delete any files: this IndexWriter hit an unrecoverable exception", writer.getTragicException());
    }
  }

  // for testing
  boolean isClosed() {
    try {
      ensureOpen();
      return false;
    } catch (AlreadyClosedException ace) {
      return true;
    }
  }

  /**
   * Remove the CommitPoints in the commitsToDelete List by
   * DecRef'ing all files from each SegmentInfos.
   * 删除队列中的提交点
   */
  private void deleteCommits() throws IOException {

    int size = commitsToDelete.size();

    if (size > 0) {

      // First decref all files that had been referred to by
      // the now-deleted commits:
      Throwable firstThrowable = null;
      for(int i=0;i<size;i++) {
        CommitPoint commit = commitsToDelete.get(i);
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "deleteCommits: now decRef commit \"" + commit.getSegmentsFileName() + "\"");
        }
        try {
          // 引用计数归0的文件会被删除
          decRef(commit.files);
        } catch (Throwable t) {
          firstThrowable = IOUtils.useOrSuppress(firstThrowable, t);
        }
      }
      commitsToDelete.clear();

      if (firstThrowable != null) {
        throw IOUtils.rethrowAlways(firstThrowable);
      }

      // Now compact commits to remove deleted ones (preserving the sort):
      // 整理 commit 列表 将已经被删除的提交点从队列移除
      size = commits.size();
      int readFrom = 0;
      int writeTo = 0;
      while(readFrom < size) {
        CommitPoint commit = commits.get(readFrom);
        if (!commit.deleted) {
          if (writeTo != readFrom) {
            commits.set(writeTo, commits.get(readFrom));
          }
          writeTo++;
        }
        readFrom++;
      }

      while(size > writeTo) {
        commits.remove(size-1);
        size--;
      }
    }
  }

  /**
   * Writer calls this when it has hit an error and had to
   * roll back, to tell us that there may now be
   * unreferenced files in the filesystem.  So we re-list
   * the filesystem and delete such files.  If segmentName
   * is non-null, we will only delete files corresponding to
   * that segment.
   */
  void refresh() throws IOException {
    assert locked();
    Set<String> toDelete = new HashSet<>();

    String[] files = directory.listAll();

    Matcher m = IndexFileNames.CODEC_FILE_PATTERN.matcher("");

    for(int i=0;i<files.length;i++) {
      String fileName = files[i];
      m.reset(fileName);
      if (!fileName.endsWith("write.lock") &&
          !refCounts.containsKey(fileName) &&
          (m.matches() || fileName.startsWith(IndexFileNames.SEGMENTS) 
              // we only try to clear out pending_segments_N during rollback(), because we don't ref-count it
              // TODO: this is sneaky, should we do this, or change TestIWExceptions? rollback closes anyway, and 
              // any leftover file will be deleted/retried on next IW bootup anyway...
              || fileName.startsWith(IndexFileNames.PENDING_SEGMENTS))) {
        // Unreferenced file, so remove it
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "refresh: removing newly created unreferenced file \"" + fileName + "\"");
        }
        toDelete.add(fileName);
      }
    }

    deleteFiles(toDelete);
  }

  @Override
  public void close() throws IOException {
    // DecRef old files from the last checkpoint, if any:
    assert locked();

    // 如果存在因为 checkpoint 而加入到 lastFile的文件  尝试删除
    if (!lastFiles.isEmpty()) {
      try {
        decRef(lastFiles);
      } finally {
        lastFiles.clear();
      }
    }
  }

  /**
   * Revisits the {@link IndexDeletionPolicy} by calling its
   * {@link IndexDeletionPolicy#onCommit(List)} again with the known commits.
   * This is useful in cases where a deletion policy which holds onto index
   * commits is used. The application may know that some commits are not held by
   * the deletion policy anymore and call
   * {@link IndexWriter#deleteUnusedFiles()}, which will attempt to delete the
   * unused commits again.
   * 手动执行删除策略
   */
  void revisitPolicy() throws IOException {
    assert locked();
    if (infoStream.isEnabled("IFD")) {
      infoStream.message("IFD", "now revisitPolicy");
    }

    if (commits.size() > 0) {
      policy.onCommit(commits);
      deleteCommits();
    }
  }

  /**
   * For definition of "check point" see IndexWriter comments:
   * "Clarification: Check Points (and commits)".
   *
   * Writer calls this when it has made a "consistent
   * change" to the index, meaning new files are written to
   * the index and the in-memory SegmentInfos have been
   * modified to point to those files.
   *
   * This may or may not be a commit (segments_N may or may
   * not have been written).
   *
   * We simply incref the files referenced by the new
   * SegmentInfos and decref the files we had previously
   * seen (if any).
   *
   * If this is a commit, we also call the policy to give it
   * a chance to remove other commits.  If any commits are
   * removed, we decref their files as well.
   *
   * @param segmentInfos  存储了所有的段信息
   * @param isCommit  本次处理是否是一次提交动作
   * 这个检查点方法的意图就是为所有有效的索引文件维护一个正确的引用计数 确保文件不会在预料外的场景被删除
   * 每当有一种持久化的改变发生时 就会触发该方法 比如某次flush 生成了新的索引文件 就会调用该方法  为其增加引用计数
   */
  public void checkpoint(SegmentInfos segmentInfos, boolean isCommit) throws IOException {
    assert locked();

    // 必须在内置锁的场景下才能调用该方法
    assert Thread.holdsLock(writer);
    long t0 = System.nanoTime();
    if (infoStream.isEnabled("IFD")) {
      infoStream.message("IFD", "now checkpoint \"" + writer.segString(writer.toLiveInfos(segmentInfos)) + "\" [" + segmentInfos.size() + " segments " + "; isCommit = " + isCommit + "]");
    }

    // Incref the files:
    // 为目标文件增加引用计数
    incRef(segmentInfos, isCommit);

    if (isCommit) {
      // Append to our commits list:
      // 在 commit的场景下调用 、会创建新的检查点对象
      commits.add(new CommitPoint(commitsToDelete, directoryOrig, segmentInfos));

      // Tell policy so it can remove commits:
      // policy一般就是 KeepOnlyLastCommitDeletionPolicy   这里会触发除了最后一个commitPoint外的其他提交点的 delete 方法
      policy.onCommit(commits);

      // Decref files for commits that were deleted by the policy:
      // 处理 commits中的提交点对象   就是将相关索引文件删除
      deleteCommits();
    } else {
      // DecRef old files from the last checkpoint, if any:
      // 为了维持引用计数不变  这里采用的策略其实是 每次为全量数据增加引用计数 ， 并记录本次所有文件 下次有新的文件进来时 还是全量增加引用计数， 但是会将上次文件的引用计数减少 这样就保证结果是相当于为所有文件只增加了一次引用计数
      try {
        decRef(lastFiles);
      } finally {
        lastFiles.clear();
      }

      // Save files so we can decr on next checkpoint/commit:
      // 确保下一次减少正确的文件的引用计数
      lastFiles.addAll(segmentInfos.files(false));
    }

    if (infoStream.isEnabled("IFD")) {
      long t1 = System.nanoTime();
      infoStream.message("IFD", ((t1-t0)/1000000) + " msec to checkpoint");
    }
  }

  /**
   * 为相关文件增加引用计数
   * @param segmentInfos
   * @param isCommit
   * @throws IOException
   */
  void incRef(SegmentInfos segmentInfos, boolean isCommit) throws IOException {
    assert locked();
    // If this is a commit point, also incRef the
    // segments_N file:
    for(final String fileName: segmentInfos.files(isCommit)) {
      incRef(fileName);
    }
  }

  void incRef(Collection<String> files) {
    assert locked();
    for(final String file : files) {
      incRef(file);
    }
  }

  void incRef(String fileName) {
    assert locked();
    RefCount rc = getRefCount(fileName);
    if (infoStream.isEnabled("IFD")) {
      if (VERBOSE_REF_COUNTS) {
        infoStream.message("IFD", "  IncRef \"" + fileName + "\": pre-incr count is " + rc.count);
      }
    }
    rc.IncRef();
  }

  /** Decrefs all provided files, even on exception; throws first exception hit, if any. */
  // 为某些文件减少引用计数
  void decRef(Collection<String> files) throws IOException {
    assert locked();
    Set<String> toDelete = new HashSet<>();
    Throwable firstThrowable = null;
    for(final String file : files) {
      try {
        if (decRef(file)) {
          // 当引用计数归0时 添加到 待删除列表
          toDelete.add(file);
        }
      } catch (Throwable t) {
        firstThrowable = IOUtils.useOrSuppress(firstThrowable, t);
      }
    }

    try {
      // 删除文件
      deleteFiles(toDelete);
    } catch (Throwable t) {
      firstThrowable = IOUtils.useOrSuppress(firstThrowable, t);
    }

    if (firstThrowable != null) {
      throw IOUtils.rethrowAlways(firstThrowable);
    }
  }

  /** Returns true if the file should now be deleted. */
  // 减少引用计数
  private boolean decRef(String fileName) {
    assert locked();
    RefCount rc = getRefCount(fileName);
    if (infoStream.isEnabled("IFD")) {
      if (VERBOSE_REF_COUNTS) {
        infoStream.message("IFD", "  DecRef \"" + fileName + "\": pre-decr count is " + rc.count);
      }
    }
    if (rc.DecRef() == 0) {
      // This file is no longer referenced by any past
      // commit points nor by the in-memory SegmentInfos:
      refCounts.remove(fileName);
      return true;
    } else {
      return false;
    }
  }

  void decRef(SegmentInfos segmentInfos) throws IOException {
    assert locked();
    decRef(segmentInfos.files(false));
  }

  public boolean exists(String fileName) {
    assert locked();
    if (!refCounts.containsKey(fileName)) {
      return false;
    } else {
      return getRefCount(fileName).count > 0;
    }
  }

  /**
   * 获取 目标文件的引用计数
   * @param fileName
   * @return
   */
  private RefCount getRefCount(String fileName) {
    assert locked();
    RefCount rc;
    if (!refCounts.containsKey(fileName)) {
      rc = new RefCount(fileName);
      refCounts.put(fileName, rc);
    } else {
      rc = refCounts.get(fileName);
    }
    return rc;
  }

  /** Deletes the specified files, but only if they are new
   *  (have not yet been incref'd). */
  // 删除一组无用的文件
  void deleteNewFiles(Collection<String> files) throws IOException {
    assert locked();
    Set<String> toDelete = new HashSet<>();
    for (final String fileName: files) {
      // NOTE: it's very unusual yet possible for the
      // refCount to be present and 0: it can happen if you
      // open IW on a crashed index, and it removes a bunch
      // of unref'd files, and then you add new docs / do
      // merging, and it reuses that segment name.
      // TestCrash.testCrashAfterReopen can hit this:
      if (!refCounts.containsKey(fileName) || refCounts.get(fileName).count == 0) {
        if (infoStream.isEnabled("IFD")) {
          infoStream.message("IFD", "will delete new file \"" + fileName + "\"");
        }
        toDelete.add(fileName);
      }
    }

    deleteFiles(toDelete);
  }

  /**
   * 删除某些文件
   * @param names
   * @throws IOException
   */
  private void deleteFiles(Collection<String> names) throws IOException {
    assert locked();
    ensureOpen();

    if (infoStream.isEnabled("IFD")) {
      if (names.size() > 0) {
        infoStream.message("IFD", "delete " + names + "");
      }
    }

    // We make two passes, first deleting any segments_N files, second deleting the rest.  We do this so that if we throw exc or JVM
    // crashes during deletions, even when not on Windows, we don't leave the index in an "apparently corrupt" state:
    // 分2次删除的原因是 为了避免在删除失败时 导致索引文件处于被破坏的状态
    for(String name : names) {
      if (name.startsWith(IndexFileNames.SEGMENTS) == false) {
        continue;
      }
      deleteFile(name);
    }

    for(String name : names) {
      if (name.startsWith(IndexFileNames.SEGMENTS) == true) {
        continue;
      }
      deleteFile(name);
    }
  }

  /**
   * 删除目标文件
   * @param fileName
   * @throws IOException
   */
  private void deleteFile(String fileName) throws IOException {
    try {
      directory.deleteFile(fileName);
    } catch (NoSuchFileException | FileNotFoundException e) {
      if (Constants.WINDOWS) {
        // TODO: can we remove this OS-specific hacky logic?  If windows deleteFile is buggy, we should instead contain this workaround in
        // a WindowsFSDirectory ...
        // LUCENE-6684: we suppress this assert for Windows, since a file could be in a confusing "pending delete" state, where we already
        // deleted it once, yet it still shows up in directory listings, and if you try to delete it again you'll hit NSFE/FNFE:
      } else {
        throw e;
      }
    }
  }

  /**
   * Tracks the reference count for a single index file:
   * 文件的引用计数
   */
  final private static class RefCount {

    // fileName used only for better assert error messages
    final String fileName;
    /**
     * 代表被激活了 也就是至少增加过一次
     */
    boolean initDone;
    RefCount(String fileName) {
      this.fileName = fileName;
    }

    int count;

    public int IncRef() {
      if (!initDone) {
        initDone = true;
      } else {
        assert count > 0: Thread.currentThread().getName() + ": RefCount is 0 pre-increment for file \"" + fileName + "\"";
      }
      return ++count;
    }

    public int DecRef() {
      assert count > 0: Thread.currentThread().getName() + ": RefCount is 0 pre-decrement for file \"" + fileName + "\"";
      return --count;
    }
  }

  /**
   * Holds details for each commit point.  This class is
   * also passed to the deletion policy.  Note: this class
   * has a natural ordering that is inconsistent with
   * equals.
   */
  // 描述一个提交点
  final private static class CommitPoint extends IndexCommit {

    /**
     * 该段文件所产生的关联索引文件
     */
    Collection<String> files;
    /**
     * 本次关联的段文件名
     */
    String segmentsFileName;
    /**
     * 该提交点是否已经被删除
     */
    boolean deleted;
    /**
     * 索引文件存储的目标目录 这个是还未包装的对象
     */
    Directory directoryOrig;
    /**
     * 外部传入的容器 当本对象已经被认定需要删除时 添加到容器中
     */
    Collection<CommitPoint> commitsToDelete;
    /**
     * 关联的 segmentInfo 年代信息
     */
    long generation;
    /**
     * 存储了一些用户数据 比如采用什么方式写入索引文件
     */
    final Map<String,String> userData;
    /**
     * 段的数量    每个 segment_N 都包含多个段  每个段对应一个 si(segmentInfo) 文件
     */
    private final int segmentCount;

    /**
     *
     * @param commitsToDelete  用于采集commitPoint的队列
     * @param directoryOrig  段文件所在的 目录
     * @param segmentInfos  该提交点对应的段信息
     * @throws IOException
     */
    public CommitPoint(Collection<CommitPoint> commitsToDelete, Directory directoryOrig, SegmentInfos segmentInfos) throws IOException {
      this.directoryOrig = directoryOrig;
      this.commitsToDelete = commitsToDelete;
      userData = segmentInfos.getUserData();
      segmentsFileName = segmentInfos.getSegmentsFileName();
      generation = segmentInfos.getGeneration();
      files = Collections.unmodifiableCollection(segmentInfos.files(true));
      segmentCount = segmentInfos.size();
    }

    @Override
    public String toString() {
      return "IndexFileDeleter.CommitPoint(" + segmentsFileName + ")";
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
      return directoryOrig;
    }

    @Override
    public long getGeneration() {
      return generation;
    }

    @Override
    public Map<String,String> getUserData() {
      return userData;
    }

    /**
     * Called only be the deletion policy, to remove this
     * commit point from the index.
     */
    @Override
    public void delete() {
      if (!deleted) {
        deleted = true;
        commitsToDelete.add(this);
      }
    }

    @Override
    public boolean isDeleted() {
      return deleted;
    }
  }
}
