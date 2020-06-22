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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * <p>This class implements a {@link MergePolicy} that tries
 * to merge segments into levels of exponentially
 * increasing size, where each level has fewer segments than
 * the value of the merge factor. Whenever extra segments
 * (beyond the merge factor upper bound) are encountered,
 * all segments within the level are merged. You can get or
 * set the merge factor using {@link #getMergeFactor()} and
 * {@link #setMergeFactor(int)} respectively.</p>
 *
 * <p>This class is abstract and requires a subclass to
 * define the {@link #size} method which specifies how a
 * segment's size is determined.  {@link LogDocMergePolicy}
 * is one subclass that measures size by document count in
 * the segment.  {@link LogByteSizeMergePolicy} is another
 * subclass that measures size as the total byte size of the
 * file(s) for the segment.</p>
 * 索引的合并策略   可以往一个段中添加多个文档  并且在flush时 同属与一个段的文档将会合并
 */

public abstract class LogMergePolicy extends MergePolicy {

  /** Defines the allowed range of log(size) for each
   *  level.  A level is computed by taking the max segment
   *  log size, minus LEVEL_LOG_SPAN, and finding all
   *  segments falling within that range. */
  public static final double LEVEL_LOG_SPAN = 0.75;

  /** Default merge factor, which is how many segments are
   *  merged at a time */
  public static final int DEFAULT_MERGE_FACTOR = 10;

  /** Default maximum segment size.  A segment of this size
   *  or larger will never be merged.  @see setMaxMergeDocs */
  public static final int DEFAULT_MAX_MERGE_DOCS = Integer.MAX_VALUE;

  /** Default noCFSRatio.  If a merge's size is {@code >= 10%} of
   *  the index, then we disable compound file for it.
   *  当合并生成的 cfs 文件(复合文件) 超过总索引大小的10%时 不生成复合文件
   *  @see MergePolicy#setNoCFSRatio */
  public static final double DEFAULT_NO_CFS_RATIO = 0.1;

  /** How many segments to merge at a time. */
  // 一次最多合并多少个段
  protected int mergeFactor = DEFAULT_MERGE_FACTOR;

  /** Any segments whose size is smaller than this value
   *  will be rounded up to this value.  This ensures that
   *  tiny segments are aggressively merged. */
  // 小于该长度的段 将会被提升到该值 确保多而小的段会参与到合并中
  protected long minMergeSize;

  /** If the size of a segment exceeds this value then it
   *  will never be merged. */
  // 当段超过这个大小时 就不会被合并
  protected long maxMergeSize;

  // Although the core MPs set it explicitly, we must default in case someone
  // out there wrote his own LMP ...
  /** If the size of a segment exceeds this value then it
   * will never be merged during {@link IndexWriter#forceMerge}. */
  protected long maxMergeSizeForForcedMerge = Long.MAX_VALUE;

  /** If a segment has more than this many documents then it
   *  will never be merged. */
  // 当内部的文档数超过这个值时 不会被合并
  protected int maxMergeDocs = DEFAULT_MAX_MERGE_DOCS;

  /** If true, we pro-rate a segment's size by the
   *  percentage of non-deleted documents. */
  // 代表在计算大小时 是否需要先除开在合并过程中被删除的大小
  protected boolean calibrateSizeByDeletes = true;

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  public LogMergePolicy() {
    super(DEFAULT_NO_CFS_RATIO, MergePolicy.DEFAULT_MAX_CFS_SEGMENT_SIZE);
  }

  /** <p>Returns the number of segments that are merged at
   * once and also controls the total number of segments
   * allowed to accumulate in the index.</p> */
  public int getMergeFactor() {
    return mergeFactor;
  }

  /** Determines how often segment indices are merged by
   * addDocument().  With smaller values, less RAM is used
   * while indexing, and searches are
   * faster, but indexing speed is slower.  With larger
   * values, more RAM is used during indexing, and while
   * searches is slower, indexing is
   * faster.  Thus larger values ({@code > 10}) are best for batch
   * index creation, and smaller values ({@code < 10}) for indices
   * that are interactively maintained. */
  public void setMergeFactor(int mergeFactor) {
    if (mergeFactor < 2)
      throw new IllegalArgumentException("mergeFactor cannot be less than 2");
    this.mergeFactor = mergeFactor;
  }

  /** Sets whether the segment size should be calibrated by
   *  the number of deletes when choosing segments for merge. */
  public void setCalibrateSizeByDeletes(boolean calibrateSizeByDeletes) {
    this.calibrateSizeByDeletes = calibrateSizeByDeletes;
  }

  /** Returns true if the segment size should be calibrated 
   *  by the number of deletes when choosing segments for merge. */
  public boolean getCalibrateSizeByDeletes() {
    return calibrateSizeByDeletes;
  }

  /** Return the number of documents in the provided {@link
   *  SegmentCommitInfo}, pro-rated by percentage of
   *  non-deleted documents if {@link
   *  #setCalibrateSizeByDeletes} is set. */
  // 返回内部 doc的数量
  protected long sizeDocs(SegmentCommitInfo info, MergeContext mergeContext) throws IOException {
    // 需要考虑 合并后会删除的数量
    if (calibrateSizeByDeletes) {
      // 计算在merge过程中 会删除多少doc
      int delCount = mergeContext.numDeletesToMerge(info);
      assert assertDelCount(delCount, info);
      return (info.info.maxDoc() - (long)delCount);
    } else {
      // maxDoc 就代表着 内部有多少 doc 每个段的doc 应该都是从1 开始的
      return info.info.maxDoc();
    }
  }

  /** Return the byte size of the provided {@link
   *  SegmentCommitInfo}, pro-rated by percentage of
   *  non-deleted documents if {@link
   *  #setCalibrateSizeByDeletes} is set. */
  // 估算某个段的大小
  protected long sizeBytes(SegmentCommitInfo info, MergeContext mergeContext) throws IOException {
    // 如果需要考虑被删除的文档 那么需要额外传入一个  mergeContext  该对象内部包含了 delete的信息
    // 这样返回的值  在原有的大小外 除开了在合并过程中删除的大小
    if (calibrateSizeByDeletes) {
      return super.size(info, mergeContext);
    }
    // 该方法就是找到段所在的 directory 下关联的所有文件 对应的 fileLength
    return info.sizeInBytes();
  }
  
  /** Returns true if the number of segments eligible for
   *  merging is less than or equal to the specified {@code
   *  maxNumSegments}. */
  protected boolean isMerged(SegmentInfos infos, int maxNumSegments, Map<SegmentCommitInfo,Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
    final int numSegments = infos.size();
    int numToMerge = 0;
    SegmentCommitInfo mergeInfo = null;
    boolean segmentIsOriginal = false;
    for(int i=0;i<numSegments && numToMerge <= maxNumSegments;i++) {
      final SegmentCommitInfo info = infos.info(i);
      final Boolean isOriginal = segmentsToMerge.get(info);
      if (isOriginal != null) {
        segmentIsOriginal = isOriginal;
        numToMerge++;
        mergeInfo = info;
      }
    }

    return numToMerge <= maxNumSegments &&
      (numToMerge != 1 || !segmentIsOriginal || isMerged(infos, mergeInfo, mergeContext));
  }

  /**
   * Returns the merges necessary to merge the index, taking the max merge
   * size or max merge docs into consideration. This method attempts to respect
   * the {@code maxNumSegments} parameter, however it might be, due to size
   * constraints, that more than that number of segments will remain in the
   * index. Also, this method does not guarantee that exactly {@code
   * maxNumSegments} will remain, but &lt;= that number.
   */
  private MergeSpecification findForcedMergesSizeLimit(
      SegmentInfos infos, int last, MergeContext mergeContext) throws IOException {
    MergeSpecification spec = new MergeSpecification();
    final List<SegmentCommitInfo> segments = infos.asList();

    int start = last - 1;
    while (start >= 0) {
      SegmentCommitInfo info = infos.info(start);
      if (size(info, mergeContext) > maxMergeSizeForForcedMerge || sizeDocs(info, mergeContext) > maxMergeDocs) {
        if (verbose(mergeContext)) {
          message("findForcedMergesSizeLimit: skip segment=" + info + ": size is > maxMergeSize (" + maxMergeSizeForForcedMerge + ") or sizeDocs is > maxMergeDocs (" + maxMergeDocs + ")", mergeContext);
        }
        // need to skip that segment + add a merge for the 'right' segments,
        // unless there is only 1 which is merged.
        if (last - start - 1 > 1 || (start != last - 1 && !isMerged(infos, infos.info(start + 1), mergeContext))) {
          // there is more than 1 segment to the right of
          // this one, or a mergeable single segment.
          spec.add(new OneMerge(segments.subList(start + 1, last)));
        }
        last = start;
      } else if (last - start == mergeFactor) {
        // mergeFactor eligible segments were found, add them as a merge.
        spec.add(new OneMerge(segments.subList(start, last)));
        last = start;
      }
      --start;
    }

    // Add any left-over segments, unless there is just 1
    // already fully merged
    if (last > 0 && (++start + 1 < last || !isMerged(infos, infos.info(start), mergeContext))) {
      spec.add(new OneMerge(segments.subList(start, last)));
    }

    return spec.merges.size() == 0 ? null : spec;
  }
  
  /**
   * Returns the merges necessary to forceMerge the index. This method constraints
   * the returned merges only by the {@code maxNumSegments} parameter, and
   * guaranteed that exactly that number of segments will remain in the index.
   */
  private MergeSpecification findForcedMergesMaxNumSegments(SegmentInfos infos, int maxNumSegments, int last, MergeContext mergeContext) throws IOException {
    MergeSpecification spec = new MergeSpecification();
    final List<SegmentCommitInfo> segments = infos.asList();

    // First, enroll all "full" merges (size
    // mergeFactor) to potentially be run concurrently:
    while (last - maxNumSegments + 1 >= mergeFactor) {
      spec.add(new OneMerge(segments.subList(last - mergeFactor, last)));
      last -= mergeFactor;
    }

    // Only if there are no full merges pending do we
    // add a final partial (< mergeFactor segments) merge:
    if (0 == spec.merges.size()) {
      if (maxNumSegments == 1) {

        // Since we must merge down to 1 segment, the
        // choice is simple:
        if (last > 1 || !isMerged(infos, infos.info(0), mergeContext)) {
          spec.add(new OneMerge(segments.subList(0, last)));
        }
      } else if (last > maxNumSegments) {

        // Take care to pick a partial merge that is
        // least cost, but does not make the index too
        // lopsided.  If we always just picked the
        // partial tail then we could produce a highly
        // lopsided index over time:

        // We must merge this many segments to leave
        // maxNumSegments in the index (from when
        // forceMerge was first kicked off):
        final int finalMergeSize = last - maxNumSegments + 1;

        // Consider all possible starting points:
        long bestSize = 0;
        int bestStart = 0;

        for(int i=0;i<last-finalMergeSize+1;i++) {
          long sumSize = 0;
          for(int j=0;j<finalMergeSize;j++) {
            sumSize += size(infos.info(j+i), mergeContext);
          }
          if (i == 0 || (sumSize < 2*size(infos.info(i-1), mergeContext) && sumSize < bestSize)) {
            bestStart = i;
            bestSize = sumSize;
          }
        }

        spec.add(new OneMerge(segments.subList(bestStart, bestStart + finalMergeSize)));
      }
    }
    return spec.merges.size() == 0 ? null : spec;
  }
  
  /** Returns the merges necessary to merge the index down
   *  to a specified number of segments.
   *  This respects the {@link #maxMergeSizeForForcedMerge} setting.
   *  By default, and assuming {@code maxNumSegments=1}, only
   *  one segment will be left in the index, where that segment
   *  has no deletions pending nor separate norms, and it is in
   *  compound file format if the current useCompoundFile
   *  setting is true.  This method returns multiple merges
   *  (mergeFactor at a time) so the {@link MergeScheduler}
   *  in use may make use of concurrency. */
  @Override
  public MergeSpecification findForcedMerges(SegmentInfos infos,
                                             int maxNumSegments, Map<SegmentCommitInfo,Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {

    assert maxNumSegments > 0;
    if (verbose(mergeContext)) {
      message("findForcedMerges: maxNumSegs=" + maxNumSegments + " segsToMerge="+ segmentsToMerge, mergeContext);
    }

    // If the segments are already merged (e.g. there's only 1 segment), or
    // there are <maxNumSegments:.
    if (isMerged(infos, maxNumSegments, segmentsToMerge, mergeContext)) {
      if (verbose(mergeContext)) {
        message("already merged; skip", mergeContext);
      }
      return null;
    }

    // Find the newest (rightmost) segment that needs to
    // be merged (other segments may have been flushed
    // since merging started):
    int last = infos.size();
    while (last > 0) {
      final SegmentCommitInfo info = infos.info(--last);
      if (segmentsToMerge.get(info) != null) {
        last++;
        break;
      }
    }

    if (last == 0) {
      if (verbose(mergeContext)) {
        message("last == 0; skip", mergeContext);
      }
      return null;
    }
    
    // There is only one segment already, and it is merged
    if (maxNumSegments == 1 && last == 1 && isMerged(infos, infos.info(0), mergeContext)) {
      if (verbose(mergeContext)) {
        message("already 1 seg; skip", mergeContext);
      }
      return null;
    }

    // Check if there are any segments above the threshold
    boolean anyTooLarge = false;
    for (int i = 0; i < last; i++) {
      SegmentCommitInfo info = infos.info(i);
      if (size(info, mergeContext) > maxMergeSizeForForcedMerge || sizeDocs(info, mergeContext) > maxMergeDocs) {
        anyTooLarge = true;
        break;
      }
    }

    if (anyTooLarge) {
      return findForcedMergesSizeLimit(infos, last, mergeContext);
    } else {
      return findForcedMergesMaxNumSegments(infos, maxNumSegments, last, mergeContext);
    }
  }

  /**
   * Finds merges necessary to force-merge all deletes from the
   * index.  We simply merge adjacent segments that have
   * deletes, up to mergeFactor at a time.
   */ 
  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext)
      throws IOException {
    final List<SegmentCommitInfo> segments = segmentInfos.asList();
    final int numSegments = segments.size();

    if (verbose(mergeContext)) {
      message("findForcedDeleteMerges: " + numSegments + " segments", mergeContext);
    }

    MergeSpecification spec = new MergeSpecification();
    int firstSegmentWithDeletions = -1;
    assert mergeContext != null;
    for(int i=0;i<numSegments;i++) {
      final SegmentCommitInfo info = segmentInfos.info(i);
      int delCount = mergeContext.numDeletesToMerge(info);
      assert assertDelCount(delCount, info);
      if (delCount > 0) {
        if (verbose(mergeContext)) {
          message("  segment " + info.info.name + " has deletions", mergeContext);
        }
        if (firstSegmentWithDeletions == -1)
          firstSegmentWithDeletions = i;
        else if (i - firstSegmentWithDeletions == mergeFactor) {
          // We've seen mergeFactor segments in a row with
          // deletions, so force a merge now:
          if (verbose(mergeContext)) {
            message("  add merge " + firstSegmentWithDeletions + " to " + (i-1) + " inclusive", mergeContext);
          }
          spec.add(new OneMerge(segments.subList(firstSegmentWithDeletions, i)));
          firstSegmentWithDeletions = i;
        }
      } else if (firstSegmentWithDeletions != -1) {
        // End of a sequence of segments with deletions, so,
        // merge those past segments even if it's fewer than
        // mergeFactor segments
        if (verbose(mergeContext)) {
          message("  add merge " + firstSegmentWithDeletions + " to " + (i-1) + " inclusive", mergeContext);
        }
        spec.add(new OneMerge(segments.subList(firstSegmentWithDeletions, i)));
        firstSegmentWithDeletions = -1;
      }
    }

    if (firstSegmentWithDeletions != -1) {
      if (verbose(mergeContext)) {
        message("  add merge " + firstSegmentWithDeletions + " to " + (numSegments-1) + " inclusive", mergeContext);
      }
      spec.add(new OneMerge(segments.subList(firstSegmentWithDeletions, numSegments)));
    }

    return spec;
  }

  /**
   * 该对象内部包含了 段信息以及 level 信息
   */
  private static class SegmentInfoAndLevel implements Comparable<SegmentInfoAndLevel> {

    SegmentCommitInfo info;
    /**
     * 合并过程中会将不同的段分为不同的级别 这里是描述段信息 以及level信息
     */
    float level;
    
    public SegmentInfoAndLevel(SegmentCommitInfo info, float level) {
      this.info = info;
      this.level = level;
    }

    // Sorts largest to smallest
    @Override
    public int compareTo(SegmentInfoAndLevel other) {
      return Float.compare(other.level, level);
    }
  }

  /** Checks if any merges are now necessary and returns a
   *  {@link MergePolicy.MergeSpecification} if so.  A merge
   *  is necessary when there are more than {@link
   *  #setMergeFactor} segments at a given level.  When
   *  multiple levels have too many segments, this method
   *  will return multiple merges, allowing the {@link
   *  MergeScheduler} to use concurrency.
   *  先寻找哪些 数据需要被合并
   * @param mergeTrigger 定义merge的触发时机
   */
  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {

    // 找到内部共有多少个段
    final int numSegments = infos.size();
    // 理解为是否打印日志
    if (verbose(mergeContext)) {
      message("findMerges: " + numSegments + " segments", mergeContext);
    }

    // Compute levels, which is just log (base mergeFactor)
    // of the size of each segment
    // 为每个segment 维护 一个SegmentInfoAndLevel 对象
    final List<SegmentInfoAndLevel> levels = new ArrayList<>(numSegments);
    // 返回以 E 为底的对数值
    final float norm = (float) Math.log(mergeFactor);

    // 返回当前正在 merge中的 段对象
    final Set<SegmentCommitInfo> mergingSegments = mergeContext.getMergingSegments();

    for(int i=0;i<numSegments;i++) {
      final SegmentCommitInfo info = infos.info(i);
      // 计算每个段的大小
      long size = size(info, mergeContext);

      // Floor tiny segments
      if (size < 1) {
        size = 1;
      }

      // 通过特殊的方式计算出了一个 level   能够得到的就是 size 越大  计算出来的level 也会越大
      final SegmentInfoAndLevel infoLevel = new SegmentInfoAndLevel(info, (float) Math.log(size)/norm);
      levels.add(infoLevel);

      // 打印日志信息
      if (verbose(mergeContext)) {
        final long segBytes = sizeBytes(info, mergeContext);
        String extra = mergingSegments.contains(info) ? " [merging]" : "";
        if (size >= maxMergeSize) {
          extra += " [skip: too large]";
        }
        message("seg=" + segString(mergeContext, Collections.singleton(info)) + " level=" + infoLevel.level + " size=" + String.format(Locale.ROOT, "%.3f MB", segBytes/1024/1024.) + extra, mergeContext);
      }
    }

    // 上面的步骤就是将 segment 计算level 并包装成 SegmentInfoAndLevel 对象
    final float levelFloor;
    // 这里代表 merge 的起点
    if (minMergeSize <= 0)
      levelFloor = (float) 0.0;
    else
      levelFloor = (float) (Math.log(minMergeSize)/norm);

    // Now, we quantize the log values into levels.  The
    // first level is any segment whose log size is within
    // LEVEL_LOG_SPAN of the max size, or, who has such as
    // segment "to the right".  Then, we find the max of all
    // other segments and use that to define the next level
    // segment, etc.

    MergeSpecification spec = null;

    // 总计有多少个段需要被合并
    final int numMergeableSegments = levels.size();

    int start = 0;
    // 这里会选取符合条件的 segment 并且写入到 spec中
    while(start < numMergeableSegments) {

      // Find max level of all segments not already
      // quantized.
      // 这里先找到最大的 level
      float maxLevel = levels.get(start).level;
      // 每次 maxLevel的选取范围都在减小
      for(int i=1+start;i<numMergeableSegments;i++) {
        final float level = levels.get(i).level;
        if (level > maxLevel) {
          maxLevel = level;
        }
      }

      // Now search backwards for the rightmost segment that
      // falls into this level:
      float levelBottom;  // 这个值是一个选取的起点  所有level超过该值的 segment都会被纳入本次的merge操作中
      // 代表每个 segment的大小都很小 都处在允许被merge的范围内
      if (maxLevel <= levelFloor) {
        // All remaining segments fall into the min level
        levelBottom = -1.0F;
      } else {
        // 这里选取的level 区间为 0.75
        levelBottom = (float) (maxLevel - LEVEL_LOG_SPAN);

        // Force a boundary at the level floor
        // bottom < floor < max  更新bottom  这里反而选择提高了下限 这样只有更少的segment 允许被merge
        if (levelBottom < levelFloor && maxLevel >= levelFloor) {
          levelBottom = levelFloor;
        }
      }

      int upto = numMergeableSegments-1;
      while(upto >= start) {
        if (levels.get(upto).level >= levelBottom) {
          break;
        }
        upto--;
      }
      if (verbose(mergeContext)) {
        message("  level " + levelBottom + " to " + maxLevel + ": " + (1+upto-start) + " segments", mergeContext);
      }

      // Finally, record all merges that are viable at this level:
      int end = start + mergeFactor;
      while(end <= 1+upto) {
        boolean anyTooLarge = false;
        boolean anyMerging = false;
        for(int i=start;i<end;i++) {
          final SegmentCommitInfo info = levels.get(i).info;
          anyTooLarge |= (size(info, mergeContext) >= maxMergeSize || sizeDocs(info, mergeContext) >= maxMergeDocs);
          if (mergingSegments.contains(info)) {
            anyMerging = true;
            break;
          }
        }

        if (anyMerging) {
          // skip
        } else if (!anyTooLarge) {
          if (spec == null)
            spec = new MergeSpecification();
          final List<SegmentCommitInfo> mergeInfos = new ArrayList<>(end-start);
          for(int i=start;i<end;i++) {
            mergeInfos.add(levels.get(i).info);
            assert infos.contains(levels.get(i).info);
          }
          if (verbose(mergeContext)) {
            message("  add merge=" + segString(mergeContext, mergeInfos) + " start=" + start + " end=" + end, mergeContext);
          }
          spec.add(new OneMerge(mergeInfos));
        } else if (verbose(mergeContext)) {
          message("    " + start + " to " + end + ": contains segment over maxMergeSize or maxMergeDocs; skipping", mergeContext);
        }

        start = end;
        end = start + mergeFactor;
      }

      start = 1+upto;
    }

    return spec;
  }

  /** <p>Determines the largest segment (measured by
   * document count) that may be merged with other segments.
   * Small values (e.g., less than 10,000) are best for
   * interactive indexing, as this limits the length of
   * pauses while indexing to a few seconds.  Larger values
   * are best for batched indexing and speedier
   * searches.</p>
   *
   * <p>The default value is {@link Integer#MAX_VALUE}.</p>
   *
   * <p>The default merge policy ({@link
   * LogByteSizeMergePolicy}) also allows you to set this
   * limit by net size (in MB) of the segment, using {@link
   * LogByteSizeMergePolicy#setMaxMergeMB}.</p>
   */
  public void setMaxMergeDocs(int maxMergeDocs) {
    this.maxMergeDocs = maxMergeDocs;
  }

  /** Returns the largest segment (measured by document
   *  count) that may be merged with other segments.
   *  @see #setMaxMergeDocs */
  public int getMaxMergeDocs() {
    return maxMergeDocs;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[" + getClass().getSimpleName() + ": ");
    sb.append("minMergeSize=").append(minMergeSize).append(", ");
    sb.append("mergeFactor=").append(mergeFactor).append(", ");
    sb.append("maxMergeSize=").append(maxMergeSize).append(", ");
    sb.append("maxMergeSizeForForcedMerge=").append(maxMergeSizeForForcedMerge).append(", ");
    sb.append("calibrateSizeByDeletes=").append(calibrateSizeByDeletes).append(", ");
    sb.append("maxMergeDocs=").append(maxMergeDocs).append(", ");
    sb.append("maxCFSSegmentSizeMB=").append(getMaxCFSSegmentSizeMB()).append(", ");
    sb.append("noCFSRatio=").append(noCFSRatio);
    sb.append("]");
    return sb.toString();
  }

}
