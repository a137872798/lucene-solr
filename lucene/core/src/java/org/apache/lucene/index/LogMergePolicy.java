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
   *  @see MergePolicy#setNoCFSRatio */
  public static final double DEFAULT_NO_CFS_RATIO = 0.1;

  /** How many segments to merge at a time. */
  protected int mergeFactor = DEFAULT_MERGE_FACTOR;

  /** Any segments whose size is smaller than this value
   *  will be rounded up to this value.  This ensures that
   *  tiny segments are aggressively merged. */
  // 满足merge的最小大小
  protected long minMergeSize;

  /** If the size of a segment exceeds this value then it
   *  will never be merged. */
  // 针对单个段的 最大长度
  protected long maxMergeSize;

  // Although the core MPs set it explicitly, we must default in case someone
  // out there wrote his own LMP ...
  /** If the size of a segment exceeds this value then it
   * will never be merged during {@link IndexWriter#forceMerge}. */
  protected long maxMergeSizeForForcedMerge = Long.MAX_VALUE;

  /** If a segment has more than this many documents then it
   *  will never be merged. */
  // 针对单个段包含的doc数量
  protected int maxMergeDocs = DEFAULT_MAX_MERGE_DOCS;

  /** If true, we pro-rate a segment's size by the
   *  percentage of non-deleted documents. */
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
  protected long sizeDocs(SegmentCommitInfo info, MergeContext mergeContext) throws IOException {
    if (calibrateSizeByDeletes) {
      int delCount = mergeContext.numDeletesToMerge(info);
      assert assertDelCount(delCount, info);
      return (info.info.maxDoc() - (long)delCount);
    } else {
      return info.info.maxDoc();
    }
  }

  /** Return the byte size of the provided {@link
   *  SegmentCommitInfo}, pro-rated by percentage of
   *  non-deleted documents if {@link
   *  #setCalibrateSizeByDeletes} is set. */
  protected long sizeBytes(SegmentCommitInfo info, MergeContext mergeContext) throws IOException {
    if (calibrateSizeByDeletes) {
      return super.size(info, mergeContext);
    }
    return info.sizeInBytes();
  }
  
  /** Returns true if the number of segments eligible for
   *  merging is less than or equal to the specified {@code
   *  maxNumSegments}. */
  protected boolean isMerged(SegmentInfos infos, int maxNumSegments, Map<SegmentCommitInfo,Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {
    // 本次传入的 segment总数
    final int numSegments = infos.size();
    int numToMerge = 0;
    SegmentCommitInfo mergeInfo = null;
    boolean segmentIsOriginal = false;
    // 当此时还没有遍历到 segment的尾部 且 还没有达到merge要求的上限时
    for(int i=0;i<numSegments && numToMerge <= maxNumSegments;i++) {
      final SegmentCommitInfo info = infos.info(i);
      // 从该容器中判断 该segment 是否已经合并了
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
   * @param last 从0到(last-1)之内的数据是考察范围
   * 当超过长度限制时 采取的强制合并
   */
  private MergeSpecification findForcedMergesSizeLimit(
      SegmentInfos infos, int last, MergeContext mergeContext) throws IOException {
    MergeSpecification spec = new MergeSpecification();
    final List<SegmentCommitInfo> segments = infos.asList();

    int start = last - 1;
    while (start >= 0) {
      SegmentCommitInfo info = infos.info(start);
      // 如果长度超过了 强制合并的长度 打印日志
      if (size(info, mergeContext) > maxMergeSizeForForcedMerge || sizeDocs(info, mergeContext) > maxMergeDocs) {
        if (verbose(mergeContext)) {
          message("findForcedMergesSizeLimit: skip segment=" + info + ": size is > maxMergeSize (" + maxMergeSizeForForcedMerge + ") or sizeDocs is > maxMergeDocs (" + maxMergeDocs + ")", mergeContext);
        }
        // need to skip that segment + add a merge for the 'right' segments,
        // unless there is only 1 which is merged.
        // 当 last 与 start的跨度超过1(至少为2)时  即使不满足mergeFactor 也会包装成OneMerge对象 并尝试合并   在findMerge方法中 要求每个OneMerge内的segment数量必须达到mergeFactor
        if (last - start - 1 > 1 || (start != last - 1 && !isMerged(infos, infos.info(start + 1), mergeContext))) {
          // there is more than 1 segment to the right of
          // this one, or a mergeable single segment.
          // 这里生成的merge 对象实际内部值包含了一个 segment    此时 start对应的segment的长度超标 所以start+1 实际上是跳过了这个segment
          spec.add(new OneMerge(segments.subList(start + 1, last)));
        }
        // 如果 last - start - 1 = 0 代表刚好扫描到的segment是大块的 由于subList 含头不含尾  不能写成(start,last) 所以就不需要进行合并
        // 这样就会跳过大块的 segment
        last = start;
      // 当长度没有超标时 必须确保之间的差值大小满足mergeFactor
      } else if (last - start == mergeFactor) {
        // mergeFactor eligible segments were found, add them as a merge.
        spec.add(new OneMerge(segments.subList(start, last)));
        last = start;
      }
      // 增加差值
      --start;
    }

    // Add any left-over segments, unless there is just 1
    // already fully merged
    // 代表最后剩余的量 不足 mergeFactor   并且差值大于1 也就是超过一个segment 或者 单个segment 还没有被merge 那么将范围内的segment merge
    //
    if (last > 0 && (++start + 1 < last || !isMerged(infos, infos.info(start), mergeContext))) {
      spec.add(new OneMerge(segments.subList(start, last)));
    }

    return spec.merges.size() == 0 ? null : spec;
  }
  
  /**
   * Returns the merges necessary to forceMerge the index. This method constraints
   * the returned merges only by the {@code maxNumSegments} parameter, and
   * guaranteed that exactly that number of segments will remain in the index.
   * 进入该方法前已经确定了 整个infos 中没有长度超标的对象
   */
  private MergeSpecification findForcedMergesMaxNumSegments(SegmentInfos infos, int maxNumSegments, int last, MergeContext mergeContext) throws IOException {
    MergeSpecification spec = new MergeSpecification();
    final List<SegmentCommitInfo> segments = infos.asList();

    // First, enroll all "full" merges (size
    // mergeFactor) to potentially be run concurrently:
    // 确保保留 超过 maxNumSegments 数量的segment
    while (last - maxNumSegments + 1 >= mergeFactor) {
      spec.add(new OneMerge(segments.subList(last - mergeFactor, last)));
      last -= mergeFactor;
    }

    // Only if there are no full merges pending do we
    // add a final partial (< mergeFactor segments) merge:
    // 代表长度 不满足mergeFactor
    if (0 == spec.merges.size()) {
      if (maxNumSegments == 1) {

        // Since we must merge down to 1 segment, the
        // choice is simple:
        // 将所有segment 作为待merge对象
        if (last > 1 || !isMerged(infos, infos.info(0), mergeContext)) {
          spec.add(new OneMerge(segments.subList(0, last)));
        }
      // 当此时剩余数量超过 maxNumSegments 时  按照这个数量进行merge
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

        // 代表从0 遍历到 maxNumSegments
        for(int i=0;i<last-finalMergeSize+1;i++) {
          long sumSize = 0;
          // TODO ???
          for(int j=0;j<finalMergeSize;j++) {
            sumSize += size(infos.info(j+i), mergeContext);
          }
          if (i == 0 || (sumSize < 2*size(infos.info(i-1), mergeContext) && sumSize < bestSize)) {
            bestStart = i;
            bestSize = sumSize;
          }
        }

        // 这里只合并 finalMergeSize 长度的数据
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
   *  in use may make use of concurrency.
   *  找到需要强制合并的对象
   */
  @Override
  public MergeSpecification findForcedMerges(SegmentInfos infos,   // 候选的segment
                                             int maxNumSegments,   // 这些数量的 segment 好像不能被merge
                                             Map<SegmentCommitInfo,Boolean> segmentsToMerge,
                                             MergeContext mergeContext) throws IOException {

    assert maxNumSegments > 0;
    if (verbose(mergeContext)) {
      message("findForcedMerges: maxNumSegs=" + maxNumSegments + " segsToMerge="+ segmentsToMerge, mergeContext);
    }

    // If the segments are already merged (e.g. there's only 1 segment), or
    // there are <maxNumSegments:.
    // 判断对象是否已经被merge  如果已经被merge了 就返回null
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

    // 如果 map内都没有设置数据返回null
    if (last == 0) {
      if (verbose(mergeContext)) {
        message("last == 0; skip", mergeContext);
      }
      return null;
    }
    
    // There is only one segment already, and it is merged
    // 如果只有一个segment 并且已经merged了 也是返回null
    if (maxNumSegments == 1 && last == 1 && isMerged(infos, infos.info(0), mergeContext)) {
      if (verbose(mergeContext)) {
        message("already 1 seg; skip", mergeContext);
      }
      return null;
    }

    // Check if there are any segments above the threshold
    // 检测当前符合 条件的segment中 是否超过了merge的最大值限制
    boolean anyTooLarge = false;
    for (int i = 0; i < last; i++) {
      SegmentCommitInfo info = infos.info(i);
      if (size(info, mergeContext) > maxMergeSizeForForcedMerge || sizeDocs(info, mergeContext) > maxMergeDocs) {
        anyTooLarge = true;
        break;
      }
    }

    // 当超过长度限制时  依然强制进行合并
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
   * 该对象内部包含了 段的描述信息 以及通过一个 Len 计算出来的 对数值
   */
  private static class SegmentInfoAndLevel implements Comparable<SegmentInfoAndLevel> {
    SegmentCommitInfo info;
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
   *  MergeScheduler} to use concurrency. */
  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {

    final int numSegments = infos.size();
    if (verbose(mergeContext)) {
      message("findMerges: " + numSegments + " segments", mergeContext);
    }

    // Compute levels, which is just log (base mergeFactor)
    // of the size of each segment
    final List<SegmentInfoAndLevel> levels = new ArrayList<>(numSegments);
    final float norm = (float) Math.log(mergeFactor);

    final Set<SegmentCommitInfo> mergingSegments = mergeContext.getMergingSegments();

    for(int i=0;i<numSegments;i++) {
      final SegmentCommitInfo info = infos.info(i);
      long size = size(info, mergeContext);

      // Floor tiny segments
      if (size < 1) {
        size = 1;
      }

      final SegmentInfoAndLevel infoLevel = new SegmentInfoAndLevel(info, (float) Math.log(size)/norm);
      levels.add(infoLevel);

      if (verbose(mergeContext)) {
        final long segBytes = sizeBytes(info, mergeContext);
        String extra = mergingSegments.contains(info) ? " [merging]" : "";
        if (size >= maxMergeSize) {
          extra += " [skip: too large]";
        }
        message("seg=" + segString(mergeContext, Collections.singleton(info)) + " level=" + infoLevel.level + " size=" + String.format(Locale.ROOT, "%.3f MB", segBytes/1024/1024.) + extra, mergeContext);
      }
    }

    final float levelFloor;
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

    final int numMergeableSegments = levels.size();

    int start = 0;
    while(start < numMergeableSegments) {

      // Find max level of all segments not already
      // quantized.
      float maxLevel = levels.get(start).level;
      for(int i=1+start;i<numMergeableSegments;i++) {
        final float level = levels.get(i).level;
        if (level > maxLevel) {
          maxLevel = level;
        }
      }

      // Now search backwards for the rightmost segment that
      // falls into this level:
      // 当所有level 都小于 floor时 bottom 为负数 也就代表不对level 做限制
      float levelBottom;
      if (maxLevel <= levelFloor) {
        // All remaining segments fall into the min level
        levelBottom = -1.0F;
      } else {
        // 默认选取的范围 是 max往下0.75 范围内
        levelBottom = (float) (maxLevel - LEVEL_LOG_SPAN);

        // Force a boundary at the level floor
        // 当bottom 小于 floor时 强制使用floor作为下限
        if (levelBottom < levelFloor && maxLevel >= levelFloor) {
          levelBottom = levelFloor;
        }
      }

      // 刚好变成list的下标
      int upto = numMergeableSegments-1;
      while(upto >= start) {
        // 从上往下 一旦发现超过 bottom的跳出循环
        if (levels.get(upto).level >= levelBottom) {
          break;
        }
        upto--;
      }
      if (verbose(mergeContext)) {
        message("  level " + levelBottom + " to " + maxLevel + ": " + (1+upto-start) + " segments", mergeContext);
      }

      // Finally, record all merges that are viable at this level:
      // 记录 从start开始 直到满足merge的数量要求时 下标是多少
      int end = start + mergeFactor;

      // 如果mergeFactor 值较小 就会分多次生成多个 OneMerge对象 每个对象对应一条merge线程
      // 当end 一开始就超过 upto+1 时 更新start 并进入下一轮
      while(end <= 1+upto) {
        boolean anyTooLarge = false;
        boolean anyMerging = false;
        // 从start 开始遍历每个 段信息对象
        for(int i=start;i<end;i++) {
          final SegmentCommitInfo info = levels.get(i).info;
          // 如果该段的长度超过预定值 或者内含的文档数超过预定值  认为无法被merge
          anyTooLarge |= (size(info, mergeContext) >= maxMergeSize || sizeDocs(info, mergeContext) >= maxMergeDocs);
          // 如果当前段 已经在merge中了
          if (mergingSegments.contains(info)) {
            anyMerging = true;
            break;
          }
        }

        if (anyMerging) {
          // skip
          // 只有 这种情况会正常处理   否则只是打印日志
        } else if (!anyTooLarge) {
          // 初始化本次合并的描述信息
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
          // 将参与合并的 merge对象整个包装成 单次合并对象 OneMerge 并设置到 MergeSpecification 中
          spec.add(new OneMerge(mergeInfos));
        } else if (verbose(mergeContext)) {
          message("    " + start + " to " + end + ": contains segment over maxMergeSize or maxMergeDocs; skipping", mergeContext);
        }

        // 实际上每次都是生成多个merge任务 并行执行  每个任务包含了多个 segment
        start = end;
        end = start + mergeFactor;
      }

      start = 1+upto;
    }

    // 将所有段对象 分组后 合并成OneMerge对象 并填充到MergeSpecification 中 返回
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
   * 设置merge的有关文档数的限制条件 (segment文档数量不能超过这个值)   也就是说 大块的segment是不推荐合并的  可能比较耗时的关系??? 并且segment过大也会增加
   * 单次加载到内存中的开销 以及降低检索速度
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
