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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Merges segments of approximately equal size, subject to
 * an allowed number of segments per tier.  This is similar
 * to {@link LogByteSizeMergePolicy}, except this merge
 * policy is able to merge non-adjacent segment, and
 * separates how many segments are merged at once ({@link
 * #setMaxMergeAtOnce}) from how many segments are allowed
 * per tier ({@link #setSegmentsPerTier}).  This merge
 * policy also does not over-merge (i.e. cascade merges).
 *
 * <p>For normal merging, this policy first computes a
 * "budget" of how many segments are allowed to be in the
 * index.  If the index is over-budget, then the policy
 * sorts segments by decreasing size (pro-rating by percent
 * deletes), and then finds the least-cost merge.  Merge
 * cost is measured by a combination of the "skew" of the
 * merge (size of largest segment divided by smallest segment),
 * total merge size and percent deletes reclaimed,
 * so that merges with lower skew, smaller size
 * and those reclaiming more deletes, are
 * favored.
 *
 * <p>If a merge will produce a segment that's larger than
 * {@link #setMaxMergedSegmentMB}, then the policy will
 * merge fewer segments (down to 1 at once, if that one has
 * deletions) to keep the segment size under budget.
 *
 * <p><b>NOTE</b>: this policy freely merges non-adjacent
 * segments; if this is a problem, use {@link
 * LogMergePolicy}.
 *
 * <p><b>NOTE</b>: This policy always merges by byte size
 * of the segments, always pro-rates by percent deletes
 *
 * <p><b>NOTE</b> Starting with Lucene 7.5, there are several changes:
 * <p>
 * - findForcedMerges and findForcedDeletesMerges) respect the max segment
 * size by default.
 * <p>
 * - When findforcedmerges is called with maxSegmentCount other than 1,
 * the resulting index is not guaranteed to have &lt;= maxSegmentCount segments.
 * Rather it is on a "best effort" basis. Specifically the theoretical ideal
 * segment size is calculated and a "fudge factor" of 25% is added as the
 * new maxSegmentSize, which is respected.
 * <p>
 * - findForcedDeletesMerges will not produce segments greater than
 * maxSegmentSize.
 *
 * @lucene.experimental
 */

// TODO
//   - we could try to take into account whether a large
//     merge is already running (under CMS) and then bias
//     ourselves towards picking smaller merges if so (or,
//     maybe CMS should do so)
// 包含分层特性的merge 对象      MergePolicy 最核心的功能还是挑选哪些segment 应该被merge

public class TieredMergePolicy extends MergePolicy {
    /**
     * Default noCFSRatio.  If a merge's size is {@code >= 10%} of
     * the index, then we disable compound file for it.
     *
     * @see MergePolicy#setNoCFSRatio
     */
    public static final double DEFAULT_NO_CFS_RATIO = 0.1;

    // User-specified maxMergeAtOnce. In practice we always take the min of its
    // value and segsPerTier to avoid suboptimal merging.
    // 单次合并 最多多少个segment
    private int maxMergeAtOnce = 10;
    /**
     * 参与merge 的每个segment 允许的最大大小
     */
    private long maxMergedSegmentBytes = 5 * 1024 * 1024 * 1024L;
    private int maxMergeAtOnceExplicit = 30;

    /**
     * 应该是在merge中要求的最小大小
     */
    private long floorSegmentBytes = 2 * 1024 * 1024L;
    /**
     * 每层维护10个段
     */
    private double segsPerTier = 10.0;
    private double forceMergeDeletesPctAllowed = 10.0;

    /**
     * 这是一个删除率的阈值  超过该值认为本次merge 的效率会高(删除了更多的无效doc)  删除率低于该值 且segment本身比较大的话是会被排除在考虑范围外的
     */
    private double deletesPctAllowed = 33.0;

    /**
     * Sole constructor, setting all settings to their
     * defaults.
     */
    public TieredMergePolicy() {
        super(DEFAULT_NO_CFS_RATIO, MergePolicy.DEFAULT_MAX_CFS_SEGMENT_SIZE);
    }

    /**
     * Maximum number of segments to be merged at a time
     * during "normal" merging.  For explicit merging (eg,
     * forceMerge or forceMergeDeletes was called), see {@link
     * #setMaxMergeAtOnceExplicit}.  Default is 10.
     */
    public TieredMergePolicy setMaxMergeAtOnce(int v) {
        if (v < 2) {
            throw new IllegalArgumentException("maxMergeAtOnce must be > 1 (got " + v + ")");
        }
        maxMergeAtOnce = v;
        return this;
    }

    /**
     * 代表合并策略采用的类型
     */
    private enum MERGE_TYPE {
        /**
         * 当索引发生变更时 触发合并
         */
        NATURAL,
        FORCE_MERGE,
        FORCE_MERGE_DELETES
    }

    /**
     * Returns the current maxMergeAtOnce setting.
     *
     * @see #setMaxMergeAtOnce
     */
    public int getMaxMergeAtOnce() {
        return maxMergeAtOnce;
    }

    // TODO: should addIndexes do explicit merging, too?  And,
    // if user calls IW.maybeMerge "explicitly"

    /**
     * Maximum number of segments to be merged at a time,
     * during forceMerge or forceMergeDeletes. Default is 30.
     */
    public TieredMergePolicy setMaxMergeAtOnceExplicit(int v) {
        if (v < 2) {
            throw new IllegalArgumentException("maxMergeAtOnceExplicit must be > 1 (got " + v + ")");
        }
        maxMergeAtOnceExplicit = v;
        return this;
    }


    /**
     * Returns the current maxMergeAtOnceExplicit setting.
     *
     * @see #setMaxMergeAtOnceExplicit
     */
    public int getMaxMergeAtOnceExplicit() {
        return maxMergeAtOnceExplicit;
    }

    /**
     * Maximum sized segment to produce during
     * normal merging.  This setting is approximate: the
     * estimate of the merged segment size is made by summing
     * sizes of to-be-merged segments (compensating for
     * percent deleted docs).  Default is 5 GB.
     */
    public TieredMergePolicy setMaxMergedSegmentMB(double v) {
        if (v < 0.0) {
            throw new IllegalArgumentException("maxMergedSegmentMB must be >=0 (got " + v + ")");
        }
        v *= 1024 * 1024;
        maxMergedSegmentBytes = v > Long.MAX_VALUE ? Long.MAX_VALUE : (long) v;
        return this;
    }

    /**
     * Returns the current maxMergedSegmentMB setting.
     *
     * @see #setMaxMergedSegmentMB
     */
    public double getMaxMergedSegmentMB() {
        return maxMergedSegmentBytes / 1024.0 / 1024.0;
    }

    /**
     * Controls the maximum percentage of deleted documents that is tolerated in
     * the index. Lower values make the index more space efficient at the
     * expense of increased CPU and I/O activity. Values must be between 20 and
     * 50. Default value is 33.
     */
    public TieredMergePolicy setDeletesPctAllowed(double v) {
        if (v < 20 || v > 50) {
            throw new IllegalArgumentException("indexPctDeletedTarget must be >= 20.0 and <= 50 (got " + v + ")");
        }
        deletesPctAllowed = v;
        return this;
    }

    /**
     * Returns the current deletesPctAllowed setting.
     *
     * @see #setDeletesPctAllowed
     */
    public double getDeletesPctAllowed() {
        return deletesPctAllowed;
    }

    /**
     * Segments smaller than this are "rounded up" to this
     * size, ie treated as equal (floor) size for merge
     * selection.  This is to prevent frequent flushing of
     * tiny segments from allowing a long tail in the index.
     * Default is 2 MB.
     */
    public TieredMergePolicy setFloorSegmentMB(double v) {
        if (v <= 0.0) {
            throw new IllegalArgumentException("floorSegmentMB must be > 0.0 (got " + v + ")");
        }
        v *= 1024 * 1024;
        floorSegmentBytes = v > Long.MAX_VALUE ? Long.MAX_VALUE : (long) v;
        return this;
    }

    /**
     * Returns the current floorSegmentMB.
     *
     * @see #setFloorSegmentMB
     */
    public double getFloorSegmentMB() {
        return floorSegmentBytes / (1024 * 1024.);
    }

    /**
     * When forceMergeDeletes is called, we only merge away a
     * segment if its delete percentage is over this
     * threshold.  Default is 10%.
     */
    public TieredMergePolicy setForceMergeDeletesPctAllowed(double v) {
        if (v < 0.0 || v > 100.0) {
            throw new IllegalArgumentException("forceMergeDeletesPctAllowed must be between 0.0 and 100.0 inclusive (got " + v + ")");
        }
        forceMergeDeletesPctAllowed = v;
        return this;
    }

    /**
     * Returns the current forceMergeDeletesPctAllowed setting.
     *
     * @see #setForceMergeDeletesPctAllowed
     */
    public double getForceMergeDeletesPctAllowed() {
        return forceMergeDeletesPctAllowed;
    }

    /**
     * Sets the allowed number of segments per tier.  Smaller
     * values mean more merging but fewer segments.
     *
     * <p>Default is 10.0.</p>
     */
    public TieredMergePolicy setSegmentsPerTier(double v) {
        if (v < 2.0) {
            throw new IllegalArgumentException("segmentsPerTier must be >= 2.0 (got " + v + ")");
        }
        segsPerTier = v;
        return this;
    }

    /**
     * Returns the current segmentsPerTier setting.
     *
     * @see #setSegmentsPerTier
     */
    public double getSegmentsPerTier() {
        return segsPerTier;
    }

    /**
     * 描述某个段的大小  以及总计有多少doc 有多少doc被标记为要删除
     */
    private static class SegmentSizeAndDocs {
        private final SegmentCommitInfo segInfo;
        /**
         * sizeInBytes + delCount = totalSize
         * 预估处理后的大小是多少
         */
        private final long sizeInBytes;
        /**
         * 总计删除了多少doc
         */
        private final int delCount;
        private final int maxDoc;
        private final String name;

        SegmentSizeAndDocs(SegmentCommitInfo info, final long sizeInBytes, final int segDelCount) throws IOException {
            segInfo = info;
            this.name = info.info.name;
            this.sizeInBytes = sizeInBytes;
            this.delCount = segDelCount;
            this.maxDoc = info.info.maxDoc();
        }
    }

    /**
     * Holds score and explanation for a single candidate
     * merge.
     */
    // 该对象可以为segment 进行打分
    protected static abstract class MergeScore {
        /**
         * Sole constructor. (For invocation by subclass
         * constructors, typically implicit.)
         */
        protected MergeScore() {
        }

        /**
         * Returns the score for this merge candidate; lower
         * scores are better.
         */
        abstract double getScore();

        /**
         * Human readable explanation of how the merge got this
         * score.
         */
        abstract String getExplanation();
    }


    // The size can change concurrently while we are running here, because deletes
    // are now applied concurrently, and this can piss off TimSort!  So we
    // call size() once per segment and sort by that:
    // 按照段内的数据大小 倒序排序
    private List<SegmentSizeAndDocs> getSortedBySegmentSize(final SegmentInfos infos, final MergeContext mergeContext) throws IOException {
        List<SegmentSizeAndDocs> sortedBySize = new ArrayList<>();

        for (SegmentCommitInfo info : infos) {
            // size() 预估处理后的大小是多少 = (原大小 * delCount/count)
            // mergeContext.numDeletesToMerge  =  delCount
            sortedBySize.add(new SegmentSizeAndDocs(info, size(info, mergeContext), mergeContext.numDeletesToMerge(info)));
        }

        // 按照处理后的大小倒序排序
        sortedBySize.sort((o1, o2) -> {
            // Sort by largest size:
            int cmp = Long.compare(o2.sizeInBytes, o1.sizeInBytes);
            if (cmp == 0) {
                cmp = o1.name.compareTo(o2.name);
            }
            return cmp;

        });

        return sortedBySize;
    }

    /**
     * 查找本次需要merge的 段 并转换成 OneMerge对象
     *
     * @param mergeTrigger the event that triggered the merge    触发本次merge的原因
     * @param infos                                              本次候选的所有segment
     * @param mergeContext the IndexWriter to find the merges on
     * @return
     * @throws IOException
     */
    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {

        // 从 writer上获取 此时已经在merge中的segment  这些对象不会被作为候选者
        final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
        // Compute total index bytes & print details about the index
        // 所有 SegmentSizeAndDocs 的大小累加
        long totIndexBytes = 0;

        // 该字段记录 所有段中最小的大小
        long minSegmentBytes = Long.MAX_VALUE;
        // 记录doc总数
        int totalDelDocs = 0;
        // 这个记录的是一个全局范围的doc数量
        int totalMaxDoc = 0;
        // 记录所有正在merging的段大小总和
        long mergingBytes = 0;

        // 将段按照大小倒序排序 （除开被删除的部分）
        List<SegmentSizeAndDocs> sortedInfos = getSortedBySegmentSize(infos, mergeContext);
        Iterator<SegmentSizeAndDocs> iter = sortedInfos.iterator();
        while (iter.hasNext()) {
            SegmentSizeAndDocs segSizeDocs = iter.next();
            final long segBytes = segSizeDocs.sizeInBytes;
            if (verbose(mergeContext)) {
                String extra = merging.contains(segSizeDocs.segInfo) ? " [merging]" : "";
                if (segBytes >= maxMergedSegmentBytes) {
                    extra += " [skip: too large]";
                } else if (segBytes < floorSegmentBytes) {
                    extra += " [floored]";
                }
                message("  seg=" + segString(mergeContext, Collections.singleton(segSizeDocs.segInfo)) + " size=" + String.format(Locale.ROOT, "%.3f", segBytes / 1024 / 1024.) + " MB" + extra, mergeContext);
            }

            // 当前segment 已经在merge中了
            if (merging.contains(segSizeDocs.segInfo)) {
                mergingBytes += segSizeDocs.sizeInBytes;
                // 移除掉 已经在merging的段
                iter.remove();
                // if this segment is merging, then its deletes are being reclaimed already.
                // only count live docs in the total max doc

                // 如果该segment 已经在被merge了 就只要将扣除 delCount后的数量加到全局doc数量上
                totalMaxDoc += segSizeDocs.maxDoc - segSizeDocs.delCount;
            } else {

                // 非merge的情况就是累加所有的doc
                totalDelDocs += segSizeDocs.delCount;
                totalMaxDoc += segSizeDocs.maxDoc;
            }

            // 找到这些segment中最小的 segBytes
            minSegmentBytes = Math.min(segBytes, minSegmentBytes);
            totIndexBytes += segBytes;
        }
        assert totalMaxDoc >= 0;
        assert totalDelDocs >= 0;

        // 本次所有删除的doc数量 占本次所有doc数量的比率  如果该比率已经超过了  deletesPctAllowed 就可以认为平均每个段的删除率都比较高 都有merge的价值
        final double totalDelPct = 100 * (double) totalDelDocs / totalMaxDoc;
        // 预估会删除的数量
        int allowedDelCount = (int) (deletesPctAllowed * totalMaxDoc / 100);

        // If we have too-large segments, grace them out of the maximum segment count
        // If we're above certain thresholds of deleted docs, we can merge very large segments.
        // 记录本次segInfos 中出现了多少 特别大的对象
        int tooBigCount = 0;

        iter = sortedInfos.iterator();

        // remove large segments from consideration under two conditions.
        // 1> Overall percent deleted docs relatively small and this segment is larger than 50% maxSegSize
        // 2>i overall percent deleted docs large and this segment is large and has few deleted docs

        while (iter.hasNext()) {
            SegmentSizeAndDocs segSizeDocs = iter.next();
            // 计算每个段自己 的删除率
            double segDelPct = 100 * (double) segSizeDocs.delCount / (double) segSizeDocs.maxDoc;
            // 如果该段的大小 超过了单个segment限制值的一半 就被认为是一个大对象   那么对它的合并必然会很耗时
            // 同时如果平均每个段的删除率没有超过要求值的情况  它自身的删除率也没有达到要求之  就认为它的合并效率很低  那么这种段是不会被考虑在merge范围内的
            if (segSizeDocs.sizeInBytes > maxMergedSegmentBytes / 2 && (totalDelPct <= deletesPctAllowed || segDelPct <= deletesPctAllowed)) {
                iter.remove();
                tooBigCount++; // Just for reporting purposes.
                // 随着segment被移除 此时total的值也减小了
                totIndexBytes -= segSizeDocs.sizeInBytes;
                // 减少预估会删除的数量
                allowedDelCount -= segSizeDocs.delCount;
            }
        }
        allowedDelCount = Math.max(0, allowedDelCount);

        // 计算单次合并包含多少个段
        final int mergeFactor = (int) Math.min(maxMergeAtOnce, segsPerTier);
        // Compute max allowed segments in the index

        // 本次segment的大小要求是 超过最小限制值
        long levelSize = Math.max(minSegmentBytes, floorSegmentBytes);
        // 除开大对象后 剩余的segment 占用的总大小  (该预估值 已经去除掉del的部分)
        long bytesLeft = totIndexBytes;
        double allowedSegCount = 0;
        while (true) {
            // 预估有多少个 segment
            final double segCountLevel = bytesLeft / (double) levelSize;
            // 此时数量以及不足一个 tier了 所以不需要进一步划分了   或者此时用于分层的size已经过大 就不需要处理了
            if (segCountLevel < segsPerTier || levelSize == maxMergedSegmentBytes) {
                allowedSegCount += Math.ceil(segCountLevel);
                break;
            }
            // 进入到这里代表 segCountLevel >= 10
            allowedSegCount += segsPerTier;
            // 这里裁剪掉10个大小
            bytesLeft -= segsPerTier * levelSize;
            // 每次分母都会变成上一次的倍数
            levelSize = Math.min(maxMergedSegmentBytes, levelSize * mergeFactor);
        }
        // allowedSegCount may occasionally be less than segsPerTier
        // if segment sizes are below the floor size
        // 实际上会merge多少个segment
        allowedSegCount = Math.max(allowedSegCount, segsPerTier);

        if (verbose(mergeContext) && tooBigCount > 0) {
            message("  allowedSegmentCount=" + allowedSegCount + " vs count=" + infos.size() +
                    " (eligible count=" + sortedInfos.size() + ") tooBigCount= " + tooBigCount, mergeContext);
        }
        // 这里才执行真正的 查找工作
        return doFindMerges(sortedInfos, maxMergedSegmentBytes, mergeFactor, (int) allowedSegCount, allowedDelCount, MERGE_TYPE.NATURAL,
                mergeContext, mergingBytes >= maxMergedSegmentBytes);
    }

    /**
     * 执行真正的查找工作
     *
     * @param sortedEligibleInfos   候选对象  已经剔除了 merging 中的segment   注意这里已经完成排序了
     * @param maxMergedSegmentBytes 单次merge的segment  bytes总大小
     * @param mergeFactor           单次merge多少个段
     * @param allowedSegCount       实际会merge多少块
     * @param allowedDelCount       这是允许删除的大小
     * @param mergeType             默认情况使用 自然方式进行merge    在强制删除模式 和强制合并模式下 传入参数不同
     * @param mergeContext          就是 indexWriter
     * @param maxMergeIsRunning     是否正在执行一些大数据块的merge (也就是正在merge的数据量要超过 最大merge限制)
     * @return
     * @throws IOException
     */
    private MergeSpecification doFindMerges(List<SegmentSizeAndDocs> sortedEligibleInfos,
                                            final long maxMergedSegmentBytes,
                                            final int mergeFactor, final int allowedSegCount,
                                            final int allowedDelCount, final MERGE_TYPE mergeType,
                                            MergeContext mergeContext,
                                            boolean maxMergeIsRunning) throws IOException {

        List<SegmentSizeAndDocs> sortedEligible = new ArrayList<>(sortedEligibleInfos);

        Map<SegmentCommitInfo, SegmentSizeAndDocs> segInfosSizes = new HashMap<>();
        for (SegmentSizeAndDocs segSizeDocs : sortedEligible) {
            segInfosSizes.put(segSizeDocs.segInfo, segSizeDocs);
        }

        int originalSortedSize = sortedEligible.size();
        // ignore
        if (verbose(mergeContext)) {
            message("findMerges: " + originalSortedSize + " segments", mergeContext);
        }
        // 已经不存在候选的 segment 了 直接返回
        if (originalSortedSize == 0) {
            return null;
        }

        // 这里存储已经分配好的段对象
        final Set<SegmentCommitInfo> toBeMerged = new HashSet<>();

        MergeSpecification spec = null;

        // Cycle to possibly select more than one merge:
        // The trigger point for total deleted documents in the index leads to a bunch of large segment
        // merges at the same time. So only put one large merge in the list of merges per cycle. We'll pick up another
        // merge next time around.
        // 代表多个结果中 只要有一次出现了 tooLarge
        boolean haveOneLargeMerge = false;

        // 这层循环是生成多个 onMerge
        while (true) {

            // Gather eligible segments for merging, ie segments
            // not already being merged and not already picked (by
            // prior iteration of this loop) for merging:

            // Remove ineligible segments. These are either already being merged or already picked by prior iterations
            Iterator<SegmentSizeAndDocs> iter = sortedEligible.iterator();
            while (iter.hasNext()) {
                // 将上一轮已经挑选出来的 segment 剔除
                SegmentSizeAndDocs segSizeDocs = iter.next();
                if (toBeMerged.contains(segSizeDocs.segInfo)) {
                    iter.remove();
                }
            }

            if (verbose(mergeContext)) {
                message("  allowedSegmentCount=" + allowedSegCount + " vs count=" + originalSortedSize + " (eligible count=" + sortedEligible.size() + ")", mergeContext);
            }

            // 代表已经处理完所有 segment了
            if (sortedEligible.size() == 0) {
                return spec;
            }

            // 计算这些segment 总的 delDoc数量
            final int remainingDelCount = sortedEligible.stream().mapToInt(c -> c.delCount).sum();
            // 看来自然删除的模式 会尽可能将多个segment 进行合并 这里剩余的段已经不多了 所以不急着一次性处理完
            // 而在 forceXXX 中 会尽可能将所有segment合并
            if (mergeType == MERGE_TYPE.NATURAL &&
                    // 当segment 列表中 段对象剔除的差不多了  并且允许删除的数量也没有超过阈值 允许直接返回
                    // 这个del就是用来判断本次merge是否有意义 比如segment内部有大量需要删除的数据 那么这一步就有必要
                    sortedEligible.size() <= allowedSegCount &&
                    remainingDelCount <= allowedDelCount) {
                return spec;
            }

            // OK we are over budget -- find best merge!
            // 代表本次在考虑范围内的段数量超过了允许的总数量
            // 这里尝试选取最优解
            MergeScore bestScore = null;
            List<SegmentCommitInfo> best = null;
            boolean bestTooLarge = false;
            long bestMergeBytes = 0;

            // 这里是根据当前候选的segment 选取bestOneMerge
            // 每次起点会+1
            for (int startIdx = 0; startIdx < sortedEligible.size(); startIdx++) {

                long totAfterMergeBytes = 0;

                final List<SegmentCommitInfo> candidate = new ArrayList<>();

                // 代表本轮挑选merge的段时  发生了超过限制的情况
                boolean hitTooLarge = false;

                // 记录内循环此时已经累加了多少待merge的值
                long bytesThisMerge = 0;

                // 内循环从外层的起点开始 直到读取到本次merge指定的段数量前  以及当merge的量已经超过本次最大bytes时 结束循环
                for (int idx = startIdx; idx < sortedEligible.size() && candidate.size() < mergeFactor && bytesThisMerge < maxMergedSegmentBytes; idx++) {
                    // 定位到当前 segment
                    final SegmentSizeAndDocs segSizeDocs = sortedEligible.get(idx);
                    final long segBytes = segSizeDocs.sizeInBytes;

                    // 代表本次会超过限制 这时还会往后遍历希望插入一个更小的值 尽可能的接近maxMergedSegmentBytes
                    if (totAfterMergeBytes + segBytes > maxMergedSegmentBytes) {

                        hitTooLarge = true;
                        // 只有当 候选对象还没有设置时 才会添加   也就是 如果第一个值特别大是可能会添加进去的
                        // 同时这个大小是不算入totAfterMergeBytes的
                        if (candidate.size() == 0) {
                            // We should never have something coming in that _cannot_ be merged, so handle singleton merges
                            candidate.add(segSizeDocs.segInfo);
                            bytesThisMerge += segBytes;
                        }
                        // NOTE: we continue, so that we can try
                        // "packing" smaller segments into this merge
                        // to see if we can get closer to the max
                        // size; this in general is not perfect since
                        // this is really "bin packing" and we'd have
                        // to try different permutations.
                        // 进入下次循环 等待一个合适大小的segment
                        continue;
                    }
                    // 正常情况就是从大到小 开始添加
                    candidate.add(segSizeDocs.segInfo);
                    bytesThisMerge += segBytes;
                    totAfterMergeBytes += segBytes;
                }

                // 上面这个循环的任务就是选取本批预备merge的所有段 下面会为它打分

                // We should never see an empty candidate: we iterated over maxMergeAtOnce
                // segments, and already pre-excluded the too-large segments:
                assert candidate.size() > 0;

                // A singleton merge with no deletes makes no sense. We can get here when forceMerge is looping around...
                // 2种情况 一种是加入了一个超规格的 segment  还有一种就是 一个略小于上限的segment  并且与剩下任何一个segment 都会超标
                // 如果这个段不支持瘦身的话  重选candidate 并且从下一个略小的segment开始

                // 单个segment 且没有delCount的话 本身是没有merge的必要的 所以直接开始下一批数据的选取
                if (candidate.size() == 1) {
                    SegmentSizeAndDocs segSizeDocs = segInfosSizes.get(candidate.get(0));
                    if (segSizeDocs.delCount == 0) {
                        continue;
                    }
                }

                // If we didn't find a too-large merge and have a list of candidates
                // whose length is less than the merge factor, it means we are reaching
                // the tail of the list of segments and will only find smaller merges.
                // Stop here.
                // hitTooLarge == false 代表每次都是顺利的插入segment 都没有因为超过限制而跳过某个segment
                // 那么这时 candidate.size() < mergeFactor 只能理解为当前已经没有足够的segment去merge了
                // 继续往下遍历只会得到更小的merge块 所以退出循环
                if (bestScore != null &&
                        hitTooLarge == false &&
                        candidate.size() < mergeFactor) {
                    break;
                }

                // 每当生成一个组合时 就要进行一次打分
                final MergeScore score = score(candidate, hitTooLarge, segInfosSizes);
                if (verbose(mergeContext)) {
                    message("  maybe=" + segString(mergeContext, candidate) + " score=" + score.getScore() + " " + score.getExplanation() + " tooLarge=" + hitTooLarge + " size=" + String.format(Locale.ROOT, "%.3f MB", totAfterMergeBytes / 1024. / 1024.), mergeContext);
                }

                // 得分越低越好
                if ((bestScore == null || score.getScore() < bestScore.getScore()) && (!hitTooLarge || !maxMergeIsRunning)) {
                    best = candidate;
                    bestScore = score;
                    bestTooLarge = hitTooLarge;
                    bestMergeBytes = totAfterMergeBytes;
                }
            }

            // 代表没有一次segment的数量是满足条件的
            if (best == null) {
                return spec;
            }
            // The mergeType == FORCE_MERGE_DELETES behaves as the code does currently and can create a large number of
            // concurrent big merges. If we make findForcedDeletesMerges behave as findForcedMerges and cycle through
            // we should remove this.
            // 这里是已经选出最优解的情况
            if (haveOneLargeMerge == false || bestTooLarge == false || mergeType == MERGE_TYPE.FORCE_MERGE_DELETES) {

                haveOneLargeMerge |= bestTooLarge;

                if (spec == null) {
                    spec = new MergeSpecification();
                }
                final OneMerge merge = new OneMerge(best);
                spec.add(merge);

                if (verbose(mergeContext)) {
                    message("  add merge=" + segString(mergeContext, merge.segments) + " size=" + String.format(Locale.ROOT, "%.3f MB", bestMergeBytes / 1024. / 1024.) + " score=" + String.format(Locale.ROOT, "%.3f", bestScore.getScore()) + " " + bestScore.getExplanation() + (bestTooLarge ? " [max merge]" : ""), mergeContext);
                }
            }
            // whether we're going to return this list in the spec of not, we need to remove it from
            // consideration on the next loop.
            // 将这些segment排除后 继续合并 生成 oneMerge 对象
            toBeMerged.addAll(best);
            // 这里才是 while 退出循环
        }
    }

    /**
     * TODO 打分逻辑就不看了
     * Expert: scores one merge; subclasses can override.
     *
     * @param hitTooLarge 只要在未满足 最大数量时   有一次 大小先超过了限制值 就会设置为true
     */
    protected MergeScore score(List<SegmentCommitInfo> candidate, boolean hitTooLarge, Map<SegmentCommitInfo, SegmentSizeAndDocs> segmentsSizes) throws IOException {

        long totBeforeMergeBytes = 0;
        long totAfterMergeBytes = 0;
        long totAfterMergeBytesFloored = 0;

        for (SegmentCommitInfo info : candidate) {
            // 找到该段的大小
            final long segBytes = segmentsSizes.get(info).sizeInBytes;
            totAfterMergeBytes += segBytes;
            // 这里额外计算一个大小  也就是当segment小于 floor时 用floor计算
            totAfterMergeBytesFloored += floorSize(segBytes);
            // 这是文件大小
            totBeforeMergeBytes += info.sizeInBytes();
        }

        // Roughly measure "skew" of the merge, i.e. how
        // "balanced" the merge is (whether the segments are
        // about the same size), which can range from
        // 1.0/numSegsBeingMerged (good) to 1.0 (poor). Heavily
        // lopsided merges (skew near 1.0) is no good; it means
        // O(N^2) merge cost over time:
        // 这是斜率吗
        final double skew;
        // 代表不是最优情况
        if (hitTooLarge) {
            // Pretend the merge has perfect skew; skew doesn't
            // matter in this case because this merge will not
            // "cascade" and so it cannot lead to N^2 merge cost
            // over time:
            final int mergeFactor = (int) Math.min(maxMergeAtOnce, segsPerTier);
            skew = 1.0 / mergeFactor;
        } else {
            // 取最大的段 / merge后的大小
            skew = ((double) floorSize(segmentsSizes.get(candidate.get(0)).sizeInBytes)) / totAfterMergeBytesFloored;
        }

        // Strongly favor merges with less skew (smaller
        // mergeScore is better):
        double mergeScore = skew;

        // 为什么这样算没看懂
        // 简单分析一下  假设 hitTooLarge 为false  那么最大的段所占总比重与score 成正比 合并后的大小在一定范围内 越大越好   同时删除的工作越少(未删除的比重高) 得分越高
        // 而在上面会发现实际上得分越低越好
        // Gently favor smaller merges over bigger ones.  We
        // don't want to make this exponent too large else we
        // can end up doing poor merges of small segments in
        // order to avoid the large merges:
        // 0.05次方  那么就是略大于1  底数越大 结果越大
        mergeScore *= Math.pow(totAfterMergeBytes, 0.05);

        // Strongly favor merges that reclaim deletes:
        // 计算未删除的比重
        final double nonDelRatio = ((double) totAfterMergeBytes) / totBeforeMergeBytes;
        mergeScore *= Math.pow(nonDelRatio, 2);

        final double finalMergeScore = mergeScore;

        return new MergeScore() {

            @Override
            public double getScore() {
                return finalMergeScore;
            }

            @Override
            public String getExplanation() {
                return "skew=" + String.format(Locale.ROOT, "%.3f", skew) + " nonDelRatio=" + String.format(Locale.ROOT, "%.3f", nonDelRatio);
            }
        };
    }

    /**
     * 寻找需要被merger的 对象 并填充到 MergeSpecification
     *
     * @param infos           此时可以选择的所有merge
     * @param maxSegmentCount requested maximum number of segments in the index (currently this
     *                        is always 1)                               本次最多允许merge多少个segment
     * @param segmentsToMerge contains the specific SegmentInfo instances that must be merged
     *                        away. This may be a subset of all
     *                        SegmentInfos.  If the value is True for a
     *                        given SegmentInfo, that means this segment was
     *                        an original segment present in the
     *                        to-be-merged index; else, it was a segment
     *                        produced by a cascaded merge.              被挑选出来的 segment 必须存在于该容器内
     * @param mergeContext    the IndexWriter to find the merges on     可以简单认为就是 IndexWriter
     * @return
     * @throws IOException
     */
    @Override
    public MergeSpecification findForcedMerges(SegmentInfos infos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) throws IOException {

        // 打印日志相关的先忽略
        if (verbose(mergeContext)) {
            message("findForcedMerges maxSegmentCount=" + maxSegmentCount + " infos=" + segString(mergeContext, infos) +
                    " segmentsToMerge=" + segmentsToMerge, mergeContext);
        }

        // 按照预估处理后的大小 倒序排序 (也就是按照去除了所有待删除doc后的大小排序)
        List<SegmentSizeAndDocs> sortedSizeAndDocs = getSortedBySegmentSize(infos, mergeContext);

        // 经过第一轮过滤后 剩余的 SegmentSizeAndDocs 总大小
        long totalMergeBytes = 0;
        // 获取此时已经在merge中的 段对象
        final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();


        // Trim the list down, remove if we're respecting max segment size and it's not original. Presumably it's been merged before and
        //   is close enough to the max segment size we shouldn't add it in again.
        Iterator<SegmentSizeAndDocs> iter = sortedSizeAndDocs.iterator();
        boolean forceMergeRunning = false;
        while (iter.hasNext()) {
            SegmentSizeAndDocs segSizeDocs = iter.next();
            // 被选择的segment 不在容器内
            final Boolean isOriginal = segmentsToMerge.get(segSizeDocs.segInfo);
            if (isOriginal == null) {
                iter.remove();
            } else {
                // 代表这个段虽然在 map中  但是已经处于merging了  也忽略
                if (merging.contains(segSizeDocs.segInfo)) {
                    forceMergeRunning = true;
                    iter.remove();
                } else {
                    // 其余的才在考虑范围内
                    totalMergeBytes += segSizeDocs.sizeInBytes;
                }
            }
        }

        // 获取参与merge 的每个segment 的最大大小
        long maxMergeBytes = maxMergedSegmentBytes;

        // Set the maximum segment size based on how many segments have been specified.
        // 如果本次仅允许merge 一个segment  那么取消对segment大小的限制
        if (maxSegmentCount == 1) {
            maxMergeBytes = Long.MAX_VALUE;

            // 如果本次merge的数量不止一个
        } else if (maxSegmentCount != Integer.MAX_VALUE) {
            // Fudge this up a bit so we have a better chance of not having to rewrite segments. If we use the exact size,
            // it's almost guaranteed that the segments won't fit perfectly and we'll be left with more segments than
            // we want and have to re-merge in the code at the bottom of this method.

            // 这里略微放大了 对merge大小的限制
            maxMergeBytes = Math.max((long) (((double) totalMergeBytes / (double) maxSegmentCount)), maxMergedSegmentBytes);
            maxMergeBytes = (long) ((double) maxMergeBytes * 1.25);
        }

        // 遍历此时在选择范围内的 所有对象  注意他们是按照大小倒序排列的
        iter = sortedSizeAndDocs.iterator();

        // 寻找这些segment中是否有被删除doc的
        boolean foundDeletes = false;
        while (iter.hasNext()) {
            SegmentSizeAndDocs segSizeDocs = iter.next();
            Boolean isOriginal = segmentsToMerge.get(segSizeDocs.segInfo);
            // 如果该segment 有部分doc被删除了 并且是一个原始段   就参与merge
            if (segSizeDocs.delCount != 0) { // This is forceMerge, all segments with deleted docs should be merged.
                if (isOriginal != null && isOriginal) {
                    foundDeletes = true;
                }
                continue;
            }
            // Let the scoring handle whether to merge large segments.
            // 如果本次参与merge的数量很多 只要不是原始段 就不参与合并
            if (maxSegmentCount == Integer.MAX_VALUE && isOriginal != null && isOriginal == false) {
                iter.remove();
            }
            // Don't try to merge a segment with no deleted docs that's over the max size.
            // 超过merge大小限制 且没有doc被删除的 就不参与merge
            if (maxSegmentCount != Integer.MAX_VALUE && segSizeDocs.sizeInBytes >= maxMergeBytes) {
                iter.remove();
            }
        }

        // Nothing to merge this round.
        // 此时没有符合条件的段了
        if (sortedSizeAndDocs.size() == 0) {
            return null;
        }

        // We should never bail if there are segments that have deleted documents, all deleted docs should be purged.
        if (foundDeletes == false) {
            SegmentCommitInfo infoZero = sortedSizeAndDocs.get(0).segInfo;
            if ((maxSegmentCount != Integer.MAX_VALUE && maxSegmentCount > 1 && sortedSizeAndDocs.size() <= maxSegmentCount) ||
                    (maxSegmentCount == 1 && sortedSizeAndDocs.size() == 1 && (segmentsToMerge.get(infoZero) != null || isMerged(infos, infoZero, mergeContext)))) {
                if (verbose(mergeContext)) {
                    message("already merged", mergeContext);
                }
                return null;
            }
        }

        if (verbose(mergeContext)) {
            message("eligible=" + sortedSizeAndDocs, mergeContext);
        }

        final int startingSegmentCount = sortedSizeAndDocs.size();
        final boolean finalMerge = startingSegmentCount < maxSegmentCount + maxMergeAtOnceExplicit - 1;
        if (finalMerge && forceMergeRunning) {
            return null;
        }

        // This is the special case of merging down to one segment
        if (sortedSizeAndDocs.size() < maxMergeAtOnceExplicit && maxSegmentCount == 1 && totalMergeBytes < maxMergeBytes) {
            MergeSpecification spec = new MergeSpecification();
            List<SegmentCommitInfo> allOfThem = new ArrayList<>();
            for (SegmentSizeAndDocs segSizeDocs : sortedSizeAndDocs) {
                allOfThem.add(segSizeDocs.segInfo);
            }
            spec.add(new OneMerge(allOfThem));
            return spec;
        }

        MergeSpecification spec = null;

        int index = startingSegmentCount - 1;
        int resultingSegments = startingSegmentCount;
        while (true) {
            List<SegmentCommitInfo> candidate = new ArrayList<>();
            long currentCandidateBytes = 0L;
            int mergesAllowed = maxMergeAtOnceExplicit;
            while (index >= 0 && resultingSegments > maxSegmentCount && mergesAllowed > 0) {
                final SegmentCommitInfo current = sortedSizeAndDocs.get(index).segInfo;
                final int initialCandidateSize = candidate.size();
                final long currentSegmentSize = current.sizeInBytes();
                // We either add to the bin because there's space or because the it is the smallest possible bin since
                // decrementing the index will move us to even larger segments.
                if (currentCandidateBytes + currentSegmentSize <= maxMergeBytes || initialCandidateSize < 2) {
                    candidate.add(current);
                    --index;
                    currentCandidateBytes += currentSegmentSize;
                    --mergesAllowed;
                    if (initialCandidateSize > 0) {
                        // Any merge that handles two or more segments reduces the resulting number of segments
                        // by the number of segments handled - 1
                        --resultingSegments;
                    }
                } else {
                    break;
                }
            }
            final int candidateSize = candidate.size();
            // While a force merge is running, only merges that cover the maximum allowed number of segments or that create a segment close to the
            // maximum allowed segment sized are permitted
            if (candidateSize > 1 && (forceMergeRunning == false || candidateSize == maxMergeAtOnceExplicit || candidateSize > 0.7 * maxMergeBytes)) {
                final OneMerge merge = new OneMerge(candidate);
                if (verbose(mergeContext)) {
                    message("add merge=" + segString(mergeContext, merge.segments), mergeContext);
                }
                if (spec == null) {
                    spec = new MergeSpecification();
                }
                spec.add(merge);
            } else {
                return spec;
            }
        }
    }

    /**
     * 找到为了瘦身的 segment
     *
     * @param infos
     * @param mergeContext the IndexWriter to find the merges on
     * @return
     * @throws IOException
     */
    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos infos, MergeContext mergeContext) throws IOException {
        if (verbose(mergeContext)) {
            message("findForcedDeletesMerges infos=" + segString(mergeContext, infos) + " forceMergeDeletesPctAllowed=" + forceMergeDeletesPctAllowed, mergeContext);
        }

        // First do a quick check that there's any work to do.
        // NOTE: this makes BaseMergePOlicyTestCase.testFindForcedDeletesMerges work
        final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();

        boolean haveWork = false;
        for (SegmentCommitInfo info : infos) {
            int delCount = mergeContext.numDeletesToMerge(info);
            assert assertDelCount(delCount, info);
            double pctDeletes = 100. * ((double) delCount) / info.info.maxDoc();
            // 当找到超过 预定值时 被选中 并会在之后参与到 del中
            if (pctDeletes > forceMergeDeletesPctAllowed && !merging.contains(info)) {
                haveWork = true;
                break;
            }
        }

        // 都没有满足最小删除量 那么就不需要合并了  单个段的合并 实际上就是瘦身
        if (haveWork == false) {
            return null;
        }

        // 排序并包装
        List<SegmentSizeAndDocs> sortedInfos = getSortedBySegmentSize(infos, mergeContext);

        Iterator<SegmentSizeAndDocs> iter = sortedInfos.iterator();
        while (iter.hasNext()) {
            SegmentSizeAndDocs segSizeDocs = iter.next();
            double pctDeletes = 100. * ((double) segSizeDocs.delCount / (double) segSizeDocs.maxDoc);
            // 已经在merge中了  就移除掉 或者不满足条件的也移除  md 为什么不在上面移除???
            if (merging.contains(segSizeDocs.segInfo) || pctDeletes <= forceMergeDeletesPctAllowed) {
                iter.remove();
            }
        }

        if (verbose(mergeContext)) {
            message("eligible=" + sortedInfos, mergeContext);
        }
        return doFindMerges(sortedInfos, maxMergedSegmentBytes,
                maxMergeAtOnceExplicit, Integer.MAX_VALUE, 0, MERGE_TYPE.FORCE_MERGE_DELETES, mergeContext, false);

    }

    private long floorSize(long bytes) {
        return Math.max(floorSegmentBytes, bytes);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[" + getClass().getSimpleName() + ": ");
        sb.append("maxMergeAtOnce=").append(maxMergeAtOnce).append(", ");
        sb.append("maxMergeAtOnceExplicit=").append(maxMergeAtOnceExplicit).append(", ");
        sb.append("maxMergedSegmentMB=").append(maxMergedSegmentBytes / 1024 / 1024.).append(", ");
        sb.append("floorSegmentMB=").append(floorSegmentBytes / 1024 / 1024.).append(", ");
        sb.append("forceMergeDeletesPctAllowed=").append(forceMergeDeletesPctAllowed).append(", ");
        sb.append("segmentsPerTier=").append(segsPerTier).append(", ");
        sb.append("maxCFSSegmentSizeMB=").append(getMaxCFSSegmentSizeMB()).append(", ");
        sb.append("noCFSRatio=").append(noCFSRatio).append(", ");
        sb.append("deletesPctAllowed=").append(deletesPctAllowed);
        return sb.toString();
    }
}
