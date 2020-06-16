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
package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * FirstPassGroupingCollector is the first of two passes necessary
 * to collect grouped hits.  This pass gathers the top N sorted
 * groups. Groups are defined by a {@link GroupSelector}
 *
 * <p>See {@link org.apache.lucene.search.grouping} for more
 * details including a full code example.</p>
 *
 * @lucene.experimental 在组间排序的对象
 */
public class FirstPassGroupingCollector<T> extends SimpleCollector {

    /**
     * 该对象负责从 将文档按组划分
     */
    private final GroupSelector<T> groupSelector;

    /**
     * 排序对象  当文档按组划分好时 会根据该对象进行排序   这里是一个数组对象 代表允许基于多个comparator进行排序
     */
    private final FieldComparator<?>[] comparators;
    private final LeafFieldComparator[] leafComparators;
    /**
     * 每个排序规则是否按倒序排列
     */
    private final int[] reversed;
    /**
     * 维护多少个组
     */
    private final int topNGroups;
    private final boolean needsScores;
    /**
     * CollectedSearchGroup 存放已经采集好的组信息  只保留 topNGroups的数量
     */
    private final HashMap<T, CollectedSearchGroup<T>> groupMap;
    /**
     * 用于控制 排序规则的数量上限
     */
    private final int compIDXEnd;

    // Set once we reach topNGroups unique groups:
    /** @lucene.internal */
    /**
     * 基于二叉树实现排序功能
     */
    protected TreeSet<CollectedSearchGroup<T>> orderedGroups;
    private int docBase;
    /**
     * 大小等同于 topNGroups;
     */
    private int spareSlot;

    /**
     * Create the first pass collector.
     *
     * @param groupSelector a GroupSelector used to defined groups
     * @param groupSort     The {@link Sort} used to sort the
     *                      groups.  The top sorted document within each group
     *                      according to groupSort, determines how that group
     *                      sorts against other groups.  This must be non-null,
     *                      ie, if you want to groupSort by relevance use
     *                      Sort.RELEVANCE.
     * @param topNGroups    How many top groups to keep.   最多维护多少个组
     *                      传入一个从文档中筛选组的 GroupSelector 以及一个 排序对象 Sort
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public FirstPassGroupingCollector(GroupSelector<T> groupSelector, Sort groupSort, int topNGroups) {
        this.groupSelector = groupSelector;
        if (topNGroups < 1) {
            throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
        }

        // TODO: allow null groupSort to mean "by relevance",
        // and specialize it?

        this.topNGroups = topNGroups;
        // 判断是否要打分
        this.needsScores = groupSort.needsScores();
        // 返回一组排序得分的对象
        final SortField[] sortFields = groupSort.getSort();
        comparators = new FieldComparator[sortFields.length];
        leafComparators = new LeafFieldComparator[sortFields.length];
        compIDXEnd = comparators.length - 1;
        reversed = new int[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
            // 获得每个影响排序的对象
            final SortField sortField = sortFields[i];

            // use topNGroups + 1 so we have a spare slot to use for comparing (tracked by this.spareSlot):
            comparators[i] = sortField.getComparator(topNGroups + 1, i);
            reversed[i] = sortField.getReverse() ? -1 : 1;
        }

        spareSlot = topNGroups;
        groupMap = new HashMap<>(topNGroups);
    }

    @Override
    public ScoreMode scoreMode() {
        return needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    /**
     * Returns top groups, starting from offset.  This may
     * return null, if no groups were collected, or if the
     * number of unique groups collected is &lt;= offset.
     *
     * @param groupOffset The offset in the collected groups   代表打算采集多少个group的数据并返回
     * @return top groups, starting from offset
     */
    public Collection<SearchGroup<T>> getTopGroups(int groupOffset) throws IOException {

        //System.out.println("FP.getTopGroups groupOffset=" + groupOffset + " fillFields=" + fillFields + " groupMap.size()=" + groupMap.size());

        if (groupOffset < 0) {
            throw new IllegalArgumentException("groupOffset must be >= 0 (got " + groupOffset + ")");
        }

        // 当前数量不满足 groupOffset要求 无法排序 返回null
        if (groupMap.size() <= groupOffset) {
            return null;
        }

        // 进行排序
        if (orderedGroups == null) {
            buildSortedSet();
        }

        final Collection<SearchGroup<T>> result = new ArrayList<>();
        int upto = 0;
        final int sortFieldCount = comparators.length;
        // 这时排序已经完成了
        for (CollectedSearchGroup<T> group : orderedGroups) {
            if (upto++ < groupOffset) {
                continue;
            }

            // 当 upto == groupOffset 时才会进入到这里
            // System.out.println("  group=" + (group.groupValue == null ? "null" : group.groupValue.toString()));
            SearchGroup<T> searchGroup = new SearchGroup<>();
            searchGroup.groupValue = group.groupValue;
            searchGroup.sortValues = new Object[sortFieldCount];
            for (int sortFieldIDX = 0; sortFieldIDX < sortFieldCount; sortFieldIDX++) {
                searchGroup.sortValues[sortFieldIDX] = comparators[sortFieldIDX].value(group.comparatorSlot);
            }
            result.add(searchGroup);
        }
        //System.out.println("  return " + result.size() + " groups");
        return result;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        groupSelector.setScorer(scorer);
        for (LeafFieldComparator comparator : leafComparators) {
            comparator.setScorer(scorer);
        }
    }

    private boolean isCompetitive(int doc) throws IOException {
        // If orderedGroups != null we already have collected N groups and
        // can short circuit by comparing this document to the bottom group,
        // without having to find what group this document belongs to.

        // Even if this document belongs to a group in the top N, we'll know that
        // we don't have to update that group.

        // Downside: if the number of unique groups is very low, this is
        // wasted effort as we will most likely be updating an existing group.
        if (orderedGroups != null) {
            for (int compIDX = 0; ; compIDX++) {
                final int c = reversed[compIDX] * leafComparators[compIDX].compareBottom(doc);
                if (c < 0) {
                    // Definitely not competitive. So don't even bother to continue
                    return false;
                } else if (c > 0) {
                    // Definitely competitive.
                    break;
                } else if (compIDX == compIDXEnd) {
                    // Here c=0. If we're at the last comparator, this doc is not
                    // competitive, since docs are visited in doc Id order, which means
                    // this doc cannot compete with any other document in the queue.
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void collect(int doc) throws IOException {

        if (isCompetitive(doc) == false)
            return;

        // TODO: should we add option to mean "ignore docs that
        // don't have the group field" (instead of stuffing them
        // under null group)?
        groupSelector.advanceTo(doc);
        T groupValue = groupSelector.currentValue();

        final CollectedSearchGroup<T> group = groupMap.get(groupValue);

        if (group == null) {

            // First time we are seeing this group, or, we've seen
            // it before but it fell out of the top N and is now
            // coming back

            if (groupMap.size() < topNGroups) {

                // Still in startup transient: we have not
                // seen enough unique groups to start pruning them;
                // just keep collecting them

                // Add a new CollectedSearchGroup:
                CollectedSearchGroup<T> sg = new CollectedSearchGroup<>();
                sg.groupValue = groupSelector.copyValue();
                sg.comparatorSlot = groupMap.size();
                sg.topDoc = docBase + doc;
                for (LeafFieldComparator fc : leafComparators) {
                    fc.copy(sg.comparatorSlot, doc);
                }
                groupMap.put(sg.groupValue, sg);

                if (groupMap.size() == topNGroups) {
                    // End of startup transient: we now have max
                    // number of groups; from here on we will drop
                    // bottom group when we insert new one:
                    buildSortedSet();
                }

                return;
            }

            // We already tested that the document is competitive, so replace
            // the bottom group with this new group.
            final CollectedSearchGroup<T> bottomGroup = orderedGroups.pollLast();
            assert orderedGroups.size() == topNGroups - 1;

            groupMap.remove(bottomGroup.groupValue);

            // reuse the removed CollectedSearchGroup
            bottomGroup.groupValue = groupSelector.copyValue();
            bottomGroup.topDoc = docBase + doc;

            for (LeafFieldComparator fc : leafComparators) {
                fc.copy(bottomGroup.comparatorSlot, doc);
            }

            groupMap.put(bottomGroup.groupValue, bottomGroup);
            orderedGroups.add(bottomGroup);
            assert orderedGroups.size() == topNGroups;

            final int lastComparatorSlot = orderedGroups.last().comparatorSlot;
            for (LeafFieldComparator fc : leafComparators) {
                fc.setBottom(lastComparatorSlot);
            }

            return;
        }

        // Update existing group:
        for (int compIDX = 0; ; compIDX++) {
            leafComparators[compIDX].copy(spareSlot, doc);

            final int c = reversed[compIDX] * comparators[compIDX].compare(group.comparatorSlot, spareSlot);
            if (c < 0) {
                // Definitely not competitive.
                return;
            } else if (c > 0) {
                // Definitely competitive; set remaining comparators:
                for (int compIDX2 = compIDX + 1; compIDX2 < comparators.length; compIDX2++) {
                    leafComparators[compIDX2].copy(spareSlot, doc);
                }
                break;
            } else if (compIDX == compIDXEnd) {
                // Here c=0. If we're at the last comparator, this doc is not
                // competitive, since docs are visited in doc Id order, which means
                // this doc cannot compete with any other document in the queue.
                return;
            }
        }

        // Remove before updating the group since lookup is done via comparators
        // TODO: optimize this

        final CollectedSearchGroup<T> prevLast;
        if (orderedGroups != null) {
            prevLast = orderedGroups.last();
            orderedGroups.remove(group);
            assert orderedGroups.size() == topNGroups - 1;
        } else {
            prevLast = null;
        }

        group.topDoc = docBase + doc;

        // Swap slots
        final int tmp = spareSlot;
        spareSlot = group.comparatorSlot;
        group.comparatorSlot = tmp;

        // Re-add the changed group
        if (orderedGroups != null) {
            orderedGroups.add(group);
            assert orderedGroups.size() == topNGroups;
            final CollectedSearchGroup<?> newLast = orderedGroups.last();
            // If we changed the value of the last group, or changed which group was last, then update bottom:
            if (group == newLast || prevLast != newLast) {
                for (LeafFieldComparator fc : leafComparators) {
                    fc.setBottom(newLast.comparatorSlot);
                }
            }
        }
    }

    /**
     * 基于当前 hashMap中存储的 group数据 进行排序
     * @throws IOException
     */
    private void buildSortedSet() throws IOException {
        // 这里排序对象就是将 comparators的组合起来
        final Comparator<CollectedSearchGroup<?>> comparator = new Comparator<CollectedSearchGroup<?>>() {
            @Override
            public int compare(CollectedSearchGroup<?> o1, CollectedSearchGroup<?> o2) {
                for (int compIDX = 0; ; compIDX++) {
                    // 这里比较器是有优先级的  如果前面的能直接决策出结果 那么直接返回
                    FieldComparator<?> fc = comparators[compIDX];
                    final int c = reversed[compIDX] * fc.compare(o1.comparatorSlot, o2.comparatorSlot);
                    if (c != 0) {
                        return c;
                    } else if (compIDX == compIDXEnd) {
                        // 一系列的比较器得到的结果都是相等时 比较该组下最大docId的大小
                        return o1.topDoc - o2.topDoc;
                    }
                }
            }
        };

        orderedGroups = new TreeSet<>(comparator);
        orderedGroups.addAll(groupMap.values());
        assert orderedGroups.size() > 0;

        // 设置垫底对象
        for (LeafFieldComparator fc : leafComparators) {
            fc.setBottom(orderedGroups.last().comparatorSlot);
        }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
        docBase = readerContext.docBase;
        for (int i = 0; i < comparators.length; i++) {
            leafComparators[i] = comparators[i].getLeafComparator(readerContext);
        }
        groupSelector.setNextReader(readerContext);
    }

    /**
     * @return the GroupSelector used for this Collector
     */
    public GroupSelector<T> getGroupSelector() {
        return groupSelector;
    }

}

