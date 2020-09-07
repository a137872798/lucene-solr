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
import java.util.List;

import org.apache.lucene.index.MergeState.DocMap;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

final class MultiSorter {

    /**
     * Does a merge sort of the leaves of the incoming reader, returning {@link DocMap} to map each leaf's
     * documents into the merged segment.  The documents for each incoming leaf reader must already be sorted by the same sort!
     * Returns null if the merge sort is not needed (segments are already in index sort order).
     * 按照该排序规则 为每个reader对象都生成一个 docMap[]
     * 每个reader读取的 排序相关的 docValue 必须已经排序完成   在perThread.flush()中可以看到 会对doc先按照field.value进行一次排序
     * 由同一个IndexWriter处理的所以可以保证 因为IndexSort始终没变 merge前的数据已经按照该顺序排序了
     */
    static MergeState.DocMap[] sort(Sort sort, List<CodecReader> readers) throws IOException {

        // TODO: optimize if only 1 reader is incoming, though that's a rare case
        SortField fields[] = sort.getSort();
        final ComparableProvider[][] comparables = new ComparableProvider[fields.length][];
        final int[] reverseMuls = new int[fields.length];

        for (int i = 0; i < fields.length; i++) {
            // 每个排序的 field都会对所有reader做处理
            // 返回一组能够反映大小关系的数据
            comparables[i] = getComparableProviders(readers, fields[i]);
            // 存储排序顺序
            reverseMuls[i] = fields[i].getReverse() ? -1 : 1;
        }
        int leafCount = readers.size();

        PriorityQueue<LeafAndDocID> queue = new PriorityQueue<LeafAndDocID>(leafCount) {
            @Override
            public boolean lessThan(LeafAndDocID a, LeafAndDocID b) {
                // 这里 按照 sortedField的顺序进行比较 也就是 越前面的 sortedField 优先级越高
                for (int i = 0; i < comparables.length; i++) {
                    int cmp = Long.compare(a.valuesAsComparableLongs[i], b.valuesAsComparableLongs[i]);
                    if (cmp != 0) {
                        return reverseMuls[i] * cmp < 0;
                    }
                }

                // tie-break by docID natural order:
                // 代表2个field在doc下的数据完全一致时 以readerIndex作为排序条件
                if (a.readerIndex != b.readerIndex) {
                    return a.readerIndex < b.readerIndex;
                } else {
                    // 同一reader的情况 返回小的docId
                    return a.docID < b.docID;
                }
            }
        };

        PackedLongValues.Builder[] builders = new PackedLongValues.Builder[leafCount];

        for (int i = 0; i < leafCount; i++) {
            // 将reader信息包装成 LeafAndDocID
            CodecReader reader = readers.get(i);
            // 将reader的信息抽取出来生成 LeafAndDocID 对象
            LeafAndDocID leaf = new LeafAndDocID(i, reader.getLiveDocs(), reader.maxDoc(), comparables.length);

            // 将每个field的值对应的表示大小关系的数据都填充到valuesAsComparableLongs上
            for (int j = 0; j < comparables.length; j++) {
                // docId 默认从0开始
                leaf.valuesAsComparableLongs[j] = comparables[j][i].getAsComparableLong(leaf.docID);
            }
            // 在初始化valuesAsComparableLongs 后才能进行比较
            queue.add(leaf);
            // 存储排序后的doc
            builders[i] = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        }

        // merge sort:
        // 这个就是merge后 全局的docId
        int mappedDocID = 0;
        // 记录上一次top对应的 reader
        int lastReaderIndex = 0;

        // 代表一开始就是有序的 不需要做任何处理
        boolean isSorted = true;

        // 这里就是通过不断迭代数据并通过优先队列排序的过程
        while (queue.size() != 0) {
            LeafAndDocID top = queue.top();

            // 实际上就是 从小的reader开始 直到所有元素弹出都没有一次 后面的reader成为 top 也就是一开始数据就是有序的
            if (lastReaderIndex > top.readerIndex) {
                // merge sort is needed
                isSorted = false;
            }
            lastReaderIndex = top.readerIndex;
            // 当doc不存在时 会写入一个旧的mappedDocID 但是只是占位 因为从下面的获取逻辑来看 不存在与liveDoc的docId只能返回-1
            builders[top.readerIndex].add(mappedDocID);
            // 必须确保这个doc存在  全局排序后的顺序与docId间的映射转换就是需要这层liveDocs的过滤 也就是去除不存在的doc
            if (top.liveDocs == null || top.liveDocs.get(top.docID)) {
                mappedDocID++;
            }
            // 往下遍历 顶层元素的 docId 以便获取新的值
            top.docID++;
            if (top.docID < top.maxDoc) {
                for (int j = 0; j < comparables.length; j++) {
                    top.valuesAsComparableLongs[j] = comparables[j][top.readerIndex].getAsComparableLong(top.docID);
                }
                queue.updateTop();
            } else {
                // 代表该reader的所有数据都已经处理完 从队列中弹出
                queue.pop();
            }
        }
        if (isSorted) {
            return null;
        }

        // 这就是个映射容器 每个reader对象上的doc 可以转换成merge后 全局doc
        MergeState.DocMap[] docMaps = new MergeState.DocMap[leafCount];
        for (int i = 0; i < leafCount; i++) {
            final PackedLongValues remapped = builders[i].build();
            final Bits liveDocs = readers.get(i).getLiveDocs();
            docMaps[i] = new MergeState.DocMap() {
                @Override
                public int get(int docID) {
                    if (liveDocs == null || liveDocs.get(docID)) {
                        return (int) remapped.get(docID);
                    } else {
                        // 代表该doc已经被删除
                        return -1;
                    }
                }
            };
        }

        return docMaps;
    }


    private static class LeafAndDocID {
        final int readerIndex;
        final Bits liveDocs;
        final int maxDoc;
        /**
         * 用于存储每个比较的结果
         */
        final long[] valuesAsComparableLongs;
        int docID;

        /**
         *
         * @param readerIndex  LeafReader的下标
         * @param liveDocs   此时存活的doc位图
         * @param maxDoc    最大的doc为多少
         * @param numComparables   涉及到多少 sortField
         */
        public LeafAndDocID(int readerIndex, Bits liveDocs, int maxDoc, int numComparables) {
            this.readerIndex = readerIndex;
            this.liveDocs = liveDocs;
            this.maxDoc = maxDoc;
            this.valuesAsComparableLongs = new long[numComparables];
        }
    }

    /**
     * Returns a long so that the natural ordering of long values matches the
     * ordering of doc IDs for the given comparator.
     */
    private interface ComparableProvider {
        long getAsComparableLong(int docID) throws IOException;
    }

    /**
     * Returns {@code ComparableProvider}s for the provided readers to represent the requested {@link SortField} sort order.
     * @return 每个reader下该field在每个doc下的值 或者说能决定大小的数据
     */
    private static ComparableProvider[] getComparableProviders(List<CodecReader> readers, SortField sortField) throws IOException {

        ComparableProvider[] providers = new ComparableProvider[readers.size()];
        // 返回排序类型
        final SortField.Type sortType = Sorter.getSortFieldType(sortField);

        switch (sortType) {

            // 这个时候 comparator对象就要由多个reader组合提供了 所以要先将他们有关排序的 docValue取出来 重新进行排序

            case STRING: {
                // this uses the efficient segment-local ordinal map:
                final SortedDocValues[] values = new SortedDocValues[readers.size()];
                for (int i = 0; i < readers.size(); i++) {
                    // 还原之前的 SortedDocValues数据
                    final SortedDocValues sorted = Sorter.getOrWrapSorted(readers.get(i), sortField);
                    values[i] = sorted;
                }
                // 这里构建了一个将每个 reader的term的ord转换到全局term.ord的映射容器
                OrdinalMap ordinalMap = OrdinalMap.build(null, values, PackedInts.DEFAULT);
                final int missingOrd;
                // 在doc下没有找到该field时 该doc的ord  注意不是term的ord
                if (sortField.getMissingValue() == SortField.STRING_LAST) {
                    missingOrd = Integer.MAX_VALUE;
                } else {
                    missingOrd = Integer.MIN_VALUE;
                }

                for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
                    final SortedDocValues readerValues = values[readerIndex];
                    // 具备将当前reader迭代的term.ord 映射成全局term.ord的能力
                    final LongValues globalOrds = ordinalMap.getGlobalOrds(readerIndex);
                    providers[readerIndex] = new ComparableProvider() {

                        /**
                         *
                         * @param docID
                         * @return
                         * @throws IOException
                         */
                        @Override
                        public long getAsComparableLong(int docID) throws IOException {
                            // 首先该field必须要存在于该doc下 否则使用missingOrd
                            if (readerValues.advanceExact(docID)) {
                                // translate segment's ord to global ord space:
                                // 在当前doc下 该field的值(term)对应的ord 在全局term的ord   ord实际上也就代表着大小关系 不需要实际获取term的值
                                return globalOrds.get(readerValues.ordValue());
                            } else {
                                return missingOrd;
                            }
                        }
                    };
                }
            }
            break;

            // 数据是数字类型
            case LONG:
            case INT: {
                final long missingValue;
                if (sortField.getMissingValue() != null) {
                    missingValue = ((Number) sortField.getMissingValue()).longValue();
                } else {
                    missingValue = 0L;
                }

                for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
                    // 该迭代器负责遍历该field在所有doc下的value 并以此作为排序的条件
                    final NumericDocValues values = Sorter.getOrWrapNumeric(readers.get(readerIndex), sortField);

                    // 感觉上就是在 NumericDocValues 的基础上增加了未命中的默认值
                    providers[readerIndex] = new ComparableProvider() {
                        @Override
                        public long getAsComparableLong(int docID) throws IOException {
                            if (values.advanceExact(docID)) {
                                return values.longValue();
                            } else {
                                return missingValue;
                            }
                        }
                    };
                }
            }
            break;

            // 下面2种类型应该就只是做了处理 要实现的功能应该跟 Int 和 Long 是一样的

            case DOUBLE: {
                final double missingValue;
                if (sortField.getMissingValue() != null) {
                    missingValue = (Double) sortField.getMissingValue();
                } else {
                    missingValue = 0.0;
                }

                for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
                    final NumericDocValues values = Sorter.getOrWrapNumeric(readers.get(readerIndex), sortField);

                    providers[readerIndex] = new ComparableProvider() {
                        @Override
                        public long getAsComparableLong(int docID) throws IOException {
                            double value = missingValue;
                            if (values.advanceExact(docID)) {
                                value = Double.longBitsToDouble(values.longValue());
                            }
                            return NumericUtils.doubleToSortableLong(value);
                        }
                    };
                }
            }
            break;

            case FLOAT: {
                final float missingValue;
                if (sortField.getMissingValue() != null) {
                    missingValue = (Float) sortField.getMissingValue();
                } else {
                    missingValue = 0.0f;
                }

                for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
                    final NumericDocValues values = Sorter.getOrWrapNumeric(readers.get(readerIndex), sortField);

                    providers[readerIndex] = new ComparableProvider() {
                        @Override
                        public long getAsComparableLong(int docID) throws IOException {
                            float value = missingValue;
                            if (values.advanceExact(docID)) {
                                value = Float.intBitsToFloat((int) values.longValue());
                            }
                            return NumericUtils.floatToSortableInt(value);
                        }
                    };
                }
            }
            break;

            default:
                throw new IllegalArgumentException("unhandled SortField.getType()=" + sortField.getType());
        }

        return providers;
    }
}
