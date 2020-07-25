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
     **/
    static MergeState.DocMap[] sort(Sort sort, List<CodecReader> readers) throws IOException {

        // TODO: optimize if only 1 reader is incoming, though that's a rare case
        // 返回所有被排序的 field     每个field可以定义自己的规则
        // 如果reader下没有该field 返回null
        SortField fields[] = sort.getSort();
        final ComparableProvider[][] comparables = new ComparableProvider[fields.length][];
        final int[] reverseMuls = new int[fields.length];

        for (int i = 0; i < fields.length; i++) {
            // 返回的 ComparableProvider 已经具备了整合的逻辑了
            comparables[i] = getComparableProviders(readers, fields[i]);
            // 存储排序顺序
            reverseMuls[i] = fields[i].getReverse() ? -1 : 1;
        }
        // 每个segment对应的 reader对象被称为 leaf
        int leafCount = readers.size();

        PriorityQueue<LeafAndDocID> queue = new PriorityQueue<LeafAndDocID>(leafCount) {
            @Override
            public boolean lessThan(LeafAndDocID a, LeafAndDocID b) {
                // 这里 按照 sortedField的顺序进行比较 也就是 越前面的 sortedField 重要性越高
                for (int i = 0; i < comparables.length; i++) {
                    int cmp = Long.compare(a.valuesAsComparableLongs[i], b.valuesAsComparableLongs[i]);
                    if (cmp != 0) {
                        // reverseMuls[i] 代表该 field 下的排序顺序
                        // 推算一下  如果是倒序 那么 valuesAsComparableLongs[] 大的 返回算是 less  符合下面的表达式
                        // 如果是正序 valuesAsComparableLongs[] 小的就是 less  也就要求 cmp 为负数 也是符合
                        // ..
                        // ..
                        return reverseMuls[i] * cmp < 0;
                    }
                }

                // tie-break by docID natural order:
                // 如果是不同的 reader 优先选前面的
                if (a.readerIndex != b.readerIndex) {
                    return a.readerIndex < b.readerIndex;
                } else {
                    // 同一reader的情况 返回小的docId
                    return a.docID < b.docID;
                }
            }
        };

        // 分别记录每个reader 内部doc 对应全局doc的映射关系
        PackedLongValues.Builder[] builders = new PackedLongValues.Builder[leafCount];

        for (int i = 0; i < leafCount; i++) {
            CodecReader reader = readers.get(i);
            // 将reader的信息抽取出来生成 LeafAndDocID 对象
            LeafAndDocID leaf = new LeafAndDocID(i, reader.getLiveDocs(), reader.maxDoc(), comparables.length);

            // 计算某个 reader所有 sortField 计算出来的值
            for (int j = 0; j < comparables.length; j++) {
                // docId 默认从0开始
                leaf.valuesAsComparableLongs[j] = comparables[j][i].getAsComparableLong(leaf.docID);
            }
            queue.add(leaf);
            builders[i] = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        }

        // merge sort:
        // 这个就是merge后 全局的docId
        int mappedDocID = 0;
        // 记录上一次top对应的 reader
        int lastReaderIndex = 0;

        // 代表一开始就是有序的 不需要做任何处理
        boolean isSorted = true;
        // 这里应该是通过优先队列 对多个reader进行整合
        while (queue.size() != 0) {
            LeafAndDocID top = queue.top();

            // 实际上就是 从小的reader开始 直到所有元素弹出都没有一次 后面的reader成为 top 也就是一开始数据就是有序的
            if (lastReaderIndex > top.readerIndex) {
                // merge sort is needed
                isSorted = false;
            }
            lastReaderIndex = top.readerIndex;
            builders[top.readerIndex].add(mappedDocID);
            // 代表该id 对应的doc 存在    在getAsComparableLong 中 如果doc没有找到会返回一个默认值 这样就无法参考了 所以还是要借助 liveDoc 判断doc是否在reader中存在 只要存在就增加全局doc
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
                        // 代表doc在原reader上不存在
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
         * @param liveDocs   对应的liveDoc 位图
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
     */
    private static ComparableProvider[] getComparableProviders(List<CodecReader> readers, SortField sortField) throws IOException {

        ComparableProvider[] providers = new ComparableProvider[readers.size()];
        // 返回排序类型
        final SortField.Type sortType = Sorter.getSortFieldType(sortField);

        switch (sortType) {

            case STRING: {
                // this uses the efficient segment-local ordinal map:
                final SortedDocValues[] values = new SortedDocValues[readers.size()];
                for (int i = 0; i < readers.size(); i++) {
                    // 获取遍历所有 docValue的迭代器 同时该迭代器还具备了 通过docId 查询ord的功能   如果该segment下没有该field的信息 返回一个 empty对象
                    final SortedDocValues sorted = Sorter.getOrWrapSorted(readers.get(i), sortField);
                    values[i] = sorted;
                }
                // 生成一个数据结构 用于映射 全局ord 与 某个segment相同term的ord  (只针对当前 sortedField)
                OrdinalMap ordinalMap = OrdinalMap.build(null, values, PackedInts.DEFAULT);
                final int missingOrd;
                // 某个doc没找到时 返回默认值
                if (sortField.getMissingValue() == SortField.STRING_LAST) {
                    missingOrd = Integer.MAX_VALUE;
                } else {
                    missingOrd = Integer.MIN_VALUE;
                }

                for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
                    final SortedDocValues readerValues = values[readerIndex];
                    // 这里reader难道已经被排序过了吗 否则不合理啊
                    final LongValues globalOrds = ordinalMap.getGlobalOrds(readerIndex);
                    providers[readerIndex] = new ComparableProvider() {
                        @Override
                        public long getAsComparableLong(int docID) throws IOException {
                            // 通过 doc 找到 文档对应的顺序
                            if (readerValues.advanceExact(docID)) {
                                // translate segment's ord to global ord space:
                                // 定位doc的时候 内部的ord 也改变了 然后通过该term的ord 换成 globalOrd
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
                    // 读取某个segment下 有关某个field 的数据
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
