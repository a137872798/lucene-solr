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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Maps per-segment ordinals to/from global ordinal space, using a compact packed-ints representation.
 *
 * <p><b>NOTE</b>: this is a costly operation, as it must merge sort all terms, and may require non-trivial RAM once done.  It's better to operate in
 * segment-private ordinal space instead when possible.
 *
 * @lucene.internal
 */
public class OrdinalMap implements Accountable {
    // TODO: we could also have a utility method to merge Terms[] and use size() as a weight when we need it
    // TODO: use more efficient packed ints structures?


    private static class TermsEnumIndex {
        public final static TermsEnumIndex[] EMPTY_ARRAY = new TermsEnumIndex[0];
        /**
         * 按照term数量从大到小的顺序
         */
        final int subIndex;
        /**
         * 迭代顺序是term从小到大
         */
        final TermsEnum termsEnum;
        BytesRef currentTerm;

        public TermsEnumIndex(TermsEnum termsEnum, int subIndex) {
            this.termsEnum = termsEnum;
            this.subIndex = subIndex;
        }

        public BytesRef next() throws IOException {
            currentTerm = termsEnum.next();
            return currentTerm;
        }
    }

    private static class SegmentMap implements Accountable {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SegmentMap.class);

        /**
         * Build a map from an index into a sorted view of `weights` to an index into `weights`.
         * @param weights  每个reader下该field有多少term
         */
        private static int[] map(final long[] weights) {
            // 使用这个数组解耦 这样排序后 不会影响到 weights数组
            final int[] newToOld = new int[weights.length];
            for (int i = 0; i < weights.length; ++i) {
                newToOld[i] = i;
            }

            // 这里按照 term的数量进行排序
            new InPlaceMergeSorter() {
                @Override
                protected void swap(int i, int j) {
                    final int tmp = newToOld[i];
                    newToOld[i] = newToOld[j];
                    newToOld[j] = tmp;
                }

                @Override
                protected int compare(int i, int j) {
                    // j first since we actually want higher weights first
                    return Long.compare(weights[newToOld[j]], weights[newToOld[i]]);
                }
            }.sort(0, weights.length);
            return newToOld;
        }

        private static int[] inverse(int[] map) {
            final int[] inverse = new int[map.length];
            for (int i = 0; i < map.length; ++i) {
                inverse[map[i]] = i;
            }
            return inverse;
        }

        /**
         * newToOld 通过按照term数量排序后的顺序可以映射到排序前的顺序
         * oldToNew 使用原顺序(reader下标) 映射到排序后的顺序
         */
        private final int[] newToOld, oldToNew;

        /**
         *
         * @param weights 描述了每个reader对应的 SortedDocValue 的 term数量  (去重后的数量)
         */
        SegmentMap(long[] weights) {
            newToOld = map(weights);
            oldToNew = inverse(newToOld);
            assert Arrays.equals(newToOld, inverse(oldToNew));
        }

        int newToOld(int segment) {
            return newToOld[segment];
        }

        int oldToNew(int segment) {
            return oldToNew[segment];
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(newToOld) + RamUsageEstimator.sizeOf(oldToNew);
        }
    }

    /**
     * Create an ordinal map that uses the number of unique values of each
     * {@link SortedDocValues} instance as a weight.
     *
     * @see #build(IndexReader.CacheKey, TermsEnum[], long[], float)
     * @param values 每个值对应某个reader对象的SortedDocValues 他们决定了本次全局的排序结果
     */
    public static OrdinalMap build(IndexReader.CacheKey owner, SortedDocValues[] values, float acceptableOverheadRatio) throws IOException {
        // 每个迭代器 还具备遍历所有field.value的能力  (已去重)
        final TermsEnum[] subs = new TermsEnum[values.length];
        final long[] weights = new long[values.length];
        for (int i = 0; i < values.length; ++i) {
            // 获取每个 docValue 下 维护所有 term的迭代器
            subs[i] = values[i].termsEnum();
            // 每个 docValue下有多少 term
            weights[i] = values[i].getValueCount();
        }
        return build(owner, subs, weights, acceptableOverheadRatio);
    }

    /**
     * Create an ordinal map that uses the number of unique values of each
     * {@link SortedSetDocValues} instance as a weight.
     *
     * @see #build(IndexReader.CacheKey, TermsEnum[], long[], float)
     */
    public static OrdinalMap build(IndexReader.CacheKey owner, SortedSetDocValues[] values, float acceptableOverheadRatio) throws IOException {
        final TermsEnum[] subs = new TermsEnum[values.length];
        final long[] weights = new long[values.length];
        for (int i = 0; i < values.length; ++i) {
            subs[i] = values[i].termsEnum();
            weights[i] = values[i].getValueCount();
        }
        return build(owner, subs, weights, acceptableOverheadRatio);
    }

    /**
     * Creates an ordinal map that allows mapping ords to/from a merged
     * space from <code>subs</code>.
     *
     * @param owner   a cache key
     * @param subs    TermsEnums that support {@link TermsEnum#ord()}. They need    每个reader的SortedDocValues 对应的term迭代器对象
     *                not be dense (e.g. can be FilteredTermsEnums}.
     * @param weights a weight for each sub. This is ideally correlated with        描述了每个reader对应的SortedDocValues 出现的总的 field.value数量 (去重后的数量)
     *                the number of unique terms that each sub introduces compared
     *                to the other subs
     * @throws IOException if an I/O error occurred.
     */
    public static OrdinalMap build(IndexReader.CacheKey owner, TermsEnum subs[], long[] weights, float acceptableOverheadRatio) throws IOException {
        if (subs.length != weights.length) {
            throw new IllegalArgumentException("subs and weights must have the same length");
        }

        // enums are not sorted, so let's sort to save memory
        final SegmentMap segmentMap = new SegmentMap(weights);
        return new OrdinalMap(owner, subs, segmentMap, acceptableOverheadRatio);
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OrdinalMap.class);

    /**
     * Cache key of whoever asked for this awful thing
     */
    public final IndexReader.CacheKey owner;
    // globalOrd -> (globalOrd - segmentOrd) where segmentOrd is the the ordinal in the first segment that contains this term
    // 每次所有reader中最小的 term 在当前reader下的ord 与 全局ord的差值
    final PackedLongValues globalOrdDeltas;
    // globalOrd -> first segment container
    // 每次最小的term首次出现的 reader下标
    final PackedLongValues firstSegments;
    // for every segment, segmentOrd -> globalOrd
    // 将当前reader下该term的ord 转换为全局term的ord
    final LongValues segmentToGlobalOrds[];
    // the map from/to segment ids
    final SegmentMap segmentMap;
    // ram usage
    final long ramBytesUsed;


    /**
     *
     * @param owner  先忽略
     * @param subs 存储reader下某参与排序的field.value 的迭代器对象 前提是该field.value是byteRef类型 这样可以使用termHash存储
     * @param segmentMap  存储映射关系的容器
     * @param acceptableOverheadRatio
     * @throws IOException
     */
    OrdinalMap(IndexReader.CacheKey owner, TermsEnum subs[], SegmentMap segmentMap, float acceptableOverheadRatio) throws IOException {
        // create the ordinal mappings by pulling a termsenum over each sub's
        // unique terms, and walking a multitermsenum over those
        this.owner = owner;
        this.segmentMap = segmentMap;
        // even though we accept an overhead ratio, we keep these ones with COMPACT
        // since they are only used to resolve values given a global ord, which is
        // slow anyway
        // 基于差值存储
        PackedLongValues.Builder globalOrdDeltas = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
        // 数据并非单调递增 采用最大值需要的位进行存储
        PackedLongValues.Builder firstSegments = PackedLongValues.packedBuilder(PackedInts.COMPACT);
        final PackedLongValues.Builder[] ordDeltas = new PackedLongValues.Builder[subs.length];
        for (int i = 0; i < ordDeltas.length; i++) {
            ordDeltas[i] = PackedLongValues.monotonicBuilder(acceptableOverheadRatio);
        }

        long[] ordDeltaBits = new long[subs.length];
        // 记录此时 该reader 已经获取到多少个 term
        long[] segmentOrds = new long[subs.length];

        // Just merge-sorts by term:  基于最小堆的优先队列
        PriorityQueue<TermsEnumIndex> queue = new PriorityQueue<TermsEnumIndex>(subs.length) {
            @Override
            protected boolean lessThan(TermsEnumIndex a, TermsEnumIndex b) {
                return a.currentTerm.compareTo(b.currentTerm) < 0;
            }
        };

        // 每个reader都往内部添加一个term  以从小到大的顺序写入
        for (int i = 0; i < subs.length; i++) {
            // 此时newToOld 已经按照term数量排序了 (从大到小的顺序)
            TermsEnumIndex sub = new TermsEnumIndex(subs[segmentMap.newToOld(i)], i);
            if (sub.next() != null) {
                queue.add(sub);
            }
        }

        BytesRefBuilder scratch = new BytesRefBuilder();

        // 这个是存放全局顺序的 也就是所有term以最小堆形式处理后 最终会存放到这里 每次插入的值使用这个 ord
        // 注意 termEnum 内部的 ord 本身也是从0 开始的
        long globalOrd = 0;

        while (queue.size() != 0) {
            // 此时top就是所有reader下的最小term
            TermsEnumIndex top = queue.top();
            // 将 term的数据填充到  refBuilder内
            scratch.copyBytes(top.currentTerm);

            // 每次都要重置下面2个参数
            int firstSegmentIndex = Integer.MAX_VALUE;
            long globalOrdDelta = Long.MAX_VALUE;

            // Advance past this term, recording the per-segment ord deltas:
            // 内循环负责将多个相同的term 合并 找到出现时最小的 segment下标
            while (true) {
                top = queue.top();
                // 返回该term在该termEnum的顺序
                long segmentOrd = top.termsEnum.ord();
                // 计算此时顶部term在对应termEnum下的 顺序信息 与 全局ord的差值
                long delta = globalOrd - segmentOrd;
                // 对应reader的下标
                int segmentIndex = top.subIndex;
                // We compute the least segment where the term occurs. In case the
                // first segment contains most (or better all) values, this will
                // help save significant memory
                // 更新全局最小的term所在的reader 下标
                // 如果有多个reader的值都相同的情况 选择下标最小的reader
                if (segmentIndex < firstSegmentIndex) {
                    firstSegmentIndex = segmentIndex;
                    globalOrdDelta = delta;
                }
                // 这个位图主要是用来鉴别 delta是否始终是0   以及计算最大的delta需要多少位表示
                ordDeltaBits[segmentIndex] |= delta;

                // for each per-segment ord, map it back to the global term; the while loop is needed
                // in case the incoming TermsEnums don't have compact ordinals (some ordinal values
                // are skipped), which can happen e.g. with a FilteredTermsEnum:
                assert segmentOrds[segmentIndex] <= segmentOrd;

                // TODO: we could specialize this case (the while loop is not needed when the ords are compact)   当ord本身是连续时就不需要这个while了
                do {
                    // 记录某个reader下每个值与全局ord的差值
                    ordDeltas[segmentIndex].add(delta);
                    // 此时将该reader的ord增加 意味着要读取下一个较大的term值
                    segmentOrds[segmentIndex]++;
                    // 照理说这里只会执行一次  因为 termEnum 本身就是单调递增的 所以当某个term变成top时 它前面的term 肯定已经处理过了
                    // 主要是考虑到 ord可能不连续的情况
                } while (segmentOrds[segmentIndex] <= segmentOrd);

                // 当 top内没有下一个元素了 将该reader对应的termEnum移除
                if (top.next() == null) {
                    queue.pop();
                    // 代表所有 term 都已经处理完毕
                    if (queue.size() == 0) {
                        break;
                    }
                } else {
                    // 重新寻找最小值  (因为上面调用next 所以堆顶元素已经改变了)
                    queue.updateTop();
                }
                // 当此时最小的term还是与之前一致的情况 就要继续循环
                if (queue.top().currentTerm.equals(scratch.get()) == false) {
                    break;
                }
            }

            // for each unique term, just mark the first segment index/delta where it occurs
            // 这里记录的是 每次某个唯一的term出现时 reader所在的下标 如果多个reader包含相同的值 那么使用最小的readerIndex
            firstSegments.add(firstSegmentIndex);
            // 该值肯定是单调递增的 最优情况就是delta不变 否则就是增加
            globalOrdDeltas.add(globalOrdDelta);
            // 全局ord始终是递增的
            globalOrd++;
        }

        // 这里构建出来的数据 就是每个 term 对应的最早出现的 segmentIndex
        this.firstSegments = firstSegments.build();
        // 每次读取出来的term在它的termEnum的ord  距离全局ord的差值
        this.globalOrdDeltas = globalOrdDeltas.build();
        // ordDeltas is typically the bottleneck, so let's see what we can do to make it faster
        segmentToGlobalOrds = new LongValues[subs.length];

        // 这里计算了一个基础的 内存使用量
        long ramBytesUsed = BASE_RAM_BYTES_USED + this.globalOrdDeltas.ramBytesUsed()
                + this.firstSegments.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds)
                + segmentMap.ramBytesUsed();

        // 开始遍历每个差值
        for (int i = 0; i < ordDeltas.length; ++i) {
            // 生成某个 segment.field 下 每个term.ord  与 全局term.ord的差值
            final PackedLongValues deltas = ordDeltas[i].build();
            // 这种情况代表某个reader下的所有term 包含了其他所有reader的term  它自身就可以作为globalTerm
            if (ordDeltaBits[i] == 0L) {
                // segment ords perfectly match global ordinals
                // likely in case of low cardinalities and large segments
                // 所以这里 传入全局ord 和返回的 segmentOrd 是一样的
                segmentToGlobalOrds[i] = LongValues.IDENTITY;
            } else {
                // 预估最大的增量数据最多需要多少bit来表示
                final int bitsRequired = ordDeltaBits[i] < 0 ? 64 : PackedInts.bitsRequired(ordDeltaBits[i]);
                // deltas.ramBytesUsed() 内部 bit的计算是做过特殊处理的 也就是不存储原数据 只存储差值 所以实际消耗的bit 很小
                final long monotonicBits = deltas.ramBytesUsed() * 8;
                final long packedBits = bitsRequired * deltas.size();

                if (deltas.size() <= Integer.MAX_VALUE
                        // 这里代表只要使用的额外内存在允许的范围内 就可以采用直接存储的方式(省略min 和期望值的计算 以便简化逻辑)
                        && packedBits <= monotonicBits * (1 + acceptableOverheadRatio)) {
                    // monotonic compression mostly adds overhead, let's keep the mapping in plain packed ints
                    final int size = (int) deltas.size();
                    final PackedInts.Mutable newDeltas = PackedInts.getMutable(size, bitsRequired, acceptableOverheadRatio);

                    // 将之前采用单调递增存储的数据 按照普通方法转存一次
                    final PackedLongValues.Iterator it = deltas.iterator();
                    for (int ord = 0; ord < size; ++ord) {
                        // 将delta 写入到newDeltas 中
                        newDeltas.set(ord, it.next());
                    }
                    assert it.hasNext() == false;
                    // 将当前reader下该term的ord 转换为全局term的ord
                    segmentToGlobalOrds[i] = new LongValues() {
                        @Override
                        public long get(long ord) {
                            // newDeltas.get((int) ord) 就是一个偏差值
                            return ord + newDeltas.get((int) ord);
                        }
                    };
                    ramBytesUsed += newDeltas.ramBytesUsed();
                } else {
                    segmentToGlobalOrds[i] = new LongValues() {
                        @Override
                        public long get(long ord) {
                            return ord + deltas.get(ord);
                        }
                    };
                    ramBytesUsed += deltas.ramBytesUsed();
                }
                ramBytesUsed += RamUsageEstimator.shallowSizeOf(segmentToGlobalOrds[i]);
            }
        }
        this.ramBytesUsed = ramBytesUsed;
    }

    /**
     * Given a segment number, return a {@link LongValues} instance that maps
     * segment ordinals to global ordinals.
     */
    public LongValues getGlobalOrds(int segmentIndex) {
        return segmentToGlobalOrds[segmentMap.oldToNew(segmentIndex)];
    }

    /**
     * Given global ordinal, returns the ordinal of the first segment which contains
     * this ordinal (the corresponding to the segment return {@link #getFirstSegmentNumber}).
     */
    public long getFirstSegmentOrd(long globalOrd) {
        return globalOrd - globalOrdDeltas.get(globalOrd);
    }

    /**
     * Given a global ordinal, returns the index of the first
     * segment that contains this term.
     */
    public int getFirstSegmentNumber(long globalOrd) {
        return segmentMap.newToOld((int) firstSegments.get(globalOrd));
    }

    /**
     * Returns the total number of unique terms in global ord space.
     */
    public long getValueCount() {
        return globalOrdDeltas.size();
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        List<Accountable> resources = new ArrayList<>();
        resources.add(Accountables.namedAccountable("global ord deltas", globalOrdDeltas));
        resources.add(Accountables.namedAccountable("first segments", firstSegments));
        resources.add(Accountables.namedAccountable("segment map", segmentMap));
        // TODO: would be nice to return actual child segment deltas too, but the optimizations are confusing
        return resources;
    }
}
