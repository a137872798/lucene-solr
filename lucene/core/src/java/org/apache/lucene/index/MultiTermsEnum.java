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
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;

/**
 * Exposes {@link TermsEnum} API, merged from {@link TermsEnum} API of sub-segments.
 * This does a merge sort, by term text, of the sub-readers.
 *
 * @lucene.experimental
 */
public final class MultiTermsEnum extends BaseTermsEnum {

    private static final Comparator<TermsEnumWithSlice> INDEX_COMPARATOR = new Comparator<TermsEnumWithSlice>() {
        @Override
        public int compare(TermsEnumWithSlice o1, TermsEnumWithSlice o2) {
            return o1.index - o2.index;
        }
    };

    private final TermMergeQueue queue;
    private final TermsEnumWithSlice[] subs;        // all of our subs (one per sub-reader)
    /**
     * 只有有效的 termEnum 才会填充到该数组中
     */
    private final TermsEnumWithSlice[] currentSubs; // current subs that have at least one term for this field
    /**
     * 此时最小值对应的迭代器
     */
    private final TermsEnumWithSlice[] top;

    /**
     * 此时 top对应的postingEnum数组
     */
    private final MultiPostingsEnum.EnumWithSlice[] subDocs;

    private BytesRef lastSeek;
    private boolean lastSeekExact;
    private final BytesRefBuilder lastSeekScratch = new BytesRefBuilder();

    /**
     * 此时最小值对应的迭代器数量
     */
    private int numTop;
    /**
     * 记录term迭代器中 有几个是有效的
     */
    private int numSubs;
    private BytesRef current;

    /**
     * 一个存储 term迭代器的临时对象
     */
    static class TermsEnumIndex {
        public final static TermsEnumIndex[] EMPTY_ARRAY = new TermsEnumIndex[0];
        /**
         * 先是这样 参与merge 的所有段信息  通过指定某个field后 各自返回了对应  terms  之后想要获取一个能够遍历所有term的迭代器
         * 这个下标就对应该迭代器在整组迭代器的位置
         */
        final int subIndex;
        final TermsEnum termsEnum;

        public TermsEnumIndex(TermsEnum termsEnum, int subIndex) {
            this.termsEnum = termsEnum;
            this.subIndex = subIndex;
        }
    }

    /**
     * Returns how many sub-reader slices contain the current
     * term.  @see #getMatchArray
     */
    public int getMatchCount() {
        return numTop;
    }

    /**
     * Returns sub-reader slices positioned to the current term.
     */
    public TermsEnumWithSlice[] getMatchArray() {
        return top;
    }

    /**
     * Sole constructor.
     *
     * @param slices Which sub-reader slices we should
     *               merge.     readSlice 内部的长度 应该是还没有整理过doc的  也就是有部分doc实际是没有数据的  不过在merge后的新段是做过整理的
     */
    public MultiTermsEnum(ReaderSlice[] slices) {
        queue = new TermMergeQueue(slices.length);
        top = new TermsEnumWithSlice[slices.length];
        subs = new TermsEnumWithSlice[slices.length];
        subDocs = new MultiPostingsEnum.EnumWithSlice[slices.length];
        for (int i = 0; i < slices.length; i++) {
            subs[i] = new TermsEnumWithSlice(i, slices[i]);
            subDocs[i] = new MultiPostingsEnum.EnumWithSlice();
            subDocs[i].slice = slices[i];
        }
        currentSubs = new TermsEnumWithSlice[slices.length];
    }

    @Override
    public BytesRef term() {
        return current;
    }

    /**
     * The terms array must be newly created TermsEnum, ie
     * {@link TermsEnum#next} has not yet been called.
     * 此时已经从所有参与merge的段中找到某个 field 的terms信息 现在根据这些现有信息进行合并
     */
    public TermsEnum reset(TermsEnumIndex[] termsEnumsIndex) throws IOException {
        assert termsEnumsIndex.length <= top.length;
        numSubs = 0;
        numTop = 0;
        queue.clear();
        for (int i = 0; i < termsEnumsIndex.length; i++) {

            final TermsEnumIndex termsEnumIndex = termsEnumsIndex[i];
            assert termsEnumIndex != null;

            // 这里将每个 迭代器的首个 term读取出来
            final BytesRef term = termsEnumIndex.termsEnum.next();
            if (term != null) {
                final TermsEnumWithSlice entry = subs[termsEnumIndex.subIndex];
                // 设置完 term后 将对象装入优先队列中
                entry.reset(termsEnumIndex.termsEnum, term);
                queue.add(entry);
                currentSubs[numSubs++] = entry;
            } else {
                // field has no terms
            }
        }

        if (queue.size() == 0) {
            return TermsEnum.EMPTY;
        } else {
            return this;
        }
    }

    /**
     * 查询某个term在该迭代器中是否存在
     * @param term
     * @return
     * @throws IOException
     */
    @Override
    public boolean seekExact(BytesRef term) throws IOException {
        // 选择重置队列 然后重新填装数据 并检测
        queue.clear();
        numTop = 0;

        // 代表之前有过 seek的数据
        boolean seekOpt = false;
        if (lastSeek != null && lastSeek.compareTo(term) <= 0) {
            seekOpt = true;
        }

        lastSeek = null;
        lastSeekExact = true;

        // 只遍历几个有效的 迭代器
        for (int i = 0; i < numSubs; i++) {
            final boolean status;
            // LUCENE-2130: if we had just seek'd already, prior
            // to this seek, and the new seek term is after the
            // previous one, don't try to re-seek this sub if its
            // current term is already beyond this new seek term.
            // Doing so is a waste because this sub will simply
            // seek to the same spot.
            if (seekOpt) {
                final BytesRef curTerm = currentSubs[i].current;
                if (curTerm != null) {
                    final int cmp = term.compareTo(curTerm);
                    if (cmp == 0) {
                        status = true;
                    } else if (cmp < 0) {
                        status = false;
                    } else {
                        status = currentSubs[i].terms.seekExact(term);
                    }
                } else {
                    status = false;
                }
            } else {
                status = currentSubs[i].terms.seekExact(term);
            }

            if (status) {
                // 只将携带该term的 迭代器设置到 top中
                top[numTop++] = currentSubs[i];
                // 并且将当前term 设置为指定term
                current = currentSubs[i].current = currentSubs[i].terms.term();
                assert term.equals(currentSubs[i].current);
            }
        }

        // if at least one sub had exact match to the requested
        // term then we found match
        return numTop > 0;
    }

    /**
     * 查找目标term
     * @param term
     * @return
     * @throws IOException
     */
    @Override
    public SeekStatus seekCeil(BytesRef term) throws IOException {
        queue.clear();
        numTop = 0;
        lastSeekExact = false;

        boolean seekOpt = false;
        if (lastSeek != null && lastSeek.compareTo(term) <= 0) {
            seekOpt = true;
        }

        // 读取上次seek 对应的数据
        lastSeekScratch.copyBytes(term);
        lastSeek = lastSeekScratch.get();

        for (int i = 0; i < numSubs; i++) {
            final SeekStatus status;
            // LUCENE-2130: if we had just seek'd already, prior
            // to this seek, and the new seek term is after the
            // previous one, don't try to re-seek this sub if its
            // current term is already beyond this new seek term.
            // Doing so is a waste because this sub will simply
            // seek to the same spot.
            if (seekOpt) {
                final BytesRef curTerm = currentSubs[i].current;
                if (curTerm != null) {
                    final int cmp = term.compareTo(curTerm);
                    if (cmp == 0) {
                        status = SeekStatus.FOUND;
                    } else if (cmp < 0) {
                        status = SeekStatus.NOT_FOUND;
                    } else {
                        status = currentSubs[i].terms.seekCeil(term);
                    }
                } else {
                    status = SeekStatus.END;
                }
            } else {
                status = currentSubs[i].terms.seekCeil(term);
            }

            // 如果在子迭代器中找到了 设置到 top数组中 跟 seekExact 的逻辑差不多
            if (status == SeekStatus.FOUND) {
                top[numTop++] = currentSubs[i];
                current = currentSubs[i].current = currentSubs[i].terms.term();
                // 定位后重新加入到队列
                queue.add(currentSubs[i]);
            } else {
                // 代表没有准确找到该值 还是选择回填到队列
                if (status == SeekStatus.NOT_FOUND) {
                    currentSubs[i].current = currentSubs[i].terms.term();
                    assert currentSubs[i].current != null;
                    queue.add(currentSubs[i]);
                } else {
                    assert status == SeekStatus.END;
                    // enum exhausted
                    // 此时读取到末尾了 也就不用回填到队列中了
                    currentSubs[i].current = null;
                }
            }
        }

        // 代表至少有一个termEnum 命中
        if (numTop > 0) {
            // at least one sub had exact match to the requested term
            return SeekStatus.FOUND;
        } else if (queue.size() > 0) {
            // no sub had exact match, but at least one sub found
            // a term after the requested term -- advance to that
            // next term:
            // 更新此时top值
            pullTop();
            return SeekStatus.NOT_FOUND;
        } else {
            return SeekStatus.END;
        }
    }

    @Override
    public void seekExact(long ord) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long ord() {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取此时最小的term 并填充到top中
     */
    private void pullTop() {
        // extract all subs from the queue that have the same
        // top term
        assert numTop == 0;
        // 获取此时最小的term 并填充到top中
        numTop = queue.fillTop(top);
        current = top[0].current;
    }

    /**
     * 更新最小值
     * @throws IOException
     */
    private void pushTop() throws IOException {
        // call next() on each top, and reorder queue
        // 此时队列中有 numTop个相同的值 都要切换
        for (int i = 0; i < numTop; i++) {
            TermsEnumWithSlice top = queue.top();
            top.current = top.terms.next();
            if (top.current == null) {
                queue.pop();
            } else {
                queue.updateTop();
            }
        }
        numTop = 0;
    }

    /**
     * 获取下一个term
     * @return
     * @throws IOException
     */
    @Override
    public BytesRef next() throws IOException {
        // 代表刚调用过 seekExact
        if (lastSeekExact) {
            // Must seekCeil at this point, so those subs that
            // didn't have the term can find the following term.
            // NOTE: we could save some CPU by only seekCeil the
            // subs that didn't match the last exact seek... but
            // most impls short-circuit if you seekCeil to term
            // they are already on.
            final SeekStatus status = seekCeil(current);
            // TODO 这里为什么能认定一定会找到 ???
            assert status == SeekStatus.FOUND;
            lastSeekExact = false;
        }
        lastSeek = null;

        // restore queue
        // 更新最小值
        pushTop();

        // gather equal top fields
        if (queue.size() > 0) {
            // TODO: we could maybe defer this somewhat costly operation until one of the APIs that
            // needs to see the top is invoked (docFreq, postings, etc.)
            // 取出最小值 设置到top[]上
            pullTop();
        } else {
            current = null;
        }

        return current;
    }

    /**
     * 获取当前 term的 频率总和
     * @return
     * @throws IOException
     */
    @Override
    public int docFreq() throws IOException {
        int sum = 0;
        for (int i = 0; i < numTop; i++) {
            sum += top[i].terms.docFreq();
        }
        return sum;
    }

    @Override
    public long totalTermFreq() throws IOException {
        long sum = 0;
        for (int i = 0; i < numTop; i++) {
            final long v = top[i].terms.totalTermFreq();
            assert v != -1;
            sum += v;
        }
        return sum;
    }

    /**
     * 获取当前term 关联的posting信息
     * @param reuse pass a prior PostingsEnum for possible reuse   有可能传入的参数为null
     * @param flags specifies which optional per-document values
     *        you require; see {@link PostingsEnum#FREQS}
     * @return
     * @throws IOException
     */
    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        MultiPostingsEnum docsEnum;

        // Can only reuse if incoming enum is also a MultiDocsEnum
        // 如果参数有效  先检测是否允许复用
        if (reuse != null && reuse instanceof MultiPostingsEnum) {
            docsEnum = (MultiPostingsEnum) reuse;
            // ... and was previously created w/ this MultiTermsEnum:
            // canReuse  ---> this.parent == parent
            if (!docsEnum.canReuse(this)) {
                docsEnum = new MultiPostingsEnum(this, subs.length);
            }
        } else {
            docsEnum = new MultiPostingsEnum(this, subs.length);
        }

        int upto = 0;

        // 将此时 term相同的所有元素 按照readSlice的顺序排序
        ArrayUtil.timSort(top, 0, numTop, INDEX_COMPARATOR);

        for (int i = 0; i < numTop; i++) {

            final TermsEnumWithSlice entry = top[i];

            assert entry.index < docsEnum.subPostingsEnums.length : entry.index + " vs " + docsEnum.subPostingsEnums.length + "; " + subs.length;
            // 这里是读取 原生term的posting信息  注意这里还打算复用之前的对象
            final PostingsEnum subPostingsEnum = entry.terms.postings(docsEnum.subPostingsEnums[entry.index], flags);
            assert subPostingsEnum != null;
            // 将数据读取出来后 重新设置到 docsEnum上
            docsEnum.subPostingsEnums[entry.index] = subPostingsEnum;
            subDocs[upto].postingsEnum = subPostingsEnum;
            subDocs[upto].slice = entry.subSlice;
            upto++;
        }

        return docsEnum.reset(subDocs, upto);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
        // implemented to not fail CheckIndex, but you shouldn't be using impacts on a slow reader
        return new SlowImpactsEnum(postings(null, flags));
    }

    /**
     * 这也是临时对象  存储一些排序用的必要信息
     */
    final static class TermsEnumWithSlice {
        private final ReaderSlice subSlice;
        TermsEnum terms;
        public BytesRef current;
        final int index;

        /**
         * 通过 分片和对应的下标来初始化该对象
         * @param index
         * @param subSlice
         */
        public TermsEnumWithSlice(int index, ReaderSlice subSlice) {
            this.subSlice = subSlice;
            this.index = index;
            assert subSlice.length >= 0 : "length=" + subSlice.length;
        }

        /**
         * 设置该分片对应的 segment 此时读取的 term 以及对应的迭代器
         * @param terms
         * @param term
         */
        public void reset(TermsEnum terms, BytesRef term) {
            this.terms = terms;
            current = term;
        }

        @Override
        public String toString() {
            return subSlice.toString() + ":" + terms;
        }
    }

    /**
     * 负责对内部的 terms进行排序
     */
    private final static class TermMergeQueue extends PriorityQueue<TermsEnumWithSlice> {

        final int[] stack;

        TermMergeQueue(int size) {
            super(size);
            this.stack = new int[size];
        }

        @Override
        protected boolean lessThan(TermsEnumWithSlice termsA, TermsEnumWithSlice termsB) {
            return termsA.current.compareTo(termsB.current) < 0;
        }

        /**
         * Add the {@link #top()} slice as well as all slices that are positionned
         * on the same term to {@code tops} and return how many of them there are.
         * 将此时最小的值取出来 存到数组中 如果此时有多个迭代器的 current都等于这个 最小值 那么都取出来
         */
        int fillTop(TermsEnumWithSlice[] tops) {
            final int size = size();
            if (size == 0) {
                return 0;
            }
            // 将头节点设置到 top中
            tops[0] = top();
            int numTop = 1;
            stack[0] = 1;
            int stackLen = 1;

            while (stackLen != 0) {
                // 下面的逻辑就是在读取二叉堆的数据

                // 第一次  stackLen是1 这里就是获取 stack[0]
                final int index = stack[--stackLen];
                // 二叉堆 左子节点 2n  右子节点 2n+1
                final int leftChild = index << 1;
                for (int child = leftChild, end = Math.min(size, leftChild + 1); child <= end; ++child) {
                    // 获取下标对应的元素
                    TermsEnumWithSlice te = get(child);
                    if (te.current.equals(tops[0].current)) {
                        tops[numTop++] = te;
                        stack[stackLen++] = child;
                    }
                }
            }
            return numTop;
        }

        private TermsEnumWithSlice get(int i) {
            return (TermsEnumWithSlice) getHeapArray()[i];
        }
    }

    @Override
    public String toString() {
        return "MultiTermsEnum(" + Arrays.toString(subs) + ")";
    }
}
