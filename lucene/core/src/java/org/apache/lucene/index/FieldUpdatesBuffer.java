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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * This class efficiently buffers numeric and binary field updates and stores
 * terms, values and metadata in a memory efficient way without creating large amounts
 * of objects. Update terms are stored without de-duplicating the update term.
 * In general we try to optimize for several use-cases. For instance we try to use constant
 * space for update terms field since the common case always updates on the same field. Also for docUpTo
 * we try to optimize for the case when updates should be applied to all docs ie. docUpTo=Integer.MAX_VALUE.
 * In other cases each update will likely have a different docUpTo.
 * Along the same lines this impl optimizes the case when all updates have a value. Lastly, if all updates share the
 * same value for a numeric field we only store the value once.
 * 描述了一系列 term的更新信息   一开始需要通过一个 Update 对象进行初始化
 * 这个对象 只允许存储  二进制流数据  或者数字   不能同时存储两种类型
 */
final class FieldUpdatesBuffer {
    private static final long SELF_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FieldUpdatesBuffer.class);
    private static final long STRING_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(String.class);
    private final Counter bytesUsed;
    /**
     * 记录内部更新信息总数
     */
    private int numUpdates = 1;
    // we use a very simple approach and store the update term values without de-duplication
    // which is also not a common case to keep updating the same value more than once...
    // we might pay a higher price in terms of memory in certain cases but will gain
    // on CPU for those. We also save on not needing to sort in order to apply the terms in order
    // since by definition we store them in order.
    // 存储每次更新相关的 term
    private final BytesRefArray termValues;
    /**
     * 存储BytesRefArray的 偏移量序号 但是排序方式不是按这个序号 而是转换成 对应序号写入的 bytes  当bytes等大时 转而比较 docId
     */
    private BytesRefArray.SortState termSortState;
    /**
     * 记录每次更新的 term 对应二进制数据
     */
    private final BytesRefArray byteValues; // this will be null if we are buffering numerics
    /**
     * 该数组记录每次更新的 term 对应的文档号
     */
    private int[] docsUpTo;
    /**
     * 记录每次更新的 term对应的 数字型数值
     */
    private long[] numericValues; // this will be null if we are buffering binaries
    /**
     * 记录每次更新term时 是否有携带值    hasValue == false 应该就是删除的意思  也就是该容器不仅记录了 term的更新情况 还记录了删除情况
     */
    private FixedBitSet hasValues;
    /**
     * 记录每次更新term时 对应的field
     */
    private String[] fields;
    // 初始情况下 这2个属性都是 initialValue
    private long maxNumeric = Long.MIN_VALUE;
    private long minNumeric = Long.MAX_VALUE;

    private final boolean isNumeric;
    private boolean finished = false;

    /**
     * @param bytesUsed    关联到 BufferedUpdates 的计数器 该对象会被并发操作
     * @param initialValue 该对象描述了 将文档中某个term更改成指定值
     * @param docUpTo      对应的文档号
     * @param isNumeric    数值类型是否是 数字
     */
    private FieldUpdatesBuffer(Counter bytesUsed, DocValuesUpdate initialValue, int docUpTo, boolean isNumeric) {
        this.bytesUsed = bytesUsed;
        this.bytesUsed.addAndGet(SELF_SHALLOW_SIZE);
        // 该对象内部由一个 ByteBlockPool  内部可以申请多个byteBuffer
        termValues = new BytesRefArray(bytesUsed);
        // 这里存储了 更新的数值
        termValues.append(initialValue.term.bytes);
        fields = new String[]{initialValue.term.field};
        bytesUsed.addAndGet(sizeOfString(initialValue.term.field));
        docsUpTo = new int[]{docUpTo};
        if (initialValue.hasValue == false) {
            hasValues = new FixedBitSet(1);
            bytesUsed.addAndGet(hasValues.ramBytesUsed());
        }
        this.isNumeric = isNumeric;
        byteValues = isNumeric ? null : new BytesRefArray(bytesUsed);
    }

    private static long sizeOfString(String string) {
        return STRING_SHALLOW_SIZE + (string.length() * Character.BYTES);
    }

    /**
     * 以这种形式构造时 就代表数值类型是 num
     *
     * @param bytesUsed
     * @param initialValue
     * @param docUpTo
     */
    FieldUpdatesBuffer(Counter bytesUsed, DocValuesUpdate.NumericDocValuesUpdate initialValue, int docUpTo) {
        this(bytesUsed, initialValue, docUpTo, true);
        if (initialValue.hasValue()) {
            numericValues = new long[]{initialValue.getValue()};
            maxNumeric = minNumeric = initialValue.getValue();
        } else {
            numericValues = new long[]{0};
        }
        bytesUsed.addAndGet(Long.BYTES);
    }

    FieldUpdatesBuffer(Counter bytesUsed, DocValuesUpdate.BinaryDocValuesUpdate initialValue, int docUpTo) {
        this(bytesUsed, initialValue, docUpTo, false);
        if (initialValue.hasValue()) {
            byteValues.append(initialValue.getValue());
        }
    }

    long getMaxNumeric() {
        assert isNumeric;
        // 这种算特殊情况  忽略 所以返回一个无效值
        if (minNumeric == Long.MAX_VALUE && maxNumeric == Long.MIN_VALUE) {
            return 0; // we don't have any value;
        }
        return maxNumeric;
    }

    long getMinNumeric() {
        assert isNumeric;
        if (minNumeric == Long.MAX_VALUE && maxNumeric == Long.MIN_VALUE) {
            return 0; // we don't have any value
        }
        return minNumeric;
    }

    /**
     * @param field    代表针对某个域 有一次更新的信息
     * @param docUpTo  对应的文档号
     * @param ord      本次更新的值 对应的序号
     * @param hasValue 本次更新是否有携带数值   (可能某次更新不携带数值)
     */
    void add(String field, int docUpTo, int ord, boolean hasValue) {
        assert finished == false : "buffer was finished already";
        // 每个改动的term的序号 就是 fields数组的下标 这样就能找到匹配的 field
        if (fields[0].equals(field) == false || fields.length != 1) {
            // 需要扩容
            if (fields.length <= ord) {
                String[] array = ArrayUtil.grow(fields, ord + 1);
                // 如果首次扩容 将 [0] 的位置置空
                if (fields.length == 1) {
                    Arrays.fill(array, 1, ord, fields[0]);
                }
                // 这里增加的是数组本身占用的 空间
                bytesUsed.addAndGet((array.length - fields.length) * RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                fields = array;
            }
            if (field != fields[0]) { // that's an easy win of not accounting if there is an outlier
                bytesUsed.addAndGet(sizeOfString(field));
            }
            fields[ord] = field;
        }

        // 存储本次更新的 docId
        if (docsUpTo[0] != docUpTo || docsUpTo.length != 1) {
            if (docsUpTo.length <= ord) {
                int[] array = ArrayUtil.grow(docsUpTo, ord + 1);
                if (docsUpTo.length == 1) {
                    Arrays.fill(array, 1, ord, docsUpTo[0]);
                }
                bytesUsed.addAndGet((array.length - docsUpTo.length) * Integer.BYTES);
                docsUpTo = array;
            }
            docsUpTo[ord] = docUpTo;
        }

        // 这个标识则使用位图进行存储 尽可能节省空间
        if (hasValue == false || hasValues != null) {
            if (hasValues == null) {
                hasValues = new FixedBitSet(ord + 1);
                hasValues.set(0, ord);
                bytesUsed.addAndGet(hasValues.ramBytesUsed());
            } else if (hasValues.length() <= ord) {
                FixedBitSet fixedBitSet = FixedBitSet.ensureCapacity(hasValues, ArrayUtil.oversize(ord + 1, 1));
                bytesUsed.addAndGet(fixedBitSet.ramBytesUsed() - hasValues.ramBytesUsed());
                hasValues = fixedBitSet;
            }
            if (hasValue) {
                hasValues.set(ord);
            }
        }
    }

    /**
     * 增加了某个 term的更新信息
     *
     * @param term
     * @param value
     * @param docUpTo
     */
    void addUpdate(Term term, long value, int docUpTo) {
        assert isNumeric;
        final int ord = append(term);
        // 查看一下本次更新是针对哪个域的
        String field = term.field;
        // 记录相关信息
        add(field, docUpTo, ord, true);
        minNumeric = Math.min(minNumeric, value);
        maxNumeric = Math.max(maxNumeric, value);
        if (numericValues[0] != value || numericValues.length != 1) {
            if (numericValues.length <= ord) {
                long[] array = ArrayUtil.grow(numericValues, ord + 1);
                if (numericValues.length == 1) {
                    Arrays.fill(array, 1, ord, numericValues[0]);
                }
                bytesUsed.addAndGet((array.length - numericValues.length) * Long.BYTES);
                numericValues = array;
            }
            numericValues[ord] = value;
        }
    }

    /**
     * 实际上这个更新应该是删除的意思吧
     *
     * @param term
     * @param docUpTo
     */
    void addNoValue(Term term, int docUpTo) {
        final int ord = append(term);
        add(term.field, docUpTo, ord, false);
    }

    /**
     * 写入二进制形式的值
     *
     * @param term
     * @param value
     * @param docUpTo
     */
    void addUpdate(Term term, BytesRef value, int docUpTo) {
        assert isNumeric == false;
        final int ord = append(term);
        byteValues.append(value);
        add(term.field, docUpTo, ord, true);
    }

    /**
     * 增加该对象维护的 有关更新的信息
     *
     * @param term
     * @return
     */
    private int append(Term term) {
        termValues.append(term.bytes);
        return numUpdates++;
    }

    /**
     * 将该对象冻结
     */
    void finish() {
        if (finished) {
            throw new IllegalStateException("buffer was finished already");
        }
        finished = true;
        // 代表当前只记录了单次更新   那为什么要排序???
        final boolean sortedTerms = hasSingleValue() && hasValues == null && fields.length == 1;
        if (sortedTerms) {
            // sort by ascending by term, then sort descending by docsUpTo so that we can skip updates with lower docUpTo.
            // 结果中存储的是  BytesRefArray 的偏移量
            termSortState = termValues.sort(Comparator.naturalOrder(),  // 前面代表 按照更新的数值进行排序
                    // 上面的比较结果无效时 使用下面的 cmp 也就是比较   也就是比较 docId
                    (i1, i2) -> Integer.compare(
                            docsUpTo[getArrayIndex(docsUpTo.length, i2)],
                            docsUpTo[getArrayIndex(docsUpTo.length, i1)]));
            bytesUsed.addAndGet(termSortState.ramBytesUsed());
        }
    }

    /**
     * 必须要冻结后 才能迭代该对象
     *
     * @return
     */
    BufferedUpdateIterator iterator() {
        if (finished == false) {
            throw new IllegalStateException("buffer is not finished yet");
        }
        return new BufferedUpdateIterator();
    }

    boolean isNumeric() {
        assert isNumeric || byteValues != null;
        return isNumeric;
    }

    /**
     * 仅包含一个 数字型数值
     *
     * @return
     */
    boolean hasSingleValue() {
        // we only do this optimization for numerics so far.
        return isNumeric && numericValues.length == 1;
    }

    long getNumericValue(int idx) {
        if (hasValues != null && hasValues.get(idx) == false) {
            return 0;
        }
        return numericValues[getArrayIndex(numericValues.length, idx)];
    }

    /**
     * Struct like class that is used to iterate over all updates in this buffer
     * 在迭代器中使用  描述返回的实体信息
     */
    static class BufferedUpdate {

        /**
         * 刚好就是对应 某次更新动作相关的参数
         */
        private BufferedUpdate() {
        }

        ;
        /**
         * the max document ID this update should be applied to
         */
        int docUpTo;
        /**
         * a numeric value or 0 if this buffer holds binary updates
         */
        long numericValue;
        /**
         * a binary value or null if this buffer holds numeric updates
         */
        BytesRef binaryValue;
        /**
         * <code>true</code> if this update has a value
         */
        boolean hasValue;
        /**
         * The update terms field. This will never be null.
         */
        String termField;
        /**
         * The update terms value. This will never be null.
         */
        BytesRef termValue;

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException(
                    "this struct should not be use in map or other data-structures that use hashCode / equals");
        }

        @Override
        public boolean equals(Object obj) {
            throw new UnsupportedOperationException(
                    "this struct should not be use in map or other data-structures that use hashCode / equals");
        }
    }

    /**
     * An iterator that iterates over all updates in insertion order
     */
    class BufferedUpdateIterator {
        /**
         * 因为 sortState 存储的是  BytesRefArray 的 偏移量信息 所以还是要借助该对象才能获取想要的数据
         */
        private final BytesRefArray.IndexedBytesRefIterator termValuesIterator;
        private final BytesRefArray.IndexedBytesRefIterator lookAheadTermIterator;
        /**
         * 上面的是 term的迭代器      当更新的值 是二进制流的时候    初始化该对象 (对更新值的迭代器)
         */
        private final BytesRefIterator byteValuesIterator;

        /**
         * 重复利用该对象
         */
        private final BufferedUpdate bufferedUpdate = new BufferedUpdate();
        /**
         * 描述 每次更新  hasValue 的位图对象
         */
        private final Bits updatesWithValue;

        BufferedUpdateIterator() {
            // 通过 sortState 生成迭代器对象    如果没有设置termSortState  按照插入顺序排序
            this.termValuesIterator = termValues.iterator(termSortState);
            this.lookAheadTermIterator = termSortState != null ? termValues.iterator(termSortState) : null;
            // 迭代该对象时 顺序与  termValuesIterator 是不一致的
            this.byteValuesIterator = isNumeric ? null : byteValues.iterator();
            updatesWithValue = hasValues == null ? new Bits.MatchAllBits(numUpdates) : hasValues;
        }

        /**
         * If all updates update a single field to the same value, then we can apply these
         * updates in the term order instead of the request order as both will yield the same result.
         * This optimization allows us to iterate the term dictionary faster and de-duplicate updates.
         */
        boolean isSortedTerms() {
            return termSortState != null;
        }

        /**
         * Moves to the next BufferedUpdate or return null if all updates are consumed.
         * The returned instance is a shared instance and must be fully consumed before the next call to this method.
         */
        BufferedUpdate next() throws IOException {
            BytesRef next = nextTerm();
            if (next != null) {
                // 找到 term对应的下标  读取相关值后 填充到 update 对象上
                final int idx = termValuesIterator.ord();
                bufferedUpdate.termValue = next;
                bufferedUpdate.hasValue = updatesWithValue.get(idx);
                bufferedUpdate.termField = fields[getArrayIndex(fields.length, idx)];
                bufferedUpdate.docUpTo = docsUpTo[getArrayIndex(docsUpTo.length, idx)];
                if (bufferedUpdate.hasValue) {
                    if (isNumeric) {
                        bufferedUpdate.numericValue = numericValues[getArrayIndex(numericValues.length, idx)];
                        bufferedUpdate.binaryValue = null;
                    } else {
                        bufferedUpdate.binaryValue = byteValuesIterator.next();
                    }
                } else {
                    bufferedUpdate.binaryValue = null;
                    bufferedUpdate.numericValue = 0;
                }
                return bufferedUpdate;
            } else {
                return null;
            }
        }

        BytesRef nextTerm() throws IOException {
            if (lookAheadTermIterator != null) {
                final BytesRef lastTerm = bufferedUpdate.termValue;
                BytesRef lookAheadTerm;
                while ((lookAheadTerm = lookAheadTermIterator.next()) != null && lookAheadTerm.equals(lastTerm)) {
                    BytesRef discardedTerm = termValuesIterator.next(); // discard as the docUpTo of the previous update is higher
                    assert discardedTerm.equals(lookAheadTerm) : "[" + discardedTerm + "] != [" + lookAheadTerm + "]";
                    assert docsUpTo[getArrayIndex(docsUpTo.length, termValuesIterator.ord())] <= bufferedUpdate.docUpTo :
                            docsUpTo[getArrayIndex(docsUpTo.length, termValuesIterator.ord())] + ">" + bufferedUpdate.docUpTo;
                }
            }
            // 正常情况下直接返回 某个term
            return termValuesIterator.next();
        }
    }

    private static int getArrayIndex(int arrayLength, int index) {
        assert arrayLength == 1 || arrayLength > index : "illegal array index length: " + arrayLength + " index: " + index;
        return Math.min(arrayLength - 1, index);
    }
}
