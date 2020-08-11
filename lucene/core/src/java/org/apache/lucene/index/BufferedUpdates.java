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


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Holds buffered deletes and updates, by docID, term or query for a
 * single segment. This is used to hold buffered pending
 * deletes and updates against the to-be-flushed segment.  Once the
 * deletes and updates are pushed (on flush in DocumentsWriter), they
 * are converted to a {@link FrozenBufferedUpdates} instance and
 * pushed to the {@link BufferedUpdatesStream}.
 */

// NOTE: instances of this class are accessed either via a private
// instance on DocumentWriterPerThread, or via sync'd code by
// DocumentsWriterDeleteQueue
// 该容器用于存储一些 删除 更新的数据    每个writerPerThread对象在创建时 都会初始化该对象 记录在本线程中发生的doc删除 更新
class BufferedUpdates implements Accountable {

    /* Rough logic: HashMap has an array[Entry] w/ varying
       load factor (say 2 * POINTER).  Entry is object w/ Term
       key, Integer val, int hash, Entry next
       (OBJ_HEADER + 3*POINTER + INT).  Term is object w/
       String field and String text (OBJ_HEADER + 2*POINTER).
       Term's field is String (OBJ_HEADER + 4*INT + POINTER +
       OBJ_HEADER + string.length*CHAR).
       Term's text is String (OBJ_HEADER + 4*INT + POINTER +
       OBJ_HEADER + string.length*CHAR).  Integer is
       OBJ_HEADER + INT. */
    final static int BYTES_PER_DEL_TERM = 9 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 7 * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 10 * Integer.BYTES;

    /* Rough logic: HashMap has an array[Entry] w/ varying
       load factor (say 2 * POINTER).  Entry is object w/
       Query key, Integer val, int hash, Entry next
       (OBJ_HEADER + 3*POINTER + INT).  Query we often
       undercount (say 24 bytes).  Integer is OBJ_HEADER + INT. */
    final static int BYTES_PER_DEL_QUERY = 5 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 2 * Integer.BYTES + 24;


    // 代表当前记录了多少个 要按照该term删除doc的 term数量
    final AtomicInteger numTermDeletes = new AtomicInteger();
    // 有多少个 field 因为更新了 docValue 导致的 删除doc 动作
    final AtomicInteger numFieldUpdates = new AtomicInteger();

    /**
     * value 代表删除的docId上限
     */
    final Map<Term, Integer> deleteTerms = new HashMap<>(); // TODO cut this over to FieldUpdatesBuffer

    /**
     * query用于定位数据  这里的含义是指  query命中的文档被删除   value对应文档号上限
     */
    final Map<Query, Integer> deleteQueries = new HashMap<>();

    /**
     * 该容器用于存储更新信息
     */
    final Map<String, FieldUpdatesBuffer> fieldUpdates = new HashMap<>();

    /**
     * 这里维护了一个 int最大值
     */
    public static final Integer MAX_INT = Integer.valueOf(Integer.MAX_VALUE);

    private final Counter bytesUsed = Counter.newCounter(true);
    final Counter fieldUpdatesBytesUsed = Counter.newCounter(true);
    private final Counter termsBytesUsed = Counter.newCounter(true);

    private final static boolean VERBOSE_DELETES = false;

    long gen;

    /**
     * 标记该容器记录的删除信息是针对哪个段的     deleteQueue 会自动创建一个叫做 global的容器
     */
    final String segmentName;

    public BufferedUpdates(String segmentName) {
        this.segmentName = segmentName;
    }

    @Override
    public String toString() {
        if (VERBOSE_DELETES) {
            return "gen=" + gen + " numTerms=" + numTermDeletes + ", deleteTerms=" + deleteTerms
                    + ", deleteQueries=" + deleteQueries + ", fieldUpdates=" + fieldUpdates
                    + ", bytesUsed=" + bytesUsed;
        } else {
            String s = "gen=" + gen;
            if (numTermDeletes.get() != 0) {
                s += " " + numTermDeletes.get() + " deleted terms (unique count=" + deleteTerms.size() + ")";
            }
            if (deleteQueries.size() != 0) {
                s += " " + deleteQueries.size() + " deleted queries";
            }
            if (numFieldUpdates.get() != 0) {
                s += " " + numFieldUpdates.get() + " field updates";
            }
            if (bytesUsed.get() != 0) {
                s += " bytesUsed=" + bytesUsed.get();
            }

            return s;
        }
    }

    /**
     * 代表被query命中的 doc 需要被删除
     * @param query
     * @param docIDUpto
     */
    public void addQuery(Query query, int docIDUpto) {
        Integer current = deleteQueries.put(query, docIDUpto);
        // increment bytes used only if the query wasn't added so far.
        if (current == null) {
            // 首次添加的话 需要增加该对象占用的bytes总数
            bytesUsed.addAndGet(BYTES_PER_DEL_QUERY);
        }
    }

    /**
     * 记录某个 term  稍后携带该term的doc 都会被删除
     * @param term
     * @param docIDUpto  允许删除的 docId上限  默认是 IntMaxValue  也就是不做删除限制
     */
    public void addTerm(Term term, int docIDUpto) {
        Integer current = deleteTerms.get(term);
        // 如果本次传入的文档限制小 忽略本次操作
        if (current != null && docIDUpto < current) {
            // Only record the new number if it's greater than the
            // current one.  This is important because if multiple
            // threads are replacing the same doc at nearly the
            // same time, it's possible that one thread that got a
            // higher docID is scheduled before the other
            // threads.  If we blindly replace than we can
            // incorrectly get both docs indexed.
            return;
        }

        deleteTerms.put(term, Integer.valueOf(docIDUpto));
        // note that if current != null then it means there's already a buffered
        // delete on that term, therefore we seem to over-count. this over-counting
        // is done to respect IndexWriterConfig.setMaxBufferedDeleteTerms.
        // 增加计数器的值
        numTermDeletes.incrementAndGet();
        if (current == null) {
            termsBytesUsed.addAndGet(BYTES_PER_DEL_TERM + term.bytes.length + (Character.BYTES * term.field().length()));
        }
    }

    /**
     * 某个 field 更新了docValue  记录这次更新信息
     * @param update   更新的数值
     * @param docIDUpto  文档的id
     */
    void addNumericUpdate(NumericDocValuesUpdate update, int docIDUpto) {
        // 共用计数器
        FieldUpdatesBuffer buffer = fieldUpdates.computeIfAbsent(update.field, k -> new FieldUpdatesBuffer(fieldUpdatesBytesUsed, update, docIDUpto));
        // 代表更新成新的值 还是删除原来的值
        if (update.hasValue) {
            buffer.addUpdate(update.term, update.getValue(), docIDUpto);
        } else {
            buffer.addNoValue(update.term, docIDUpto);
        }
        numFieldUpdates.incrementAndGet();
    }

    void addBinaryUpdate(BinaryDocValuesUpdate update, int docIDUpto) {
        FieldUpdatesBuffer buffer = fieldUpdates.computeIfAbsent(update.field, k -> new FieldUpdatesBuffer(fieldUpdatesBytesUsed, update, docIDUpto));
        if (update.hasValue) {
            buffer.addUpdate(update.term, update.getValue(), docIDUpto);
        } else {
            buffer.addNoValue(update.term, docIDUpto);
        }
        numFieldUpdates.incrementAndGet();
    }

    // 当delete动作刷盘后 允许重置这些容器内的数据
    void clearDeleteTerms() {
        numTermDeletes.set(0);
        termsBytesUsed.addAndGet(-termsBytesUsed.get());
        deleteTerms.clear();
    }

    void clear() {
        deleteTerms.clear();
        deleteQueries.clear();
        numTermDeletes.set(0);
        numFieldUpdates.set(0);
        fieldUpdates.clear();
        bytesUsed.addAndGet(-bytesUsed.get());
        fieldUpdatesBytesUsed.addAndGet(-fieldUpdatesBytesUsed.get());
        termsBytesUsed.addAndGet(-termsBytesUsed.get());
    }

    boolean any() {
        return deleteTerms.size() > 0 || deleteQueries.size() > 0 || numFieldUpdates.get() > 0;
    }

    @Override
    public long ramBytesUsed() {
        return bytesUsed.get() + fieldUpdatesBytesUsed.get() + termsBytesUsed.get();
    }
}
