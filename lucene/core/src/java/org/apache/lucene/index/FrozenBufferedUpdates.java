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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Holds buffered deletes and updates by term or query, once pushed. Pushed
 * deletes/updates are write-once, so we shift to more memory efficient data
 * structure to hold them.  We don't hold docIDs because these are applied on
 * flush.
 * 该对象 跟FieldUpdatesBuffer 一样是描述 删除/更新信息的 不同点是 该对象无法再追加新的信息
 */
final class FrozenBufferedUpdates {

  /* NOTE: we now apply this frozen packet immediately on creation, yet this process is heavy, and runs
   * in multiple threads, and this compression is sizable (~8.3% of the original size), so it's important
   * we run this before applying the deletes/updates. */

  /* Query we often undercount (say 24 bytes), plus int. */
  final static int BYTES_PER_DEL_QUERY = RamUsageEstimator.NUM_BYTES_OBJECT_REF + Integer.BYTES + 24;
  
  // Terms, in sorted order:
  // 该对象采用 共享公共前缀的方式减少了 内存开销  并且可以在迭代器中还原之前的term
  // 代表包含这些 term的 doc都需要被删除
  final PrefixCodedTerms deleteTerms;

  // Parallel array of deleted query, and the docIDUpto for each
  // 被这些query 命中的doc 需要被删除
  final Query[] deleteQueries;
  /**
   * 代表每个 query 对应的 docId  上限
   */
  final int[] deleteQueryLimits;
  
  /** Counts down once all deletes/updates have been applied */
  public final CountDownLatch applied = new CountDownLatch(1);

  /**
   * 该对象本身最终会存储到任务队列中 并被多线程争用  所以只有获取到锁的对象才具备处理它的独占权
   */
  private final ReentrantLock applyLock = new ReentrantLock();
  /**
   * 以 field 为单位存储了每个 field内部数据的更新情况
   */
  private final Map<String, FieldUpdatesBuffer> fieldUpdates;

  /** How many total documents were deleted/updated. */
  // 增加删除的doc总数
  public long totalDelCount;

  /**
   * 统计总计有多少次更新动作
   */
  private final int fieldUpdatesCount;
  
  final int bytesUsed;
  /**
   * 统计当前总计处理多少 termNode
   */
  final int numTermDeletes;

  /**
   * 仅设置一次 且通过BufferedUpdatesStream 设置
   */
  private long delGen = -1; // assigned by BufferedUpdatesStream once pushed

  /**
   * 代表这次更新 是关于这个段
   */
  final SegmentCommitInfo privateSegment;  // non-null iff this frozen packet represents 
                                   // a segment private deletes. in that case is should
                                   // only have Queries and doc values updates
  private final InfoStream infoStream;

  /**
   * 该对象 主要就是通过一个 BufferedUpdates 对象进行初始化
   * @param infoStream
   * @param updates  该对象内部 包含多个 删除/更新信息
   * @param privateSegment   如果是某个 perThread 的deleteSlice根据当前globalSlice 信息生成的 update副本 那么在生成冻结对象时会携带 perThread对应的段信息 代表本次处理范围仅针对这个段
   *                         如果是基于 globalSlice 生成的段对象则传入null
   */
  public FrozenBufferedUpdates(InfoStream infoStream, BufferedUpdates updates, SegmentCommitInfo privateSegment) {
    this.infoStream = infoStream;
    this.privateSegment = privateSegment;
    assert privateSegment == null || updates.deleteTerms.isEmpty() : "segment private packet should only have del queries";
    // 代表包含这些 term的doc都需要被删除
    Term termsArray[] = updates.deleteTerms.keySet().toArray(new Term[updates.deleteTerms.size()]);
    // 按照 term -> fieldName 的顺序排序
    ArrayUtil.timSort(termsArray);
    // 构建公共前缀对象
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    for (Term term : termsArray) {
      builder.add(term);
    }
    // 所有term 通过这种压缩方式 存储在PrefixCodedTerms 中
    deleteTerms = builder.finish();

    // 这里代表命中哪些 query 的doc会被删除
    deleteQueries = new Query[updates.deleteQueries.size()];
    // 对应每个query在命中时 允许删除的docId上限是多少  默认情况下都是不做限制的
    deleteQueryLimits = new int[updates.deleteQueries.size()];
    int upto = 0;
    for(Map.Entry<Query,Integer> ent : updates.deleteQueries.entrySet()) {
      deleteQueries[upto] = ent.getKey();
      deleteQueryLimits[upto] = ent.getValue();
      upto++;
    }
    // TODO if a Term affects multiple fields, we could keep the updates key'd by Term
    // so that it maps to all fields it affects, sorted by their docUpto, and traverse
    // that Term only once, applying the update to all fields that still need to be
    // updated.

    // 将内部标记为  finished
    updates.fieldUpdates.values().forEach(FieldUpdatesBuffer::finish);
    this.fieldUpdates = Map.copyOf(updates.fieldUpdates);
    // 内部总共产生了多少 更新数据
    this.fieldUpdatesCount = updates.numFieldUpdates.get();

    bytesUsed = (int) ((deleteTerms.ramBytesUsed() + deleteQueries.length * BYTES_PER_DEL_QUERY)
        + updates.fieldUpdatesBytesUsed.get());
    
    numTermDeletes = updates.numTermDeletes.get();
    if (infoStream != null && infoStream.isEnabled("BD")) {
      infoStream.message("BD", String.format(Locale.ROOT,
                                             "compressed %d to %d bytes (%.2f%%) for deletes/updates; private segment %s",
                                             updates.ramBytesUsed(), bytesUsed, 100.*bytesUsed/updates.ramBytesUsed(),
                                             privateSegment));
    }
  }

  /**
   * Tries to lock this buffered update instance
   * @return true if the lock was successfully acquired. otherwise false.
   */
  boolean tryLock() {
    return applyLock.tryLock();
  }

  /**
   * locks this buffered update instance
   */
  void lock() {
    applyLock.lock();
  }

  /**
   * Releases the lock of this buffered update instance
   */
  void unlock() {
    applyLock.unlock();
  }

  /**
   * Returns true if this buffered updates instance was already applied
   * 代表该数据已经被处理过
   */
  boolean isApplied() {
    assert applyLock.isHeldByCurrentThread();
    return applied.getCount() == 0;
  }

  /** Applies pending delete-by-term, delete-by-query and doc values updates to all segments in the index, returning
   *  the number of new deleted or updated documents.
   * @param segStates 每个对象对应一个段 内部包含了可以读取该段所有索引数据的reader对象
   */
  long apply(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {
    assert applyLock.isHeldByCurrentThread();
    if (delGen == -1) {
      // we were not yet pushed
      throw new IllegalArgumentException("gen is not yet set; call BufferedUpdatesStream.push first");
    }

    assert applied.getCount() != 0;

    if (privateSegment != null) {
      assert segStates.length == 1;
      assert privateSegment == segStates[0].reader.getOriginalSegmentInfo();
    }

    // 这里将删除信息作用到所有相关的segment上 并返回删除的数量   同时在 pendingDeletes对象中会增加一组待删除的数据
    totalDelCount += applyTermDeletes(segStates);
    // 找到所有被 query命中的doc
    totalDelCount += applyQueryDeletes(segStates);
    // 处理对 DV的更新数据
    totalDelCount += applyDocValuesUpdates(segStates);

    return totalDelCount;
  }

  private long applyDocValuesUpdates(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {

    if (fieldUpdates.isEmpty()) {
      return 0;
    }

    long startNS = System.nanoTime();

    long updateCount = 0;

    for (BufferedUpdatesStream.SegmentState segState : segStates) {

      if (delGen < segState.delGen) {
        // segment is newer than this deletes packet
        continue;
      }

      if (segState.rld.refCount() == 1) {
        // This means we are the only remaining reference to this segment, meaning
        // it was merged away while we were running, so we can safely skip running
        // because we will run on the newly merged segment next:
        continue;
      }

      // 代表本次是否是针对私有段的处理
      final boolean isSegmentPrivateDeletes = privateSegment != null;
      if (fieldUpdates.isEmpty() == false) {
        // 将所有 DV 更新对象作用到 segmentState上
        updateCount += applyDocValuesUpdates(segState, fieldUpdates, delGen, isSegmentPrivateDeletes);
      }

    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD",
                         String.format(Locale.ROOT, "applyDocValuesUpdates %.1f msec for %d segments, %d field updates; %d new updates",
                                       (System.nanoTime()-startNS)/1000000.,
                                       segStates.length,
                                       fieldUpdatesCount,
                                       updateCount));
    }

    return updateCount;
  }

  /**
   * 处理针对某个 segment对象的 DV update
   */
  private static long  applyDocValuesUpdates(BufferedUpdatesStream.SegmentState segState,
                                            Map<String, FieldUpdatesBuffer> updates,
                                            long delGen,
                                            boolean segmentPrivateDeletes) throws IOException {

    // TODO: we can process the updates per DV field, from last to first so that
    // if multiple terms affect same document for the same field, we add an update
    // only once (that of the last term). To do that, we can keep a bitset which
    // marks which documents have already been updated. So e.g. if term T1
    // updates doc 7, and then we process term T2 and it updates doc 7 as well,
    // we don't apply the update since we know T1 came last and therefore wins
    // the update.
    // We can also use that bitset as 'liveDocs' to pass to TermEnum.docs(), so
    // that these documents aren't even returned.

    long updateCount = 0;

    // We first write all our updates private, and only in the end publish to the ReadersAndUpdates */
    // 存储以field为单位 合并所有update信息后的对象
    final List<DocValuesFieldUpdates> resolvedUpdates = new ArrayList<>();

    for (Map.Entry<String, FieldUpdatesBuffer> fieldUpdate : updates.entrySet()) {

      // 找到本次更新对应 fieldName
      String updateField = fieldUpdate.getKey();

      // 这个对象会吸纳所有的 update 信息
      DocValuesFieldUpdates dvUpdates = null;

      // 存储更新信息的容器
      FieldUpdatesBuffer value = fieldUpdate.getValue();
      // 更新的类型
      boolean isNumeric = value.isNumeric();
      // 获取更新信息的迭代器
      FieldUpdatesBuffer.BufferedUpdateIterator iterator = value.iterator();

      // 用于存储迭代器返回信息的对象
      FieldUpdatesBuffer.BufferedUpdate bufferedUpdate;
      // 基于 reader对象 生成能遍历 term所有doc的迭代器
      TermDocsIterator termDocsIterator = new TermDocsIterator(segState.reader, iterator.isSortedTerms());

      // 迭代更新信息
      while ((bufferedUpdate = iterator.next()) != null) {
        // TODO: we traverse the terms in update order (not term order) so that we
        // apply the updates in the correct order, i.e. if two terms update the
        // same document, the last one that came in wins, irrespective of the
        // terms lexical order.
        // we can apply the updates in terms order if we keep an updatesGen (and
        // increment it with every update) and attach it to each NumericUpdate. Note
        // that we cannot rely only on docIDUpto because an app may send two updates
        // which will get same docIDUpto, yet will still need to respect the order
        // those updates arrived.
        // TODO: we could at least *collate* by field?
        // 返回该term 所在的所有docId的迭代器
        // 即使在全局范围内存在针对某个field的更新数据  但是当前segment 没有这个 field的信息是无法生成 DocIdSetIterator 的
        final DocIdSetIterator docIdSetIterator = termDocsIterator.nextTerm(bufferedUpdate.termField, bufferedUpdate.termValue);
        if (docIdSetIterator != null) {
          final int limit;
          // 代表当前 update对象是由本次 segment刷盘时生成的  这时采用设置的 upTo
          if (delGen == segState.delGen) {
            assert segmentPrivateDeletes;
            limit = bufferedUpdate.docUpTo;
          } else {
            // 否则没有doc限制
            limit = Integer.MAX_VALUE;
          }
          final BytesRef binaryValue;
          final long longValue;
          if (bufferedUpdate.hasValue == false) {
            longValue = -1;
            binaryValue = null;
          } else {
            longValue = bufferedUpdate.numericValue;
            binaryValue = bufferedUpdate.binaryValue;
          }
          // 包装成一个  DV update 对象
          if (dvUpdates == null) {
            if (isNumeric) {
              // 代表该 对象下所有更新的结果都是同一个值
              if (value.hasSingleValue()) {
                dvUpdates = new NumericDocValuesFieldUpdates
                    .SingleValueNumericDocValuesFieldUpdates(delGen, updateField, segState.reader.maxDoc(),
                    value.getNumericValue(0));
              } else {
                dvUpdates = new NumericDocValuesFieldUpdates(delGen, updateField, value.getMinNumeric(),
                    value.getMaxNumeric(), segState.reader.maxDoc());
              }
            } else {
              // 创建一个二进制的update 对象
              dvUpdates = new BinaryDocValuesFieldUpdates(delGen, updateField, segState.reader.maxDoc());
            }
            // 将结果设置到 list中
            resolvedUpdates.add(dvUpdates);
          }
          final IntConsumer docIdConsumer;
          final DocValuesFieldUpdates update = dvUpdates;
          // 将更新信息插入到 DocValuesFieldUpdates 中
          if (bufferedUpdate.hasValue == false) {
            docIdConsumer = doc -> update.reset(doc);
          } else if (isNumeric) {
            docIdConsumer = doc -> update.add(doc, longValue);
          } else {
            docIdConsumer = doc -> update.add(doc, binaryValue);
          }
          // 经过上面 termDelete 和 queryDelete的处理后 此时已经更新了存活的doc  以及被标记成待删除的doc 就不需要再处理了
          final Bits acceptDocs = segState.rld.getLiveDocs();
          // TODO 先忽略
          if (segState.rld.sortMap != null && segmentPrivateDeletes) {
            // This segment was sorted on flush; we must apply seg-private deletes carefully in this case:
            int doc;
            while ((doc = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              if (acceptDocs == null || acceptDocs.get(doc)) {
                // The limit is in the pre-sorted doc space:
                if (segState.rld.sortMap.newToOld(doc) < limit) {
                  docIdConsumer.accept(doc);
                  updateCount++;
                }
              }
            }
          } else {
            int doc;
            // 遍历当前term 所在的所有doc
            while ((doc = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              if (doc >= limit) {
                break; // no more docs that can be updated for this term
              }
              // 确定当前doc 还存活
              if (acceptDocs == null || acceptDocs.get(doc)) {
                // 进行处理
                docIdConsumer.accept(doc);
                updateCount++;
              }
            }
          }
        }
      }
    }

    // now freeze & publish:
    // 这里已经存储了所有的结果
    for (DocValuesFieldUpdates update : resolvedUpdates) {
      if (update.any()) {
        // 将内部数据按照docId 排序
        update.finish();
        segState.rld.addDVUpdate(update);
      }
    }

    return updateCount;
  }

  // Delete by query
  // 代表被 query 命中的doc 都需要被删除
  // TODO
  private long applyQueryDeletes(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {

    if (deleteQueries.length == 0) {
      return 0;
    }

    long startNS = System.nanoTime();

    long delCount = 0;
    for (BufferedUpdatesStream.SegmentState segState : segStates) {

      // 这应该只是一道保险 从目前的逻辑来看 在 外面挑选segStates时 就已经挡掉这些不合法的数据了
      if (delGen < segState.delGen) {
        // segment is newer than this deletes packet
        continue;
      }

      // 从注释上理解 当引用计数为1 时 代表该对象已经被其他地方放弃了 会出现这种情况的原因是之前的数据 已经被merge掉了 所以该segment应当被废弃 就忽略对它的处理
      if (segState.rld.refCount() == 1) {
        // This means we are the only remaining reference to this segment, meaning
        // it was merged away while we were running, so we can safely skip running
        // because we will run on the newly merged segment next:
        continue;
      }

      final LeafReaderContext readerContext = segState.reader.getContext();
      for (int i = 0; i < deleteQueries.length; i++) {
        Query query = deleteQueries[i];
        int limit;
        if (delGen == segState.delGen) {
          assert privateSegment != null;
          limit = deleteQueryLimits[i];
        } else {
          limit = Integer.MAX_VALUE;
        }
        final IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        searcher.setQueryCache(null);
        query = searcher.rewrite(query);
        final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1);
        final Scorer scorer = weight.scorer(readerContext);
        if (scorer != null) {
          final DocIdSetIterator it = scorer.iterator();
          if (segState.rld.sortMap != null && limit != Integer.MAX_VALUE) {
            assert privateSegment != null;
            // This segment was sorted on flush; we must apply seg-private deletes carefully in this case:
            int docID;
            while ((docID = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              // The limit is in the pre-sorted doc space:
              if (segState.rld.sortMap.newToOld(docID) < limit) {
                if (segState.rld.delete(docID)) {
                  delCount++;
                }
              }
            }
          } else {
            int docID;
            while ((docID = it.nextDoc()) < limit) {
              if (segState.rld.delete(docID)) {
                delCount++;
              }
            }
          }
        }
      }
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD",
                         String.format(Locale.ROOT, "applyQueryDeletes took %.2f msec for %d segments and %d queries; %d new deletions",
                                       (System.nanoTime()-startNS)/1000000.,
                                       segStates.length,
                                       deleteQueries.length,
                                       delCount));
    }
    
    return delCount;
  }

  /**
   * 找到命中  termNode 的所有doc 并标记成删除
   * @param segStates
   * @return
   * @throws IOException
   */
  private long applyTermDeletes(BufferedUpdatesStream.SegmentState[] segStates) throws IOException {

    if (deleteTerms.size() == 0) {
      return 0;
    }

    // We apply segment-private deletes on flush:
    // 仅针对单个段的 删除term操作应当在flush时就顺便完成
    assert privateSegment == null;

    long startNS = System.nanoTime();

    long delCount = 0;

    for (BufferedUpdatesStream.SegmentState segState : segStates) {
      assert segState.delGen != delGen: "segState.delGen=" + segState.delGen + " vs this.gen=" + delGen;
      if (segState.delGen > delGen) {
        // our deletes don't apply to this segment
        continue;
      }
      // 代表该state之前已经处理过了
      if (segState.rld.refCount() == 1) {
        // This means we are the only remaining reference to this segment, meaning
        // it was merged away while we were running, so we can safely skip running
        // because we will run on the newly merged segment next:
        continue;
      }

      // 包含所有需要删除的 term(以及该term所属的field)   在构建 deleteTerms时 会先将term 排序，使用共享前缀能构建最小的结构
      FieldTermIterator iter = deleteTerms.iterator();
      BytesRef delTerm;
      // reader 内部包含读取各种所有文件的输入流 这里遍历term 与deleteTerm做匹配 并将命中的term所在的doc标记成待删除
      TermDocsIterator termDocsIterator = new TermDocsIterator(segState.reader, true);
      while ((delTerm = iter.next()) != null) {
        // 通过 term， field 去定位存在于哪些doc中
        final DocIdSetIterator iterator = termDocsIterator.nextTerm(iter.field(), delTerm);
        if (iterator != null) {
          int docID;
          // 迭代该term所在的doc
          while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            // NOTE: there is no limit check on the docID
            // when deleting by Term (unlike by Query)
            // because on flush we apply all Term deletes to
            // each segment.  So all Term deleting here is
            // against prior segments:
            // 将该doc 标记成待删除
            if (segState.rld.delete(docID)) {
              delCount++;
            }
          }
        }
      }
    }

    if (infoStream.isEnabled("BD")) {
      infoStream.message("BD",
                         String.format(Locale.ROOT, "applyTermDeletes took %.2f msec for %d segments and %d del terms; %d new deletions",
                                       (System.nanoTime()-startNS)/1000000.,
                                       segStates.length,
                                       deleteTerms.size(),
                                       delCount));
    }

    return delCount;
  }

  /**
   * 设置该对象被处理时对应的 delGen
   * BufferedUpdatesStream 负责管理所有 FrozenBufferedUpdates  每当添加一个新的FrozenBufferedUpdates 到队列中就会为它设置一个新的 delGen
   * @param delGen
   */
  public void setDelGen(long delGen) {
    assert this.delGen == -1: "delGen was already previously set to " + this.delGen;
    this.delGen = delGen;
    deleteTerms.setDelGen(delGen);
  }
  
  public long delGen() {
    assert delGen != -1;
    return delGen;
  }

  @Override
  public String toString() {
    String s = "delGen=" + delGen;
    if (numTermDeletes != 0) {
      s += " numDeleteTerms=" + numTermDeletes;
      if (numTermDeletes != deleteTerms.size()) {
        s += " (" + deleteTerms.size() + " unique)";
      }
    }
    if (deleteQueries.length != 0) {
      s += " numDeleteQueries=" + deleteQueries.length;
    }
    if (fieldUpdates.size() > 0) {
      s += " fieldUpdates=" + fieldUpdatesCount;
    }
    if (bytesUsed != 0) {
      s += " bytesUsed=" + bytesUsed;
    }
    if (privateSegment != null) {
      s += " privateSegment=" + privateSegment;
    }

    return s;
  }
  
  boolean any() {
    return deleteTerms.size() > 0 || deleteQueries.length > 0 || fieldUpdatesCount > 0 ;
  }

  /**
   * This class helps iterating a term dictionary and consuming all the docs for each terms.
   * It accepts a field, value tuple and returns a {@link DocIdSetIterator} if the field has an entry
   * for the given value. It has an optimized way of iterating the term dictionary if the terms are
   * passed in sorted order and makes sure terms and postings are reused as much as possible.
   * 该对象通过 segmentState内部的reader对象初始化 按照docId 读取文档下关联的term 便于 配合deleteTerm 进行删除工作
   */
  static final class TermDocsIterator {
    /**
     * 该对象通过指定 field 可以获取到该field下所有的term
     */
    private final TermsProvider provider;
    /**
     * 当前 termsEnum 属于哪个field
     */
    private String field;
    private TermsEnum termsEnum;
    /**
     * 这个 position 实际上描述的是 某个term在哪些doc中存在
     */
    private PostingsEnum postingsEnum;
    /**
     * provider 提供的term 是否已经按照大小排序
     */
    private final boolean sortedTerms;
    /**
     * 对应此时的 term
     */
    private BytesRef readerTerm;
    private BytesRef lastTerm; // only set with asserts

    @FunctionalInterface
    interface TermsProvider {
      Terms terms(String field) throws IOException;
    }

    /**
     *
     * @param fields
     * @param sortedTerms  该field下的term 是否已经排序过了  在写入到索引文件前 term会先被排序所以该标识一般是true
     */
    TermDocsIterator(Fields fields, boolean sortedTerms) {
      this(fields::terms, sortedTerms);
    }

    TermDocsIterator(LeafReader reader, boolean sortedTerms) {
      this(reader::terms, sortedTerms);
    }

    private TermDocsIterator(TermsProvider provider, boolean sortedTerms) {
      this.sortedTerms = sortedTerms;
      this.provider = provider;
    }

    /**
     * 切换内部的 term迭代器
     * @param field
     * @throws IOException
     */
    private void setField(String field) throws IOException {
      // 切换内部的 termEnum
      if (this.field == null || this.field.equals(field) == false) {
        this.field = field;

        // 针对使用 blockTreeTermWriter的场景 返回的是  FreqProxTerms
        // 针对读取数据 以便作用于update时 使用的是 FieldReader   就是使用fst作为term词典
        Terms terms = provider.terms(field);
        if (terms != null) {
          // 针对使用 blockTreeTermWriter的场景 返回的是  FreqProxTermsEnum
          termsEnum = terms.iterator();
          // 代表当前term 已经排序过了
          if (sortedTerms) {
            assert (lastTerm = null) == null; // need to reset otherwise we fail the assertSorted below since we sort per field
            // 如果已经完成排序了 就可以先读取第一个值
            readerTerm = termsEnum.next();
          }
        } else {
          termsEnum = null;
        }
      }
    }

    /**
     * 找到该field 关联的所有doc 且field在这些doc 中携带了 term 信息
     * @param field
     * @param term  返回匹配的 term所携带的  docId迭代器  意味着该field下的该term 总计出现在这么多doc上
     * @return
     * @throws IOException
     */
    DocIdSetIterator nextTerm(String field, BytesRef term) throws IOException {
      // 这里会将 term迭代器切换成该field下的
      setField(field);
      // 代表成功生成了 term的迭代器对象
      if (termsEnum != null) {
        // 代表term 已经按照大小顺序排序过了
        if (sortedTerms) {
          assert assertSorted(term);
          // in the sorted case we can take advantage of the "seeking forward" property
          // this allows us depending on the term dict impl to reuse data-structures internally
          // which speed up iteration over terms and docs significantly.
          // 代表传入的term 比当前所有的term 都要小 也就无法找到对应的doc 返回null
          int cmp = term.compareTo(readerTerm);
          if (cmp < 0) {
            return null; // requested term does not exist in this segment
            // 代表不需要继续遍历 此时 field下遍历到的term 就是查询的 term    将相关的所有docId 包装成迭代器返回
          } else if (cmp == 0) {
            return getDocs();
          } else {
            // 这个时候就是检测 该term在 terms 中能否被找到
            TermsEnum.SeekStatus status = termsEnum.seekCeil(term);
            switch (status) {
              case FOUND:
                return getDocs();
              case NOT_FOUND:
                readerTerm = termsEnum.term();
                return null;
              case END:
                // no more terms in this segment
                termsEnum = null;
                return null;
              default:
                throw new AssertionError("unknown status");
            }
          }
          // 如果没有排序的话 精确匹配
        } else if (termsEnum.seekExact(term)) {
          return getDocs();
        }
      }
      return null;
    }

    private boolean assertSorted(BytesRef term) {
      assert sortedTerms;
      assert lastTerm == null || term.compareTo(lastTerm) >= 0 : "boom: " + term.utf8ToString() + " last: " + lastTerm.utf8ToString();
      lastTerm = BytesRef.deepCopyOf(term);
      return true;
    }

    /**
     * 仅返回 docId 迭代器时 不需要读取其他 termVector信息
     * @return
     * @throws IOException
     */
    private DocIdSetIterator getDocs() throws IOException {
      assert termsEnum != null;
      return postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
    }
  }
}
