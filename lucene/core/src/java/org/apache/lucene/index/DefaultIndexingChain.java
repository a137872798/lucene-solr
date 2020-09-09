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


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.MaxBytesLengthExceededException;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default general purpose indexing chain, which handles
 * indexing all types of fields.
 */
// 该对象会从doc 中解析相关信息并生成索引
final class DefaultIndexingChain extends DocConsumer {

    /**
     * 记录当前使用了多少byte
     */
    final Counter bytesUsed;
    /**
     * 当前正在处理的 doc 的状态  当要处理的doc发生变化时 该属性也要变化
     */
    final DocumentsWriterPerThread.DocState docState;
    /**
     * 处理该doc 的 thread 对象
     */
    final DocumentsWriterPerThread docWriter;
    /**
     * 该对象负责构建 域信息
     */
    final FieldInfos.Builder fieldInfos;

    // Writes postings and term vectors:
    final TermsHash termsHash;

    // Writes stored fields
    // 该对象负责存储域信息
    final StoredFieldsConsumer storedFieldsConsumer;

    // NOTE: I tried using Hash Map<String,PerField>
    // but it was ~2% slower on Wiki and Geonames with Java
    // 1.7.0_25:
    // 缓存了每个field 需要存储到索引文件的信息  每当processField时 通过fieldName 找到之前的perField对象 这也代表着 fieldName 是IndexWriter唯一的 (通过那个全局容器去重)
    private PerField[] fieldHash = new PerField[2];
    private int hashMask = 1;

    /**
     * 记录总计处理了多少field
     */
    private int totalFieldCount;
    /**
     * 每当处理一个新的  doc时 就会增加gen  代表某些field是同一批写入的
     */
    private long nextFieldGen;

    // Holds fields seen in each document
    // 存储当前doc 下正在处理的 field  只有首次出现的field 才会存储
    private PerField[] fields = new PerField[1];

    /**
     * 记录已经执行过 finish的容器 避免重复执行
     */
    private final Set<String> finishedDocValues = new HashSet<>();

    /**
     * 每个 负责处理 doc的writer线程 都会对应一个 chain 对象
     *
     * @param docWriter
     */
    public DefaultIndexingChain(DocumentsWriterPerThread docWriter) {
        this.docWriter = docWriter;
        this.fieldInfos = docWriter.getFieldInfosBuilder();
        // 该对象记录了此时正在处理的doc 信息 以及当前 docId
        this.docState = docWriter.docState;
        this.bytesUsed = docWriter.bytesUsed;

        final TermsHash termVectorsWriter;
        if (docWriter.getSegmentInfo().getIndexSort() == null) {
            // 该对象专门负责存储 field相关信息 (生成索引结构 并存储)
            storedFieldsConsumer = new StoredFieldsConsumer(docWriter);
            // 该对象负责写入term的向量信息
            termVectorsWriter = new TermVectorsConsumer(docWriter);
        } else {
            // 当声明了排序规则时 创建2个排序对象   在存储doc数据时 就会按照该规则进行排序
            storedFieldsConsumer = new SortingStoredFieldsConsumer(docWriter);
            termVectorsWriter = new SortingTermVectorsConsumer(docWriter);
        }
        // 该对象负责记录 term的频率信息  同时设置下游对象
        termsHash = new FreqProxTermsWriter(docWriter, termVectorsWriter);
    }

    /**
     * 在执行 flush 之前 检测是否需要对内部的数据进行排序
     *
     * @param state
     * @return
     * @throws IOException
     */
    private Sorter.DocMap maybeSortSegment(SegmentWriteState state) throws IOException {
        // 获取排序对象  如果没有设置 那么不需要排序  该属性是从 IndexConfig.indexSort 传递过来的
        Sort indexSort = state.segmentInfo.getIndexSort();
        if (indexSort == null) {
            return null;
        }

        List<Sorter.DocComparator> comparators = new ArrayList<>();
        // sort 对象本身是由多个 SortField 组成的 每个对象都会影响排序的结果
        for (int i = 0; i < indexSort.getSort().length; i++) {
            SortField sortField = indexSort.getSort()[i];
            // 首先要确保这个field在本次解析的所有doc中存在 如果不存在就没必要处理了
            PerField perField = getPerField(sortField.getField());

            if (perField != null && perField.docValuesWriter != null &&
                    // 如果该field 已经执行过finish 就不需要再处理了
                    finishedDocValues.contains(perField.fieldInfo.name) == false) {
                // 代表所有doc都已经处理完了 避免docValuesWriter中还有残留数据
                perField.docValuesWriter.finish(state.segmentInfo.maxDoc());
                // 获取排序对象 因为最终排序结果是多个comparator一起作用的结果
                Sorter.DocComparator cmp = perField.docValuesWriter.getDocComparator(state.segmentInfo.maxDoc(), sortField);
                comparators.add(cmp);
                finishedDocValues.add(perField.fieldInfo.name);
            } else {
                // safe to ignore, sort field with no values or already seen before
            }
        }
        Sorter sorter = new Sorter(indexSort);
        // returns null if the documents are already sorted
        // 根据上面获得的 com 对象进行排序
        return sorter.sort(state.segmentInfo.maxDoc(), comparators.toArray(new Sorter.DocComparator[comparators.size()]));
    }

    /**
     * 将之前解析doc生成的索引数据持久化到segment文件中
     * @param state  描述本次刷盘信息的对象
     * @return
     * @throws IOException
     */
    @Override
    public Sorter.DocMap flush(SegmentWriteState state) throws IOException {

        // NOTE: caller (DocumentsWriterPerThread) handles
        // aborting on any exception from this method
        // 这里要根据 segment的 Sort 为doc排序 不是按照解析doc的顺序 而是 sortedField定义的顺序
        Sorter.DocMap sortMap = maybeSortSegment(state);
        // 获取该段总计解析了多少doc  (这里是包含解析失败的)  解析失败的doc 会通过delCountOnFlush 展示
        int maxDoc = state.segmentInfo.maxDoc();
        long t0 = System.nanoTime();
        // 先写入标准因子
        writeNorms(state, sortMap);
        if (docState.infoStream.isEnabled("IW")) {
            docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write norms");
        }
        // 该对象是描述读取segment的bean对象
        SegmentReadState readState = new SegmentReadState(state.directory, state.segmentInfo, state.fieldInfos, IOContext.READ, state.segmentSuffix);

        t0 = System.nanoTime();
        // 将 docValue的数据写入到 索引文件中
        writeDocValues(state, sortMap);
        if (docState.infoStream.isEnabled("IW")) {
            docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write docValues");
        }

        t0 = System.nanoTime();
        // 写入point 数据
        writePoints(state, sortMap);
        if (docState.infoStream.isEnabled("IW")) {
            docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write points");
        }

        // it's possible all docs hit non-aborting exceptions...
        t0 = System.nanoTime();
        // 存储 field.value
        storedFieldsConsumer.finish(maxDoc);
        // 将所有内存中的数据刷盘到索引文件中
        storedFieldsConsumer.flush(state, sortMap);
        if (docState.infoStream.isEnabled("IW")) {
            docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to finish stored fields");
        }

        t0 = System.nanoTime();
        // 将携带 term信息的 field抽取到 hashMap中
        Map<String, TermsHashPerField> fieldsToFlush = new HashMap<>();
        for (int i = 0; i < fieldHash.length; i++) {
            PerField perField = fieldHash[i];
            while (perField != null) {
                if (perField.invertState != null) {
                    fieldsToFlush.put(perField.fieldInfo.name, perField.termsHashPerField);
                }
                perField = perField.next;
            }
        }

        // 如果有标准因子 就获取标准因子
        try (NormsProducer norms = readState.fieldInfos.hasNorms()
                ? state.segmentInfo.getCodec().normsFormat().normsProducer(readState)
                : null) {
            NormsProducer normsMergeInstance = null;
            if (norms != null) {
                // Use the merge instance in order to reuse the same IndexInput for all terms
                // 这里会创建一个副本对象
                normsMergeInstance = norms.getMergeInstance();
            }
            // 在这里已经将所有term信息写入到索引文件中了   利用跳跃表做了 doc的索引结构  利用fst做了 term的词典
            termsHash.flush(fieldsToFlush, state, sortMap, normsMergeInstance);
        }
        if (docState.infoStream.isEnabled("IW")) {
            docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write postings and finish vectors");
        }

        // Important to save after asking consumer to flush so
        // consumer can alter the FieldInfo* if necessary.  EG,
        // FreqProxTermsWriter does this with
        // FieldInfo.storePayload.
        t0 = System.nanoTime();
        // 这里只是简单的将 fileInfo的各个信息写入到索引文件
        docWriter.codec.fieldInfosFormat().write(state.directory, state.segmentInfo, "", state.fieldInfos, IOContext.DEFAULT);
        if (docState.infoStream.isEnabled("IW")) {
            docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write fieldInfos");
        }

        return sortMap;
    }

    /**
     * Writes all buffered points.
     * 写入 points 信息
     */
    private void writePoints(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
        PointsWriter pointsWriter = null;
        boolean success = false;
        // 套路一样 先找到所有的perField对象 然后通过format对象将数据写入到索引文件中
        try {
            for (int i = 0; i < fieldHash.length; i++) {
                PerField perField = fieldHash[i];
                while (perField != null) {
                    if (perField.pointValuesWriter != null) {
                        if (perField.fieldInfo.getPointDimensionCount() == 0) {
                            // BUG
                            throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has no points but wrote them");
                        }
                        if (pointsWriter == null) {
                            // lazy init
                            PointsFormat fmt = state.segmentInfo.getCodec().pointsFormat();
                            if (fmt == null) {
                                throw new IllegalStateException("field=\"" + perField.fieldInfo.name + "\" was indexed as points but codec does not support points");
                            }
                            pointsWriter = fmt.fieldsWriter(state);
                        }

                        perField.pointValuesWriter.flush(state, sortMap, pointsWriter);
                        perField.pointValuesWriter = null;
                    } else if (perField.fieldInfo.getPointDimensionCount() != 0) {
                        // BUG
                        throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has points but did not write them");
                    }
                    perField = perField.next;
                }
            }
            if (pointsWriter != null) {
                pointsWriter.finish();
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(pointsWriter);
            } else {
                IOUtils.closeWhileHandlingException(pointsWriter);
            }
        }
    }

    /**
     * Writes all buffered doc values (called from {@link #flush}).
     */
    // 将docValue写入到索引文件中
    private void writeDocValues(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
        int maxDoc = state.segmentInfo.maxDoc();
        DocValuesConsumer dvConsumer = null;
        boolean success = false;
        try {
            // 获取之前解析doc时暂存在内存中的 PerField对象
            for (int i = 0; i < fieldHash.length; i++) {
                PerField perField = fieldHash[i];
                while (perField != null) {
                    // 只要 writer被初始化 那么 docValuesType 一定不是 NONE 详见 解析doc时的逻辑
                    if (perField.docValuesWriter != null) {
                        // 该属性是要提前设置的
                        if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
                            // BUG
                            throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has no docValues but wrote them");
                        }
                        if (dvConsumer == null) {
                            // lazy init
                            // 初始化 写入docValue的对象
                            DocValuesFormat fmt = state.segmentInfo.getCodec().docValuesFormat();
                            dvConsumer = fmt.fieldsConsumer(state);
                        }

                        // 代表还没有执行过finish
                        if (finishedDocValues.contains(perField.fieldInfo.name) == false) {
                            perField.docValuesWriter.finish(maxDoc);
                        }
                        perField.docValuesWriter.flush(state, sortMap, dvConsumer);
                        perField.docValuesWriter = null;
                    } else if (perField.fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                        // BUG
                        throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has docValues but did not write them");
                    }
                    perField = perField.next;
                }
            }

            // TODO: catch missing DV fields here?  else we have
            // null/"" depending on how docs landed in segments?
            // but we can't detect all cases, and we should leave
            // this behavior undefined. dv is not "schemaless": it's column-stride.
            success = true;
        } finally {
            if (success) {
                IOUtils.close(dvConsumer);
            } else {
                IOUtils.closeWhileHandlingException(dvConsumer);
            }
        }

        if (state.fieldInfos.hasDocValues() == false) {
            if (dvConsumer != null) {
                // BUG
                throw new AssertionError("segment=" + state.segmentInfo + ": fieldInfos has no docValues but wrote them");
            }
        } else if (dvConsumer == null) {
            // BUG
            throw new AssertionError("segment=" + state.segmentInfo + ": fieldInfos has docValues but did not wrote them");
        }
    }

    /**
     * 将标准因子信息持久化
     *
     * @param state
     * @param sortMap
     * @throws IOException
     */
    private void writeNorms(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
        boolean success = false;
        NormsConsumer normsConsumer = null;
        try {
            // 只要本次涉及到的所有field 有至少一个写入了标准因子  就要将数据持久化到文件中
            if (state.fieldInfos.hasNorms()) {
                // 当存在标准因子时  获取当前版本支持的 标准因子格式   默认使用的 Codec 就是 Lucene84Codec
                NormsFormat normsFormat = state.segmentInfo.getCodec().normsFormat();
                assert normsFormat != null;
                // 调用该方法 会间接的为segment 创建有关标准因子的 数据索引文件 和 元数据索引文件
                normsConsumer = normsFormat.normsConsumer(state);

                for (FieldInfo fi : state.fieldInfos) {
                    // 从hash桶中找到 对应的域信息   这个是之前处理doc的时候写进去的  之前解析doc数据时 对应的 标准因子信息 value信息 termVector信息等都缓存在 PerField对象中
                    PerField perField = getPerField(fi.name);
                    assert perField != null;

                    // we must check the final value of omitNorms for the fieldinfo: it could have
                    // changed for this field since the first time we added it.
                    // 代表该field存储了标准因子 且存储了词向量信息
                    if (fi.omitsNorms() == false && fi.getIndexOptions() != IndexOptions.NONE) {
                        assert perField.norms != null : "field=" + fi.name;
                        // 当前版本 finish是NOOP
                        perField.norms.finish(state.segmentInfo.maxDoc());
                        // 将信息写入到 索引文件中
                        perField.norms.flush(state, sortMap, normsConsumer);
                    }
                }
            }
            success = true;
        } finally {
            // 写入完成 正常关闭索引文件
            if (success) {
                IOUtils.close(normsConsumer);
            } else {
                IOUtils.closeWhileHandlingException(normsConsumer);
            }
        }
    }

    /**
     * 当该对象关联的 perThread 被废弃时 触发该方法 关闭and删除所有之前写入的索引文件
     *
     * @throws IOException
     */
    @Override
    @SuppressWarnings("try")
    public void abort() throws IOException {
        // finalizer will e.g. close any open files in the term vectors writer:
        try (Closeable finalizer = termsHash::abort) {
            // 删除临时文件并关闭句柄  (关闭失败时抛出异常)
            storedFieldsConsumer.abort();
        } finally {
            // 将此时解析doc 并存储在fieldHash的数据全部释放
            Arrays.fill(fieldHash, null);
        }
    }

    /**
     * hash桶扩容
     */
    private void rehash() {
        int newHashSize = (fieldHash.length * 2);
        assert newHashSize > fieldHash.length;

        PerField newHashArray[] = new PerField[newHashSize];

        // Rehash
        int newHashMask = newHashSize - 1;
        for (int j = 0; j < fieldHash.length; j++) {
            PerField fp0 = fieldHash[j];
            while (fp0 != null) {
                final int hashPos2 = fp0.fieldInfo.name.hashCode() & newHashMask;
                PerField nextFP0 = fp0.next;
                fp0.next = newHashArray[hashPos2];
                newHashArray[hashPos2] = fp0;
                fp0 = nextFP0;
            }
        }

        fieldHash = newHashArray;
        hashMask = newHashMask;
    }

    /**
     * Calls StoredFieldsWriter.startDocument, aborting the
     * segment if it hits any exception.
     */
    // 开始存储某个doc下所有的field
    private void startStoredFields(int docID) throws IOException {
        try {
            // 做writer的初始化工作  并且发现 lastDocId 落后于 docId时 填充一些空的doc
            storedFieldsConsumer.startDocument(docID);
        } catch (Throwable th) {
            docWriter.onAbortingException(th);
            throw th;
        }
    }

    /**
     * Calls StoredFieldsWriter.finishDocument, aborting the
     * segment if it hits any exception.
     * 这里就是更新 StoredFieldWriter 内部的属性 以便处理新的doc   如果此时缓存在内存中的数据已经比较多了 那么会被动的触发刷盘
     */
    private void finishStoredFields() throws IOException {
        try {
            storedFieldsConsumer.finishDocument();
        } catch (Throwable th) {
            docWriter.onAbortingException(th);
            throw th;
        }
    }

    /**
     * 处理当前读取到的 doc  chain会拆解doc上的信息 并根据这些数据生成相关的索引
     *
     * @throws IOException
     */
    @Override
    public void processDocument() throws IOException {

        // How many indexed field names we've seen (collapses
        // multiple field instances by the same name):
        // 记录的是新增的 field 之前已经出现过的field 在 processField方法后返回的是原值
        int fieldCount = 0;

        // 默认情况下 该值总是与 docId一致 标明了某个field是在处理哪个doc时存储的
        long fieldGen = nextFieldGen++;

        // NOTE: we need two passes here, in case there are
        // multi-valued fields, because we must process all
        // instances of a given field at once, since the
        // analyzer is free to reuse TokenStream across fields
        // (i.e., we cannot have more than one TokenStream
        // running "at once"):
        //
        // 在 TermVectorsConsumer中 会清除上一个doc时产生的field信息
        termsHash.startDocument();

        // 让 StoredFieldsConsumer 做一些准备工作
        startStoredFields(docState.docID);
        try {
            // 遍历该文档下所有的 field
            for (IndexableField field : docState.doc) {
                fieldCount = processField(field, fieldGen, fieldCount);
            }
        } finally {
            // 代表正常执行的情况 触发finish 方法
            if (docWriter.hasHitAbortingException() == false) {
                // Finish each indexed field name seen in the document:
                // 只要在该doc中出现过的field 都会设置到 fields[] 中
                for (int i = 0; i < fieldCount; i++) {
                    fields[i].finish();
                }
                // 这里代表所有field的数据都写入完成了  注意只是 field.value 相关的信息  不涉及(term offset/position/freq等)
                // 做一些收尾动作 但是不是强制刷盘 也就是数据还是留存在内存中
                finishStoredFields();
            }
        }

        try {
            // 将bytePool中的数据转移到 writer中 并释放之前bytePool的数据 此时不一定会刷盘
            termsHash.finishDocument();
        } catch (Throwable th) {
            // Must abort, on the possibility that on-disk term
            // vectors are now corrupt:
            docWriter.onAbortingException(th);
            throw th;
        }
    }

    /**
     * 在 processDocument中 会遍历doc下所有field 并进行处理
     *
     * @param field      因为field实现了 IndexableField接口 会记录field上哪些信息需要存储到索引文件
     * @param fieldGen   代表在docId为多少的doc时被处理
     * @param fieldCount 这个count是指处理某个doc时 总计遇到了几个field  去重
     * @return
     * @throws IOException
     */
    private int processField(IndexableField field, long fieldGen, int fieldCount) throws IOException {
        String fieldName = field.name();
        // 这里描述了 该field 需要存储哪些信息
        IndexableFieldType fieldType = field.fieldType();

        PerField fp = null;

        if (fieldType.indexOptions() == null) {
            throw new NullPointerException("IndexOptions must not be null (field: \"" + field.name() + "\")");
        }

        // 实际上只看到了从 NONE 到 != NONE 时    对fieldPer的修改     而如果本次设置的是NONE 没有覆盖的入口

        // Invert indexed fields:
        // 代表field的某些信息需要写入到索引文件中
        if (fieldType.indexOptions() != IndexOptions.NONE) {
            // 将内部信息抽取出来生成 perField 对象
            // 当 IndexOptions 不为 NONE 的时候 才会创建invertState
            fp = getOrAddField(fieldName, fieldType, true);
            // field每次出现在不同的 doc中  fieldGen 就会变化
            boolean first = fp.fieldGen != fieldGen;
            // 将field.value的数据 经过分词器处理后 将term的偏移量 位置 频率等信息 写入到bytePool中
            fp.invert(field, first);

            // 代表在当前doc中首次处理该field
            if (first) {
                fields[fieldCount++] = fp;
                fp.fieldGen = fieldGen;
            }
        } else {
            // 这里只是做了一致性校验  当IndexOptional为NONE 时  很多FieldType.storeXXX属性应该为false
            verifyUnIndexedFieldType(fieldName, fieldType);
        }

        // 此时已经将term的相关信息存储到 PerField.bytePool中了

        // Add stored fields:
        // 这里代表需要存储 field本身的值
        if (fieldType.stored()) {
            if (fp == null) {
                // 基于存储 .value本身的情况 是不需要生成 invertState的
                fp = getOrAddField(fieldName, fieldType, false);
            }
            if (fieldType.stored()) {
                String value = field.stringValue();
                if (value != null && value.length() > IndexWriter.MAX_STORED_STRING_LENGTH) {
                    throw new IllegalArgumentException("stored field \"" + field.name() + "\" is too large (" + value.length() + " characters) to store");
                }
                try {
                    // 这里主要就是写入 field的 num/valueType/value
                    storedFieldsConsumer.writeField(fp.fieldInfo, field);
                } catch (Throwable th) {
                    docWriter.onAbortingException(th);
                    throw th;
                }
            }
        }

        // 该值规定了 field.value 在所有doc下类型都相同  如果没有指定的情况 那么同一fieldName 的field 在不同的doc下 它的数据可以不一致
        DocValuesType dvType = fieldType.docValuesType();
        if (dvType == null) {
            throw new NullPointerException("docValuesType must not be null (field: \"" + fieldName + "\")");
        }


        // 注意 没有指定类型时是不需要存储 docValue的
        if (dvType != DocValuesType.NONE) {
            if (fp == null) {
                // 如果只是存储field信息 那么是不需要存储 invert数据的  同时 如果需要存储term的情况  那么在上面的逻辑中肯定已经完成了对 fp的创建
                fp = getOrAddField(fieldName, fieldType, false);
            }
            // 按照 docValue 的类型 生成不同的 DocValueWriter对象 并将数据写入到容器中
            indexDocValue(fp, dvType, field);
        }
        // 如果存在维度信息  现在简单的将多维度信息理解成 type.value 可以拆解成多个值 每个值对应一个维度
        if (fieldType.pointDimensionCount() != 0) {
            if (fp == null) {
                fp = getOrAddField(fieldName, fieldType, false);
            }
            // 存储维度信息
            indexPoint(fp, field);
        }

        return fieldCount;
    }

    private static void verifyUnIndexedFieldType(String name, IndexableFieldType ft) {
        if (ft.storeTermVectors()) {
            throw new IllegalArgumentException("cannot store term vectors "
                    + "for a field that is not indexed (field=\"" + name + "\")");
        }
        if (ft.storeTermVectorPositions()) {
            throw new IllegalArgumentException("cannot store term vector positions "
                    + "for a field that is not indexed (field=\"" + name + "\")");
        }
        if (ft.storeTermVectorOffsets()) {
            throw new IllegalArgumentException("cannot store term vector offsets "
                    + "for a field that is not indexed (field=\"" + name + "\")");
        }
        if (ft.storeTermVectorPayloads()) {
            throw new IllegalArgumentException("cannot store term vector payloads "
                    + "for a field that is not indexed (field=\"" + name + "\")");
        }
    }

    /**
     * Called from processDocument to index one field's point
     * 存储某个field 的point信息
     */
    private void indexPoint(PerField fp, IndexableField field) throws IOException {
        // 维度数 和会被索引的维度数
        int pointDimensionCount = field.fieldType().pointDimensionCount();
        int pointIndexDimensionCount = field.fieldType().pointIndexDimensionCount();

        // 每个维度 的数据占用多少byte
        int dimensionNumBytes = field.fieldType().pointNumBytes();

        // Record dimensions for this field; this setter will throw IllegalArgExc if
        // the dimensions were already set to something different:
        // 如果之前维度数为0 会尝试更新
        if (fp.fieldInfo.getPointDimensionCount() == 0) {
            fieldInfos.globalFieldNumbers.setDimensions(fp.fieldInfo.number, fp.fieldInfo.name, pointDimensionCount, pointIndexDimensionCount, dimensionNumBytes);
        }

        fp.fieldInfo.setPointDimensions(pointDimensionCount, pointIndexDimensionCount, dimensionNumBytes);

        // 这里才是写入动作
        if (fp.pointValuesWriter == null) {
            fp.pointValuesWriter = new PointValuesWriter(docWriter, fp.fieldInfo);
        }
        // 写入 多维度信息  啥意思 实际上写入的还是 field.value
        fp.pointValuesWriter.addPackedValue(docState.docID, field.binaryValue());
    }

    private void validateIndexSortDVType(Sort indexSort, String fieldName, DocValuesType dvType) {
        for (SortField sortField : indexSort.getSort()) {
            if (sortField.getField().equals(fieldName)) {
                switch (dvType) {
                    case NUMERIC:
                        if (sortField.getType().equals(SortField.Type.INT) == false &&
                                sortField.getType().equals(SortField.Type.LONG) == false &&
                                sortField.getType().equals(SortField.Type.FLOAT) == false &&
                                sortField.getType().equals(SortField.Type.DOUBLE) == false) {
                            throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);
                        }
                        break;

                    case BINARY:
                        throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);

                    case SORTED:
                        if (sortField.getType().equals(SortField.Type.STRING) == false) {
                            throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);
                        }
                        break;

                    case SORTED_NUMERIC:
                        if (sortField instanceof SortedNumericSortField == false) {
                            throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);
                        }
                        break;

                    case SORTED_SET:
                        if (sortField instanceof SortedSetSortField == false) {
                            throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);
                        }
                        break;

                    default:
                        throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);
                }
                break;
            }
        }
    }

    /**
     * Called from processDocument to index one field's doc value
     *
     * @param fp     以 PerThread为单位 存储field信息 (包括下面的 term信息)
     * @param dvType 描述docValue的类型信息
     *               docValue 代表某一field在所有doc下 的value都是同一类型
     * @param field  描述field的信息
     */
    private void indexDocValue(PerField fp, DocValuesType dvType, IndexableField field) throws IOException {

        // 如果原本没有 docValue信息
        if (fp.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
            // This is the first time we are seeing this field indexed with doc values, so we
            // now record the DV type so that any future attempt to (illegally) change
            // the DV type of this field, will throw an IllegalArgExc:
            // 检测排序类型是否合法
            if (docWriter.getSegmentInfo().getIndexSort() != null) {
                final Sort indexSort = docWriter.getSegmentInfo().getIndexSort();
                validateIndexSortDVType(indexSort, fp.fieldInfo.name, dvType);
            }
            // 为field在全局范围内设置 docValue
            fieldInfos.globalFieldNumbers.setDocValuesType(fp.fieldInfo.number, fp.fieldInfo.name, dvType);

        }
        // 更新 perThread范围下 field的 docValueType
        fp.fieldInfo.setDocValuesType(dvType);

        int docID = docState.docID;

        switch (dvType) {

            case NUMERIC:
                if (fp.docValuesWriter == null) {
                    fp.docValuesWriter = new NumericDocValuesWriter(fp.fieldInfo, bytesUsed);
                }
                if (field.numericValue() == null) {
                    throw new IllegalArgumentException("field=\"" + fp.fieldInfo.name + "\": null value not allowed");
                }
                // 将 field.value 直接写入
                ((NumericDocValuesWriter) fp.docValuesWriter).addValue(docID, field.numericValue().longValue());
                break;

                // 该种类型不支持排序
            case BINARY:
                if (fp.docValuesWriter == null) {
                    fp.docValuesWriter = new BinaryDocValuesWriter(fp.fieldInfo, bytesUsed);
                }
                ((BinaryDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
                break;

                // 基于 field.value (此时value为 byteRef类型)为doc进行排序
            case SORTED:
                if (fp.docValuesWriter == null) {
                    fp.docValuesWriter = new SortedDocValuesWriter(fp.fieldInfo, bytesUsed);
                }
                ((SortedDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
                break;
            // 基于field.value 为doc进行排序 此时value为数字类型
            // 该对象支持往一个doc中同时插入多个相同的field 这样在doc之间的排序是基于这些field下的 MIN/MAX
            case SORTED_NUMERIC:
                if (fp.docValuesWriter == null) {
                    fp.docValuesWriter = new SortedNumericDocValuesWriter(fp.fieldInfo, bytesUsed);
                }
                ((SortedNumericDocValuesWriter) fp.docValuesWriter).addValue(docID, field.numericValue().longValue());
                break;

            // 与SORTED_NUMERIC类似 也就是支持同一个doc下存储多个相同field (他们的value可以相同也可以不相同
            // 这时排序就是基于selector选出该field在该doc下的代表值 在代表值经过比较后 将doc进行重排序)
            case SORTED_SET:
                if (fp.docValuesWriter == null) {
                    fp.docValuesWriter = new SortedSetDocValuesWriter(fp.fieldInfo, bytesUsed);
                }
                ((SortedSetDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
                break;

            default:
                throw new AssertionError("unrecognized DocValues.Type: " + dvType);
        }
    }

    /**
     * Returns a previously created {@link PerField}, or null
     * if this field name wasn't seen yet.
     */
    private PerField getPerField(String name) {
        // 在hash桶中 找到存储数据的字段
        final int hashPos = name.hashCode() & hashMask;
        // 看来是拉链法解决 hash冲突
        PerField fp = fieldHash[hashPos];
        while (fp != null && !fp.fieldInfo.name.equals(name)) {
            fp = fp.next;
        }
        return fp;
    }

    /**
     * Returns a previously created {@link PerField},
     * absorbing the type information from {@link FieldType},
     * and creates a new {@link PerField} if this field name
     * wasn't seen yet.
     *
     * @param invert 是否需要反转
     *               通过记录了 该field下需要存储哪些信息 生成 PerField对象
     */
    private PerField getOrAddField(String name, IndexableFieldType fieldType, boolean invert) {

        // Make sure we have a PerField allocated
        final int hashPos = name.hashCode() & hashMask;
        // 先通过hash值检测是否已经为该field 生成过PerField信息了 也就是默认 fieldName 作为了field的唯一标识 不允许重复
        PerField fp = fieldHash[hashPos];
        // 通过 拉链法解决hash冲突
        while (fp != null && !fp.fieldInfo.name.equals(name)) {
            fp = fp.next;
        }

        // 代表之前并不存在该field 对应的 PerField 对象
        if (fp == null) {
            // First time we are seeing this field in this segment

            // 如果该fieldName 对应的field已经存在 选择复用 否则创建一个新的field 对象
            // doc是 IndexableField的集合 那时候并没有生成 FieldInfo对象
            // 在getOrAdd方法上加了并发控制
            FieldInfo fi = fieldInfos.getOrAdd(name);
            // 上面创建的FieldInfo 还是一个空对象  将需要存储到索引文件中的信息填充到 fieldInfo上  也就是除了 field需要被索引的选项能提前被确定外 其他信息都是不确定的
            initIndexOptions(fi, fieldType.indexOptions());
            // 如果fieldType 上设置了用户自定义的属性 会转移到 fieldInfo.attribute
            Map<String, String> attributes = fieldType.getAttributes();
            if (attributes != null) {
                attributes.forEach((k, v) -> fi.putAttribute(k, v));
            }

            // 根据索引版本号,fieldInfo 创建 perField 对象
            fp = new PerField(docWriter.getIndexCreatedVersionMajor(), fi, invert);
            // 存储到hash桶中 这样 当处理下一批doc时 发现了相同的 field 就可以共用了
            fp.next = fieldHash[hashPos];
            fieldHash[hashPos] = fp;
            totalFieldCount++;

            // At most 50% load factor:
            // 当存储的field 量比较多了 就对hash桶进行扩容
            if (totalFieldCount >= fieldHash.length / 2) {
                rehash();
            }

            // 预先对数组扩容 确保之后能正常插入 field   这里只所以不直接设置到 fields的原因是 需要在外面检测该field是否之前已经存储过了
            if (totalFieldCount > fields.length) {
                PerField[] newFields = new PerField[ArrayUtil.oversize(totalFieldCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
                System.arraycopy(fields, 0, newFields, 0, fields.length);
                fields = newFields;
            }

            // 代表在hash结构中已经存在该field的信息   此时发现invert为 true 且之前的fieldInfo中并没有设置反向信息
            // 那么根据此时fieldInfo内的反向索引信息  初始化NormWriter,perFieldWriter,Invert
        } else if (invert && fp.invertState == null) {
            initIndexOptions(fp.fieldInfo, fieldType.indexOptions());
            fp.setInvertState();
        }

        return fp;
    }

    /**
     * 填充field 索引相关的信息
     *
     * @param info
     * @param indexOptions
     */
    private void initIndexOptions(FieldInfo info, IndexOptions indexOptions) {
        // Messy: must set this here because e.g. FreqProxTermsWriterPerField looks at the initial
        // IndexOptions to decide what arrays it must create).
        assert info.getIndexOptions() == IndexOptions.NONE;
        // This is the first time we are seeing this field indexed, so we now
        // record the index options so that any future attempt to (illegally)
        // change the index options of this field, will throw an IllegalArgExc:
        // 覆盖之前的   indexOptions
        fieldInfos.globalFieldNumbers.setIndexOptions(info.number, info.name, indexOptions);
        info.setIndexOptions(indexOptions);
    }

    /**
     * 该对象内部封装了 写入域信息的逻辑
     */
    private final class PerField implements Comparable<PerField> {

        /**
         * 为域创建索引文件的版本  主要用于判断索引文件是否兼容
         */
        final int indexCreatedVersionMajor;
        /**
         * 该对象描述了这个域的信息
         */
        final FieldInfo fieldInfo;
        /**
         * 该对象负责打分
         */
        final Similarity similarity;

        /**
         * 这个对象内部除了 域的常规属性后 还有一组 attr 对象
         */
        FieldInvertState invertState;
        /**
         * 该对象负责存储field下面的term信息
         */
        TermsHashPerField termsHashPerField;

        // Non-null if this field ever had doc values in this
        // segment:
        // 该对象写入 docValule 信息
        DocValuesWriter docValuesWriter;

        // Non-null if this field ever had points in this segment:
        // 写入点数据 点数据可能就是那种 有多个维度的
        PointValuesWriter pointValuesWriter;

        /**
         * We use this to know when a PerField is seen for the
         * first time in the current document.
         * 每当处理一个新的 doc 时会产生一个新的 fieldGen 然后如果此时刚好处理到这个field 就将gen更新成与该doc相关的
         */
        long fieldGen = -1;

        // Used by the hash table
        // 通过拉链法解决hash冲突 因为该对象主要是存放在 hash数组中
        PerField next;

        // Lazy init'd:
        // 写入标准因子
        NormValuesWriter norms;

        // reused  该对象在处理 fieldName相同的 field时会被共用 减少GC的发生
        TokenStream tokenStream;

        /**
         * @param indexCreatedVersionMajor 索引的主版本号
         * @param fieldInfo                描述该field的信息 相同name的field 会共用内部的属性
         * @param invert                   是否存储反转信息
         */
        public PerField(int indexCreatedVersionMajor, FieldInfo fieldInfo, boolean invert) {
            this.indexCreatedVersionMajor = indexCreatedVersionMajor;
            this.fieldInfo = fieldInfo;
            // TODO 该属性是什么时候设置的  在 perThread.updateDocument 中没有设置该属性
            similarity = docState.similarity;
            // 如果需要反转的话 存储反转信息   这个反转信息的含义 好像是通过 docId 反向查询field???
            if (invert) {
                setInvertState();
            }
        }

        /**
         * 初始化 反转信息对象
         */
        void setInvertState() {
            invertState = new FieldInvertState(indexCreatedVersionMajor, fieldInfo.name, fieldInfo.getIndexOptions());
            // 生成一个 基于 field抽取term信息的对象
            termsHashPerField = termsHash.addField(invertState, fieldInfo);
            // 如果域信息存在 标准因子 那么需要初始化一个标准因子 writer   一开始通过 fieldInfos.getOrAdd 方法返回的对象 该属性是false
            if (fieldInfo.omitsNorms() == false) {
                assert norms == null;
                // Even if no documents actually succeed in setting a norm, we still write norms for this segment:
                norms = new NormValuesWriter(fieldInfo, docState.docWriter.bytesUsed);
            }
        }

        @Override
        public int compareTo(PerField other) {
            return this.fieldInfo.name.compareTo(other.fieldInfo.name);
        }

        /**
         * @throws IOException
         */
        public void finish() throws IOException {
            // 只要没有忽略标准因子 就要进行打分  打分的逻辑是委托给 Similarity
            if (fieldInfo.omitsNorms() == false) {
                long normValue;
                // 如果没有任何 term信息存储 无法打分 使用默认值0
                if (invertState.length == 0) {
                    // the field exists in this document, but it did not have
                    // any indexed tokens, so we assign a default value of zero
                    // to the norm
                    normValue = 0;
                } else {
                    // TODO 具体怎么打分的逻辑可以先忽略  看到有基于field打分的模板
                    normValue = similarity.computeNorm(invertState);
                    if (normValue == 0) {
                        throw new IllegalStateException("Similarity " + similarity + " return 0 for non-empty field");
                    }
                }
                // 存储该field在每个doc下的得分
                norms.addValue(docState.docID, normValue);
            }

            // 将 PerField设置到 termsHash的field数组中
            termsHashPerField.finish();
        }

        /**
         * Inverts one field for one document; first is true
         * if this is the first time we are seeing this field
         * name in this document.
         * <p>
         * 解析field的信息 并存储到 invertState 中
         *
         * @param first 代表该field 在当前doc中首次出现
         */
        public void invert(IndexableField field, boolean first) throws IOException {
            if (first) {
                // First time we're seeing this field (indexed) in
                // this document:
                // field 的信息以一个doc为单位 会选择复用 每当切换到一个新的doc时 之前存储的invert信息都要清除
                invertState.reset();
            }

            IndexableFieldType fieldType = field.fieldType();

            IndexOptions indexOptions = fieldType.indexOptions();
            // 更新索引存储选项
            fieldInfo.setIndexOptions(indexOptions);

            // 当设置了 忽略标准因子时 修改状态  当PerField对象首次被创建时 一般都会先创建 NormWriter 对象
            if (fieldType.omitNorms()) {
                fieldInfo.setOmitsNorms();
            }

            // tokenized() 标识 该field相关的value信息是否应当被分词器处理
            final boolean analyzed = fieldType.tokenized() && docState.analyzer != null;

            /*
             * To assist people in tracking down problems in analysis components, we wish to write the field name to the infostream
             * when we fail. We expect some caller to eventually deal with the real exception, so we don't want any 'catch' clauses,
             * but rather a finally that takes note of the problem.
             * 确定本次处理是否成功
             */
            boolean succeededInProcessingField = false;
            // 当首次调用时 tokenStream还未初始化 只有当tokenStream 是 StringTokenStream/ByteRefTokenStream 类型才会尝试复用
            // 一般就是返回 StandardTokenizer
            try (TokenStream stream = tokenStream = field.tokenStream(docState.analyzer, tokenStream)) {
                // reset the TokenStream to the first token
                // 重置内部的 StandardTokenizerImpl 开始解析一个新的数据流
                stream.reset();
                invertState.setAttributeSource(stream);
                // 设置 perField内部的 attr属性
                termsHashPerField.start(field, first);

                // 解析field.value 抽取出来的token就叫做term  每当解析到一个token时 会更新内部的 positionIncreaseAttr  offsetAttr termAttr 等属性
                while (stream.incrementToken()) {

                    // If we hit an exception in stream.next below
                    // (which is fairly common, e.g. if analyzer
                    // chokes on a given document), then it's
                    // non-aborting and (above) this one document
                    // will be marked as deleted, but still
                    // consume a docID

                    // 获取位置的增量信息  position 相当于是token的逻辑偏移量 就像是存储到kafka的每条批消息都有自己的逻辑偏移量  而且这里只存储增量值 相当于使用更小的内存
                    int posIncr = invertState.posIncrAttribute.getPositionIncrement();
                    // 通过attr 修改state 内部的值
                    invertState.position += posIncr;
                    // 异常情况
                    if (invertState.position < invertState.lastPosition) {
                        if (posIncr == 0) {
                            throw new IllegalArgumentException("first position increment must be > 0 (got 0) for field '" + field.name() + "'");
                        } else if (posIncr < 0) {
                            throw new IllegalArgumentException("position increment must be >= 0 (got " + posIncr + ") for field '" + field.name() + "'");
                        } else {
                            throw new IllegalArgumentException("position overflowed Integer.MAX_VALUE (got posIncr=" + posIncr + " lastPosition=" + invertState.lastPosition + " position=" + invertState.position + ") for field '" + field.name() + "'");
                        }
                        // 代表该field下解析出来的 token数量超过一开始的限定值  抛出异常
                    } else if (invertState.position > IndexWriter.MAX_POSITION) {
                        throw new IllegalArgumentException("position " + invertState.position + " is too large for field '" + field.name() + "': max allowed position is " + IndexWriter.MAX_POSITION);
                    }
                    invertState.lastPosition = invertState.position;
                    if (posIncr == 0) {
                        invertState.numOverlap++;
                    }

                    // 本次token 在整个reader流中的起始位置和终止位置
                    int startOffset = invertState.offset + invertState.offsetAttribute.startOffset();
                    int endOffset = invertState.offset + invertState.offsetAttribute.endOffset();
                    if (startOffset < invertState.lastStartOffset || endOffset < startOffset) {
                        throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, and offsets must not go backwards "
                                + "startOffset=" + startOffset + ",endOffset=" + endOffset + ",lastStartOffset=" + invertState.lastStartOffset + " for field '" + field.name() + "'");
                    }
                    // 校验完毕后更新 lastStartOffset
                    invertState.lastStartOffset = startOffset;

                    try {
                        // 在StandardAnalyzer中 并没有看到抽取 termFreq的逻辑   默认情况下getTermFrequency() 就是1  也就是length每次累加1
                        invertState.length = Math.addExact(invertState.length, invertState.termFreqAttribute.getTermFrequency());
                    } catch (ArithmeticException ae) {
                        throw new IllegalArgumentException("too many tokens for field \"" + field.name() + "\"");
                    }

                    //System.out.println("  term=" + invertState.termAttribute);

                    // If we hit an exception in here, we abort
                    // all buffered documents since the last
                    // flush, on the likelihood that the
                    // internal state of the terms hash is now
                    // corrupt and should not be flushed to a
                    // new segment:
                    try {
                        // 将term的位置信息 频率信息等抽取出来 写入到 bytePool中 某些属性则会更新到 FieldInvertState
                        termsHashPerField.add();
                    } catch (MaxBytesLengthExceededException e) {
                        byte[] prefix = new byte[30];
                        BytesRef bigTerm = invertState.termAttribute.getBytesRef();
                        System.arraycopy(bigTerm.bytes, bigTerm.offset, prefix, 0, 30);
                        String msg = "Document contains at least one immense term in field=\"" + fieldInfo.name + "\" (whose UTF8 encoding is longer than the max length " + DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8 + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '" + Arrays.toString(prefix) + "...', original message: " + e.getMessage();
                        if (docState.infoStream.isEnabled("IW")) {
                            docState.infoStream.message("IW", "ERROR: " + msg);
                        }
                        // Document will be deleted above:
                        throw new IllegalArgumentException(msg, e);
                    } catch (Throwable th) {
                        docWriter.onAbortingException(th);
                        throw th;
                    }
                }

                // trigger streams to perform end-of-stream operations
                // 代表某个field下所有term的信息都已经写入  此时将pos 和 offset更新成最终值
                stream.end();

                // TODO: maybe add some safety? then again, it's already checked
                // when we come back around to the field...
                // 更新 field在当前doc的 pos和offset 当下次解析到相同的field时 就会将当前偏移量作为基础值    形成逻辑上的连续存储
                invertState.position += invertState.posIncrAttribute.getPositionIncrement();
                invertState.offset += invertState.offsetAttribute.endOffset();

                /* if there is an exception coming through, we won't set this to true here:*/
                // 标记处理成功
                succeededInProcessingField = true;
            } finally {
                if (!succeededInProcessingField && docState.infoStream.isEnabled("DW")) {
                    docState.infoStream.message("DW", "An exception was thrown while processing field " + fieldInfo.name);
                }
            }

            // 这里感觉像是在做矫正   但是标准分词器返回的都是默认值
            if (analyzed) {
                invertState.position += docState.analyzer.getPositionIncrementGap(fieldInfo.name);
                invertState.offset += docState.analyzer.getOffsetGap(fieldInfo.name);
            }
        }
    }

    /**
     * 一般是查询软删除的field关联的doc时使用
     * @param field
     * @return
     */
    @Override
    DocIdSetIterator getHasDocValues(String field) {
        PerField perField = getPerField(field);
        if (perField != null) {
            if (perField.docValuesWriter != null) {
                if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
                    return null;
                }

                return perField.docValuesWriter.getDocIdSet();
            }
        }
        return null;
    }
}
