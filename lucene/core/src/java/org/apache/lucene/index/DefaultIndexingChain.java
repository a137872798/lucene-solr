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


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    // hash桶 当内部 totalFieldCount 长度达到1/2时 进行扩容  多个fieldInfo 以链表形式连接
    private PerField[] fieldHash = new PerField[2];
    private int hashMask = 1;

    /**
     * 应该是记录 总计处理了多少 field 注意只记录 fieldName 不同的field   (相同的name 看作一个field)
     */
    private int totalFieldCount;
    /**
     * 每当处理一个新的  doc时 就会增加年代
     */
    private long nextFieldGen;

    // Holds fields seen in each document
    // 只包含了当前正在处理的doc下所有的field
    // 每当某个文档新添加了一个 field 设置到数组中 该数组会自动扩容
    private PerField[] fields = new PerField[1];

    /**
     * 当某个 field 对应的docValue 已经全部写入到 docValueWriter 并触发了 finish后 才会按照 fieldName 填入到这个set
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
        // 当写入一组 doc 时 每次docState都会更新 同时会通过该对象挨个写入doc
        this.docState = docWriter.docState;
        this.bytesUsed = docWriter.bytesUsed;

        final TermsHash termVectorsWriter;
        // 每个thread 存储的文档会被归结到 某个segment中 在这个段中 可以规定排序规则
        if (docWriter.getSegmentInfo().getIndexSort() == null) {
            // 该对象对外暴露处理doc的api 内部维护了 向索引文件写入数据的 writer
            storedFieldsConsumer = new StoredFieldsConsumer(docWriter);
            // 同上 不过该对象写入的是向量数据
            termVectorsWriter = new TermVectorsConsumer(docWriter);
        } else {
            // 当声明了排序规则时 创建2个排序对象
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
        // 获取排序对象  如果没有设置 那么不需要排序
        Sort indexSort = state.segmentInfo.getIndexSort();
        if (indexSort == null) {
            return null;
        }

        List<Sorter.DocComparator> comparators = new ArrayList<>();
        // sort 对象本身是由多个 SortField 组成的 每个对象都会影响排序的结果
        for (int i = 0; i < indexSort.getSort().length; i++) {
            SortField sortField = indexSort.getSort()[i];
            // 尝试从 hash桶中 找到 field 对应的 PerField 对象 如果对象不存在 则忽略
            PerField perField = getPerField(sortField.getField());

            if (perField != null && perField.docValuesWriter != null &&
                    // 标记某个field 还没有处理完
                    finishedDocValues.contains(perField.fieldInfo.name) == false) {
                // 原本该对象在感知到docId发生变化时 会自动将之前已经写入的 docValue 做排序
                // 这里是手动触发
                perField.docValuesWriter.finish(state.segmentInfo.maxDoc());
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
     * 根据 携带的描述信息 执行刷盘动作
     *
     * @param state
     * @return
     * @throws IOException
     */
    @Override
    public Sorter.DocMap flush(SegmentWriteState state) throws IOException {

        // NOTE: caller (DocumentsWriterPerThread) handles
        // aborting on any exception from this method
        // 首先尝试根据 segmentInfo 内部包含的排序对象 进行排序
        Sorter.DocMap sortMap = maybeSortSegment(state);
        // 该段下最大的文档号
        int maxDoc = state.segmentInfo.maxDoc();
        long t0 = System.nanoTime();
        // 这里是写入 标准因子
        writeNorms(state, sortMap);
        if (docState.infoStream.isEnabled("IW")) {
            docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write norms");
        }
        // 描述段的bean 对象
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
        // 这里对存储 field 信息的索引文件进行持久化
        storedFieldsConsumer.finish(maxDoc);
        storedFieldsConsumer.flush(state, sortMap);
        if (docState.infoStream.isEnabled("IW")) {
            docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to finish stored fields");
        }

        t0 = System.nanoTime();
        // 将 hash桶的数据 转存到 hashMap中
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

        try (NormsProducer norms = readState.fieldInfos.hasNorms()
                ? state.segmentInfo.getCodec().normsFormat().normsProducer(readState)
                : null) {
            NormsProducer normsMergeInstance = null;
            if (norms != null) {
                // Use the merge instance in order to reuse the same IndexInput for all terms
                // 这里返回一个副本对象 并且标记 merging 为true
                normsMergeInstance = norms.getMergeInstance();
            }
            // 在这里将 有关term的数据写入到索引文件中  TODO 这里的逻辑比较复杂 还涉及到了 FST 先放着
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
            // 这是一个hash桶结构
            for (int i = 0; i < fieldHash.length; i++) {
                PerField perField = fieldHash[i];
                while (perField != null) {
                    // 首先确保之前已经往内存中写入了 docValue(往writer对象内部的内存对象)
                    if (perField.docValuesWriter != null) {
                        if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
                            // BUG
                            throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has no docValues but wrote them");
                        }
                        if (dvConsumer == null) {
                            // lazy init
                            DocValuesFormat fmt = state.segmentInfo.getCodec().docValuesFormat();
                            dvConsumer = fmt.fieldsConsumer(state);
                        }

                        // 如果这个域对应的 docValue 还没有全部写入  手动触发 finish
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
     * 写入标准因子
     *
     * @param state
     * @param sortMap
     * @throws IOException
     */
    private void writeNorms(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
        boolean success = false;
        NormsConsumer normsConsumer = null;
        try {
            // 首先检测是否有标准因子
            if (state.fieldInfos.hasNorms()) {
                // 当存在标准因子时  获取当前版本支持的 标准因子格式   默认使用的 Codec 就是 Lucene84Codec
                NormsFormat normsFormat = state.segmentInfo.getCodec().normsFormat();
                assert normsFormat != null;
                // 调用该方法 会间接的为segment 创建有关标准因子的 数据索引文件 和 元数据索引文件
                normsConsumer = normsFormat.normsConsumer(state);

                for (FieldInfo fi : state.fieldInfos) {
                    // 从hash桶中找到 对应的域信息   这个是之前处理doc的时候写进去的
                    PerField perField = getPerField(fi.name);
                    assert perField != null;

                    // we must check the final value of omitNorms for the fieldinfo: it could have
                    // changed for this field since the first time we added it.
                    // 如果没有设置  不写入任何索引文件  并且 忽略标准因子的标识为 false 就会将标准因子写入到索引文件
                    if (fi.omitsNorms() == false && fi.getIndexOptions() != IndexOptions.NONE) {
                        assert perField.norms != null : "field=" + fi.name;
                        // 当前版本 finish是NOOP
                        perField.norms.finish(state.segmentInfo.maxDoc());
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
     * 将当前对象标记成不可用
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
            storedFieldsConsumer.startDocument(docID);
        } catch (Throwable th) {
            docWriter.onAbortingException(th);
            throw th;
        }
    }

    /**
     * Calls StoredFieldsWriter.finishDocument, aborting the
     * segment if it hits any exception.
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
        int fieldCount = 0;

        long fieldGen = nextFieldGen++;

        // NOTE: we need two passes here, in case there are
        // multi-valued fields, because we must process all
        // instances of a given field at once, since the
        // analyzer is free to reuse TokenStream across fields
        // (i.e., we cannot have more than one TokenStream
        // running "at once"):
        // 实际上是转发到 词向量对象的 .startDocument()   会重置 词向量消费者内部的perField属性  perField 代表以field为单位解析 doc内部的数据
        termsHash.startDocument();

        // 为存储该doc下所有的field做准备 一般就是做清理工作
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
                for (int i = 0; i < fieldCount; i++) {
                    fields[i].finish();
                }
                // 代表某个doc 已经处理完毕了 将他们持久化到磁盘中 (在索引writer中又做了一层优化 也就是先将数据存储在内存中只有满足了 flush()的条件才会真正刷盘)
                // 这里对应 storedField
                finishStoredFields();
            }
        }

        try {
            // 这里对应 termVector
            termsHash.finishDocument();
        } catch (Throwable th) {
            // Must abort, on the possibility that on-disk term
            // vectors are now corrupt:
            docWriter.onAbortingException(th);
            throw th;
        }
    }

    /**
     * 以 field 为单位 解析内部数据
     *
     * @param field
     * @param fieldGen
     * @param fieldCount  这个count是指处理某个doc时 总计遇到了几个field
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

        // Invert indexed fields:
        // 首先要确保 索引选项不为 NONE
        if (fieldType.indexOptions() != IndexOptions.NONE) {
            // 将内部信息抽取出来生成 perField 对象
            fp = getOrAddField(fieldName, fieldType, true);
            // 代表该field 在本次处理的doc中首次出现 就要生成 invert信息
            boolean first = fp.fieldGen != fieldGen;
            // 这里应该就是生成倒排索引的地方
            fp.invert(field, first);

            if (first) {
                fields[fieldCount++] = fp;
                fp.fieldGen = fieldGen;
            }
        } else {
            // 这里只是做了一致性校验  当没有设置 IndexOptional 时  很多storeXXX属性应该为false
            verifyUnIndexedFieldType(fieldName, fieldType);
        }

        // Add stored fields:
        // 这里代表需要存储 field本身的值
        if (fieldType.stored()) {
            if (fp == null) {
                fp = getOrAddField(fieldName, fieldType, false);
            }
            if (fieldType.stored()) {
                String value = field.stringValue();
                if (value != null && value.length() > IndexWriter.MAX_STORED_STRING_LENGTH) {
                    throw new IllegalArgumentException("stored field \"" + field.name() + "\" is too large (" + value.length() + " characters) to store");
                }
                try {
                    // 存储域相关的信息(写入到索引文件)
                    storedFieldsConsumer.writeField(fp.fieldInfo, field);
                } catch (Throwable th) {
                    docWriter.onAbortingException(th);
                    throw th;
                }
            }
        }

        // 获取 docValue数据类型
        DocValuesType dvType = fieldType.docValuesType();
        if (dvType == null) {
            throw new NullPointerException("docValuesType must not be null (field: \"" + fieldName + "\")");
        }
        if (dvType != DocValuesType.NONE) {
            if (fp == null) {
                fp = getOrAddField(fieldName, fieldType, false);
            }
            // 如果field 携带了docValue 需要存储
            indexDocValue(fp, dvType, field);
        }
        // 如果存在维度信息
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
        // 获取维护和索引维度
        int pointDimensionCount = field.fieldType().pointDimensionCount();
        int pointIndexDimensionCount = field.fieldType().pointIndexDimensionCount();

        int dimensionNumBytes = field.fieldType().pointNumBytes();

        // Record dimensions for this field; this setter will throw IllegalArgExc if
        // the dimensions were already set to something different:
        if (fp.fieldInfo.getPointDimensionCount() == 0) {
            fieldInfos.globalFieldNumbers.setDimensions(fp.fieldInfo.number, fp.fieldInfo.name, pointDimensionCount, pointIndexDimensionCount, dimensionNumBytes);
        }

        fp.fieldInfo.setPointDimensions(pointDimensionCount, pointIndexDimensionCount, dimensionNumBytes);

        // 这里才是写入动作
        if (fp.pointValuesWriter == null) {
            fp.pointValuesWriter = new PointValuesWriter(docWriter, fp.fieldInfo);
        }
        // TODO
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
     * 存储 某个field对应的 docValue 信息
     * 在外层会对某个doc下所有的field 做处理 这时就可能遇到field重复的情况  就对应sorted 为每个doc下出现的docValue做排序的动作
     * @param dvType 描述docValue的类型信息
     */
    private void indexDocValue(PerField fp, DocValuesType dvType, IndexableField field) throws IOException {

        if (fp.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
            // This is the first time we are seeing this field indexed with doc values, so we
            // now record the DV type so that any future attempt to (illegally) change
            // the DV type of this field, will throw an IllegalArgExc:
            // TODO 段携带 排序信息的情况 先忽略
            if (docWriter.getSegmentInfo().getIndexSort() != null) {
                final Sort indexSort = docWriter.getSegmentInfo().getIndexSort();
                validateIndexSortDVType(indexSort, fp.fieldInfo.name, dvType);
            }
            // 这里设置映射关系
            fieldInfos.globalFieldNumbers.setDocValuesType(fp.fieldInfo.number, fp.fieldInfo.name, dvType);

        }
        fp.fieldInfo.setDocValuesType(dvType);

        int docID = docState.docID;

        // DocValue 就是 fp.xxxValue();
        switch (dvType) {

            case NUMERIC:
                if (fp.docValuesWriter == null) {
                    fp.docValuesWriter = new NumericDocValuesWriter(fp.fieldInfo, bytesUsed);
                }
                if (field.numericValue() == null) {
                    throw new IllegalArgumentException("field=\"" + fp.fieldInfo.name + "\": null value not allowed");
                }
                ((NumericDocValuesWriter) fp.docValuesWriter).addValue(docID, field.numericValue().longValue());
                break;

            case BINARY:
                if (fp.docValuesWriter == null) {
                    fp.docValuesWriter = new BinaryDocValuesWriter(fp.fieldInfo, bytesUsed);
                }
                ((BinaryDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
                break;

            case SORTED:
                if (fp.docValuesWriter == null) {
                    fp.docValuesWriter = new SortedDocValuesWriter(fp.fieldInfo, bytesUsed);
                }
                ((SortedDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
                break;

            case SORTED_NUMERIC:
                if (fp.docValuesWriter == null) {
                    fp.docValuesWriter = new SortedNumericDocValuesWriter(fp.fieldInfo, bytesUsed);
                }
                ((SortedNumericDocValuesWriter) fp.docValuesWriter).addValue(docID, field.numericValue().longValue());
                break;

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
     */
    // 根据 field的索引信息 生成对象
    private PerField getOrAddField(String name, IndexableFieldType fieldType, boolean invert) {

        // Make sure we have a PerField allocated
        final int hashPos = name.hashCode() & hashMask;
        PerField fp = fieldHash[hashPos];
        // 对应hashMap的 get()
        while (fp != null && !fp.fieldInfo.name.equals(name)) {
            fp = fp.next;
        }

        // 代表该 field的信息还是首次生成
        if (fp == null) {
            // First time we are seeing this field in this segment

            // 如果该fieldName 对应的field已经存在 选择复用 否则创建一个新的field 对象
            FieldInfo fi = fieldInfos.getOrAdd(name);
            // 使用该对象的索引信息填充 该对象
            initIndexOptions(fi, fieldType.indexOptions());
            // 找到 描述该field的一组额外信息 全部转移到fi对象中
            Map<String, String> attributes = fieldType.getAttributes();
            if (attributes != null) {
                attributes.forEach((k, v) -> fi.putAttribute(k, v));
            }

            // 根据索引版本号 创建 perField 对象
            fp = new PerField(docWriter.getIndexCreatedVersionMajor(), fi, invert);
            // 链表操作  这里有必要刻意弄成一个 hash桶吗
            fp.next = fieldHash[hashPos];
            fieldHash[hashPos] = fp;
            totalFieldCount++;

            // At most 50% load factor:
            if (totalFieldCount >= fieldHash.length / 2) {
                rehash();
            }

            if (totalFieldCount > fields.length) {
                PerField[] newFields = new PerField[ArrayUtil.oversize(totalFieldCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
                System.arraycopy(fields, 0, newFields, 0, fields.length);
                fields = newFields;
            }

            // 当 fp对象已经存在时  如果invert 为true 并且该state 还没有初始化 那么创建 invert 对象
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
         * 该对象存储了这个 field 下所有的 term
         */
        TermsHashPerField termsHashPerField;

        // Non-null if this field ever had doc values in this
        // segment:
        // 该对象负责写入 doc的数据
        DocValuesWriter docValuesWriter;

        // Non-null if this field ever had points in this segment:
        // 写入点数据 点数据可能就是那种 有多个维度的
        PointValuesWriter pointValuesWriter;

        /**
         * We use this to know when a PerField is seen for the
         * first time in the current document.
         */
        long fieldGen = -1;

        // Used by the hash table
        PerField next;

        // Lazy init'd:
        // 写入标准因子
        NormValuesWriter norms;

        // reused
        // token 是term 通过处理后生成的词元
        TokenStream tokenStream;

        /**
         * @param indexCreatedVersionMajor 索引的主版本号
         * @param fieldInfo                描述该field的信息 相同name的field 会共用内部的属性
         * @param invert                   是否存储反转信息
         */
        public PerField(int indexCreatedVersionMajor, FieldInfo fieldInfo, boolean invert) {
            this.indexCreatedVersionMajor = indexCreatedVersionMajor;
            this.fieldInfo = fieldInfo;
            // 每个doc 有自己的打分规则
            similarity = docState.similarity;
            // 如果需要反转的话 存储反转信息
            if (invert) {
                setInvertState();
            }
        }

        /**
         * 初始化 反转信息对象
         */
        void setInvertState() {
            invertState = new FieldInvertState(indexCreatedVersionMajor, fieldInfo.name, fieldInfo.getIndexOptions());
            // addField 会封装出一个 FreqPerField 对象 （同时该对象内部还以链表形式连接到一个 termVectorPerField对象）
            termsHashPerField = termsHash.addField(invertState, fieldInfo);
            // 如果域信息存在 标准因子 那么需要初始化一个标准因子 writer
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

        public void finish() throws IOException {
            // 如果没有设置标准因子 就要对结果进行打分
            if (fieldInfo.omitsNorms() == false) {
                long normValue;
                if (invertState.length == 0) {
                    // the field exists in this document, but it did not have
                    // any indexed tokens, so we assign a default value of zero
                    // to the norm
                    normValue = 0;
                } else {
                    normValue = similarity.computeNorm(invertState);
                    if (normValue == 0) {
                        throw new IllegalStateException("Similarity " + similarity + " return 0 for non-empty field");
                    }
                }
                // 存储每个doc的得分
                norms.addValue(docState.docID, normValue);
            }

            termsHashPerField.finish();
        }

        /**
         * Inverts one field for one document; first is true
         * if this is the first time we are seeing this field
         * name in this document.
         * <p>
         *     生成倒排索引的逻辑
         *
         * @param first 代表该field 在该doc中首次出现
         */
        public void invert(IndexableField field, boolean first) throws IOException {
            if (first) {
                // First time we're seeing this field (indexed) in
                // this document:
                invertState.reset();
            }

            IndexableFieldType fieldType = field.fieldType();

            IndexOptions indexOptions = fieldType.indexOptions();
            // 重置 fieldInfo 内部的索引选项
            fieldInfo.setIndexOptions(indexOptions);

            if (fieldType.omitNorms()) {
                fieldInfo.setOmitsNorms();
            }

            // 代表已经通过词法解析器处理过
            final boolean analyzed = fieldType.tokenized() && docState.analyzer != null;

            /*
             * To assist people in tracking down problems in analysis components, we wish to write the field name to the infostream
             * when we fail. We expect some caller to eventually deal with the real exception, so we don't want any 'catch' clauses,
             * but rather a finally that takes note of the problem.
             */
            boolean succeededInProcessingField = false;
            // 相同fieldName 的token流对象可能会被复用
            try (TokenStream stream = tokenStream = field.tokenStream(docState.analyzer, tokenStream)) {
                // reset the TokenStream to the first token
                stream.reset();
                // stream 内部包含了某个 field下所有term 的相关信息 这些信息就是用来生成倒排索引的
                invertState.setAttributeSource(stream);
                // 这里就是做一些清理工作 同时将 perField 内部的 attr 赋值 (从invertState中获取属性)
                termsHashPerField.start(field, first);

                // 不断解析 token 并且根据每次解析的token信息 设置attr内部的属性
                while (stream.incrementToken()) {

                    // If we hit an exception in stream.next below
                    // (which is fairly common, e.g. if analyzer
                    // chokes on a given document), then it's
                    // non-aborting and (above) this one document
                    // will be marked as deleted, but still
                    // consume a docID

                    // 这是有关指针增量的属性
                    int posIncr = invertState.posIncrAttribute.getPositionIncrement();
                    // 通过attr 修改state 内部的值
                    invertState.position += posIncr;
                    if (invertState.position < invertState.lastPosition) {
                        if (posIncr == 0) {
                            throw new IllegalArgumentException("first position increment must be > 0 (got 0) for field '" + field.name() + "'");
                        } else if (posIncr < 0) {
                            throw new IllegalArgumentException("position increment must be >= 0 (got " + posIncr + ") for field '" + field.name() + "'");
                        } else {
                            throw new IllegalArgumentException("position overflowed Integer.MAX_VALUE (got posIncr=" + posIncr + " lastPosition=" + invertState.lastPosition + " position=" + invertState.position + ") for field '" + field.name() + "'");
                        }
                    // 解析的数据量异常
                    } else if (invertState.position > IndexWriter.MAX_POSITION) {
                        throw new IllegalArgumentException("position " + invertState.position + " is too large for field '" + field.name() + "': max allowed position is " + IndexWriter.MAX_POSITION);
                    }
                    invertState.lastPosition = invertState.position;
                    if (posIncr == 0) {
                        invertState.numOverlap++;
                    }

                    // 更新本次解析后的 startOff 和 endOff
                    int startOffset = invertState.offset + invertState.offsetAttribute.startOffset();
                    int endOffset = invertState.offset + invertState.offsetAttribute.endOffset();
                    if (startOffset < invertState.lastStartOffset || endOffset < startOffset) {
                        throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, and offsets must not go backwards "
                                + "startOffset=" + startOffset + ",endOffset=" + endOffset + ",lastStartOffset=" + invertState.lastStartOffset + " for field '" + field.name() + "'");
                    }
                    invertState.lastStartOffset = startOffset;

                    try {
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
                        // 这里会将term的信息 写入到索引中 (不一定触发刷盘)
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
                // 代表某个数据流全部处理完毕了  会转发给 attr.end()
                stream.end();

                // TODO: maybe add some safety? then again, it's already checked
                // when we come back around to the field...
                // 在最后更新 position 和 offset
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

            // 代表已经被处理过 TODO 这里设置了一个 gap 啥意思???
            if (analyzed) {
                invertState.position += docState.analyzer.getPositionIncrementGap(fieldInfo.name);
                invertState.offset += docState.analyzer.getOffsetGap(fieldInfo.name);
            }
        }
    }

    /**
     * 找到某个field出现在了哪些 doc下
     *
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
