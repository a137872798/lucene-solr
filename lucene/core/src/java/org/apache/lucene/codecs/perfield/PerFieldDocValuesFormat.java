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
package org.apache.lucene.codecs.perfield;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.IOUtils;

/**
 * Enables per field docvalues support.
 * <p>
 * Note, when extending this class, the name ({@link #getName}) is
 * written into the index. In order for the field to be read, the
 * name must resolve to your implementation via {@link #forName(String)}.
 * This method uses Java's
 * {@link ServiceLoader Service Provider Interface} to resolve format names.
 * <p>
 * Files written by each docvalues format have an additional suffix containing the
 * format name. For example, in a per-field configuration instead of <code>_1.dat</code>
 * filenames would look like <code>_1_Lucene40_0.dat</code>.
 *
 * @lucene.experimental
 * @see ServiceLoader
 * 该索引文件以 field为单位
 */

public abstract class PerFieldDocValuesFormat extends DocValuesFormat {
    /**
     * Name of this {@link PostingsFormat}.
     */
    public static final String PER_FIELD_NAME = "PerFieldDV40";

    /**
     * {@link FieldInfo} attribute name used to store the
     * format name for each field.
     */
    public static final String PER_FIELD_FORMAT_KEY = PerFieldDocValuesFormat.class.getSimpleName() + ".format";

    /**
     * {@link FieldInfo} attribute name used to store the
     * segment suffix name for each field.
     */
    public static final String PER_FIELD_SUFFIX_KEY = PerFieldDocValuesFormat.class.getSimpleName() + ".suffix";


    /**
     * Sole constructor.
     */
    public PerFieldDocValuesFormat() {
        super(PER_FIELD_NAME);
    }

    /**
     * 通过某个段的描述信息 生成一个处理 以  field 为单位 处理docValue 的对象
     *
     * @param state
     * @return
     * @throws IOException
     */
    @Override
    public final DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new FieldsWriter(state);
    }

    /**
     * 每个负责存储 docValue 的索引写入对象 会携带一个后缀
     */
    static class ConsumerAndSuffix implements Closeable {
        DocValuesConsumer consumer;
        int suffix;

        @Override
        public void close() throws IOException {
            consumer.close();
        }
    }

    /**
     * 一个 DefaultIndexingChain 对应一个该对象 每次要写入某个field的数据时 创建一个新对象
     */
    private class FieldsWriter extends DocValuesConsumer {

        private final Map<DocValuesFormat, ConsumerAndSuffix> formats = new HashMap<>();
        /**
         * 存储了每个 formatName 与 后缀的映射关系
         */
        private final Map<String, Integer> suffixes = new HashMap<>();

        /**
         * 代表相关的段
         */
        private final SegmentWriteState segmentWriteState;

        public FieldsWriter(SegmentWriteState state) {
            segmentWriteState = state;
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            getInstance(field).addNumericField(field, valuesProducer);
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            // 先找到 field对应的consumer 后再调用 addBinaryField
            getInstance(field).addBinaryField(field, valuesProducer);
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            getInstance(field).addSortedField(field, valuesProducer);
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            getInstance(field).addSortedNumericField(field, valuesProducer);
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            getInstance(field).addSortedSetField(field, valuesProducer);
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
            Map<DocValuesConsumer, Collection<String>> consumersToField = new IdentityHashMap<>();

            // Group each consumer by the fields it handles
            for (FieldInfo fi : mergeState.mergeFieldInfos) {
                if (fi.getDocValuesType() == DocValuesType.NONE) {
                    continue;
                }
                // merge should ignore current format for the fields being merged
                DocValuesConsumer consumer = getInstance(fi, true);
                Collection<String> fieldsForConsumer = consumersToField.get(consumer);
                if (fieldsForConsumer == null) {
                    fieldsForConsumer = new ArrayList<>();
                    consumersToField.put(consumer, fieldsForConsumer);
                }
                fieldsForConsumer.add(fi.name);
            }

            // Delegate the merge to the appropriate consumer
            PerFieldMergeState pfMergeState = new PerFieldMergeState(mergeState);
            try {
                for (Map.Entry<DocValuesConsumer, Collection<String>> e : consumersToField.entrySet()) {
                    e.getKey().merge(pfMergeState.apply(e.getValue()));
                }
            } finally {
                pfMergeState.reset();
            }
        }

        private DocValuesConsumer getInstance(FieldInfo field) throws IOException {
            return getInstance(field, false);
        }

        /**
         * DocValuesConsumer for the given field.
         *
         * @param field               - FieldInfo object.
         * @param ignoreCurrentFormat - ignore the existing format attributes.
         * @return DocValuesConsumer for the field.
         * @throws IOException if there is a low-level IO error
         *                     通过field 找到对应的 consumer
         */
        private DocValuesConsumer getInstance(FieldInfo field, boolean ignoreCurrentFormat) throws IOException {
            DocValuesFormat format = null;
            // TODO 这个值是什么时候设置的???
            if (field.getDocValuesGen() != -1) {
                String formatName = null;
                if (ignoreCurrentFormat == false) {
                    // 查询是否设置了自定义格式
                    formatName = field.getAttribute(PER_FIELD_FORMAT_KEY);
                }
                // this means the field never existed in that segment, yet is applied updates
                // 通过 SPI 机制进行加载
                if (formatName != null) {
                    format = DocValuesFormat.forName(formatName);
                }
            }
            if (format == null) {
                // 采用默认的格式
                format = getDocValuesFormatForField(field.name);
            }
            if (format == null) {
                throw new IllegalStateException("invalid null DocValuesFormat for field=\"" + field.name + "\"");
            }
            final String formatName = format.getName();

            // 这里为啥要回填这个属性
            field.putAttribute(PER_FIELD_FORMAT_KEY, formatName);
            Integer suffix = null;

            // 以 format为单位存储了特殊的后缀
            ConsumerAndSuffix consumer = formats.get(format);
            if (consumer == null) {
                // First time we are seeing this format; create a new instance

                // 获取后缀属性 如果存在设置到 suffix容器中
                if (field.getDocValuesGen() != -1) {
                    String suffixAtt = null;
                    if (!ignoreCurrentFormat) {
                        suffixAtt = field.getAttribute(PER_FIELD_SUFFIX_KEY);
                    }
                    // even when dvGen is != -1, it can still be a new field, that never
                    // existed in the segment, and therefore doesn't have the recorded
                    // attributes yet.
                    if (suffixAtt != null) {
                        suffix = Integer.valueOf(suffixAtt);
                    }
                }

                if (suffix == null) {
                    // bump the suffix
                    suffix = suffixes.get(formatName);
                    if (suffix == null) {
                        suffix = 0;
                    } else {
                        suffix = suffix + 1;
                    }
                }
                suffixes.put(formatName, suffix);

                // 将段名与 后缀数字拼接
                final String segmentSuffix = getFullSegmentSuffix(segmentWriteState.segmentSuffix,
                        getSuffix(formatName, Integer.toString(suffix)));
                consumer = new ConsumerAndSuffix();
                consumer.consumer = format.fieldsConsumer(new SegmentWriteState(segmentWriteState, segmentSuffix));
                consumer.suffix = suffix;
                formats.put(format, consumer);
            } else {
                // we've already seen this format, so just grab its suffix
                assert suffixes.containsKey(formatName);
                suffix = consumer.suffix;
            }

            // 回填后缀
            field.putAttribute(PER_FIELD_SUFFIX_KEY, Integer.toString(suffix));
            // TODO: we should only provide the "slice" of FIS
            // that this DVF actually sees ...
            return consumer.consumer;
        }

        @Override
        public void close() throws IOException {
            // Close all subs
            IOUtils.close(formats.values());
        }
    }

    static String getSuffix(String formatName, String suffix) {
        return formatName + "_" + suffix;
    }

    static String getFullSegmentSuffix(String outerSegmentSuffix, String segmentSuffix) {
        if (outerSegmentSuffix.length() == 0) {
            return segmentSuffix;
        } else {
            return outerSegmentSuffix + "_" + segmentSuffix;
        }
    }

    /**
     * 该对象是一个总控对象  内部数据 以field 为单位 存储了读取对应docValue的reader 对象
     */
    private class FieldsReader extends DocValuesProducer {

        /**
         * key 对应fieldName  value 对应该field相关的 docValueType信息
         */
        private final Map<String, DocValuesProducer> fields = new TreeMap<>();
        /**
         * 以索引文件名为key 存储对应的 docValue
         */
        private final Map<String, DocValuesProducer> formats = new HashMap<>();

        // clone for merge
        FieldsReader(FieldsReader other) {
            Map<DocValuesProducer, DocValuesProducer> oldToNew = new IdentityHashMap<>();
            // First clone all formats
            for (Map.Entry<String, DocValuesProducer> ent : other.formats.entrySet()) {
                DocValuesProducer values = ent.getValue().getMergeInstance();
                formats.put(ent.getKey(), values);
                oldToNew.put(ent.getValue(), values);
            }

            // Then rebuild fields:
            for (Map.Entry<String, DocValuesProducer> ent : other.fields.entrySet()) {
                DocValuesProducer producer = oldToNew.get(ent.getValue());
                assert producer != null;
                fields.put(ent.getKey(), producer);
            }
        }

        /**
         * 通过 描述段信息的上下文对象 初始化
         * @param readState
         * @throws IOException
         */
        public FieldsReader(final SegmentReadState readState) throws IOException {

            // Init each unique format:
            boolean success = false;
            try {
                // Read field name -> format name
                // 根据本次 包含的所有field  添加映射关系
                for (FieldInfo fi : readState.fieldInfos) {
                    // 首先要确定这个 field 内部存储了数据
                    if (fi.getDocValuesType() != DocValuesType.NONE) {
                        final String fieldName = fi.name;
                        final String formatName = fi.getAttribute(PER_FIELD_FORMAT_KEY);
                        if (formatName != null) {
                            // null formatName means the field is in fieldInfos, but has no docvalues!
                            final String suffix = fi.getAttribute(PER_FIELD_SUFFIX_KEY);
                            if (suffix == null) {
                                throw new IllegalStateException("missing attribute: " + PER_FIELD_SUFFIX_KEY + " for field: " + fieldName);
                            }
                            // 根据索引文件格式信息找到 索引文件 并读取数据
                            DocValuesFormat format = DocValuesFormat.forName(formatName);
                            String segmentSuffix = getFullSegmentSuffix(readState.segmentSuffix, getSuffix(formatName, suffix));
                            if (!formats.containsKey(segmentSuffix)) {
                                // 相同format 下每个field 会有唯一的后缀数字  通过该数字定位索引文件并生成 producer
                                formats.put(segmentSuffix, format.fieldsProducer(new SegmentReadState(readState, segmentSuffix)));
                            }
                            fields.put(fieldName, formats.get(segmentSuffix));
                        }
                    }
                }
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(formats.values());
                }
            }
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            DocValuesProducer producer = fields.get(field.name);
            return producer == null ? null : producer.getNumeric(field);
        }

        /**
         * 看来一个 field 应该是对应
         *
         * @param field
         * @return
         * @throws IOException
         */
        @Override
        public BinaryDocValues getBinary(FieldInfo field) throws IOException {
            DocValuesProducer producer = fields.get(field.name);
            return producer == null ? null : producer.getBinary(field);
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            DocValuesProducer producer = fields.get(field.name);
            return producer == null ? null : producer.getSorted(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            DocValuesProducer producer = fields.get(field.name);
            return producer == null ? null : producer.getSortedNumeric(field);
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            DocValuesProducer producer = fields.get(field.name);
            return producer == null ? null : producer.getSortedSet(field);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(formats.values());
        }

        @Override
        public long ramBytesUsed() {
            long size = 0;
            for (Map.Entry<String, DocValuesProducer> entry : formats.entrySet()) {
                size += (entry.getKey().length() * Character.BYTES) + entry.getValue().ramBytesUsed();
            }
            return size;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Accountables.namedAccountables("format", formats);
        }

        @Override
        public void checkIntegrity() throws IOException {
            for (DocValuesProducer format : formats.values()) {
                format.checkIntegrity();
            }
        }

        @Override
        public DocValuesProducer getMergeInstance() {
            return new FieldsReader(this);
        }

        @Override
        public String toString() {
            return "PerFieldDocValues(formats=" + formats.size() + ")";
        }
    }

    @Override
    public final DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FieldsReader(state);
    }

    /**
     * Returns the doc values format that should be used for writing
     * new segments of <code>field</code>.
     * <p>
     * The field to format mapping is written to the index, so
     * this method is only invoked when writing, not when reading.
     */
    public abstract DocValuesFormat getDocValuesFormatForField(String field);
}
