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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.util.ArrayUtil;

/**
 * Collection of {@link FieldInfo}s (accessible by number or by name).
 *
 * @lucene.experimental 内部对应一组 fieldInfo
 * FieldInfo 本身可以看作一个 javaBean  用于记录各种信息
 */
public class FieldInfos implements Iterable<FieldInfo> {

    /**
     * An instance without any fields.
     */
    public final static FieldInfos EMPTY = new FieldInfos(new FieldInfo[0]);

    private final boolean hasFreq;
    private final boolean hasProx;
    private final boolean hasPayloads;
    private final boolean hasOffsets;
    private final boolean hasVectors;
    private final boolean hasNorms;
    private final boolean hasDocValues;
    private final boolean hasPointValues;
    /**
     * 代表会采用软删除的 field
     */
    private final String softDeletesField;

    // used only by fieldInfo(int)
    // 内部包含的一组 fieldInfo
    private final FieldInfo[] byNumber;

    /**
     * 可以基于名字来进行检索
     */
    private final HashMap<String, FieldInfo> byName = new HashMap<>();
    private final Collection<FieldInfo> values; // for an unmodifiable iterator

    /**
     * Constructs a new FieldInfos from an array of FieldInfo objects
     * 构造时 默认情况下相关字段都为false
     */
    public FieldInfos(FieldInfo[] infos) {
        boolean hasVectors = false;
        boolean hasProx = false;
        boolean hasPayloads = false;
        boolean hasOffsets = false;
        boolean hasFreq = false;
        boolean hasNorms = false;
        boolean hasDocValues = false;
        boolean hasPointValues = false;
        String softDeletesField = null;

        int size = 0; // number of elements in byNumberTemp, number of used array slots
        // 数组初始大小为10   而不是跟随 infos
        FieldInfo[] byNumberTemp = new FieldInfo[10]; // initial array capacity of 10
        for (FieldInfo info : infos) {
            if (info.number < 0) {
                throw new IllegalArgumentException("illegal field number: " + info.number + " for field " + info.name);
            }
            size = info.number >= size ? info.number + 1 : size;
            // 代表需要进行扩容
            if (info.number >= byNumberTemp.length) { //grow array
                byNumberTemp = ArrayUtil.grow(byNumberTemp, info.number + 1);
            }
            // 根据编号(下标)  为数组对应的slot设置数据
            FieldInfo previous = byNumberTemp[info.number];
            // 当发现 相同编号已经有其他info 时 抛出异常
            if (previous != null) {
                throw new IllegalArgumentException("duplicate field numbers: " + previous.name + " and " + info.name + " have: " + info.number);
            }
            byNumberTemp[info.number] = info;

            // 通过info.name 设置键值对
            previous = byName.put(info.name, info);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate field names: " + previous.number + " and " + info.number + " have: " + info.name);
            }

            // 只要 填入的info中 有一个设置了相关标识 那么本对象的标识就为true
            hasVectors |= info.hasVectors();
            hasProx |= info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
            hasFreq |= info.getIndexOptions() != IndexOptions.DOCS;
            hasOffsets |= info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
            hasNorms |= info.hasNorms();
            hasDocValues |= info.getDocValuesType() != DocValuesType.NONE;
            hasPayloads |= info.hasPayloads();
            hasPointValues |= (info.getPointDimensionCount() != 0);
            // 当前字段是否采用软删除    软删除是什么???
            // 随着不断的 迭代 info对象 那么  softDeletesField 不就变成了最后一个info了吗
            if (info.isSoftDeletesField()) {
                if (softDeletesField != null && softDeletesField.equals(info.name) == false) {
                    throw new IllegalArgumentException("multiple soft-deletes fields [" + info.name + ", " + softDeletesField + "]");
                }
                softDeletesField = info.name;
            }
        }

        this.hasVectors = hasVectors;
        this.hasProx = hasProx;
        this.hasPayloads = hasPayloads;
        this.hasOffsets = hasOffsets;
        this.hasFreq = hasFreq;
        this.hasNorms = hasNorms;
        this.hasDocValues = hasDocValues;
        this.hasPointValues = hasPointValues;
        this.softDeletesField = softDeletesField;

        List<FieldInfo> valuesTemp = new ArrayList<>();
        // 基于目标大小创建数组
        byNumber = new FieldInfo[size];
        // 深拷贝
        for (int i = 0; i < size; i++) {
            byNumber[i] = byNumberTemp[i];
            if (byNumberTemp[i] != null) {
                // 同时在list中在维护一份数据
                valuesTemp.add(byNumberTemp[i]);
            }
        }
        values = Collections.unmodifiableCollection(Arrays.asList(valuesTemp.toArray(new FieldInfo[0])));
    }

    /**
     * Call this to get the (merged) FieldInfos for a
     * composite reader.
     * <p>
     * NOTE: the returned field numbers will likely not
     * correspond to the actual field numbers in the underlying
     * readers, and codec metadata ({@link FieldInfo#getAttribute(String)}
     * will be unavailable.
     * 大体逻辑 先找到reader 下面所有的叶子节点 然后获取每个叶子节点关联的reader对象所绑定的 fieldInfos 然后将他们组装到一个 fieldInfos中
     */
    public static FieldInfos getMergedFieldInfos(IndexReader reader) {
        // 2种情况 如果是 compositeReader  必须确保当前节点是 top节点才能调用该方法  并返回下面所有的 leaf节点
        // 如果是 leafReader 那么返回自身所关联的 context 因为leafReader 在创建时 默认就是top节点
        final List<LeafReaderContext> leaves = reader.leaves();
        // 如果此时叶子为null 返回一个空的信息
        if (leaves.isEmpty()) {
            return FieldInfos.EMPTY;
        } else if (leaves.size() == 1) {
            return leaves.get(0).reader().getFieldInfos();
        } else {
            // 代表存在多个叶子节点  从每个节点关联的reader 的 fieldInfo 找到 软删除的字段   如果没有 返回null
            // 从这里可以看出来 一组 fieldInfo 应该只存在一个 软删除字段
            final String softDeletesField = leaves.stream()
                    .map(l -> l.reader().getFieldInfos().getSoftDeletesField()) // 过滤只剩 需要被软删除的字段
                    .filter(Objects::nonNull)
                    .findAny().orElse(null);
            final Builder builder = new Builder(new FieldNumbers(softDeletesField));
            for (final LeafReaderContext ctx : leaves) {
                builder.add(ctx.reader().getFieldInfos());
            }
            return builder.finish();
        }
    }

    /**
     * Returns a set of names of fields that have a terms index.  The order is undefined.
     */
    // 只获取 fieldInfo的name
    public static Collection<String> getIndexedFields(IndexReader reader) {
        return reader.leaves().stream()
                // 这里是将上游数据 通过 数据流拆解迭代器 拆解成多个 stream
                .flatMap(l -> StreamSupport.stream(l.reader().getFieldInfos().spliterator(), false)
                        // !!! 这里还有一个约束条件 就是 indexOptions 不为 NONE
                        .filter(fi -> fi.getIndexOptions() != IndexOptions.NONE))
                .map(fi -> fi.name)
                .collect(Collectors.toSet());
    }

    /**
     * Returns true if any fields have freqs
     */
    public boolean hasFreq() {
        return hasFreq;
    }

    /**
     * Returns true if any fields have positions
     */
    public boolean hasProx() {
        return hasProx;
    }

    /**
     * Returns true if any fields have payloads
     */
    public boolean hasPayloads() {
        return hasPayloads;
    }

    /**
     * Returns true if any fields have offsets
     */
    public boolean hasOffsets() {
        return hasOffsets;
    }

    /**
     * Returns true if any fields have vectors
     */
    public boolean hasVectors() {
        return hasVectors;
    }

    /**
     * Returns true if any fields have norms
     */
    public boolean hasNorms() {
        return hasNorms;
    }

    /**
     * Returns true if any fields have DocValues
     */
    public boolean hasDocValues() {
        return hasDocValues;
    }

    /**
     * Returns true if any fields have PointValues
     */
    public boolean hasPointValues() {
        return hasPointValues;
    }

    /**
     * Returns the soft-deletes field name if exists; otherwise returns null
     */
    public String getSoftDeletesField() {
        return softDeletesField;
    }

    /**
     * Returns the number of fields
     */
    public int size() {
        return byName.size();
    }

    /**
     * Returns an iterator over all the fieldinfo objects present,
     * ordered by ascending field number
     */
    // TODO: what happens if in fact a different order is used?
    @Override
    public Iterator<FieldInfo> iterator() {
        return values.iterator();
    }

    /**
     * Return the fieldinfo object referenced by the field name
     *
     * @return the FieldInfo object or null when the given fieldName
     * doesn't exist.
     */
    public FieldInfo fieldInfo(String fieldName) {
        return byName.get(fieldName);
    }

    /**
     * Return the fieldinfo object referenced by the fieldNumber.
     *
     * @param fieldNumber field's number.
     * @return the FieldInfo object or null when the given fieldNumber
     * doesn't exist.
     * @throws IllegalArgumentException if fieldNumber is negative
     */
    public FieldInfo fieldInfo(int fieldNumber) {
        if (fieldNumber < 0) {
            throw new IllegalArgumentException("Illegal field number: " + fieldNumber);
        }
        if (fieldNumber >= byNumber.length) {
            return null;
        }
        return byNumber[fieldNumber];
    }

    /**
     * 单独描述 field的维度信息
     */
    static final class FieldDimensions {
        public final int dimensionCount;
        public final int indexDimensionCount;
        public final int dimensionNumBytes;

        public FieldDimensions(int dimensionCount, int indexDimensionCount, int dimensionNumBytes) {
            this.dimensionCount = dimensionCount;
            this.indexDimensionCount = indexDimensionCount;
            this.dimensionNumBytes = dimensionNumBytes;
        }
    }

    /**
     * 该对象负责为 field生成编号  类似一个id生成器
     */
    static final class FieldNumbers {

        private final Map<Integer, String> numberToName;
        private final Map<String, Integer> nameToNumber;
        private final Map<String, IndexOptions> indexOptions;
        // We use this to enforce that a given field never
        // changes DV type, even across segments / IndexWriter
        // sessions:
        private final Map<String, DocValuesType> docValuesType;

        private final Map<String, FieldDimensions> dimensions;

        // TODO: we should similarly catch an attempt to turn
        // norms back on after they were already committed; today
        // we silently discard the norm but this is badly trappy
        // 当前分配到了哪个数字
        private int lowestUnassignedFieldNumber = -1;

        // The soft-deletes field from IWC to enforce a single soft-deletes field
        // 对应的软删除字段
        private final String softDeletesFieldName;

        /**
         * 通过一个软删除字段进行初始化
         *
         * @param softDeletesFieldName
         */
        FieldNumbers(String softDeletesFieldName) {
            this.nameToNumber = new HashMap<>();
            this.numberToName = new HashMap<>();
            this.indexOptions = new HashMap<>();
            this.docValuesType = new HashMap<>();
            this.dimensions = new HashMap<>();
            this.softDeletesFieldName = softDeletesFieldName;
        }

        /**
         * Returns the global field number for the given field name. If the name
         * does not exist yet it tries to add it with the given preferred field
         * number assigned if possible otherwise the first unassigned field number
         * is used as the field number.
         * 每当通过 Builder对象 add or update 一个 fieldInfo的数据时 会从该方法中返回一个id
         * 使用 synchronized 进行并发控制了  确保lowestUnassignedFieldNumber 同步增加
         */
        synchronized int addOrGet(String fieldName, int preferredFieldNumber, IndexOptions indexOptions, DocValuesType dvType, int dimensionCount, int indexDimensionCount, int dimensionNumBytes, boolean isSoftDeletesField) {

            // 同时将数据存储到map中
            if (indexOptions != IndexOptions.NONE) {
                IndexOptions currentOpts = this.indexOptions.get(fieldName);
                if (currentOpts == null) {
                    this.indexOptions.put(fieldName, indexOptions);
                } else if (currentOpts != IndexOptions.NONE && currentOpts != indexOptions) {
                    throw new IllegalArgumentException("cannot change field \"" + fieldName + "\" from index options=" + currentOpts + " to inconsistent index options=" + indexOptions);
                }
            }
            if (dvType != DocValuesType.NONE) {
                DocValuesType currentDVType = docValuesType.get(fieldName);
                if (currentDVType == null) {
                    docValuesType.put(fieldName, dvType);
                } else if (currentDVType != DocValuesType.NONE && currentDVType != dvType) {
                    throw new IllegalArgumentException("cannot change DocValues type from " + currentDVType + " to " + dvType + " for field \"" + fieldName + "\"");
                }
            }
            if (dimensionCount != 0) {
                FieldDimensions dims = dimensions.get(fieldName);
                if (dims != null) {
                    if (dims.dimensionCount != dimensionCount) {
                        throw new IllegalArgumentException("cannot change point dimension count from " + dims.dimensionCount + " to " + dimensionCount + " for field=\"" + fieldName + "\"");
                    }
                    if (dims.indexDimensionCount != indexDimensionCount) {
                        throw new IllegalArgumentException("cannot change point index dimension count from " + dims.indexDimensionCount + " to " + indexDimensionCount + " for field=\"" + fieldName + "\"");
                    }
                    if (dims.dimensionNumBytes != dimensionNumBytes) {
                        throw new IllegalArgumentException("cannot change point numBytes from " + dims.dimensionNumBytes + " to " + dimensionNumBytes + " for field=\"" + fieldName + "\"");
                    }
                } else {
                    dimensions.put(fieldName, new FieldDimensions(dimensionCount, indexDimensionCount, dimensionNumBytes));
                }
            }
            // 找到为该field 分配的数字
            Integer fieldNumber = nameToNumber.get(fieldName);
            if (fieldNumber == null) {
                final Integer preferredBoxed = Integer.valueOf(preferredFieldNumber);
                // 如果当前数字不是-1 且当前数字还没有分配给 某个 field 那么直接使用该数字
                if (preferredFieldNumber != -1 && !numberToName.containsKey(preferredBoxed)) {
                    // cool - we can use this number globally
                    fieldNumber = preferredBoxed;
                } else {
                    // 由 fieldNumber 进行分配
                    // find a new FieldNumber
                    while (numberToName.containsKey(++lowestUnassignedFieldNumber)) {
                        // might not be up to date - lets do the work once needed
                    }
                    fieldNumber = lowestUnassignedFieldNumber;
                }
                assert fieldNumber >= 0;
                // 将关联关系保存到容器中
                numberToName.put(fieldNumber, fieldName);
                nameToNumber.put(fieldName, fieldNumber);
            }

            // 当前字段是否是一个软删除字段
            if (isSoftDeletesField) {
                if (softDeletesFieldName == null) {
                    throw new IllegalArgumentException("this index has [" + fieldName + "] as soft-deletes already but soft-deletes field is not configured in IWC");
                    // 如果存在软删除字段 必须确保2个字段相同
                } else if (fieldName.equals(softDeletesFieldName) == false) {
                    throw new IllegalArgumentException("cannot configure [" + softDeletesFieldName + "] as soft-deletes; this index uses [" + fieldName + "] as soft-deletes already");
                }
                // 如果不是软删除字段  那么名称不能重复
            } else if (fieldName.equals(softDeletesFieldName)) {
                throw new IllegalArgumentException("cannot configure [" + softDeletesFieldName + "] as soft-deletes; this index uses [" + fieldName + "] as non-soft-deletes already");
            }

            return fieldNumber.intValue();
        }

        synchronized void verifyConsistent(Integer number, String name, IndexOptions indexOptions) {
            if (name.equals(numberToName.get(number)) == false) {
                throw new IllegalArgumentException("field number " + number + " is already mapped to field name \"" + numberToName.get(number) + "\", not \"" + name + "\"");
            }
            if (number.equals(nameToNumber.get(name)) == false) {
                throw new IllegalArgumentException("field name \"" + name + "\" is already mapped to field number \"" + nameToNumber.get(name) + "\", not \"" + number + "\"");
            }
            IndexOptions currentIndexOptions = this.indexOptions.get(name);
            if (indexOptions != IndexOptions.NONE && currentIndexOptions != null && currentIndexOptions != IndexOptions.NONE && indexOptions != currentIndexOptions) {
                throw new IllegalArgumentException("cannot change field \"" + name + "\" from index options=" + currentIndexOptions + " to inconsistent index options=" + indexOptions);
            }
        }

        synchronized void verifyConsistent(Integer number, String name, DocValuesType dvType) {
            if (name.equals(numberToName.get(number)) == false) {
                throw new IllegalArgumentException("field number " + number + " is already mapped to field name \"" + numberToName.get(number) + "\", not \"" + name + "\"");
            }
            if (number.equals(nameToNumber.get(name)) == false) {
                throw new IllegalArgumentException("field name \"" + name + "\" is already mapped to field number \"" + nameToNumber.get(name) + "\", not \"" + number + "\"");
            }
            DocValuesType currentDVType = docValuesType.get(name);
            if (dvType != DocValuesType.NONE && currentDVType != null && currentDVType != DocValuesType.NONE && dvType != currentDVType) {
                throw new IllegalArgumentException("cannot change DocValues type from " + currentDVType + " to " + dvType + " for field \"" + name + "\"");
            }
        }

        synchronized void verifyConsistentDimensions(Integer number, String name, int dataDimensionCount, int indexDimensionCount, int dimensionNumBytes) {
            if (name.equals(numberToName.get(number)) == false) {
                throw new IllegalArgumentException("field number " + number + " is already mapped to field name \"" + numberToName.get(number) + "\", not \"" + name + "\"");
            }
            if (number.equals(nameToNumber.get(name)) == false) {
                throw new IllegalArgumentException("field name \"" + name + "\" is already mapped to field number \"" + nameToNumber.get(name) + "\", not \"" + number + "\"");
            }
            FieldDimensions dim = dimensions.get(name);
            if (dim != null) {
                if (dim.dimensionCount != dataDimensionCount) {
                    throw new IllegalArgumentException("cannot change point dimension count from " + dim.dimensionCount + " to " + dataDimensionCount + " for field=\"" + name + "\"");
                }
                if (dim.indexDimensionCount != indexDimensionCount) {
                    throw new IllegalArgumentException("cannot change point index dimension count from " + dim.indexDimensionCount + " to " + indexDimensionCount + " for field=\"" + name + "\"");
                }
                if (dim.dimensionNumBytes != dimensionNumBytes) {
                    throw new IllegalArgumentException("cannot change point numBytes from " + dim.dimensionNumBytes + " to " + dimensionNumBytes + " for field=\"" + name + "\"");
                }
            }
        }

        /**
         * Returns true if the {@code fieldName} exists in the map and is of the
         * same {@code dvType}.
         */
        synchronized boolean contains(String fieldName, DocValuesType dvType) {
            // used by IndexWriter.updateNumericDocValue
            if (!nameToNumber.containsKey(fieldName)) {
                return false;
            } else {
                // only return true if the field has the same dvType as the requested one
                return dvType == docValuesType.get(fieldName);
            }
        }

        synchronized void clear() {
            numberToName.clear();
            nameToNumber.clear();
            indexOptions.clear();
            docValuesType.clear();
            dimensions.clear();
        }

        synchronized void setIndexOptions(int number, String name, IndexOptions indexOptions) {
            verifyConsistent(number, name, indexOptions);
            this.indexOptions.put(name, indexOptions);
        }

        synchronized void setDocValuesType(int number, String name, DocValuesType dvType) {
            verifyConsistent(number, name, dvType);
            docValuesType.put(name, dvType);
        }

        synchronized void setDimensions(int number, String name, int dimensionCount, int indexDimensionCount, int dimensionNumBytes) {
            if (dimensionCount > PointValues.MAX_DIMENSIONS) {
                throw new IllegalArgumentException("dimensionCount must be <= PointValues.MAX_DIMENSIONS (= " + PointValues.MAX_DIMENSIONS + "); got " + dimensionCount + " for field=\"" + name + "\"");
            }
            if (dimensionNumBytes > PointValues.MAX_NUM_BYTES) {
                throw new IllegalArgumentException("dimension numBytes must be <= PointValues.MAX_NUM_BYTES (= " + PointValues.MAX_NUM_BYTES + "); got " + dimensionNumBytes + " for field=\"" + name + "\"");
            }
            if (indexDimensionCount > dimensionCount) {
                throw new IllegalArgumentException("indexDimensionCount must be <= dimensionCount (= " + dimensionCount + "); got " + indexDimensionCount + " for field=\"" + name + "\"");
            }
            if (indexDimensionCount > PointValues.MAX_INDEX_DIMENSIONS) {
                throw new IllegalArgumentException("indexDimensionCount must be <= PointValues.MAX_INDEX_DIMENSIONS (= " + PointValues.MAX_INDEX_DIMENSIONS + "); got " + indexDimensionCount + " for field=\"" + name + "\"");
            }
            verifyConsistentDimensions(number, name, dimensionCount, indexDimensionCount, dimensionNumBytes);
            dimensions.put(name, new FieldDimensions(dimensionCount, indexDimensionCount, dimensionNumBytes));
        }
    }

    /**
     * 该对象负责生成  FieldInfos 对象
     */
    static final class Builder {

        /**
         * 记录每个 fieldInfo  以及用于查询的name
         */
        private final HashMap<String, FieldInfo> byName = new HashMap<>();
        /**
         * 该对象内部包含了 一个 软删除字段  以及一堆存放数据的容器
         */
        final FieldNumbers globalFieldNumbers;

        private boolean finished;

        /**
         * Creates a new instance with the given {@link FieldNumbers}.
         */
        Builder(FieldNumbers globalFieldNumbers) {
            assert globalFieldNumbers != null;
            this.globalFieldNumbers = globalFieldNumbers;
        }

        /**
         * 追加某个 fieldInfos
         *
         * @param other
         */
        public void add(FieldInfos other) {
            assert assertNotFinished();
            // 遍历内部的 fieldInfo  并将数据填充到 内部容器中
            for (FieldInfo fieldInfo : other) {
                add(fieldInfo);
            }
        }

        /**
         * Create a new field, or return existing one.
         */
        // 通过一个 fieldName 查询 FieldInfo  如果存在 直接返回 否则新建一个对象
        public FieldInfo getOrAdd(String name) {
            FieldInfo fi = fieldInfo(name);
            // 新建对象
            if (fi == null) {
                assert assertNotFinished();
                // This field wasn't yet added to this in-RAM
                // segment's FieldInfo, so now we get a global
                // number for this field.  If the field was seen
                // before then we'll get the same name and number,
                // else we'll allocate a new one:
                // 判断是否是那个 软删除的 fieldName
                final boolean isSoftDeletesField = name.equals(globalFieldNumbers.softDeletesFieldName);
                // 某个名称对应的 FieldInfo 默认number 是-1   新建空的field时 大多数参数都是用默认值
                final int fieldNumber = globalFieldNumbers.addOrGet(name, -1, IndexOptions.NONE, DocValuesType.NONE, 0, 0, 0, isSoftDeletesField);
                fi = new FieldInfo(name, fieldNumber, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, new HashMap<>(), 0, 0, 0, isSoftDeletesField);
                assert !byName.containsKey(fi.name);
                globalFieldNumbers.verifyConsistent(Integer.valueOf(fi.number), fi.name, DocValuesType.NONE);
                byName.put(fi.name, fi);
            }

            return fi;
        }

        /**
         * 新增 or 更新内部的数据
         * 该方法 应该是单线程调用的  没有任何并发措施
         *
         * @param name
         * @param preferredFieldNumber
         * @param storeTermVector
         * @param omitNorms
         * @param storePayloads
         * @param indexOptions
         * @param docValues
         * @param dvGen
         * @param attributes
         * @param dataDimensionCount
         * @param indexDimensionCount
         * @param dimensionNumBytes
         * @param isSoftDeletesField
         * @return
         */
        private FieldInfo addOrUpdateInternal(String name,
                                              int preferredFieldNumber,
                                              boolean storeTermVector,
                                              boolean omitNorms,
                                              boolean storePayloads,
                                              IndexOptions indexOptions,
                                              DocValuesType docValues,
                                              long dvGen,
                                              Map<String, String> attributes,
                                              int dataDimensionCount,
                                              int indexDimensionCount,
                                              int dimensionNumBytes,
                                              boolean isSoftDeletesField) {
            assert assertNotFinished();
            if (docValues == null) {
                throw new NullPointerException("DocValuesType must not be null");
            }
            // 从 info中获取的attribute是一个 unmodify容器 所以这里要重建一个新的attr
            if (attributes != null) {
                // original attributes is UnmodifiableMap
                attributes = new HashMap<>(attributes);
            }

            // 查询name 对应的fieldInfo 是否已经存在
            FieldInfo fi = fieldInfo(name);
            if (fi == null) {
                // This field wasn't yet added to this in-RAM
                // segment's FieldInfo, so now we get a global
                // number for this field.  If the field was seen
                // before then we'll get the same name and number,
                // else we'll allocate a new one:
                // 将
                final int fieldNumber = globalFieldNumbers.addOrGet(name, preferredFieldNumber, indexOptions, docValues, dataDimensionCount, indexDimensionCount, dimensionNumBytes, isSoftDeletesField);
                // 通过分配的数字 重新构建  fieldInfo 对象
                fi = new FieldInfo(name, fieldNumber, storeTermVector, omitNorms, storePayloads, indexOptions, docValues, dvGen, attributes, dataDimensionCount, indexDimensionCount, dimensionNumBytes, isSoftDeletesField);
                assert !byName.containsKey(fi.name);
                // 校验当前数据是否合法
                globalFieldNumbers.verifyConsistent(Integer.valueOf(fi.number), fi.name, fi.getDocValuesType());
                // 将结果存放到容器中
                byName.put(fi.name, fi);
            } else {
                // 进行更新
                fi.update(storeTermVector, omitNorms, storePayloads, indexOptions, attributes, dataDimensionCount, indexDimensionCount, dimensionNumBytes);

                // docValue 的类型发生了变化 那么重新设置类型
                if (docValues != DocValuesType.NONE) {
                    // Only pay the synchronization cost if fi does not already have a DVType
                    boolean updateGlobal = fi.getDocValuesType() == DocValuesType.NONE;
                    if (updateGlobal) {
                        // Must also update docValuesType map so it's
                        // aware of this field's DocValuesType.  This will throw IllegalArgumentException if
                        // an illegal type change was attempted.
                        globalFieldNumbers.setDocValuesType(fi.number, name, docValues);
                    }

                    fi.setDocValuesType(docValues); // this will also perform the consistency check.
                    fi.setDocValuesGen(dvGen);
                }
            }
            return fi;
        }

        public FieldInfo add(FieldInfo fi) {
            return add(fi, -1);
        }

        /**
         * @param fi
         * @param dvGen 在没有指定年代的情况下 总是-1
         * @return
         */
        public FieldInfo add(FieldInfo fi, long dvGen) {
            // IMPORTANT - reuse the field number if possible for consistent field numbers across segments
            // 从fi中取出各种数据 并尝试添加到当前容器中 如果出现重复 则进行覆盖
            return addOrUpdateInternal(fi.name, fi.number, fi.hasVectors(),
                    fi.omitsNorms(), fi.hasPayloads(),
                    fi.getIndexOptions(), fi.getDocValuesType(), dvGen,
                    fi.attributes(),
                    fi.getPointDimensionCount(), fi.getPointIndexDimensionCount(), fi.getPointNumBytes(),
                    fi.isSoftDeletesField());
        }

        public FieldInfo fieldInfo(String fieldName) {
            return byName.get(fieldName);
        }

        /**
         * Called only from assert
         */
        private boolean assertNotFinished() {
            if (finished) {
                throw new IllegalStateException("FieldInfos.Builder was already finished; cannot add new fields");
            }
            return true;
        }

        /**
         * 使用当前内部已经处理好的数据 构成 fieldInfos
         *
         * @return
         */
        FieldInfos finish() {
            finished = true;
            return new FieldInfos(byName.values().toArray(new FieldInfo[byName.size()]));
        }
    }
}
