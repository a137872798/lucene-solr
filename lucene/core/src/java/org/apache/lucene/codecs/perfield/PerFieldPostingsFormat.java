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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader.FilterFields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MergedIterator;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Enables per field postings support.
 * <p>
 * Note, when extending this class, the name ({@link #getName}) is 
 * written into the index. In order for the field to be read, the
 * name must resolve to your implementation via {@link #forName(String)}.
 * This method uses Java's 
 * {@link ServiceLoader Service Provider Interface} to resolve format names.
 * <p>
 * Files written by each posting format have an additional suffix containing the 
 * format name. For example, in a per-field configuration instead of <code>_1.prx</code> 
 * filenames would look like <code>_1_Lucene40_0.prx</code>.
 * @see ServiceLoader
 * @lucene.experimental
 * 该对象会以 field为单位 单独创建索引文件
 */

public abstract class PerFieldPostingsFormat extends PostingsFormat {
  /** Name of this {@link PostingsFormat}. */
  public static final String PER_FIELD_NAME = "PerField40";

  /** {@link FieldInfo} attribute name used to store the
   *  format name for each field. */
  public static final String PER_FIELD_FORMAT_KEY = PerFieldPostingsFormat.class.getSimpleName() + ".format";

  /** {@link FieldInfo} attribute name used to store the
   *  segment suffix name for each field. */
  public static final String PER_FIELD_SUFFIX_KEY = PerFieldPostingsFormat.class.getSimpleName() + ".suffix";

  /** Sole constructor. */
  public PerFieldPostingsFormat() {
    super(PER_FIELD_NAME);
  }

  /** Group of fields written by one PostingsFormat */
  // 代表一个 field 组对象  同一个组内的 field 会使用同一种格式写入到索引文件
  static class FieldsGroup {
    final List<String> fields;
    final int suffix;
    /** Custom SegmentWriteState for this group of fields,
     *  with the segmentSuffix uniqueified for this
     *  PostingsFormat */
    final SegmentWriteState state;

    private FieldsGroup(List<String> fields, int suffix, SegmentWriteState state) {
      this.fields = fields;
      this.suffix = suffix;
      this.state = state;
    }

    /**
     * 基于建造器模式创建
     */
    static class Builder {
      final Set<String> fields;
      final int suffix;
      /**
       * 代表这个field 属于哪个段
       */
      final SegmentWriteState state;

      Builder(int suffix, SegmentWriteState state) {
        this.suffix = suffix;
        this.state = state;
        fields = new HashSet<>();
      }

      Builder addField(String field) {
        fields.add(field);
        return this;
      }

      FieldsGroup build() {
        List<String> fieldList = new ArrayList<>(fields);
        fieldList.sort(null);
        return new FieldsGroup(fieldList, suffix, state);
      }
    }
  };

  static String getSuffix(String formatName, String suffix) {
    return formatName + "_" + suffix;
  }

  static String getFullSegmentSuffix(String fieldName, String outerSegmentSuffix, String segmentSuffix) {
    if (outerSegmentSuffix.length() == 0) {
      return segmentSuffix;
    } else {
      // TODO: support embedding; I think it should work but
      // we need a test confirm to confirm
      // return outerSegmentSuffix + "_" + segmentSuffix;
      throw new IllegalStateException("cannot embed PerFieldPostingsFormat inside itself (field \"" + fieldName + "\" returned PerFieldPostingsFormat)");
    }
  }

  /**
   * lucene 的词数据就是写入到该对象中
   */
  private class FieldsWriter extends FieldsConsumer {
    final SegmentWriteState writeState;

    /**
     * 管理多个关闭钩子
     */
    final List<Closeable> toClose = new ArrayList<Closeable>();

    public FieldsWriter(SegmentWriteState writeState) {
      this.writeState = writeState;
    }

    /**
=     * @param fields  可以先当作是 FreqProxFields 对象
     * @param norms
     * @throws IOException
     */
    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
      Map<PostingsFormat, FieldsGroup> formatToGroups = buildFieldsGroupMapping(fields);

      // Write postings
      boolean success = false;
      try {
        // 分组后 以 format为单位 将归属到下面的所有field 信息写入到索引文件
        for (Map.Entry<PostingsFormat, FieldsGroup> ent : formatToGroups.entrySet()) {
          PostingsFormat format = ent.getKey();
          final FieldsGroup group = ent.getValue();

          // Exposes only the fields from this group:
          // 这里只暴露属于该group的 field
          Fields maskedFields = new FilterFields(fields) {
            @Override
            public Iterator<String> iterator() {
              return group.fields.iterator();
            }
          };

          // 使用 Lucene84PositionsFormat  写入数据
          FieldsConsumer consumer = format.fieldsConsumer(group.state);
          toClose.add(consumer);
          // 将相关数据写入到 索引文件
          consumer.write(maskedFields, norms);
        }
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(toClose);
        }
      }
    }

    /**
     * 将term信息合并
     * @param mergeState
     * @param norms   合并后的标准因子
     * @throws IOException
     */
    @Override
    public void merge(MergeState mergeState, NormsProducer norms) throws IOException {


      // 这里就是将每个 segment下的fieldName 以迭代器形式范围  并将它们合并成一个
      @SuppressWarnings("unchecked") Iterable<String> indexedFieldNames = () ->
          new MergedIterator<>(true,
              Arrays.stream(mergeState.fieldsProducers).map(FieldsProducer::iterator).toArray(Iterator[]::new));

      // 将 field 按照写入的格式进行分组  一般来说就是那个最新的格式
      Map<PostingsFormat, FieldsGroup> formatToGroups = buildFieldsGroupMapping(indexedFieldNames);

      // Merge postings
      // 先开始合并 postings 信息
      PerFieldMergeState pfMergeState = new PerFieldMergeState(mergeState);
      boolean success = false;
      try {
        for (Map.Entry<PostingsFormat, FieldsGroup> ent : formatToGroups.entrySet()) {
          PostingsFormat format = ent.getKey();
          final FieldsGroup group = ent.getValue();

          // 最简单的情况 所有field使用同一种格式写入 并且此时格式是Lucene84PostingsFormat
          FieldsConsumer consumer = format.fieldsConsumer(group.state);
          // 存储到钩子队列中 以便该对象关闭时  触发所有钩子
          toClose.add(consumer);
          // 实际是consumer是 BlockTreeTermsWriter
          // pfMergeState.apply(group.fields) 主要是做了一层过滤  只能访问到当前 group 下的field
          consumer.merge(pfMergeState.apply(group.fields), norms);
        }
        success = true;
      } finally {
        pfMergeState.reset();
        if (!success) {
          IOUtils.closeWhileHandlingException(toClose);
        }
      }
    }

    /**
     * 将 field 按照不同的写入格式分组
     * @param indexedFieldNames
     * @return
     */
    private Map<PostingsFormat, FieldsGroup> buildFieldsGroupMapping(Iterable<String> indexedFieldNames) {
      // Maps a PostingsFormat instance to the suffix it should use
      // 每个 field 可以单独定义 format 所以这里按照写入格式进行分组
      Map<PostingsFormat,FieldsGroup.Builder> formatToGroupBuilders = new HashMap<>();

      // Holds last suffix of each PostingFormat name
      Map<String,Integer> suffixes = new HashMap<>();

      // Assign field -> PostingsFormat
      for(String field : indexedFieldNames) {
        // 先获取该field的 描述信息
        FieldInfo fieldInfo = writeState.fieldInfos.fieldInfo(field);
        // TODO: This should check current format from the field attribute?
        // 这里返回的是 Lucene84PostingsFormat
        final PostingsFormat format = getPostingsFormatForField(field);

        if (format == null) {
          throw new IllegalStateException("invalid null PostingsFormat for field=\"" + field + "\"");
        }
        // name 是 LUCENE84
        String formatName = format.getName();

        FieldsGroup.Builder groupBuilder = formatToGroupBuilders.get(format);
        // 这是首次构建 group的情况
        if (groupBuilder == null) {
          // First time we are seeing this format; create a new instance

          // bump the suffix
          // 比如所有 field 都使用 Lucene84PostingsFormat 结构写入时 在 suffixes中会冲突 这时就会更新后缀值
          Integer suffix = suffixes.get(formatName);
          if (suffix == null) {
            suffix = 0;
          } else {
            suffix = suffix + 1;
          }
          // 用新的后缀覆盖原来的值
          suffixes.put(formatName, suffix);

          // 这里根据格式名  生成了不同的段后缀名
          String segmentSuffix = getFullSegmentSuffix(field,
                                                      writeState.segmentSuffix,  // 如果指定了段后缀名的情况 使用该后缀名 否则使用  getSuffix() 的返回结果
                                                      // 生成后缀名
                                                      getSuffix(formatName, Integer.toString(suffix)));
          // 这里创建一个 builder对象  该对象内部存储了 suffix state  以及一个fieldName列表
          groupBuilder = new FieldsGroup.Builder(suffix, new SegmentWriteState(writeState, segmentSuffix));
          formatToGroupBuilders.put(format, groupBuilder);
        } else {
          // we've already seen this format, so just grab its suffix
          if (!suffixes.containsKey(formatName)) {
            throw new IllegalStateException("no suffix for format name: " + formatName + ", expected: " + groupBuilder.suffix);
          }
        }

        // 将field 添加到相同 format的组中
        groupBuilder.addField(field);

        // 在 fieldInfo 中 设置指定的 索引文件格式 以及后缀
        fieldInfo.putAttribute(PER_FIELD_FORMAT_KEY, formatName);
        fieldInfo.putAttribute(PER_FIELD_SUFFIX_KEY, Integer.toString(groupBuilder.suffix));
      }

      Map<PostingsFormat,FieldsGroup> formatToGroups = new HashMap<>((int) (formatToGroupBuilders.size() / 0.75f) + 1);
      formatToGroupBuilders.forEach((postingsFormat, builder) -> formatToGroups.put(postingsFormat, builder.build()));
      return formatToGroups;
    }

    /**
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
      IOUtils.close(toClose);
    }
  }

  /**
   * 该对象是一个总控对象 存储了以field 为单位的数据
   */
  private static class FieldsReader extends FieldsProducer {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FieldsReader.class);

    /**
     * 存储 fieldName 与索引文件reader的映射关系  实际上是一个树结构
     */
    private final Map<String,FieldsProducer> fields = new TreeMap<>();
    /**
     * 该段对应的所有 fieldInfo对应的索引文件名 与 reader的映射关系
     */
    private final Map<String,FieldsProducer> formats = new HashMap<>();
    private final String segment;
    
    // clone for merge
    FieldsReader(FieldsReader other) {
      Map<FieldsProducer,FieldsProducer> oldToNew = new IdentityHashMap<>();
      // First clone all formats
      for(Map.Entry<String,FieldsProducer> ent : other.formats.entrySet()) {
        FieldsProducer values = ent.getValue().getMergeInstance();
        formats.put(ent.getKey(), values);
        oldToNew.put(ent.getValue(), values);
      }

      // Then rebuild fields:
      for(Map.Entry<String,FieldsProducer> ent : other.fields.entrySet()) {
        FieldsProducer producer = oldToNew.get(ent.getValue());
        assert producer != null;
        fields.put(ent.getKey(), producer);
      }

      segment = other.segment;
    }

    /**
     * 该对象会以segment下面的field 为基本单位 分别创建读取这些field 信息的输入流
     * @param readState  该对象内部包含了本次读取的 目标segment及关联的fieldInfo  还有所在的目录等信息
     * @throws IOException
     */
    public FieldsReader(final SegmentReadState readState) throws IOException {

      // Read _X.per and init each format:
      boolean success = false;
      try {
        // Read field name -> format name
        for (FieldInfo fi : readState.fieldInfos) {
          // 确保field下某些信息 确实被设定为需要存储
          if (fi.getIndexOptions() != IndexOptions.NONE) {
            final String fieldName = fi.name;
            // 检测该fieldInfo 有没有指定 使用的存储格式  默认情况会设置  Lucene84PostingsFormat
            final String formatName = fi.getAttribute(PER_FIELD_FORMAT_KEY);
            if (formatName != null) {
              // null formatName means the field is in fieldInfos, but has no postings!
              // 格式名 和使用的后缀名必须同时存在
              final String suffix = fi.getAttribute(PER_FIELD_SUFFIX_KEY);
              if (suffix == null) {
                throw new IllegalStateException("missing attribute: " + PER_FIELD_SUFFIX_KEY + " for field: " + fieldName);
              }
              // 创建指定的索引格式对象
              PostingsFormat format = PostingsFormat.forName(formatName);
              // 生成段后缀名
              String segmentSuffix = getSuffix(formatName, suffix);
              if (!formats.containsKey(segmentSuffix)) {
                // 生成用于读取 field position信息的 reader对象
                formats.put(segmentSuffix, format.fieldsProducer(new SegmentReadState(readState, segmentSuffix)));
              }
              // 以fieldName 为key 存储producer
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

      this.segment = readState.segmentInfo.name;
    }

    @Override
    public Iterator<String> iterator() {
      return Collections.unmodifiableSet(fields.keySet()).iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      FieldsProducer fieldsProducer = fields.get(field);
      return fieldsProducer == null ? null : fieldsProducer.terms(field);
    }
    
    @Override
    public int size() {
      return fields.size();
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(formats.values());
    }

    @Override
    public long ramBytesUsed() {
      long ramBytesUsed = BASE_RAM_BYTES_USED;
      ramBytesUsed += fields.size() * 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      ramBytesUsed += formats.size() * 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      for(Map.Entry<String,FieldsProducer> entry: formats.entrySet()) {
        ramBytesUsed += entry.getValue().ramBytesUsed();
      }
      return ramBytesUsed;
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
      return Accountables.namedAccountables("format", formats);
    }

    @Override
    public void checkIntegrity() throws IOException {
      for (FieldsProducer producer : formats.values()) {
        producer.checkIntegrity();
      }
    }

    @Override
    public FieldsProducer getMergeInstance() {
      return new FieldsReader(this);
    }

    @Override
    public String toString() {
      return "PerFieldPostings(segment=" + segment + " formats=" + formats.size() + ")";
    }
  }

  @Override
  public final FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    return new FieldsWriter(state);
  }

  /**
   * 获取一个以 field为单位的读取对象
   * @param state
   * @return
   * @throws IOException
   */
  @Override
  public final FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    return new FieldsReader(state);
  }

  /** 
   * Returns the postings format that should be used for writing 
   * new segments of <code>field</code>.
   * <p>
   * The field to format mapping is written to the index, so
   * this method is only invoked when writing, not when reading. */
  public abstract PostingsFormat getPostingsFormatForField(String field);
}
