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

    static class Builder {
      final Set<String> fields;
      final int suffix;
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
   * 该对象负责将 多个field 的数据写入到索引文件
   */
  private class FieldsWriter extends FieldsConsumer {
    final SegmentWriteState writeState;

    /**
     * 该对象统一管理多个 consumer
     */
    final List<Closeable> toClose = new ArrayList<Closeable>();

    public FieldsWriter(SegmentWriteState writeState) {
      this.writeState = writeState;
    }

    /**
     * 在FreqProxTermsWriter中会调用该方法
     * @param fields
     * @param norms
     * @throws IOException
     */
    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
      Map<PostingsFormat, FieldsGroup> formatToGroups = buildFieldsGroupMapping(fields);

      // Write postings
      boolean success = false;
      try {
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

    @Override
    public void merge(MergeState mergeState, NormsProducer norms) throws IOException {
      @SuppressWarnings("unchecked") Iterable<String> indexedFieldNames = () ->
          new MergedIterator<>(true,
              Arrays.stream(mergeState.fieldsProducers).map(FieldsProducer::iterator).toArray(Iterator[]::new));
      Map<PostingsFormat, FieldsGroup> formatToGroups = buildFieldsGroupMapping(indexedFieldNames);

      // Merge postings
      PerFieldMergeState pfMergeState = new PerFieldMergeState(mergeState);
      boolean success = false;
      try {
        for (Map.Entry<PostingsFormat, FieldsGroup> ent : formatToGroups.entrySet()) {
          PostingsFormat format = ent.getKey();
          final FieldsGroup group = ent.getValue();

          FieldsConsumer consumer = format.fieldsConsumer(group.state);
          toClose.add(consumer);
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
     * 以field 为单位构建映射关系
     * @param indexedFieldNames
     * @return
     */
    private Map<PostingsFormat, FieldsGroup> buildFieldsGroupMapping(Iterable<String> indexedFieldNames) {
      // Maps a PostingsFormat instance to the suffix it should use
      // 使用相同索引格式存储field信息的 会被转存到同一个 fieldsGroup 中
      Map<PostingsFormat,FieldsGroup.Builder> formatToGroupBuilders = new HashMap<>();

      // Holds last suffix of each PostingFormat name
      // 当formatName发生碰撞时  增大后缀值
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
        if (groupBuilder == null) {
          // First time we are seeing this format; create a new instance

          // bump the suffix
          Integer suffix = suffixes.get(formatName);
          if (suffix == null) {
            suffix = 0;
          } else {
            suffix = suffix + 1;
          }
          // 用新的后缀覆盖原来的值
          suffixes.put(formatName, suffix);

          // 这个方法 在   writeState.segmentSuffix  长度不为0的时候抛出异常 不知道啥意思
          String segmentSuffix = getFullSegmentSuffix(field,
                                                      writeState.segmentSuffix,
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

        // 将field 转存到 builder 中
        groupBuilder.addField(field);

        // 在map中追加2个属性
        fieldInfo.putAttribute(PER_FIELD_FORMAT_KEY, formatName);
        fieldInfo.putAttribute(PER_FIELD_SUFFIX_KEY, Integer.toString(groupBuilder.suffix));
      }

      Map<PostingsFormat,FieldsGroup> formatToGroups = new HashMap<>((int) (formatToGroupBuilders.size() / 0.75f) + 1);
      formatToGroupBuilders.forEach((postingsFormat, builder) -> formatToGroups.put(postingsFormat, builder.build()));
      return formatToGroups;
    }

    /**
     * 以field为单位存储数据的对象 由 FieldsWriter 进行统一管理
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
      IOUtils.close(toClose);
    }
  }

  private static class FieldsReader extends FieldsProducer {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FieldsReader.class);

    private final Map<String,FieldsProducer> fields = new TreeMap<>();
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

    public FieldsReader(final SegmentReadState readState) throws IOException {

      // Read _X.per and init each format:
      boolean success = false;
      try {
        // Read field name -> format name
        for (FieldInfo fi : readState.fieldInfos) {
          if (fi.getIndexOptions() != IndexOptions.NONE) {
            final String fieldName = fi.name;
            final String formatName = fi.getAttribute(PER_FIELD_FORMAT_KEY);
            if (formatName != null) {
              // null formatName means the field is in fieldInfos, but has no postings!
              final String suffix = fi.getAttribute(PER_FIELD_SUFFIX_KEY);
              if (suffix == null) {
                throw new IllegalStateException("missing attribute: " + PER_FIELD_SUFFIX_KEY + " for field: " + fieldName);
              }
              PostingsFormat format = PostingsFormat.forName(formatName);
              String segmentSuffix = getSuffix(formatName, suffix);
              if (!formats.containsKey(segmentSuffix)) {
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
