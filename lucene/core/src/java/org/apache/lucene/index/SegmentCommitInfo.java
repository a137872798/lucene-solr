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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.util.StringHelper;

/** Embeds a [read-only] SegmentInfo and adds per-commit
 *  fields.
 *
 *  @lucene.experimental */
public class SegmentCommitInfo {
  
  /** The {@link SegmentInfo} that we wrap. */
  // 内部描述了该段的基本信息
  public final SegmentInfo info;

  /** Id that uniquely identifies this segment commit. */
  // 每当一次数据刷盘后  id都会重新生成
  private byte[] id;

  // How many deleted docs in the segment:
  // 记录当前段 删除了多少doc
  private int delCount;

  // How many soft-deleted docs in the segment that are not also hard-deleted:
  // 软删除数量   TODO 什么是软删除
  private int softDelCount;

  // Generation number of the live docs file (-1 if there
  // are no deletes yet):  每当触发一次删除动作时 该值都会增加   一般在初始化时都会设置成-1 代表还未进行过一次删除
  private long delGen;

  // Normally 1+delGen, unless an exception was hit on last
  // attempt to write:
  // 下一次删除doc的话 生成的文件名年代信息 默认就是 delGen+1
  private long nextWriteDelGen;

  // Generation number of the FieldInfos (-1 if there are no updates)
  // 如果field信息发生了变化 则会修改该值
  private long fieldInfosGen;
  
  // Normally 1+fieldInfosGen, unless an exception was hit on last attempt to
  // write
  // 代表下次写入fieldInfo 数据时 使用的后缀
  private long nextWriteFieldInfosGen;
  
  // Generation number of the DocValues (-1 if there are no updates)
  // 代表 docValue的年代  每当将docValue的变化刷盘后 该值+1
  private long docValuesGen;
  
  // Normally 1+dvGen, unless an exception was hit on last attempt to
  // write
  // 代表下一次写入 docValue的年代
  private long nextWriteDocValuesGen;

  // Track the per-field DocValues update files
  // 代表在哪些文件中存储了有关 docValue 变化的数据
  private final Map<Integer,Set<String>> dvUpdatesFiles = new HashMap<>();
  
  // TODO should we add .files() to FieldInfosFormat, like we have on
  // LiveDocsFormat?
  // track the fieldInfos update files
  // 记录哪些文件存储了描述 filedInfo的信息
  private final Set<String> fieldInfosFiles = new HashSet<>();

  /**
   * 代表当前segment下所有文件的总长度
   * 每次删除动作完成时 更新年代同时会重置该值 以及重置id
   */
  private volatile long sizeInBytes = -1;

  // NOTE: only used in-RAM by IW to track buffered deletes;
  // this is never written to/read from the Directory
  // 代表该对象会被 delGen为多少的 BufferedUpdatesStream 处理
  private long bufferedDeletesGen = -1;

  /**
   * Sole constructor.
   * @param info   描述该片段的基础信息
   *          {@link SegmentInfo} that we wrap
   * @param delCount
   *          number of deleted documents in this segment   已经删除了多少数据
   * @param softDelCount   通过软删除 删除了多少数据
   * @param delGen
   *          deletion generation number (used to name deletion files)
   * @param fieldInfosGen
   *          FieldInfos generation number (used to name field-infos files)
   * @param docValuesGen
   *          DocValues generation number (used to name doc-values updates files)
   * @param id Id that uniquely identifies this segment commit. This id must be 16 bytes long. See {@link StringHelper#randomId()}
   */
  public SegmentCommitInfo(SegmentInfo info, int delCount, int softDelCount, long delGen, long fieldInfosGen, long docValuesGen, byte[] id) {
    this.info = info;
    this.delCount = delCount;
    this.softDelCount = softDelCount;
    this.delGen = delGen;
    this.nextWriteDelGen = delGen == -1 ? 1 : delGen + 1;
    this.fieldInfosGen = fieldInfosGen;
    this.nextWriteFieldInfosGen = fieldInfosGen == -1 ? 1 : fieldInfosGen + 1;
    this.docValuesGen = docValuesGen;
    this.nextWriteDocValuesGen = docValuesGen == -1 ? 1 : docValuesGen + 1;
    this.id = id;
    if (id != null && id.length != StringHelper.ID_LENGTH) {
      throw new IllegalArgumentException("invalid id: " + Arrays.toString(id));
    }
  }
  
  /** Returns the per-field DocValues updates files. */
  // 返回更新的 docValue的文件列表
  public Map<Integer,Set<String>> getDocValuesUpdatesFiles() {
    return Collections.unmodifiableMap(dvUpdatesFiles);
  }
  
  /** Sets the DocValues updates file names, per field number. Does not deep clone the map. */
  // 代表在哪些文件中存储了有关 docValue 变化的数据
  public void setDocValuesUpdatesFiles(Map<Integer,Set<String>> dvUpdatesFiles) {
    this.dvUpdatesFiles.clear();
    for (Map.Entry<Integer,Set<String>> kv : dvUpdatesFiles.entrySet()) {
      // rename the set
      Set<String> set = new HashSet<>();
      for (String file : kv.getValue()) {
        // 为文件追加 segment.name 后 添加到容器中
        set.add(info.namedForThisSegment(file));
      }
      this.dvUpdatesFiles.put(kv.getKey(), set);
    }
  }
  
  /** Returns the FieldInfos file names. */
  public Set<String> getFieldInfosFiles() {
    return Collections.unmodifiableSet(fieldInfosFiles);
  }
  
  /** Sets the FieldInfos file names. */
  public void setFieldInfosFiles(Set<String> fieldInfosFiles) {
    this.fieldInfosFiles.clear();
    for (String file : fieldInfosFiles) {
      this.fieldInfosFiles.add(info.namedForThisSegment(file));
    }
  }

  /** Called when we succeed in writing deletes */
  // 每当成功将删除后的doc位图持久化 ， 会增加该值
  void advanceDelGen() {
    delGen = nextWriteDelGen;
    nextWriteDelGen = delGen+1;
    // 当更新年代后 重置 id
    generationAdvanced();
  }

  /** Called if there was an exception while writing
   *  deletes, so that we don't try to write to the same
   *  file more than once. */
  void advanceNextWriteDelGen() {
    nextWriteDelGen++;
  }
  
  /** Gets the nextWriteDelGen. */
  long getNextWriteDelGen() {
    return nextWriteDelGen;
  }
  
  /** Sets the nextWriteDelGen. */
  void setNextWriteDelGen(long v) {
    nextWriteDelGen = v;
  }
  
  /** Called when we succeed in writing a new FieldInfos generation. */
  void advanceFieldInfosGen() {
    fieldInfosGen = nextWriteFieldInfosGen;
    nextWriteFieldInfosGen = fieldInfosGen + 1;
    generationAdvanced();
  }
  
  /**
   * Called if there was an exception while writing a new generation of
   * FieldInfos, so that we don't try to write to the same file more than once.
   */
  void advanceNextWriteFieldInfosGen() {
    nextWriteFieldInfosGen++;
  }
  
  /** Gets the nextWriteFieldInfosGen. */
  long getNextWriteFieldInfosGen() {
    return nextWriteFieldInfosGen;
  }
  
  /** Sets the nextWriteFieldInfosGen. */
  void setNextWriteFieldInfosGen(long v) {
    nextWriteFieldInfosGen = v;
  }

  /** Called when we succeed in writing a new DocValues generation. */
  void advanceDocValuesGen() {
    docValuesGen = nextWriteDocValuesGen;
    nextWriteDocValuesGen = docValuesGen + 1;
    generationAdvanced();
  }

  // 增加对应的年代值

  /**
   * Called if there was an exception while writing a new generation of
   * DocValues, so that we don't try to write to the same file more than once.
   */
  void advanceNextWriteDocValuesGen() {
    nextWriteDocValuesGen++;
  }

  /** Gets the nextWriteDocValuesGen. */
  long getNextWriteDocValuesGen() {
    return nextWriteDocValuesGen;
  }
  
  /** Sets the nextWriteDocValuesGen. */
  void setNextWriteDocValuesGen(long v) {
    nextWriteDocValuesGen = v;
  }
  
  /** Returns total size in bytes of all files for this
   *  segment. */
  // 返回当前segment下所有文件的总长度
  public long sizeInBytes() throws IOException {
    if (sizeInBytes == -1) {
      long sum = 0;
      for (final String fileName : files()) {
        sum += info.dir.fileLength(fileName);
      }
      sizeInBytes = sum;
    }

    return sizeInBytes;
  }

  /** Returns all files in use by this segment. */
  // 返回当前segment 下所有的文件
  public Collection<String> files() throws IOException {
    // Start from the wrapped info's files:
    Collection<String> files = new HashSet<>(info.files());

    // TODO we could rely on TrackingDir.getCreatedFiles() (like we do for
    // updates) and then maybe even be able to remove LiveDocsFormat.files().
    
    // Must separately add any live docs files:
    // 判断是否要增加描述 alive 信息的索引文件
    info.getCodec().liveDocsFormat().files(this, files);

    // must separately add any field updates files
    // 追加描述 docValue 变化的文件
    for (Set<String> updatefiles : dvUpdatesFiles.values()) {
      files.addAll(updatefiles);
    }
    
    // must separately add fieldInfos files
    // 追加描述 fieldInfo 信息的文件
    files.addAll(fieldInfosFiles);
    
    return files;
  }

  long getBufferedDeletesGen() {
    return bufferedDeletesGen;
  }

  void setBufferedDeletesGen(long v) {
    if (bufferedDeletesGen == -1) {
      bufferedDeletesGen = v;
      generationAdvanced();
    } else {
      throw new IllegalStateException("buffered deletes gen should only be set once");
    }
  }
  
  /** Returns true if there are any deletions for the 
   * segment at this commit. */
  public boolean hasDeletions() {
    return delGen != -1;
  }

  /** Returns true if there are any field updates for the segment in this commit. */
  public boolean hasFieldUpdates() {
    return fieldInfosGen != -1;
  }
  
  /** Returns the next available generation number of the FieldInfos files. */
  public long getNextFieldInfosGen() {
    return nextWriteFieldInfosGen;
  }
  
  /**
   * Returns the generation number of the field infos file or -1 if there are no
   * field updates yet.
   */
  public long getFieldInfosGen() {
    return fieldInfosGen;
  }
  
  /** Returns the next available generation number of the DocValues files. */
  public long getNextDocValuesGen() {
    return nextWriteDocValuesGen;
  }
  
  /**
   * Returns the generation number of the DocValues file or -1 if there are no
   * doc-values updates yet.
   */
  public long getDocValuesGen() {
    return docValuesGen;
  }
  
  /**
   * Returns the next available generation number
   * of the live docs file.
   */
  public long getNextDelGen() {
    return nextWriteDelGen;
  }

  /**
   * Returns generation number of the live docs file 
   * or -1 if there are no deletes yet.
   */
  public long getDelGen() {
    return delGen;
  }
  
  /**
   * Returns the number of deleted docs in the segment.
   */
  public int getDelCount() {
    return delCount;
  }

  /**
   * Returns the number of only soft-deleted docs.
   */
  public int getSoftDelCount() {
    return softDelCount;
  }

  /**
   * 只有将删除后的doc位图写入到索引文件后 才会设置该属性 同时还会设置删除的 gen
   * @param delCount
   */
  void setDelCount(int delCount) {
    if (delCount < 0 || delCount > info.maxDoc()) {
      throw new IllegalArgumentException("invalid delCount=" + delCount + " (maxDoc=" + info.maxDoc() + ")");
    }
    assert softDelCount + delCount <= info.maxDoc() : "maxDoc=" + info.maxDoc() + ",delCount=" + delCount + ",softDelCount=" + softDelCount;
    this.delCount = delCount;
  }

  void setSoftDelCount(int softDelCount) {
    if (softDelCount < 0 || softDelCount > info.maxDoc()) {
      throw new IllegalArgumentException("invalid softDelCount=" + softDelCount + " (maxDoc=" + info.maxDoc() + ")");
    }
    assert softDelCount + delCount <= info.maxDoc() : "maxDoc=" + info.maxDoc() + ",delCount=" + delCount + ",softDelCount=" + softDelCount;
    this.softDelCount = softDelCount;
  }

  /** Returns a description of this segment. */
  public String toString(int pendingDelCount) {
    String s = info.toString(delCount + pendingDelCount);
    if (delGen != -1) {
      s += ":delGen=" + delGen;
    }
    if (fieldInfosGen != -1) {
      s += ":fieldInfosGen=" + fieldInfosGen;
    }
    if (docValuesGen != -1) {
      s += ":dvGen=" + docValuesGen;
    }
    if (softDelCount > 0) {
      s += " :softDel=" + softDelCount;
    }
    if (this.id != null) {
      s += " :id=" + StringHelper.idToString(id);
    }

    return s;
  }

  @Override
  public String toString() {
    return toString(0);
  }

  @Override
  public SegmentCommitInfo clone() {
    SegmentCommitInfo other = new SegmentCommitInfo(info, delCount, softDelCount, delGen, fieldInfosGen, docValuesGen, getId());
    // Not clear that we need to carry over nextWriteDelGen
    // (i.e. do we ever clone after a failed write and
    // before the next successful write?), but just do it to
    // be safe:
    other.nextWriteDelGen = nextWriteDelGen;
    other.nextWriteFieldInfosGen = nextWriteFieldInfosGen;
    other.nextWriteDocValuesGen = nextWriteDocValuesGen;
    
    // deep clone
    for (Entry<Integer,Set<String>> e : dvUpdatesFiles.entrySet()) {
      other.dvUpdatesFiles.put(e.getKey(), new HashSet<>(e.getValue()));
    }
    
    other.fieldInfosFiles.addAll(fieldInfosFiles);
    
    return other;
  }

  final int getDelCount(boolean includeSoftDeletes) {
    return includeSoftDeletes ? getDelCount() + getSoftDelCount() : getDelCount();
  }

  private void generationAdvanced() {
    sizeInBytes = -1;
    id = StringHelper.randomId();
  }

  /**
   * Returns and Id that uniquely identifies this segment commit or <code>null</code> if there is no ID assigned.
   * This ID changes each time the the segment changes due to a delete, doc-value or field update.
   */
  public byte[] getId() {
    return id == null ? null : id.clone();
  }
}
