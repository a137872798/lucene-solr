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


import java.util.Map;
import java.util.Objects;

/**
 *  Access to the Field Info file that describes document fields and whether or
 *  not they are indexed. Each segment has a separate Field Info file. Objects
 *  of this class are thread-safe for multiple readers, but only one thread can
 *  be adding documents at a time, with no other reader or writer threads
 *  accessing this object.
 *  描述 field的信息
 **/

public final class FieldInfo {

  // 名字和编号
  /** Field's name */
  public final String name;
  /** Internal field number */
  // field 的编号 或者说类似id的东西
  public final int number;

  /**
   * 代表该 field 描述的相关docValue 类型
   */
  private DocValuesType docValuesType = DocValuesType.NONE;

  // True if any document indexed term vectors
  // 代表当前field下的词向量信息已经存储到内存中了(通过writer写入到合适的数据结构中 但是还没有持久化)
  private boolean storeTermVector;

  private boolean omitNorms; // omit norms associated with indexed fields  

  private IndexOptions indexOptions = IndexOptions.NONE;
  /**
   * 代表是否要存储field携带的 payload信息 只有 IndexOptional 超过某个值时 才会存储
   */
  private boolean storePayloads; // whether this field stores payloads together with term positions

  private final Map<String,String> attributes;

  private long dvGen;

  /** If both of these are positive it means this field indexed points
   *  (see {@link org.apache.lucene.codecs.PointsFormat}). */
  private int pointDimensionCount;
  private int pointIndexDimensionCount;
  private int pointNumBytes;

  // whether this field is used as the soft-deletes field
  private final boolean softDeletesField;

  /**
   * Sole constructor.
   *
   * @lucene.experimental
   * 代表一个 field 对象   这里只是简单的赋值操作
   */
  public FieldInfo(String name, int number, boolean storeTermVector, boolean omitNorms, boolean storePayloads,
                   IndexOptions indexOptions, DocValuesType docValues, long dvGen, Map<String,String> attributes,
                   int pointDimensionCount, int pointIndexDimensionCount, int pointNumBytes, boolean softDeletesField) {
    this.name = Objects.requireNonNull(name);
    this.number = number;
    this.docValuesType = Objects.requireNonNull(docValues, "DocValuesType must not be null (field: \"" + name + "\")");
    this.indexOptions = Objects.requireNonNull(indexOptions, "IndexOptions must not be null (field: \"" + name + "\")");
    if (indexOptions != IndexOptions.NONE) {
      this.storeTermVector = storeTermVector;
      this.storePayloads = storePayloads;
      this.omitNorms = omitNorms;
    } else { // for non-indexed fields, leave defaults
      this.storeTermVector = false;
      this.storePayloads = false;
      this.omitNorms = false;
    }
    this.dvGen = dvGen;
    this.attributes = Objects.requireNonNull(attributes);
    this.pointDimensionCount = pointDimensionCount;
    this.pointIndexDimensionCount = pointIndexDimensionCount;
    this.pointNumBytes = pointNumBytes;
    this.softDeletesField = softDeletesField;
    assert checkConsistency();
  }

  /** 
   * Performs internal consistency checks.
   * Always returns true (or throws IllegalStateException) 
   */
  public boolean checkConsistency() {
    if (indexOptions != IndexOptions.NONE) {
      // Cannot store payloads unless positions are indexed:
      if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0 && storePayloads) {
        throw new IllegalStateException("indexed field '" + name + "' cannot have payloads without positions");
      }
    } else {
      if (storeTermVector) {
        throw new IllegalStateException("non-indexed field '" + name + "' cannot store term vectors");
      }
      if (storePayloads) {
        throw new IllegalStateException("non-indexed field '" + name + "' cannot store payloads");
      }
      if (omitNorms) {
        throw new IllegalStateException("non-indexed field '" + name + "' cannot omit norms");
      }
    }

    if (pointDimensionCount < 0) {
      throw new IllegalStateException("pointDimensionCount must be >= 0; got " + pointDimensionCount);
    }

    if (pointIndexDimensionCount < 0) {
      throw new IllegalStateException("pointIndexDimensionCount must be >= 0; got " + pointIndexDimensionCount);
    }

    if (pointNumBytes < 0) {
      throw new IllegalStateException("pointNumBytes must be >= 0; got " + pointNumBytes);
    }

    if (pointDimensionCount != 0 && pointNumBytes == 0) {
      throw new IllegalStateException("pointNumBytes must be > 0 when pointDimensionCount=" + pointDimensionCount);
    }

    if (pointIndexDimensionCount != 0 && pointDimensionCount == 0) {
      throw new IllegalStateException("pointIndexDimensionCount must be 0 when pointDimensionCount=0");
    }

    if (pointNumBytes != 0 && pointDimensionCount == 0) {
      throw new IllegalStateException("pointDimensionCount must be > 0 when pointNumBytes=" + pointNumBytes);
    }
    
    if (dvGen != -1 && docValuesType == DocValuesType.NONE) {
      throw new IllegalStateException("field '" + name + "' cannot have a docvalues update generation without having docvalues");
    }

    return true;
  }

  // should only be called by FieldInfos#addOrUpdate
  void update(boolean storeTermVector, boolean omitNorms, boolean storePayloads, IndexOptions indexOptions,
              Map<String, String> attributes, int dimensionCount, int indexDimensionCount, int dimensionNumBytes) {
    if (indexOptions == null) {
      throw new NullPointerException("IndexOptions must not be null (field: \"" + name + "\")");
    }
    //System.out.println("FI.update field=" + name + " indexed=" + indexed + " omitNorms=" + omitNorms + " this.omitNorms=" + this.omitNorms);
    if (this.indexOptions != indexOptions) {
      if (this.indexOptions == IndexOptions.NONE) {
        this.indexOptions = indexOptions;
      } else if (indexOptions != IndexOptions.NONE) {
        throw new IllegalArgumentException("cannot change field \"" + name + "\" from index options=" + this.indexOptions + " to inconsistent index options=" + indexOptions);
      }
    }

    if (this.pointDimensionCount == 0 && dimensionCount != 0) {
      this.pointDimensionCount = dimensionCount;
      this.pointIndexDimensionCount = indexDimensionCount;
      this.pointNumBytes = dimensionNumBytes;
    } else if (dimensionCount != 0 && (this.pointDimensionCount != dimensionCount || this.pointIndexDimensionCount != indexDimensionCount || this.pointNumBytes != dimensionNumBytes)) {
      throw new IllegalArgumentException("cannot change field \"" + name + "\" from points dimensionCount=" + this.pointDimensionCount + ", indexDimensionCount=" + this.pointIndexDimensionCount + ", numBytes=" + this.pointNumBytes + " to inconsistent dimensionCount=" + dimensionCount +", indexDimensionCount=" + indexDimensionCount + ", numBytes=" + dimensionNumBytes);
    }

    if (this.indexOptions != IndexOptions.NONE) { // if updated field data is not for indexing, leave the updates out
      this.storeTermVector |= storeTermVector;                // once vector, always vector
      this.storePayloads |= storePayloads;

      // Awkward: only drop norms if incoming update is indexed:
      if (indexOptions != IndexOptions.NONE && this.omitNorms != omitNorms) {
        this.omitNorms = true;                // if one require omitNorms at least once, it remains off for life
      }
    }
    if (this.indexOptions == IndexOptions.NONE || this.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
      // cannot store payloads if we don't store positions:
      this.storePayloads = false;
    }
    if (attributes != null) {
      this.attributes.putAll(attributes);
    }
    assert checkConsistency();
  }

  /** Record that this field is indexed with points, with the
   *  specified number of dimensions and bytes per dimension. */
  public void setPointDimensions(int dimensionCount, int indexDimensionCount, int numBytes) {
    if (dimensionCount <= 0) {
      throw new IllegalArgumentException("point dimension count must be >= 0; got " + dimensionCount + " for field=\"" + name + "\"");
    }
    if (indexDimensionCount > PointValues.MAX_INDEX_DIMENSIONS) {
      throw new IllegalArgumentException("point index dimension count must be < PointValues.MAX_INDEX_DIMENSIONS (= " + PointValues.MAX_INDEX_DIMENSIONS + "); got " + indexDimensionCount + " for field=\"" + name + "\"");
    }
    if (indexDimensionCount > dimensionCount) {
      throw new IllegalArgumentException("point index dimension count must be <= point dimension count (= " + dimensionCount + "); got " + indexDimensionCount + " for field=\"" + name + "\"");
    }
    if (numBytes <= 0) {
      throw new IllegalArgumentException("point numBytes must be >= 0; got " + numBytes + " for field=\"" + name + "\"");
    }
    if (numBytes > PointValues.MAX_NUM_BYTES) {
      throw new IllegalArgumentException("point numBytes must be <= PointValues.MAX_NUM_BYTES (= " + PointValues.MAX_NUM_BYTES + "); got " + numBytes + " for field=\"" + name + "\"");
    }
    if (pointDimensionCount != 0 && pointDimensionCount != dimensionCount) {
      throw new IllegalArgumentException("cannot change point dimension count from " + pointDimensionCount + " to " + dimensionCount + " for field=\"" + name + "\"");
    }
    if (pointIndexDimensionCount != 0 && pointIndexDimensionCount != indexDimensionCount) {
      throw new IllegalArgumentException("cannot change point index dimension count from " + pointIndexDimensionCount + " to " + indexDimensionCount + " for field=\"" + name + "\"");
    }
    if (pointNumBytes != 0 && pointNumBytes != numBytes) {
      throw new IllegalArgumentException("cannot change point numBytes from " + pointNumBytes + " to " + numBytes + " for field=\"" + name + "\"");
    }

    pointDimensionCount = dimensionCount;
    pointIndexDimensionCount = indexDimensionCount;
    pointNumBytes = numBytes;

    assert checkConsistency();
  }

  /** Return point data dimension count */
  public int getPointDimensionCount() {
    return pointDimensionCount;
  }

  /** Return point data dimension count */
  public int getPointIndexDimensionCount() {
    return pointIndexDimensionCount;
  }

  /** Return number of bytes per dimension */
  public int getPointNumBytes() {
    return pointNumBytes;
  }

  /** Record that this field is indexed with docvalues, with the specified type */
  public void setDocValuesType(DocValuesType type) {
    if (type == null) {
      throw new NullPointerException("DocValuesType must not be null (field: \"" + name + "\")");
    }
    if (docValuesType != DocValuesType.NONE && type != DocValuesType.NONE && docValuesType != type) {
      throw new IllegalArgumentException("cannot change DocValues type from " + docValuesType + " to " + type + " for field \"" + name + "\"");
    }
    docValuesType = type;
    assert checkConsistency();
  }
  
  /** Returns IndexOptions for the field, or IndexOptions.NONE if the field is not indexed */
  public IndexOptions getIndexOptions() {
    return indexOptions;
  }

  /** Record the {@link IndexOptions} to use with this field. */
  // 填充索引相关选项
  public void setIndexOptions(IndexOptions newIndexOptions) {
    if (indexOptions != newIndexOptions) {
      // 只允许 A->NONE NONE->A  A->A  禁止其他情况
      if (indexOptions == IndexOptions.NONE) {
        indexOptions = newIndexOptions;
      } else if (newIndexOptions != IndexOptions.NONE) {
        throw new IllegalArgumentException("cannot change field \"" + name + "\" from index options=" + indexOptions + " to inconsistent index options=" + newIndexOptions);
      }
    }

    // DOCS_AND_FREQS_AND_POSITIONS 以上代表会存储 payload 否则不会存储
    if (indexOptions == IndexOptions.NONE || indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
      // cannot store payloads if we don't store positions:
      storePayloads = false;
    }
  }
  
  /**
   * Returns {@link DocValuesType} of the docValues; this is
   * {@code DocValuesType.NONE} if the field has no docvalues.
   */
  public DocValuesType getDocValuesType() {
    return docValuesType;
  }
  
  /** Sets the docValues generation of this field. */
  void setDocValuesGen(long dvGen) {
    this.dvGen = dvGen;
    assert checkConsistency();
  }
  
  /**
   * Returns the docValues generation of this field, or -1 if no docValues
   * updates exist for it.
   */
  public long getDocValuesGen() {
    return dvGen;
  }
  
  void setStoreTermVectors() {
    storeTermVector = true;
    assert checkConsistency();
  }
  
  void setStorePayloads() {
    if (indexOptions != IndexOptions.NONE && indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
      storePayloads = true;
    }
    assert checkConsistency();
  }

  /**
   * Returns true if norms are explicitly omitted for this field
   */
  public boolean omitsNorms() {
    return omitNorms;
  }

  /** Omit norms for this field. */
  public void setOmitsNorms() {
    if (indexOptions == IndexOptions.NONE) {
      throw new IllegalStateException("cannot omit norms: this field is not indexed");
    }
    omitNorms = true;
  }
  
  /**
   * Returns true if this field actually has any norms.
   */
  public boolean hasNorms() {
    return indexOptions != IndexOptions.NONE && omitNorms == false;
  }
  
  /**
   * Returns true if any payloads exist for this field.
   */
  public boolean hasPayloads() {
    return storePayloads;
  }
  
  /**
   * Returns true if any term vectors exist for this field.
   */
  public boolean hasVectors() {
    return storeTermVector;
  }
  
  /**
   * Get a codec attribute value, or null if it does not exist
   */
  public String getAttribute(String key) {
    return attributes.get(key);
  }
  
  /**
   * Puts a codec attribute value.
   * <p>
   * This is a key-value mapping for the field that the codec can use
   * to store additional metadata, and will be available to the codec
   * when reading the segment via {@link #getAttribute(String)}
   * <p>
   * If a value already exists for the key in the field, it will be replaced with
   * the new value. If the value of the attributes for a same field is changed between
   * the documents, the behaviour after merge is undefined.
   */
  public String putAttribute(String key, String value) {
    return attributes.put(key, value);
  }
  
  /**
   * Returns internal codec attributes map.
   */
  public Map<String,String> attributes() {
    return attributes;
  }

  /**
   * Returns true if this field is configured and used as the soft-deletes field.
   * See {@link IndexWriterConfig#softDeletesField}
   */
  public boolean isSoftDeletesField() {
    return softDeletesField;
  }
}
