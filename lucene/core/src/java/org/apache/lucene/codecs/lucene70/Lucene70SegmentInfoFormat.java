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
package org.apache.lucene.codecs.lucene70;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter; // javadocs
import org.apache.lucene.index.SegmentInfo; // javadocs
import org.apache.lucene.index.SegmentInfos; // javadocs
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput; // javadocs
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;

/**
 * Lucene 7.0 Segment info format.
 * <p>
 * Files:
 * <ul>
 *   <li><code>.si</code>: Header, SegVersion, SegSize, IsCompoundFile, Diagnostics, Files, Attributes, IndexSort, Footer
 * </ul>
 * Data types:
 * <ul>
 *   <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *   <li>SegSize --&gt; {@link DataOutput#writeInt Int32}</li>
 *   <li>SegVersion --&gt; {@link DataOutput#writeString String}</li>
 *   <li>SegMinVersion --&gt; {@link DataOutput#writeString String}</li>
 *   <li>Files --&gt; {@link DataOutput#writeSetOfStrings Set&lt;String&gt;}</li>
 *   <li>Diagnostics,Attributes --&gt; {@link DataOutput#writeMapOfStrings Map&lt;String,String&gt;}</li>
 *   <li>IsCompoundFile --&gt; {@link DataOutput#writeByte Int8}</li>
 *   <li>IndexSort --&gt; {@link DataOutput#writeVInt Int32} count, followed by {@code count} SortField</li>
 *   <li>SortField --&gt; {@link DataOutput#writeString String} field name, followed by {@link DataOutput#writeVInt Int32} sort type ID,
 *       followed by {@link DataOutput#writeByte Int8} indicating reversed sort, followed by a type-specific encoding of the optional missing value
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * Field Descriptions:
 * <ul>
 *   <li>SegVersion is the code version that created the segment.</li>
 *   <li>SegMinVersion is the minimum code version that contributed documents to the segment.</li>
 *   <li>SegSize is the number of documents contained in the segment index.</li>
 *   <li>IsCompoundFile records whether the segment is written as a compound file or
 *       not. If this is -1, the segment is not a compound file. If it is 1, the segment
 *       is a compound file.</li>
 *   <li>The Diagnostics Map is privately written by {@link IndexWriter}, as a debugging aid,
 *       for each segment it creates. It includes metadata like the current Lucene
 *       version, OS, Java version, why the segment was created (merge, flush,
 *       addIndexes), etc.</li>
 *   <li>Files is a list of files referred to by this segment.</li>
 * </ul>
 *
 * @see SegmentInfos
 * @lucene.experimental
 * 该索引文件存储的是 有关段信息的
 */
public class Lucene70SegmentInfoFormat extends SegmentInfoFormat {

  /** Sole constructor. */
  public Lucene70SegmentInfoFormat() {
  }

  /**
   * 指定某个目录 以及段名称和段id 读取数据
   * @param dir
   * @param segment
   * @param segmentID expected identifier for the segment
   * @param context
   * @return
   * @throws IOException
   */
  @Override
  public SegmentInfo read(Directory dir, String segment, byte[] segmentID, IOContext context) throws IOException {
    // 读取si文件
    final String fileName = IndexFileNames.segmentFileName(segment, "", Lucene70SegmentInfoFormat.SI_EXTENSION);
    try (ChecksumIndexInput input = dir.openChecksumInput(fileName, context)) {
      Throwable priorE = null;
      SegmentInfo si = null;
      try {
        int format = CodecUtil.checkIndexHeader(input, Lucene70SegmentInfoFormat.CODEC_NAME,
                                                Lucene70SegmentInfoFormat.VERSION_START,
                                                Lucene70SegmentInfoFormat.VERSION_CURRENT,
                                                segmentID, "");
        final Version version = Version.fromBits(input.readInt(), input.readInt(), input.readInt());
        // 获取描述最小版本的信息
        byte hasMinVersion = input.readByte();
        final Version minVersion;
        switch (hasMinVersion) {
          case 0:
            minVersion = null;
            break;
          case 1:
            minVersion = Version.fromBits(input.readInt(), input.readInt(), input.readInt());
            break;
          default:
            throw new CorruptIndexException("Illegal boolean value " + hasMinVersion, input);
        }

        // 这里存储的是 该segment下最多允许存放多少doc
        final int docCount = input.readInt();
        if (docCount < 0) {
          throw new CorruptIndexException("invalid docCount: " + docCount, input);
        }
        // 是否使用 复合索引文件存储数据
        final boolean isCompoundFile = input.readByte() == SegmentInfo.YES;

        // 获取诊断信息
        final Map<String,String> diagnostics = input.readMapOfStrings();
        // 这些是什么???
        final Set<String> files = input.readSetOfStrings();
        // 该段携带的 attr
        final Map<String,String> attributes = input.readMapOfStrings();

        // 代表该segment 下有多少个描述 如何对数据进行排序的 信息
        int numSortFields = input.readVInt();
        Sort indexSort;
        if (numSortFields > 0) {
          // 初始化对应数量的 sortField
          SortField[] sortFields = new SortField[numSortFields];
          for(int i=0;i<numSortFields;i++) {
            // 描述排序的域名
            String fieldName = input.readString();
            // 排序的类型
            int sortTypeID = input.readVInt();
            SortField.Type sortType;
            SortedSetSelector.Type sortedSetSelector = null;
            SortedNumericSelector.Type sortedNumericSelector = null;
            switch(sortTypeID) {
            case 0:
              sortType = SortField.Type.STRING;
              break;
            case 1:
              sortType = SortField.Type.LONG;
              break;
            case 2:
              sortType = SortField.Type.INT;
              break;
            case 3:
              sortType = SortField.Type.DOUBLE;
              break;
            case 4:
              sortType = SortField.Type.FLOAT;
              break;
              // 下面2种排序 除了本身的排序类型外 还会携带 selector信息
            case 5:
              sortType = SortField.Type.STRING;
              byte selector = input.readByte();
              if (selector == 0) {
                sortedSetSelector = SortedSetSelector.Type.MIN;
              } else if (selector == 1) {
                sortedSetSelector = SortedSetSelector.Type.MAX;
              } else if (selector == 2) {
                sortedSetSelector = SortedSetSelector.Type.MIDDLE_MIN;
              } else if (selector == 3) {
                sortedSetSelector = SortedSetSelector.Type.MIDDLE_MAX;
              } else {
                throw new CorruptIndexException("invalid index SortedSetSelector ID: " + selector, input);
              }
              break;
            case 6:
              byte type = input.readByte();
              if (type == 0) {
                sortType = SortField.Type.LONG;
              } else if (type == 1) {
                sortType = SortField.Type.INT;
              } else if (type == 2) {
                sortType = SortField.Type.DOUBLE;
              } else if (type == 3) {
                sortType = SortField.Type.FLOAT;
              } else {
                throw new CorruptIndexException("invalid index SortedNumericSortField type ID: " + type, input);
              }
              byte numericSelector = input.readByte();
              if (numericSelector == 0) {
                sortedNumericSelector = SortedNumericSelector.Type.MIN;
              } else if (numericSelector == 1) {
                sortedNumericSelector = SortedNumericSelector.Type.MAX;
              } else {
                throw new CorruptIndexException("invalid index SortedNumericSelector ID: " + numericSelector, input);
              }
              break;
            default:
              throw new CorruptIndexException("invalid index sort field type ID: " + sortTypeID, input);
            }
            // 按照正序/倒序排序
            byte b = input.readByte();
            boolean reverse;
            if (b == 0) {
              reverse = true;
            } else if (b == 1) {
              reverse = false;
            } else {
              throw new CorruptIndexException("invalid index sort reverse: " + b, input);
            }

            // 如果选择器属性存在 创建不同的 sortField否则创建普通的 sortField
            if (sortedSetSelector != null) {
              sortFields[i] = new SortedSetSortField(fieldName, reverse, sortedSetSelector);
            } else if (sortedNumericSelector != null) {
              sortFields[i] = new SortedNumericSortField(fieldName, sortType, reverse, sortedNumericSelector);
            } else {
              sortFields[i] = new SortField(fieldName, sortType, reverse);
            }

            // 读取默认值 并追加到 sortField中
            Object missingValue;
            b = input.readByte();
            if (b == 0) {
              missingValue = null;
            } else {
              switch(sortType) {
              case STRING:
                // 字符串类型只能设置这2种默认值
                if (b == 1) {
                  missingValue = SortField.STRING_LAST;
                } else if (b == 2) {
                  missingValue = SortField.STRING_FIRST;
                } else {
                  throw new CorruptIndexException("invalid missing value flag: " + b, input);
                }
                break;
              case LONG:
                if (b != 1) {
                  throw new CorruptIndexException("invalid missing value flag: " + b, input);
                }
                missingValue = input.readLong();
                break;
              case INT:
                if (b != 1) {
                  throw new CorruptIndexException("invalid missing value flag: " + b, input);
                }
                missingValue = input.readInt();
                break;
              case DOUBLE:
                if (b != 1) {
                  throw new CorruptIndexException("invalid missing value flag: " + b, input);
                }
                missingValue = Double.longBitsToDouble(input.readLong());
                break;
              case FLOAT:
                if (b != 1) {
                  throw new CorruptIndexException("invalid missing value flag: " + b, input);
                }
                missingValue = Float.intBitsToFloat(input.readInt());
                break;
              default:
                throw new AssertionError("unhandled sortType=" + sortType);
              }
            }
            if (missingValue != null) {
              sortFields[i].setMissingValue(missingValue);
            }
          }
          // 该对象描述了 按照某个query条件查询出来的多个doc 应该按照什么顺序排序  多个 sortField 代表doc的排序需要考虑到多个field内数据的排序结果
          indexSort = new Sort(sortFields);
        } else if (numSortFields < 0) {
          throw new CorruptIndexException("invalid index sort field count: " + numSortFields, input);
        } else {
          indexSort = null;
        }

        si = new SegmentInfo(dir, version, minVersion, segment, docCount, isCompoundFile, null, diagnostics, segmentID, attributes, indexSort);
        si.setFiles(files);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(input, priorE);
      }
      return si;
    }
  }

  /**
   * 将段的信息 写入到索引文件中   当某次 DocumentsWriterPerThread 将解析完doc并暂存在内存的数据flush后触发
   * @param dir
   * @param si
   * @param ioContext
   * @throws IOException
   */
  @Override
  public void write(Directory dir, SegmentInfo si, IOContext ioContext) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(si.name, "", Lucene70SegmentInfoFormat.SI_EXTENSION);

    try (IndexOutput output = dir.createOutput(fileName, ioContext)) {
      // Only add the file once we've successfully created it, else IFD assert can trip:
      // 将描述段信息的索引文件也加入到 segmentInfo中   这之内还包含了之前刷盘时生成的各种文件
      si.addFile(fileName);
      CodecUtil.writeIndexHeader(output,
                                   Lucene70SegmentInfoFormat.CODEC_NAME,
                                   Lucene70SegmentInfoFormat.VERSION_CURRENT,
                                   si.getId(),
                                   "");
      Version version = si.getVersion();
      if (version.major < 7) {
        throw new IllegalArgumentException("invalid major version: should be >= 7 but got: " + version.major + " segment=" + si);
      }
      // Write the Lucene version that created this segment, since 3.1
      // 写入版本号信息
      output.writeInt(version.major);
      output.writeInt(version.minor);
      output.writeInt(version.bugfix);

      // Write the min Lucene version that contributed docs to the segment, since 7.0
      // 写入兼容的版本号
      if (si.getMinVersion() != null) {
        output.writeByte((byte) 1);
        Version minVersion = si.getMinVersion();
        output.writeInt(minVersion.major);
        output.writeInt(minVersion.minor);
        output.writeInt(minVersion.bugfix);
      } else {
        output.writeByte((byte) 0);
      }

      assert version.prerelease == 0;
      // 写入最大的docId   对应 本次解析的所有doc数量-1
      output.writeInt(si.maxDoc());

      // 是否使用复合文件
      output.writeByte((byte) (si.getUseCompoundFile() ? SegmentInfo.YES : SegmentInfo.NO));
      // 写入诊断信息
      output.writeMapOfStrings(si.getDiagnostics());
      Set<String> files = si.files();
      for (String file : files) {
        if (!IndexFileNames.parseSegmentName(file).equals(si.name)) {
          throw new IllegalArgumentException("invalid files: expected segment=" + si.name + ", got=" + files);
        }
      }
      // 将写入的索引文件名 以及各种attr写入到索引文件
      output.writeSetOfStrings(files);
      output.writeMapOfStrings(si.getAttributes());

      // TODO 是否有携带 sort对象
      Sort indexSort = si.getIndexSort();
      // 获取排序字段数量 并写入
      int numSortFields = indexSort == null ? 0 : indexSort.getSort().length;
      output.writeVInt(numSortFields);
      for (int i = 0; i < numSortFields; ++i) {
        SortField sortField = indexSort.getSort()[i];
        SortField.Type sortType = sortField.getType();
        // 写入被排序的field 以及类型信息
        output.writeString(sortField.getField());
        int sortTypeID;
        switch (sortField.getType()) {
          case STRING:
            sortTypeID = 0;
            break;
          case LONG:
            sortTypeID = 1;
            break;
          case INT:
            sortTypeID = 2;
            break;
          case DOUBLE:
            sortTypeID = 3;
            break;
          case FLOAT:
            sortTypeID = 4;
            break;
          case CUSTOM:
            if (sortField instanceof SortedSetSortField) {
              sortTypeID = 5;
              sortType = SortField.Type.STRING;
            } else if (sortField instanceof SortedNumericSortField) {
              sortTypeID = 6;
              sortType = ((SortedNumericSortField) sortField).getNumericType();
            } else {
              throw new IllegalStateException("Unexpected SortedNumericSortField " + sortField);
            }
            break;
          default:
            throw new IllegalStateException("Unexpected sort type: " + sortField.getType());
        }
        output.writeVInt(sortTypeID);
        if (sortTypeID == 5) {
          SortedSetSortField ssf = (SortedSetSortField) sortField;
          if (ssf.getSelector() == SortedSetSelector.Type.MIN) {
            output.writeByte((byte) 0);
          } else if (ssf.getSelector() == SortedSetSelector.Type.MAX) {
            output.writeByte((byte) 1);
          } else if (ssf.getSelector() == SortedSetSelector.Type.MIDDLE_MIN) {
            output.writeByte((byte) 2);
          } else if (ssf.getSelector() == SortedSetSelector.Type.MIDDLE_MAX) {
            output.writeByte((byte) 3);
          } else {
            throw new IllegalStateException("Unexpected SortedSetSelector type: " + ssf.getSelector());
          }
        } else if (sortTypeID == 6) {
          SortedNumericSortField snsf = (SortedNumericSortField) sortField;
          if (snsf.getNumericType() == SortField.Type.LONG) {
            output.writeByte((byte) 0);
          } else if (snsf.getNumericType() == SortField.Type.INT) {
            output.writeByte((byte) 1);
          } else if (snsf.getNumericType() == SortField.Type.DOUBLE) {
            output.writeByte((byte) 2);
          } else if (snsf.getNumericType() == SortField.Type.FLOAT) {
            output.writeByte((byte) 3);
          } else {
            throw new IllegalStateException("Unexpected SortedNumericSelector type: " + snsf.getNumericType());
          }
          if (snsf.getSelector() == SortedNumericSelector.Type.MIN) {
            output.writeByte((byte) 0);
          } else if (snsf.getSelector() == SortedNumericSelector.Type.MAX) {
            output.writeByte((byte) 1);
          } else {
            throw new IllegalStateException("Unexpected sorted numeric selector type: " + snsf.getSelector());
          }
        }
        // 写入排序顺序
        output.writeByte((byte) (sortField.getReverse() ? 0 : 1));

        // write missing value 
        // 写入默认值
        Object missingValue = sortField.getMissingValue();
        if (missingValue == null) {
          output.writeByte((byte) 0);
        } else {
          switch(sortType) {
          case STRING:
            if (missingValue == SortField.STRING_LAST) {
              output.writeByte((byte) 1);
            } else if (missingValue == SortField.STRING_FIRST) {
              output.writeByte((byte) 2);
            } else {
              throw new AssertionError("unrecognized missing value for STRING field \"" + sortField.getField() + "\": " + missingValue);
            }
            break;
          case LONG:
            output.writeByte((byte) 1);
            output.writeLong(((Long) missingValue).longValue());
            break;
          case INT:
            output.writeByte((byte) 1);
            output.writeInt(((Integer) missingValue).intValue());
            break;
          case DOUBLE:
            output.writeByte((byte) 1);
            output.writeLong(Double.doubleToLongBits(((Double) missingValue).doubleValue()));
            break;
          case FLOAT:
            output.writeByte((byte) 1);
            output.writeInt(Float.floatToIntBits(((Float) missingValue).floatValue()));
            break;
          default:
            throw new IllegalStateException("Unexpected sort type: " + sortField.getType());
          }
        }
      }

      CodecUtil.writeFooter(output);
    }
  }

  /** File extension used to store {@link SegmentInfo}. */
  public final static String SI_EXTENSION = "si";
  static final String CODEC_NAME = "Lucene70SegmentInfo";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
}
