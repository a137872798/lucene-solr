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
import java.util.Comparator;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.TimSorter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Sorts documents of a given index by returning a permutation on the document
 * IDs.
 * @lucene.experimental
 */
final class Sorter {

  /**
   * 该对象内部 包含多个 SortField 定义了排序的规则
   */
  final Sort sort;
  
  /** Creates a new Sorter to sort the index with {@code sort} */
  Sorter(Sort sort) {
    // 描述排序规则的对象 类型不能是 Score 为啥???
    if (sort.needsScores()) {
      throw new IllegalArgumentException("Cannot sort an index with a Sort that refers to the relevance score");
    }
    this.sort = sort;
  }

  /**
   * A permutation of doc IDs. For every document ID between <code>0</code> and
   * {@link IndexReader#maxDoc()}, <code>oldToNew(newToOld(docID))</code> must
   * return <code>docID</code>.
   * 代表排序完成后的容器
   */
  static abstract class DocMap {

    /**
     * Given a doc ID from the original index, return its ordinal in the
     *  sorted index.
     *  传入docId 返回此时的顺序
     */
    abstract int oldToNew(int docID);

    /**
     * Given the ordinal of a doc ID, return its doc ID in the original index.
     * 传入 ord 返回排序后的docId
     */
    abstract int newToOld(int docID);

    /** Return the number of documents in this map. This must be equal to the
     *  {@link org.apache.lucene.index.LeafReader#maxDoc() number of documents} of the
     *  {@link org.apache.lucene.index.LeafReader} which is sorted. */
    abstract int size();
  }

  /** Check consistency of a {@link DocMap}, useful for assertions. */
  // 一致性检测    也就是正反向索引的结果是否一致
  static boolean isConsistent(DocMap docMap) {
    final int maxDoc = docMap.size();
    for (int i = 0; i < maxDoc; ++i) {
      final int newID = docMap.oldToNew(i);
      final int oldID = docMap.newToOld(newID);
      assert newID >= 0 && newID < maxDoc : "doc IDs must be in [0-" + maxDoc + "[, got " + newID;
      assert i == oldID : "mapping is inconsistent: " + i + " --oldToNew--> " + newID + " --newToOld--> " + oldID;
      if (i != oldID || newID < 0 || newID >= maxDoc) {
        return false;
      }
    }
    return true;
  }

  /** A comparator of doc IDs. */
  // 这里很可能是先通过 docId 获得一个别的什么属性 然后按照那个属性进行排序
  static abstract class DocComparator {

    /** Compare docID1 against docID2. The contract for the return value is the
     *  same as {@link Comparator#compare(Object, Object)}. */
    public abstract int compare(int docID1, int docID2);
  }

  /**
   * 该对象 基于 TimSort 算法进行排序  排序算法先不看吧 毕竟已经有这么多种现成的实现了 无非就是性能差距
   * 推测 TimSort 可能就是 （归并排序+快速排序）   因为快速排序受原始数组影响 在最坏的情况下 性能不是 O(log)
   */
  private static final class DocValueSorter extends TimSorter {

    /**
     * 存储原顺序的容器
     */
    private final int[] docs;
    /**
     * 该对象专门抽象出 比较的逻辑
     */
    private final Sorter.DocComparator comparator;
    /**
     * 存放中转数据的临时数组
     */
    private final int[] tmp;

    /**
     * @param docs
     * @param comparator
     */
    DocValueSorter(int[] docs, Sorter.DocComparator comparator) {
      super(docs.length / 64);
      this.docs = docs;
      this.comparator = comparator;
      tmp = new int[docs.length / 64];
    }
    
    @Override
    protected int compare(int i, int j) {
      return comparator.compare(docs[i], docs[j]);
    }
    
    @Override
    protected void swap(int i, int j) {
      int tmpDoc = docs[i];
      docs[i] = docs[j];
      docs[j] = tmpDoc;
    }

    @Override
    protected void copy(int src, int dest) {
      docs[dest] = docs[src];
    }

    @Override
    protected void save(int i, int len) {
      System.arraycopy(docs, i, tmp, 0, len);
    }

    @Override
    protected void restore(int i, int j) {
      docs[j] = tmp[i];
    }

    @Override
    protected int compareSaved(int i, int j) {
      return comparator.compare(tmp[i], docs[j]);
    }
  }

  /**
   *  Computes the old-to-new permutation over the given comparator.
   *  生成 新旧docId的映射容器
   */
  private static Sorter.DocMap sort(final int maxDoc, DocComparator comparator) {
    // check if the index is sorted
    boolean sorted = true;
    for (int i = 1; i < maxDoc; ++i) {
      // 排序结果如果与当前docId 顺序一致 那么不需要生成映射容器
      if (comparator.compare(i-1, i) > 0) {
        sorted = false;
        break;
      }
    }
    if (sorted) {
      return null;
    }

    // sort doc IDs
    // 先填充旧顺序的容器
    final int[] docs = new int[maxDoc];
    for (int i = 0; i < maxDoc; i++) {
      docs[i] = i;
    }

    DocValueSorter sorter = new DocValueSorter(docs, comparator);
    // It can be common to sort a reader, add docs, sort it again, ... and in
    // that case timSort can save a lot of time
    // 进行重排序  此时 docs已经完成排序了
    sorter.sort(0, docs.length); // docs is now the newToOld mapping

    // The reason why we use MonotonicAppendingLongBuffer here is that it
    // wastes very little memory if the index is in random order but can save
    // a lot of memory if the index is already "almost" sorted
    // 先通过减去期望值 使得数据尽可能平均 之后再基于min 做差值存储   nb
    final PackedLongValues.Builder newToOldBuilder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    for (int i = 0; i < maxDoc; ++i) {
      newToOldBuilder.add(docs[i]);
    }
    // 这里存储的是 排序后的结果  以顺序为下标 指向docId
    final PackedLongValues newToOld = newToOldBuilder.build();

    // invert the docs mapping:
    for (int i = 0; i < maxDoc; ++i) {
      // 创建反向映射  以docId为下标 指向此时的顺序
      docs[(int) newToOld.get(i)] = i;
    } // docs is now the oldToNew mapping

    // 将反向数据也存储到 packed中
    final PackedLongValues.Builder oldToNewBuilder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    for (int i = 0; i < maxDoc; ++i) {
      oldToNewBuilder.add(docs[i]);
    }
    final PackedLongValues oldToNew = oldToNewBuilder.build();
    
    return new Sorter.DocMap() {

      @Override
      public int oldToNew(int docID) {
        return (int) oldToNew.get(docID);
      }

      @Override
      public int newToOld(int docID) {
        return (int) newToOld.get(docID);
      }

      @Override
      public int size() {
        return maxDoc;
      }
    };
  }

  /** Returns the native sort type for {@link SortedSetSortField} and {@link SortedNumericSortField},
   * {@link SortField#getType()} otherwise
   * 排序类型 (基于什么原则进行排序)
   * */
  static SortField.Type getSortFieldType(SortField sortField) {
    // 主要是为了这2种特殊类型服务的
    if (sortField instanceof SortedSetSortField) {
      return SortField.Type.STRING;
    } else if (sortField instanceof SortedNumericSortField) {
      return ((SortedNumericSortField) sortField).getNumericType();
    } else {
      return sortField.getType();
    }
  }

  /** Wraps a {@link SortedNumericDocValues} as a single-valued view if the field is an instance of {@link SortedNumericSortField},
   * returns {@link NumericDocValues} for the field otherwise. */
  static NumericDocValues getOrWrapNumeric(LeafReader reader, SortField sortField) throws IOException {
    if (sortField instanceof SortedNumericSortField) {
      SortedNumericSortField sf = (SortedNumericSortField) sortField;
      // 转换成NumericDocValues
      return SortedNumericSelector.wrap(DocValues.getSortedNumeric(reader, sf.getField()), sf.getSelector(), sf.getNumericType());
    } else {
      return DocValues.getNumeric(reader, sortField.getField());
    }
  }

  /** Wraps a {@link SortedSetDocValues} as a single-valued view if the field is an instance of {@link SortedSetSortField},
   * returns {@link SortedDocValues} for the field otherwise. */
  static SortedDocValues getOrWrapSorted(LeafReader reader, SortField sortField) throws IOException {

    // 将 SortedSetSortField 转换成 SortedDocValues
    if (sortField instanceof SortedSetSortField) {
      SortedSetSortField sf = (SortedSetSortField) sortField;
      return SortedSetSelector.wrap(DocValues.getSortedSet(reader, sf.getField()), sf.getSelector());
    } else {
      // 先看普通情况
      return DocValues.getSorted(reader, sortField.getField());
    }
  }

  /**
   * 基于某个排序字段 构建一个com 对象
   * @param reader
   * @param sortField
   * @return
   * @throws IOException
   */
  static DocComparator getDocComparator(LeafReader reader, SortField sortField) throws IOException {
    return getDocComparator(reader.maxDoc(), sortField,
        () -> getOrWrapSorted(reader, sortField),
        () -> getOrWrapNumeric(reader, sortField));
  }

  interface NumericDocValuesSupplier {
    NumericDocValues get() throws IOException;
  }

  interface SortedDocValuesSupplier {
    SortedDocValues get() throws IOException;
  }

  /** We cannot use the {@link FieldComparator} API because that API requires that you send it docIDs in order.  Note that this API
   *  allocates arrays[maxDoc] to hold the native values needed for comparison, but 1) they are transient (only alive while sorting this one
   *  segment), and 2) in the typical index sorting case, they are only used to sort newly flushed segments, which will be smaller than
   *  merged segments.
   * @param maxDoc 本次处理的最大doc是多少
   * @param sortField  该对象定义了如何根据该field进行排序
   * @param numericProvider  返回该field 出现在哪些doc 下 以及对应的value值
   */
  static DocComparator getDocComparator(int maxDoc,
                                        SortField sortField,
                                        SortedDocValuesSupplier sortedProvider,
                                        NumericDocValuesSupplier numericProvider) throws IOException {

    // 正序or倒序
    final int reverseMul = sortField.getReverse() ? -1 : 1;
    // 获取本次的排序类型
    final SortField.Type sortType = getSortFieldType(sortField);

    switch(sortType) {

      case STRING:
      {
        final SortedDocValues sorted = sortedProvider.get();
        final int missingOrd;
        // 如果默认值是最小的String  那么 默认ord 就是max 实际上就是最小值
        if (sortField.getMissingValue() == SortField.STRING_LAST) {
          missingOrd = Integer.MAX_VALUE;
        } else {
          missingOrd = Integer.MIN_VALUE;
        }

        final int[] ords = new int[maxDoc];
        // 先全部使用默认顺序进行填充
        Arrays.fill(ords, missingOrd);
        int docID;
        // 将field在该doc下对应的field.value 对应的ord填入到数组中  value可能会重复  那么此时ord是一样的
        while ((docID = sorted.nextDoc()) != NO_MORE_DOCS) {
          ords[docID] = sorted.ordValue();
        }

        // 如果设置了 ord值 按照ord 排序
        return new DocComparator() {
          @Override
          public int compare(int docID1, int docID2) {
            return reverseMul * Integer.compare(ords[docID1], ords[docID2]);
          }
        };
      }

      case LONG:
      {
        final NumericDocValues dvs = numericProvider.get();
        long[] values = new long[maxDoc];
        // 预先为所有槽设置默认值
        if (sortField.getMissingValue() != null) {
          Arrays.fill(values, (Long) sortField.getMissingValue());
        }
        // 在对应的槽上设置对应的值  此时还没有进行排序
        while (true) {
          int docID = dvs.nextDoc();
          if (docID == NO_MORE_DOCS) {
            break;
          }
          values[docID] = dvs.longValue();
        }

        // 按照long进行排序
        return new DocComparator() {
          @Override
          public int compare(int docID1, int docID2) {
            return reverseMul * Long.compare(values[docID1], values[docID2]);
          }
        };
      }

      // 剩余数字类型排序套路都是一致的

      case INT:
      {
        final NumericDocValues dvs = numericProvider.get();
        int[] values = new int[maxDoc];
        if (sortField.getMissingValue() != null) {
          Arrays.fill(values, (Integer) sortField.getMissingValue());
        }

        while (true) {
          int docID = dvs.nextDoc();
          if (docID == NO_MORE_DOCS) {
            break;
          }
          values[docID] = (int) dvs.longValue();
        }

        return new DocComparator() {
          @Override
          public int compare(int docID1, int docID2) {
            return reverseMul * Integer.compare(values[docID1], values[docID2]);
          }
        };
      }

      case DOUBLE:
      {
        final NumericDocValues dvs = numericProvider.get();
        double[] values = new double[maxDoc];
        if (sortField.getMissingValue() != null) {
          Arrays.fill(values, (Double) sortField.getMissingValue());
        }
        while (true) {
          int docID = dvs.nextDoc();
          if (docID == NO_MORE_DOCS) {
            break;
          }
          values[docID] = Double.longBitsToDouble(dvs.longValue());
        }

        return new DocComparator() {
          @Override
          public int compare(int docID1, int docID2) {
            return reverseMul * Double.compare(values[docID1], values[docID2]);
          }
        };
      }

      case FLOAT:
      {
        final NumericDocValues dvs = numericProvider.get();
        float[] values = new float[maxDoc];
        if (sortField.getMissingValue() != null) {
          Arrays.fill(values, (Float) sortField.getMissingValue());
        }
        while (true) {
          int docID = dvs.nextDoc();
          if (docID == NO_MORE_DOCS) {
            break;
          }
          values[docID] = Float.intBitsToFloat((int) dvs.longValue());
        }

        return new DocComparator() {
          @Override
          public int compare(int docID1, int docID2) {
            return reverseMul * Float.compare(values[docID1], values[docID2]);
          }
        };
      }

      default:
        throw new IllegalArgumentException("unhandled SortField.getType()=" + sortField.getType());
    }
  }


  /**
   * Returns a mapping from the old document ID to its new location in the
   * sorted index. Implementations can use the auxiliary
   * {@link #sort(int, DocComparator)} to compute the old-to-new permutation
   * given a list of documents and their corresponding values.
   * <p>
   * A return value of <code>null</code> is allowed and means that
   * <code>reader</code> is already sorted.
   * <p>
   * <b>NOTE:</b> deleted documents are expected to appear in the mapping as
   * well, they will however be marked as deleted in the sorted view.
   * 这里 基于reader中的数据 生成comparator 对象
   */
  DocMap sort(LeafReader reader) throws IOException {
    SortField fields[] = sort.getSort();
    // 每个 排序对象都可以生成一个 com对象  而排序结果是这些排序对象共同作用的结果
    final DocComparator comparators[] = new DocComparator[fields.length];

    for (int i = 0; i < fields.length; i++) {
      comparators[i] = getDocComparator(reader, fields[i]);
    }
    return sort(reader.maxDoc(), comparators);
  }


  /**
   * 将一组为doc重排序的cmp对象 合并 得到一个唯一的排序规则后 将此时的doc按照该排序规则生成映射容器
   * @param maxDoc
   * @param comparators  越前面的cmp 优先级越高
   * @return
   * @throws IOException
   */
  DocMap sort(int maxDoc, DocComparator[] comparators) throws IOException {
    final DocComparator comparator = new DocComparator() {
      @Override
      public int compare(int docID1, int docID2) {
        for (int i = 0; i < comparators.length; i++) {
          int comp = comparators[i].compare(docID1, docID2);
          if (comp != 0) {
            return comp;
          }
        }
        // 按照其他情况排序结果都一致时 按照docId进行排序
        return Integer.compare(docID1, docID2); // docid order tiebreak
      }
    };

    return sort(maxDoc, comparator);
  }

  /**
   * Returns the identifier of this {@link Sorter}.
   * <p>This identifier is similar to {@link Object#hashCode()} and should be
   * chosen so that two instances of this class that sort documents likewise
   * will have the same identifier. On the contrary, this identifier should be
   * different on different {@link Sort sorts}.
   */
  public String getID() {
    return sort.toString();
  }

  @Override
  public String toString() {
    return getID();
  }
  
}
