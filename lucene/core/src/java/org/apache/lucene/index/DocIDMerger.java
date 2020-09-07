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
import java.util.List;

import org.apache.lucene.search.DocIdSetIterator; // javadocs
import org.apache.lucene.util.PriorityQueue;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Utility class to help merging documents from sub-readers according to either simple
 *  concatenated (unsorted) order, or by a specified index-time sort, skipping
 *  deleted documents and remapping non-deleted documents. */

public abstract class DocIDMerger<T extends DocIDMerger.Sub> {

  /** Represents one sub-reader being merged */
  public static abstract class Sub {
    /** Mapped doc ID */
    // 映射到的merge后的docId
    public int mappedDocID;

    /**
     * 作用是将当前reader下的doc 映射到merge后的doc  当传入已经被del的doc时 返回-1
     */
    final MergeState.DocMap docMap;

    /** Sole constructor */
    public Sub(MergeState.DocMap docMap) {
      this.docMap = docMap;
    }

    /** Returns the next document ID from this sub reader, and {@link DocIdSetIterator#NO_MORE_DOCS} when done */
    public abstract int nextDoc() throws IOException;
  }

  /** Construct this from the provided subs, specifying the maximum sub count
   * @param maxCount 默认就是 sub.size()
   * 将多个Sub对象组合
   * */
  public static <T extends DocIDMerger.Sub> DocIDMerger<T> of(List<T> subs, int maxCount, boolean indexIsSorted) throws IOException {
    if (indexIsSorted && maxCount > 1) {
      // 代表需要排序
      return new SortedDocIDMerger<>(subs, maxCount);
    } else {
      // 代表不需要做排序吧 挨个数据所有元素即可
      return new SequentialDocIDMerger<>(subs);
    }
  }

  /** Construct this from the provided subs */
  // 将多个 Sub对象合并
  public static <T extends DocIDMerger.Sub> DocIDMerger<T> of(List<T> subs, boolean indexIsSorted) throws IOException {
    return of(subs, subs.size(), indexIsSorted);
  }

  /** Reuse API, currently only used by postings during merge */
  public abstract void reset() throws IOException;

  /** Returns null when done.
   *  <b>NOTE:</b> after the iterator has exhausted you should not call this
   *  method, as it may result in unpredicted behavior. */
  public abstract T next() throws IOException;

  private DocIDMerger() {}

  /**
   * 挨个输出所有元素 不需要做额外处理
   * @param <T>
   */
  private static class SequentialDocIDMerger<T extends DocIDMerger.Sub> extends DocIDMerger<T> {

    private final List<T> subs;
    /**
     * 当前正被引用的reader
     */
    private T current;

    /**
     * 对应 subs 的下标
     */
    private int nextIndex;

    private SequentialDocIDMerger(List<T> subs) throws IOException {
      this.subs = subs;
      reset();
    }

    @Override
    public void reset() throws IOException {
      if (subs.size() > 0) {
        // 每次重置的时候 都会切换成某个reader对象
        current = subs.get(0);
        nextIndex = 1;
      } else {
        current = null;
        nextIndex = 0;
      }
    }

    @Override
    public T next() throws IOException {
      while (true) {
        int docID = current.nextDoc();
        // 代表此时reader已经读取完了  切换到下一个reader
        if (docID == NO_MORE_DOCS) {
          if (nextIndex == subs.size()) {
            current = null;
            return null;
          }
          current = subs.get(nextIndex);
          nextIndex++;
          continue;
        }

        // 这里会转换成 merge后的 globalDocId
        int mappedDocID = current.docMap.get(docID);
        // 等于-1时 该doc的数据会被忽略  也就是比如 term信息在合并时 之前被标记成删除的doc 在合并时 就会被忽略了
        if (mappedDocID != -1) {
          current.mappedDocID = mappedDocID;
          return current;
        }
      }
    }

  }

  /**
   * 通过优先队列确保每次读取出来的数据是整个readerList中的最小值
   * @param <T>
   */
  private static class SortedDocIDMerger<T extends DocIDMerger.Sub> extends DocIDMerger<T> {

    private final List<T> subs;
    private final PriorityQueue<T> queue;

    private SortedDocIDMerger(List<T> subs, int maxCount) throws IOException {
      this.subs = subs;
      queue = new PriorityQueue<T>(maxCount) {
        @Override
        protected boolean lessThan(Sub a, Sub b) {
          assert a.mappedDocID != b.mappedDocID;
          return a.mappedDocID < b.mappedDocID;
        }
      };
      reset();
    }

    /**
     * 将数据填充到queue中
     * @throws IOException
     */
    @Override
    public void reset() throws IOException {
      // caller may not have fully consumed the queue:
      queue.clear();
      boolean first = true;
      for(T sub : subs) {
        if (first) {
          // by setting mappedDocID = -1, this entry is guaranteed to be the top of the queue
          // so the first call to next() will advance it
          sub.mappedDocID = -1;
          first = false;
        } else {
          int mappedDocID;
          // 取出每个 reader的首个 globalDocId
          while (true) {
            int docID = sub.nextDoc();
            if (docID == NO_MORE_DOCS) {
              mappedDocID = NO_MORE_DOCS;
              break;
            }
            mappedDocID = sub.docMap.get(docID);
            if (mappedDocID != -1) {
              break;
            }
          }
          if (mappedDocID == NO_MORE_DOCS) {
            // all docs in this sub were deleted; do not add it to the queue!
            continue;
          }
          sub.mappedDocID = mappedDocID;
        }
        queue.add(sub);
      }
    }

    /**
     * 弹出所有reader中的下一个值  同时更新top
     * @return
     * @throws IOException
     */
    @Override
    public T next() throws IOException {
      T top = queue.top();

      while (true) {
        int docID = top.nextDoc();
        if (docID == NO_MORE_DOCS) {
          queue.pop();
          top = queue.top();
          break;
        }
        int mappedDocID = top.docMap.get(docID);
        // -1 代表该 doc 已经被删除
        if (mappedDocID == -1) {
          // doc was deleted
          continue;
        } else {
          top.mappedDocID = mappedDocID;
          top = queue.updateTop();
          break;
        }
      }

      return top;
    }
  }

}
