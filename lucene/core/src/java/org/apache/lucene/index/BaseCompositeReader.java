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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Base class for implementing {@link CompositeReader}s based on an array
 * of sub-readers. The implementing class has to add code for
 * correctly refcounting and closing the sub-readers.
 * 
 * <p>User code will most likely use {@link MultiReader} to build a
 * composite reader on a set of sub-readers (like several
 * {@link DirectoryReader}s).
 * 
 * <p> For efficiency, in this API documents are often referred to via
 * <i>document numbers</i>, non-negative integers which each name a unique
 * document in the index.  These document numbers are ephemeral -- they may change
 * as documents are added to and deleted from an index.  Clients should thus not
 * rely on a given document having the same number between sessions.
 * 
 * <p><a id="thread-safety"></a><p><b>NOTE</b>: {@link
 * IndexReader} instances are completely thread
 * safe, meaning multiple threads can call any of its methods,
 * concurrently.  If your application requires external
 * synchronization, you should <b>not</b> synchronize on the
 * <code>IndexReader</code> instance; use your own
 * (non-Lucene) objects instead.
 * @see MultiReader
 * @lucene.internal
 * 骨架类
 */
public abstract class BaseCompositeReader<R extends IndexReader> extends CompositeReader {
  /**
   * 该reader对象可以维护一组子对象
   */
  private final R[] subReaders;
  /**
   * 每个元素对应 每个子reader的读取的首个 docNo
   */
  private final int[] starts;       // 1st docno for each reader
  /**
   * 所有子节点的doc总和
   */
  private final int maxDoc;
  /**
   * 当前已经组装了多少个doc
   */
  private AtomicInteger numDocs = new AtomicInteger(-1); // computed lazily

  /** List view solely for {@link #getSequentialSubReaders()},
   * for effectiveness the array is used internally. */
  // 对应 subReaders 列表形式
  private final List<R> subReadersList;

  /**
   * Constructs a {@code BaseCompositeReader} on the given subReaders.
   * @param subReaders the wrapped sub-readers. This array is returned by
   * {@link #getSequentialSubReaders} and used to resolve the correct
   * subreader for docID-based methods. <b>Please note:</b> This array is <b>not</b>
   * cloned and not protected for modification, the subclass is responsible 
   * to do this.
   * 使用一组reader 对象进行初始化
   */
  protected BaseCompositeReader(R[] subReaders) throws IOException {
    this.subReaders = subReaders;
    this.subReadersList = Collections.unmodifiableList(Arrays.asList(subReaders));
    // 对start数组进行扩容
    starts = new int[subReaders.length + 1];    // build starts array
    long maxDoc = 0;
    for (int i = 0; i < subReaders.length; i++) {
      starts[i] = (int) maxDoc;
      final IndexReader r = subReaders[i];
      // 该值会不断累加 并设置到 starts 对应的位置上
      // 这里采用累加的方式是为了便于之后使用二分查找
      maxDoc += r.maxDoc();      // compute maxDocs
      // 为子reader 建立关联关系
      r.registerParentReader(this);
    }

    // doc 数量超过限制 抛出异常
    if (maxDoc > IndexWriter.getActualMaxDocs()) {
      if (this instanceof DirectoryReader) {
        // A single index has too many documents and it is corrupt (IndexWriter prevents this as of LUCENE-6299)
        throw new CorruptIndexException("Too many documents: an index cannot exceed " + IndexWriter.getActualMaxDocs() + " but readers have total maxDoc=" + maxDoc, Arrays.toString(subReaders));
      } else {
        // Caller is building a MultiReader and it has too many documents; this case is just illegal arguments:
        throw new IllegalArgumentException("Too many documents: composite IndexReaders cannot exceed " + IndexWriter.getActualMaxDocs() + " but readers have total maxDoc=" + maxDoc);
      }
    }

    // 记录最后的 maxDoc
    this.maxDoc = Math.toIntExact(maxDoc);
    starts[subReaders.length] = this.maxDoc;
  }

  /**
   * 通过传入一个 docId 找到对应的reader 之后从reader中返回目标字段
   * @param docID
   * @return
   * @throws IOException
   */
  @Override
  public final Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    // 找到目标reader的下标
    final int i = readerIndex(docID);        // find subreader num
    // 转发给对应的reader 查询词  注意传入参数时 由绝对偏移量变成了相对偏移量
    return subReaders[i].getTermVectors(docID - starts[i]); // dispatch to subreader
  }

  /**
   * 返回当前所有的doc
   * @return
   */
  @Override
  public final int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    // We want to compute numDocs() lazily so that creating a wrapper that hides
    // some documents isn't slow at wrapping time, but on the first time that
    // numDocs() is called. This can help as there are lots of use-cases of a
    // reader that don't involve calling numDocs().
    // However it's not crucial to make sure that we don't call numDocs() more
    // than once on the sub readers, since they likely cache numDocs() anyway,
    // hence the opaque read.
    // http://gee.cs.oswego.edu/dl/html/j9mm.html#opaquesec.
    // 就当获取当前值吧
    int numDocs = this.numDocs.getOpaque();
    if (numDocs == -1) {
      numDocs = 0;
      for (IndexReader r : subReaders) {
        // 本对象的doc数量就是将每个子节点的组合起来    CompositeReader相当于一个树中的父节点  子节点应该就是 LeafReader
        numDocs += r.numDocs();
      }
      assert numDocs >= 0;
      // 采用缓存的方式
      this.numDocs.set(numDocs);
    }
    return numDocs;
  }

  @Override
  public final int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return maxDoc;
  }

  /**
   * 通过docId 找到下面对应的reader 并转发到目标对象上
   * @param docID
   * @param visitor  当定位到doc后 使用该对象处理doc
   * @throws IOException
   */
  @Override
  public final void document(int docID, StoredFieldVisitor visitor) throws IOException {
    ensureOpen();
    final int i = readerIndex(docID);                          // find subreader num
    subReaders[i].document(docID - starts[i], visitor);    // dispatch to subreader
  }

  /**
   * 计算某个
   * @param term
   * @return
   * @throws IOException
   */
  @Override
  public final int docFreq(Term term) throws IOException {
    ensureOpen();
    int total = 0;          // sum freqs in subreaders
    // 累加目标词在所有reader出现的次数总和
    for (int i = 0; i < subReaders.length; i++) {
      int sub = subReaders[i].docFreq(term);
      assert sub >= 0;
      assert sub <= subReaders[i].getDocCount(term.field());
      total += sub;
    }
    return total;
  }
  
  @Override
  public final long totalTermFreq(Term term) throws IOException {
    ensureOpen();
    long total = 0;        // sum freqs in subreaders
    for (int i = 0; i < subReaders.length; i++) {
      long sub = subReaders[i].totalTermFreq(term);
      assert sub >= 0;
      assert sub <= subReaders[i].getSumTotalTermFreq(term.field());
      total += sub;
    }
    return total;
  }
  
  @Override
  public final long getSumDocFreq(String field) throws IOException {
    ensureOpen();
    long total = 0; // sum doc freqs in subreaders
    for (R reader : subReaders) {
      long sub = reader.getSumDocFreq(field);
      assert sub >= 0;
      assert sub <= reader.getSumTotalTermFreq(field);
      total += sub;
    }
    return total;
  }
  
  @Override
  public final int getDocCount(String field) throws IOException {
    ensureOpen();
    int total = 0; // sum doc counts in subreaders
    for (R reader : subReaders) {
      int sub = reader.getDocCount(field);
      assert sub >= 0;
      assert sub <= reader.maxDoc();
      total += sub;
    }
    return total;
  }

  @Override
  public final long getSumTotalTermFreq(String field) throws IOException {
    ensureOpen();
    long total = 0; // sum doc total term freqs in subreaders
    for (R reader : subReaders) {
      long sub = reader.getSumTotalTermFreq(field);
      assert sub >= 0;
      assert sub >= reader.getSumDocFreq(field);
      total += sub;
    }
    return total;
  }
  
  /** Helper method for subclasses to get the corresponding reader for a doc ID */
  // 通过docId 定位到某个reader
  protected final int readerIndex(int docID) {
    if (docID < 0 || docID >= maxDoc) {
      throw new IllegalArgumentException("docID must be >= 0 and < maxDoc=" + maxDoc + " (got docID=" + docID + ")");
    }
    return ReaderUtil.subIndex(docID, this.starts);
  }
  
  /** Helper method for subclasses to get the docBase of the given sub-reader index. */
  protected final int readerBase(int readerIndex) {
    if (readerIndex < 0 || readerIndex >= subReaders.length) {
      throw new IllegalArgumentException("readerIndex must be >= 0 and < getSequentialSubReaders().size()");
    }
    return this.starts[readerIndex];
  }
  
  @Override
  protected final List<? extends R> getSequentialSubReaders() {
    return subReadersList;
  }
}
