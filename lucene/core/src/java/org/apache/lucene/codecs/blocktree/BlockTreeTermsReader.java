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
package org.apache.lucene.codecs.blocktree;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.Outputs;

/** A block-based terms index and dictionary that assigns
 *  terms to variable length blocks according to how they
 *  share prefixes.  The terms index is a prefix trie
 *  whose leaves are term blocks.  The advantage of this
 *  approach is that seekExact is often able to
 *  determine a term cannot exist without doing any IO, and
 *  intersection with Automata is very fast.  Note that this
 *  terms dictionary has its own fixed terms index (ie, it
 *  does not support a pluggable terms index
 *  implementation).
 *
 *  <p><b>NOTE</b>: this terms dictionary supports
 *  min/maxItemsPerBlock during indexing to control how
 *  much memory the terms index uses.</p>
 *
 *  <p>The data structure used by this implementation is very
 *  similar to a burst trie
 *  (http://citeseer.ist.psu.edu/viewdoc/summary?doi=10.1.1.18.3499),
 *  but with added logic to break up too-large blocks of all
 *  terms sharing a given prefix into smaller ones.</p>
 *
 *  <p>Use {@link org.apache.lucene.index.CheckIndex} with the <code>-verbose</code>
 *  option to see summary statistics on the blocks in the
 *  dictionary.
 *
 *  See {@link BlockTreeTermsWriter}.
 *
 * @lucene.experimental
 * 该对象基于 term词典读取数据
 */

public final class BlockTreeTermsReader extends FieldsProducer {

  static final Outputs<BytesRef> FST_OUTPUTS = ByteSequenceOutputs.getSingleton();
  
  static final BytesRef NO_OUTPUT = FST_OUTPUTS.getNoOutput();

  static final int OUTPUT_FLAGS_NUM_BITS = 2;
  static final int OUTPUT_FLAGS_MASK = 0x3;
  static final int OUTPUT_FLAG_IS_FLOOR = 0x1;
  static final int OUTPUT_FLAG_HAS_TERMS = 0x2;

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tim";
  final static String TERMS_CODEC_NAME = "BlockTreeTermsDict";

  /** Initial terms format. */
  public static final int VERSION_START = 3;

  /** The long[] + byte[] metadata has been replaced with a single byte[]. */
  public static final int VERSION_META_LONGS_REMOVED = 4;

  /** Suffixes are compressed to save space. */
  public static final int VERSION_COMPRESSED_SUFFIXES = 5;

  /** Current terms format. */
  public static final int VERSION_CURRENT = VERSION_COMPRESSED_SUFFIXES;

  /** Extension of terms index file */
  static final String TERMS_INDEX_EXTENSION = "tip";
  final static String TERMS_INDEX_CODEC_NAME = "BlockTreeTermsIndex";

  // Open input to the main terms dict file (_X.tib)
  final IndexInput termsIn;
  // Open input to the terms index file (_X.tip)
  final IndexInput indexIn;

  //private static final boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  // Reads the terms dict entries, to gather state to
  // produce DocsEnum on demand
  final PostingsReaderBase postingsReader;

  /**
   * 之前通过 BlockTreeTermsWriter 写入的所有field 信息 会被本对象读取出来 并设置到map中
   */
  private final Map<String,FieldReader> fieldMap;
  private final List<String> fieldList;

  final String segment;
  
  final int version;

  /** Sole constructor. */
  public BlockTreeTermsReader(PostingsReaderBase postingsReader, SegmentReadState state) throws IOException {
    boolean success = false;
    
    this.postingsReader = postingsReader;
    this.segment = state.segmentInfo.name;
    
    String termsName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_EXTENSION);
    try {
      termsIn = state.directory.openInput(termsName, state.context);
      version = CodecUtil.checkIndexHeader(termsIn, TERMS_CODEC_NAME, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

      String indexName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_INDEX_EXTENSION);
      indexIn = state.directory.openInput(indexName, state.context);
      CodecUtil.checkIndexHeader(indexIn, TERMS_INDEX_CODEC_NAME, version, version, state.segmentInfo.getId(), state.segmentSuffix);

      // Have PostingsReader init itself
      postingsReader.init(termsIn, state);

      // Verifying the checksum against all bytes would be too costly, but for now we at least
      // verify proper structure of the checksum footer. This is cheap and can detect some forms
      // of corruption such as file truncation.
      CodecUtil.retrieveChecksum(indexIn);
      CodecUtil.retrieveChecksum(termsIn);

      // Read per-field details
      // 这里先将位置定位到了开始写入field信息的位置
      seekDir(termsIn);
      seekDir(indexIn);

      // 此时读取的是  field总数    该值的写入是在 BlockTreeTermsWriter.close() 触发的
      final int numFields = termsIn.readVInt();
      if (numFields < 0) {
        throw new CorruptIndexException("invalid numFields: " + numFields, termsIn);
      }
      fieldMap = new HashMap<>((int) (numFields / 0.75f) + 1);
      for (int i = 0; i < numFields; ++i) {
        /**
         * 对应 BlockTreeTermsWriter的这段代码
         *     for (FieldMetaData field : fields) {
         *                 //System.out.println("  field " + field.fieldInfo.name + " " + field.numTerms + " terms");
         *                 termsOut.writeVInt(field.fieldInfo.number);
         *                 assert field.numTerms > 0;
         *                 termsOut.writeVLong(field.numTerms);
         *                 termsOut.writeVInt(field.rootCode.length);
         *                 termsOut.writeBytes(field.rootCode.bytes, field.rootCode.offset, field.rootCode.length);
         *                 assert field.fieldInfo.getIndexOptions() != IndexOptions.NONE;
         *                 if (field.fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
         *                     termsOut.writeVLong(field.sumTotalTermFreq);
         *                 }
         *                 termsOut.writeVLong(field.sumDocFreq);
         *                 termsOut.writeVInt(field.docCount);
         *                 indexOut.writeVLong(field.indexStartFP);
         *                 writeBytesRef(termsOut, field.minTerm);
         *                 writeBytesRef(termsOut, field.maxTerm);
         *             }
         */
        final int field = termsIn.readVInt();
        final long numTerms = termsIn.readVLong();
        if (numTerms <= 0) {
          throw new CorruptIndexException("Illegal numTerms for field number: " + field, termsIn);
        }
        // 先读取rootCode长度 在根据长度信息读取byte  （rootCode是 在将所有term信息抽取整合成一个fst对象后 (作为term词典) 该fst下 emptyString 对应的权重）
        final BytesRef rootCode = readBytesRef(termsIn);
        // 因为在SegmentCoreReaders(维护所有段索引文件的对象)中最先读取的就是 fieldInfo的信息 所以现在可以知道某个fieldInfo 下 会将哪些信息写入到索引文件
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        if (fieldInfo == null) {
          throw new CorruptIndexException("invalid field number: " + field, termsIn);
        }
        // 因为 sumTotalTermFreq sumDocFreq 类型都是long 所以直接读取 如果 indexOptions 是 DOC 那么之前读取的值就作为 sumDocFreq 而不是 sumTotalTermFreq
        final long sumTotalTermFreq = termsIn.readVLong();
        // when frequencies are omitted, sumDocFreq=sumTotalTermFreq and only one value is written.
        final long sumDocFreq = fieldInfo.getIndexOptions() == IndexOptions.DOCS ? sumTotalTermFreq : termsIn.readVLong();
        final int docCount = termsIn.readVInt();
        if (version < VERSION_META_LONGS_REMOVED) {
          final int longsSize = termsIn.readVInt();
          if (longsSize < 0) {
            throw new CorruptIndexException("invalid longsSize for field: " + fieldInfo.name + ", longsSize=" + longsSize, termsIn);
          }
        }
        BytesRef minTerm = readBytesRef(termsIn);
        BytesRef maxTerm = readBytesRef(termsIn);
        // 以上已经读取完所有写入termsIn中 有关 fieldInfo的数据了

        if (docCount < 0 || docCount > state.segmentInfo.maxDoc()) { // #docs with field must be <= #docs
          throw new CorruptIndexException("invalid docCount: " + docCount + " maxDoc: " + state.segmentInfo.maxDoc(), termsIn);
        }
        if (sumDocFreq < docCount) {  // #postings must be >= #docs with field
          throw new CorruptIndexException("invalid sumDocFreq: " + sumDocFreq + " docCount: " + docCount, termsIn);
        }
        if (sumTotalTermFreq < sumDocFreq) { // #positions must be >= #postings
          throw new CorruptIndexException("invalid sumTotalTermFreq: " + sumTotalTermFreq + " sumDocFreq: " + sumDocFreq, termsIn);
        }

        // 每当某个 FieldWriter 处理完某个 field所有的term时 就会将此时 tip文件此时的偏移量写入   term词典 或者说 fst 就是写入到 tip文件中的
        final long indexStartFP = indexIn.readVLong();
        // 反向生成reader对象
        FieldReader previous = fieldMap.put(fieldInfo.name,
                                          new FieldReader(this, fieldInfo, numTerms, rootCode, sumTotalTermFreq, sumDocFreq, docCount,
                                                          indexStartFP, indexIn, minTerm, maxTerm));
        if (previous != null) {
          throw new CorruptIndexException("duplicate field: " + fieldInfo.name, termsIn);
        }
      }
      List<String> fieldList = new ArrayList<>(fieldMap.keySet());
      fieldList.sort(null);
      this.fieldList = Collections.unmodifiableList(fieldList);
      success = true;
    } finally {
      if (!success) {
        // this.close() will close in:
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  private static BytesRef readBytesRef(IndexInput in) throws IOException {
    int numBytes = in.readVInt();
    if (numBytes < 0) {
      throw new CorruptIndexException("invalid bytes length: " + numBytes, in);
    }
    
    BytesRef bytes = new BytesRef();
    bytes.length = numBytes;
    bytes.bytes = new byte[numBytes];
    in.readBytes(bytes.bytes, 0, numBytes);

    return bytes;
  }

  /**
   * Seek {@code input} to the directory offset.
   * 这里先跳跃到了input的末尾 实际上是为了跳转到 开始写入 field信息的位置
   * */
  private static void seekDir(IndexInput input) throws IOException {
    input.seek(input.length() - CodecUtil.footerLength() - 8);
    long offset = input.readLong();
    input.seek(offset);
  }

  // for debugging
  // private static String toHex(int v) {
  //   return "0x" + Integer.toHexString(v);
  // }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(indexIn, termsIn, postingsReader);
    } finally { 
      // Clear so refs to terms index is GCable even if
      // app hangs onto us:
      fieldMap.clear();
    }
  }

  @Override
  public Iterator<String> iterator() {
    return fieldList.iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    assert field != null;
    return fieldMap.get(field);
  }

  @Override
  public int size() {
    return fieldMap.size();
  }

  // for debugging
  String brToString(BytesRef b) {
    if (b == null) {
      return "null";
    } else {
      try {
        return b.utf8ToString() + " " + b;
      } catch (Throwable t) {
        // If BytesRef isn't actually UTF8, or it's eg a
        // prefix of UTF8 that ends mid-unicode-char, we
        // fallback to hex:
        return b.toString();
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    long sizeInBytes = postingsReader.ramBytesUsed();
    for(FieldReader reader : fieldMap.values()) {
      sizeInBytes += reader.ramBytesUsed();
    }
    return sizeInBytes;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>(Accountables.namedAccountables("field", fieldMap));
    resources.add(Accountables.namedAccountable("delegate", postingsReader));
    return Collections.unmodifiableList(resources);
  }

  @Override
  public void checkIntegrity() throws IOException { 
    // terms index
    CodecUtil.checksumEntireFile(indexIn);

    // term dictionary
    CodecUtil.checksumEntireFile(termsIn);
      
    // postings
    postingsReader.checkIntegrity();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + fieldMap.size() + ",delegate=" + postingsReader + ")";
  }
}
