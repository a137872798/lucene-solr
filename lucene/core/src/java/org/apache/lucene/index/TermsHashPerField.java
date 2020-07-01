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

import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRefHash.BytesStartArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/**
 * 对 TermsHash 调用 addField 时 会返回该对象的子类
 * 将词的信息 以 field为单位进行统计
 */
abstract class TermsHashPerField implements Comparable<TermsHashPerField> {

    private static final int HASH_INIT_SIZE = 4;

    /**
     * 多个 field的term 都存储在同一个桶中
     */
    final TermsHash termsHash;

    /**
     * 目前只有一种情况 就是  freqPerField 下面连接 termVectorPerField
     * 使用一组 perfield 对 field下的term 进行统计  每个对象只抽取自己需要的值
     */
    final TermsHashPerField nextPerField;
    protected final DocumentsWriterPerThread.DocState docState;
    protected final FieldInvertState fieldState;
    TermToBytesRefAttribute termAtt;
    protected TermFrequencyAttribute termFreqAtt;

    // Copied from our perThread
    // 这些对象 使用 thread 的 allocator进行初始化 从 TermHash 传过来

    /**
     * 存储 termId 到 bytePool 的映射关系
     */
    final IntBlockPool intPool;
    /**
     * 存储 term的特殊数据
     */
    final ByteBlockPool bytePool;
    /**
     * 该对象存储 term数据
     */
    final ByteBlockPool termBytePool;

    final int streamCount;
    final int numPostingInt;

    protected final FieldInfo fieldInfo;

    /**
     * 该对象借助 pool 对象写入数据  利用了pool自动扩容且不需要大量的数据拷贝的特点
     * 该对象本身只维护了  hash到 id 到 pool偏移量的映射关系
     * 该对象连接到 termBytePool 这样就可以通过 termId 映射到 pool的偏移量
     */
    final BytesRefHash bytesHash;

    /**
     * 该对象内部就是很多int[] 每个数组负责存储一种数据
     */
    ParallelPostingsArray postingsArray;
    private final Counter bytesUsed;

    /** streamCount: how many streams this field stores per term.
     * E.g. doc(+freq) is 1 stream, prox+offset is a second. */

    /**
     * @param streamCount  代表存储几种数据流  只有freq时是1   除此之外如果有 offset position 之类的就是2
     * @param fieldState
     * @param termsHash
     * @param nextPerField 该对象本身也是链式结构  目前只有 FreqProxTermsWriterPerField 有下游对象
     * @param fieldInfo
     */
    public TermsHashPerField(int streamCount, FieldInvertState fieldState, TermsHash termsHash, TermsHashPerField nextPerField, FieldInfo fieldInfo) {
        intPool = termsHash.intPool;
        bytePool = termsHash.bytePool;
        termBytePool = termsHash.termBytePool;
        docState = termsHash.docState;
        this.termsHash = termsHash;
        bytesUsed = termsHash.bytesUsed;
        this.fieldState = fieldState;
        this.streamCount = streamCount;
        numPostingInt = 2 * streamCount;
        this.fieldInfo = fieldInfo;
        this.nextPerField = nextPerField;
        PostingsBytesStartArray byteStarts = new PostingsBytesStartArray(this, bytesUsed);
        // 默认情况下 存储 bytesID(在这个场景下应该就是 termID) 与 pool偏移量关系的对象是  DirectBytesStartArray 对象 而这里替换成了 PostingsBytesStartArray
        bytesHash = new BytesRefHash(termBytePool, HASH_INIT_SIZE, byteStarts);
    }

    /**
     * 重置 hash桶内部的数据 同时将重置指令传递到下游
     */
    void reset() {
        bytesHash.clear(false);
        if (nextPerField != null) {
            nextPerField.reset();
        }
    }

    /**
     * 在初始化了 reader后 才可以用该对象读取到 目标term的信息  (term的信息 以多个slice的形式存储在pool中)
     *
     * @param reader 此时reader对象还没有初始化
     * @param termID 这里传入了某个词的id  看来一个文档被分词器处理后 每个词应该有自己的id
     * @param stream 标记此时要查询的数据流   每个term可能会存储多种数据流 每个数据流对应一个分片的起点
     */
    public void initReader(ByteSliceReader reader, int termID, int stream) {
        assert stream < streamCount;
        // 将id 转换成 pool的 绝对偏移量   也就是所有的词都存储在 pool 对象中
        int intStart = postingsArray.intStarts[termID];
        // 存储int 类型数据的 block
        final int[] ints = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
        final int upto = intStart & IntBlockPool.INT_BLOCK_MASK;

        // 偏移量信息通过intPool存储  而存储实际数据的 通过bytePool存储
        reader.init(bytePool,
                postingsArray.byteStarts[termID] + stream * ByteBlockPool.FIRST_LEVEL_SIZE,  // 这里就要考量 stream (这样就变成了该stream对应的slice链表的起点)
                ints[upto + stream]);  // endIndex 是从 ints中获取的  记录了分片链表最后的偏移量 (分片是一个链表结构)  因为每次写入ints[] 的偏移量都会变化 所以最终停止的位置就是 endIndex
    }

    /**
     * 存储排序后的 termId
     */
    int[] sortedTermIDs;

    /**
     * Collapse the hash table and sort in-place; also sets
     * this.sortedTermIDs to the results
     */
    public int[] sortPostings() {
        // 这里使用基数排序  为 termId 排序
        sortedTermIDs = bytesHash.sort();
        return sortedTermIDs;
    }

    private boolean doNextCall;

    // Secondary entry point (for 2nd & subsequent TermsHash),
    // because token text has already been "interned" into
    // textStart, so we hash by textStart.  term vectors use
    // this API.
    public void add(int textStart) throws IOException {
        // 这里在  pool 中提前预定一个偏移量  并返回此时已经写入的数据量  那么 term就是写入到 bytesHash中 并且 内部的entry 按照添加顺序生成对应的id
        int termID = bytesHash.addByPoolOffset(textStart);
        if (termID >= 0) {      // New posting
            // First time we are seeing this token since we last
            // flushed the hash.
            // Init stream slices

            // 下面为了简化逻辑 让每次分配的所有 stream在同一个block中  这样通过某个termId 定位到block时 不需要再考虑下一个stream是否会切换到下一个block
            if (numPostingInt + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
                // 代表此时没有足够的空间了  切换到下个block
                intPool.nextBuffer();
            }

            if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt * ByteBlockPool.FIRST_LEVEL_SIZE) {
                bytePool.nextBuffer();
            }


            intUptos = intPool.buffer;
            // 这个位置用于写入 stream信息
            intUptoStart = intPool.intUpto;
            // 将pool的起点偏移量 先往前挪
            intPool.intUpto += streamCount;

            // 记录该term 对应的数据起点 在block的绝对偏移量  (还未处理stream)
            postingsArray.intStarts[termID] = intUptoStart + intPool.intOffset;

            // 根据stream数量 创建对应的分片
            for (int i = 0; i < streamCount; i++) {
                final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
                // 对应  ints[upto+stream]  也就是读取偏移量时 数据从upto开始,每存在一个 stream 就要往后数一位
                intUptos[intUptoStart + i] = upto + bytePool.byteOffset;
            }
            postingsArray.byteStarts[termID] = intUptos[intUptoStart];

            newTerm(termID);

        } else {
            // 代表当前 term已经存在于 block了  还原成之前存在的 termId
            termID = (-termID) - 1;
            int intStart = postingsArray.intStarts[termID];
            // 这里把 block 调整到该term所在的block 因为上面的处理可以确保该block 一定完整包含了这个term的所有stream分片
            intUptos = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
            intUptoStart = intStart & IntBlockPool.INT_BLOCK_MASK;
            addTerm(termID);
        }
    }

    /**
     * Called once per inverted token.  This is the primary
     * entry point (for first TermsHash); postings use this
     * API.
     */
    void add() throws IOException {
        // We are first in the chain so we must "intern" the
        // term text into textStart address
        // Get the text & hash of this term.
        // 将 term 写入到 hash桶中
        int termID = bytesHash.add(termAtt.getBytesRef());

        //System.out.println("add term=" + termBytesRef.utf8ToString() + " doc=" + docState.docID + " termID=" + termID);

        if (termID >= 0) {// New posting
            bytesHash.byteStart(termID);
            // Init stream slices
            // 开始为 slice 创建足够空间
            if (numPostingInt + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
                intPool.nextBuffer();
            }

            if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt * ByteBlockPool.FIRST_LEVEL_SIZE) {
                bytePool.nextBuffer();
            }

            intUptos = intPool.buffer;
            intUptoStart = intPool.intUpto;
            intPool.intUpto += streamCount;

            postingsArray.intStarts[termID] = intUptoStart + intPool.intOffset;

            for (int i = 0; i < streamCount; i++) {
                final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
                intUptos[intUptoStart + i] = upto + bytePool.byteOffset;
            }
            postingsArray.byteStarts[termID] = intUptos[intUptoStart];

            newTerm(termID);

        } else {
            termID = (-termID) - 1;
            int intStart = postingsArray.intStarts[termID];
            intUptos = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
            intUptoStart = intStart & IntBlockPool.INT_BLOCK_MASK;
            addTerm(termID);
        }

        // 如果需要自动传递到下游
        if (doNextCall) {
            nextPerField.add(postingsArray.textStarts[termID]);
        }
    }

    int[] intUptos;

    /**
     * 每次打算写入数据时 会携带2个参数  一个是 stream   一个是要写入的数据byte
     * 该偏移量是一个基准偏移量  每次 [intUptoStart+stream]   才能对应到  byteBlock的某个偏移量  (这个偏移量就是写入byte的偏移量)
     */
    int intUptoStart;

    /**
     * @param stream 根据stream的数量 提前空出多少空位 每个stream相关的数据收集对象 会将数据设置到 空位中
     * @param b
     */
    void writeByte(int stream, byte b) {
        // 通过映射 变成 bytePool当前要写入的下标
        int upto = intUptos[intUptoStart + stream];
        byte[] bytes = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
        assert bytes != null;
        int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;

        // 在匹配的位置写入数据   这里代表分片没有空位了 需要创建下一个分片
        if (bytes[offset] != 0) {
            // End of slice; allocate a new one
            offset = bytePool.allocSlice(bytes, offset);
            bytes = bytePool.buffer;
            // 这里更新了 之前存储的偏移量 下一次就会直接定位到 可写入的位置
            intUptos[intUptoStart + stream] = offset + bytePool.byteOffset;
        }
        bytes[offset] = b;
        (intUptos[intUptoStart + stream])++;
    }

    public void writeBytes(int stream, byte[] b, int offset, int len) {
        // TODO: optimize
        final int end = offset + len;
        for (int i = offset; i < end; i++)
            writeByte(stream, b[i]);
    }

    /**
     * 写入 变长int
     *
     * @param stream
     * @param i
     */
    void writeVInt(int stream, int i) {
        assert stream < streamCount;
        while ((i & ~0x7F) != 0) {
            writeByte(stream, (byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByte(stream, (byte) i);
    }

    /**
     * 该对象替代了 BytesRefHash 默认的 DirectBytesStartArray
     */
    private static final class PostingsBytesStartArray extends BytesStartArray {

        /**
         * 以field为单位存储term信息
         */
        private final TermsHashPerField perField;
        private final Counter bytesUsed;

        private PostingsBytesStartArray(
                TermsHashPerField perField, Counter bytesUsed) {
            this.perField = perField;
            this.bytesUsed = bytesUsed;
        }

        @Override
        public int[] init() {
            if (perField.postingsArray == null) {
                // 创建的 该对象包含了 term内部的信息
                perField.postingsArray = perField.createPostingsArray(2);
                // 为子对象 array 赋值
                perField.newPostingsArray();
                bytesUsed.addAndGet(perField.postingsArray.size * perField.postingsArray.bytesPerPosting());
            }
            return perField.postingsArray.textStarts;
        }

        @Override
        public int[] grow() {
            ParallelPostingsArray postingsArray = perField.postingsArray;
            final int oldSize = perField.postingsArray.size;
            postingsArray = perField.postingsArray = postingsArray.grow();
            perField.newPostingsArray();
            bytesUsed.addAndGet((postingsArray.bytesPerPosting() * (postingsArray.size - oldSize)));
            return postingsArray.textStarts;
        }

        @Override
        public int[] clear() {
            if (perField.postingsArray != null) {
                bytesUsed.addAndGet(-(perField.postingsArray.size * perField.postingsArray.bytesPerPosting()));
                perField.postingsArray = null;
                perField.newPostingsArray();
            }
            return null;
        }

        @Override
        public Counter bytesUsed() {
            return bytesUsed;
        }
    }

    @Override
    public int compareTo(TermsHashPerField other) {
        return fieldInfo.name.compareTo(other.fieldInfo.name);
    }

    /**
     * Finish adding all instances of this field to the
     * current document.
     */
    void finish() throws IOException {
        if (nextPerField != null) {
            nextPerField.finish();
        }
    }

    /**
     * Start adding a new field instance; first is true if
     * this is the first time this field name was seen in the
     * document.
     */
    // 当调用start 时 从 state中读取属性
    boolean start(IndexableField field, boolean first) {
        termAtt = fieldState.termAttribute;
        termFreqAtt = fieldState.termFreqAttribute;
        if (nextPerField != null) {
            doNextCall = nextPerField.start(field, first);
        }

        return true;
    }

    /**
     * Called when a term is seen for the first time.
     */
    abstract void newTerm(int termID) throws IOException;

    /**
     * Called when a previously seen term is seen again.
     */
    abstract void addTerm(int termID) throws IOException;

    /**
     * Called when the postings array is initialized or
     * resized.
     */
    abstract void newPostingsArray();

    /**
     * Creates a new postings array of the specified size.
     */
    abstract ParallelPostingsArray createPostingsArray(int size);
}
