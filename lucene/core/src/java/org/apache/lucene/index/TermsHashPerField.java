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
 * 这是一个基类 代表以field为单位 从field中解析term 并设置到 TermsHash中
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
    /**
     * 该属性 对应 fieldState.termAttribute
     */
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
    /**
     * 记录写入的 stream信息 总计需要占用多少int
     */
    final int numPostingInt;

    protected final FieldInfo fieldInfo;

    /**
     * size 对应分配的 termID的数量
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
     * @param streamCount  代表要抽取几种维度的数据
     * @param fieldState    抽取的结果会存储到该对象中
     * @param termsHash    存储term信息的容器
     * @param nextPerField 该对象本身也是链式结构  目前只有 FreqProxTermsWriterPerField 有下游对象
     * @param fieldInfo
     */
    public TermsHashPerField(int streamCount, FieldInvertState fieldState, TermsHash termsHash, TermsHashPerField nextPerField, FieldInfo fieldInfo) {
        intPool = termsHash.intPool;
        bytePool = termsHash.bytePool;
        termBytePool = termsHash.termBytePool;
        // 获取描述本次正在被处理的doc的上下文信息
        docState = termsHash.docState;
        this.termsHash = termsHash;
        bytesUsed = termsHash.bytesUsed;
        this.fieldState = fieldState;
        this.streamCount = streamCount;
        numPostingInt = 2 * streamCount;
        this.fieldInfo = fieldInfo;
        this.nextPerField = nextPerField;
        // 该数组内除了存储 有关bytePool等的基础偏移量以外 还会根据子类生成定制化的array
        PostingsBytesStartArray byteStarts = new PostingsBytesStartArray(this, bytesUsed);
        // 上下游 termsHash 会共用一个termBytePool
        bytesHash = new BytesRefHash(termBytePool, HASH_INIT_SIZE, byteStarts);
    }

    /**
     * 重置 hash桶内部的数据 同时将重置指令传递到下游
     */
    void reset() {
        // 只清理 hash桶指针  不清理 pool内的数据
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
        // 这里使用基数排序
        sortedTermIDs = bytesHash.sort();
        return sortedTermIDs;
    }

    private boolean doNextCall;

    // Secondary entry point (for 2nd & subsequent TermsHash),
    // because token text has already been "interned" into
    // textStart, so we hash by textStart.  term vectors use
    // this API.
    // 这是传播到下游时 使用的api
    public void add(int textStart) throws IOException {
        // 因为上游和下游使用的 bytesHash内部的 termPool指向同一地址 所以数据是可以共用的(但是指针是各自维护的)  这里是在初始化该hash的指针
        int termID = bytesHash.addByPoolOffset(textStart);
        // 代表指针首次被设置 也就是对应下游首次解析某个term

        // 之后的套路基本跟 add() 一样
        if (termID >= 0) {      // New posting
            // First time we are seeing this token since we last
            // flushed the hash.
            // Init stream slices

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
     * 此时已经解析了一个token信息(在lucene中被称为 term) 将解析它时产生的XXXAttr存储到索引文件中
     */
    void add() throws IOException {
        // We are first in the chain so we must "intern" the
        // term text into textStart address
        // Get the text & hash of this term.
        // 将 term 写入到 hash桶中  注意 该hash桶与上游的 hash桶连接到同一个bytesPool上
        int termID = bytesHash.add(termAtt.getBytesRef());

        //System.out.println("add term=" + termBytesRef.utf8ToString() + " doc=" + docState.docID + " termID=" + termID);

        // 代表首次添加
        if (termID >= 0) {// New posting
            // 这里实际上只是做一个校验  因为没有利用返回值
            bytesHash.byteStart(termID);
            // Init stream slices
            // term本身存储在 BytesPool中  而一些position/offset等信息 就存储到 IntPool中

            // 检查是否有足够的空间写入 后面的数据
            if (numPostingInt + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
                intPool.nextBuffer();
            }
            if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt * ByteBlockPool.FIRST_LEVEL_SIZE) {
                bytePool.nextBuffer();
            }

            // .buffer 对应此时正在写入的数组
            intUptos = intPool.buffer;
            // 对应下一个要写入的偏移量  相对于此时的buffer 是一个相对偏移量
            intUptoStart = intPool.intUpto;
            intPool.intUpto += streamCount;

            // intUptoStart + intPool.intOffset 对应绝对偏移量
            postingsArray.intStarts[termID] = intUptoStart + intPool.intOffset;

            for (int i = 0; i < streamCount; i++) {
                final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
                // 接下来记录2个 stream信息在 bytePool的偏移量
                intUptos[intUptoStart + i] = upto + bytePool.byteOffset;
            }
            // 记录 bytePool的绝对偏移量
            postingsArray.byteStarts[termID] = intUptos[intUptoStart];

            newTerm(termID);

        } else {
            // 代表该term之前出现过 就在原来的数据分片上继续写入数据   原termID为0时 返回-1 原termID为1时 返回-2  所以这里是还原
            termID = (-termID) - 1;
            // 找到上次 intPool的绝对偏移量
            int intStart = postingsArray.intStarts[termID];
            // 还原偏移量 此时存储的值就是 termID 对应的上个term  bytePool存储的最后指针位置
            intUptos = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
            intUptoStart = intStart & IntBlockPool.INT_BLOCK_MASK;
            addTerm(termID);
        }

        // 如果需要自动传递到下游
        if (doNextCall) {
            nextPerField.add(postingsArray.textStarts[termID]);
        }
    }

    /**
     * 对应此时正在使用的 IntPool.current
     */
    int[] intUptos;

    /**
     * 对应IntPool 当前正写入的位置
     */
    int intUptoStart;

    /**
     * @param stream 每个stream 代表一种数据
     * @param b
     */
    void writeByte(int stream, byte b) {
        // 对应 bytePool 此时分片的起始偏移量
        int upto = intUptos[intUptoStart + stream];
        byte[] bytes = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
        assert bytes != null;
        int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;

        // 定位到分片的位置 开始写入数据
        if (bytes[offset] != 0) {
            // End of slice; allocate a new one
            offset = bytePool.allocSlice(bytes, offset);
            bytes = bytePool.buffer;
            // 这里更新了之前存储的偏移量 下一次就会直接定位到可写入的位置
            intUptos[intUptoStart + stream] = offset + bytePool.byteOffset;
        }
        bytes[offset] = b;
        // 因为写入了一个值 所以往后移动
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

        private final TermsHashPerField perField;
        private final Counter bytesUsed;

        private PostingsBytesStartArray(
                TermsHashPerField perField, Counter bytesUsed) {
            this.perField = perField;
            this.bytesUsed = bytesUsed;
        }

        @Override
        public int[] init() {
            // postingsArray 存储3种描述位置信息的数组
            if (perField.postingsArray == null) {
                // 子类会根据自己统计的数据维度 创建一个 拓展了更多数组以便存储数据的 ParallelPostingsArray对象 比如 TermVectorsPostingsArray
                // 可以发现一般lucene创建的数组初始大小都是2 之后每次根据需要进行扩容
                perField.postingsArray = perField.createPostingsArray(2);
                // 为子对象的array 赋值
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
     *
     * @param field 本次解析的field信息
     * @param first 代表是否是该 perThread下首次解析  该field的信息
     */
    boolean start(IndexableField field, boolean first) {
        // 此前 已经为 invertState对象设置了 attr
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
