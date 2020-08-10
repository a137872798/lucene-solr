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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.util.BytesRef;

/**
 * 该对象以field为单位 从中抽取term信息
 */
final class TermVectorsConsumerPerField extends TermsHashPerField {

    /**
     * 存储term基础信息 以及词向量信息的数组集合
     */
    private TermVectorsPostingsArray termVectorsPostingsArray;

    /**
     * 该对象关联的 词向量对象
     */
    final TermVectorsConsumer termsWriter;

    /**
     * 是否存储词向量信息  我对词向量的理解就是一些描述term的信息 而position/offset/payload就是具体的信息类型
     */
    boolean doVectors;
    boolean doVectorPositions;
    boolean doVectorOffsets;
    boolean doVectorPayloads;

    OffsetAttribute offsetAttribute;
    PayloadAttribute payloadAttribute;
    boolean hasPayloads; // if enabled, and we actually saw any for this field

    /**
     * @param invertState 该对象负责存储field的一些属性
     * @param termsWriter 该对象是由哪个对象创建的
     * @param fieldInfo  此时fieldInfo还是一个空对象
     */
    public TermVectorsConsumerPerField(FieldInvertState invertState, TermVectorsConsumer termsWriter, FieldInfo fieldInfo) {
        super(2, invertState, termsWriter, null, fieldInfo);
        this.termsWriter = termsWriter;
    }

    /**
     * Called once per field per document if term vectors
     * are enabled, to write the vectors to
     * RAMOutputStream, which is then quickly flushed to
     * the real term vectors files in the Directory.
     * 代表当前这个文档已经处理完了
     */
    @Override
    void finish() {
        // 如果本身就不需要存储 向量信息 或者当前没有读取到任何信息 直接返回
        if (!doVectors || bytesHash.size() == 0) {
            return;
        }
        termsWriter.addFieldToFlush(this);
    }

    /**
     * 当 TermVectorsConsumer.finishDocument() 调用时 转发到这里    TermVectorsConsumer 在处理一个doc时可能会有很多的field 就会创建多个该对象
     * @throws IOException
     */
    void finishDocument() throws IOException {
        // 如果该文档下的域 声明不需要存储向量信息 直接返回
        if (doVectors == false) {
            return;
        }

        doVectors = false;

        // 代表写入了多少数据
        final int numPostings = bytesHash.size();

        final BytesRef flushTerm = termsWriter.flushTerm;

        assert numPostings >= 0;

        // This is called once, after inverting all occurrences
        // of a given field in the doc.  At this point we flush
        // our hash into the DocWriter.

        // 该数组内部此时记录了各种信息
        TermVectorsPostingsArray postings = termVectorsPostingsArray;
        final TermVectorsWriter tv = termsWriter.writer;

        // 获取当前field内所有的 term
        final int[] termIDs = sortPostings();

        // writer 可以创建索引结构对象 这里是给索引对象追加field 信息   (对应实体 fieldData)
        tv.startField(fieldInfo, numPostings, doVectorPositions, doVectorOffsets, hasPayloads);

        // 创建2个用来读取分片信息的对象  此时内部的指针还没有初始化
        final ByteSliceReader posReader = doVectorPositions ? termsWriter.vectorSliceReaderPos : null;
        final ByteSliceReader offReader = doVectorOffsets ? termsWriter.vectorSliceReaderOff : null;

        // TODO 推测 numPosition 记录的是不同term的数量  下面通过freq 可以知道同一个term 有多少个 然后相同term的position等信息是连续存储的
        for (int j = 0; j < numPostings; j++) {
            final int termID = termIDs[j];
            // 通过 termID 找到频率   频率实际上也就记录了这个termId在这个field中出现了几次  同时出现几次就有多少 position offset 存储
            final int freq = postings.freqs[termID];

            // Get BytesRef  定位到 目标term 并将flushTerm 指向内存块
            termBytePool.setBytesRef(flushTerm, postings.textStarts[termID]);
            // 将 term 写入到 FieldData 内
            tv.startTerm(flushTerm, freq);

            if (doVectorPositions || doVectorOffsets) {
                // 开始初始化 reader 对象   pos作为第一个数据流
                if (posReader != null) {
                    initReader(posReader, termID, 0);
                }
                // offset 作为第二个数据流
                if (offReader != null) {
                    initReader(offReader, termID, 1);
                }
                // 这里将 pos 和 off 写入到索引文件中
                tv.addProx(freq, posReader, offReader);
            }
            // 代表该term的相关数据采集完了
            tv.finishTerm();
        }
        // 代表这个 field 的数据采集完了
        tv.finishField();

        reset();

        fieldInfo.setStoreTermVectors();
    }

    /**
     * 代表预备读取一个新的 field   XXXPerField.start() 会最先调用该对象的start 之后调用  freqProxTermsWriterPerField.start()
     * @param field
     * @param first
     * @return
     */
    @Override
    boolean start(IndexableField field, boolean first) {
        super.start(field, first);
        assert field.fieldType().indexOptions() != IndexOptions.NONE;

        // 代表该 perField 关联的 perThread 首次处理该field
        if (first) {

            // 先清理之前的数据   该对象的 bytesHash和FreqProxTermsWriterPerField 共用同一个bytesPool
            if (bytesHash.size() != 0) {
                // Only necessary if previous doc hit a
                // non-aborting exception while writing vectors in
                // this field:
                reset();
            }

            // 重新初始化内部字段
            bytesHash.reinit();

            hasPayloads = false;

            // 是否要存储词向量信息  在用户创建field时 可以自定义fieldType
            doVectors = field.fieldType().storeTermVectors();

            // 当需要存储向量信息时 查看需要存储词向量的哪些具体信息
            if (doVectors) {

                termsWriter.hasVectors = true;

                // 是否要存储 position 信息
                doVectorPositions = field.fieldType().storeTermVectorPositions();

                // Somewhat confusingly, unlike postings, you are
                // allowed to index TV offsets without TV positions:
                // 是否要存储 offset 信息
                doVectorOffsets = field.fieldType().storeTermVectorOffsets();

                // 只有当需要存储position时 才检测是否需要存储payload信息
                if (doVectorPositions) {
                    doVectorPayloads = field.fieldType().storeTermVectorPayloads();
                } else {
                    doVectorPayloads = false;
                    // 如果设置了 不存储position的话 是不能单独设置 storeTermVectorPayloads = true的
                    if (field.fieldType().storeTermVectorPayloads()) {
                        // TODO: move this check somewhere else, and impl the other missing ones
                        throw new IllegalArgumentException("cannot index term vector payloads without term vector positions (field=\"" + field.name() + "\")");
                    }
                }

                // 当不需要存储向量信息时   不能设置下面这些属性
            } else {
                if (field.fieldType().storeTermVectorOffsets()) {
                    throw new IllegalArgumentException("cannot index term vector offsets when term vectors are not indexed (field=\"" + field.name() + "\")");
                }
                if (field.fieldType().storeTermVectorPositions()) {
                    throw new IllegalArgumentException("cannot index term vector positions when term vectors are not indexed (field=\"" + field.name() + "\")");
                }
                if (field.fieldType().storeTermVectorPayloads()) {
                    throw new IllegalArgumentException("cannot index term vector payloads when term vectors are not indexed (field=\"" + field.name() + "\")");
                }
            }
            // 代表不是第一次加载 这里确保传入的 域的属性始终是一致的 不允许中途变化
        } else {
            if (doVectors != field.fieldType().storeTermVectors()) {
                throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectors changed for field=\"" + field.name() + "\")");
            }
            if (doVectorPositions != field.fieldType().storeTermVectorPositions()) {
                throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectorPositions changed for field=\"" + field.name() + "\")");
            }
            if (doVectorOffsets != field.fieldType().storeTermVectorOffsets()) {
                throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectorOffsets changed for field=\"" + field.name() + "\")");
            }
            if (doVectorPayloads != field.fieldType().storeTermVectorPayloads()) {
                throw new IllegalArgumentException("all instances of a given field name must have the same term vectors settings (storeTermVectorPayloads changed for field=\"" + field.name() + "\")");
            }
        }
        // 上面就是检测field需要存储的各种词向量信息  确保一致性或者进行初始化

        if (doVectors) {
            // 根据需要设置相关 attr
            if (doVectorOffsets) {
                offsetAttribute = fieldState.offsetAttribute;
                assert offsetAttribute != null;
            }

            if (doVectorPayloads) {
                // Can be null:
                payloadAttribute = fieldState.payloadAttribute;
            } else {
                payloadAttribute = null;
            }
        }

        return doVectors;
    }

    /**
     * addTerm 和 newTerm 会触发该方法   将term的信息填充到 postings 中
     * @param postings
     * @param termID  相同内容的term不会重复写入 会使用同一个 termID
     */
    void writeProx(TermVectorsPostingsArray postings, int termID) {

        // 代表需要记录 term对应的起点和长度
        if (doVectorOffsets) {
            int startOffset = fieldState.offset + offsetAttribute.startOffset();
            int endOffset = fieldState.offset + offsetAttribute.endOffset();

            // 如果此时是该termID对应的第一个term 那么第一个值就是 startOffset-0   之后的term 写入的就是 本次startOff - 上次term.endOffset
            // 第二个是长度信息
            // offset 是第二个stream
            writeVInt(1, startOffset - postings.lastOffsets[termID]);
            writeVInt(1, endOffset - startOffset);
            postings.lastOffsets[termID] = endOffset;
        }

        // 代表需要记录 逻辑偏移量的信息
        if (doVectorPositions) {
            final BytesRef payload;
            // 首先检查一下是否有携带 payload 默认的标准分词器是不携带这些 Attr的
            if (payloadAttribute == null) {
                payload = null;
            } else {
                payload = payloadAttribute.getPayload();
            }

            // 类似的套路 因为一个term可能会出现多次 那么共用
            final int pos = fieldState.position - postings.lastPositions[termID];
            // 第一个值的最后一位标记了是否携带 payload 信息
            if (payload != null && payload.length > 0) {
                writeVInt(0, (pos << 1) | 1);
                writeVInt(0, payload.length);
                writeBytes(0, payload.bytes, payload.offset, payload.length);
                hasPayloads = true;
            } else {
                writeVInt(0, pos << 1);
            }
            postings.lastPositions[termID] = fieldState.position;
        }
    }

    /**
     * 通过 termId 找到term 并写入信息
     * @param termID
     */
    @Override
    void newTerm(final int termID) {
        TermVectorsPostingsArray postings = termVectorsPostingsArray;

        // 设置频率信息
        postings.freqs[termID] = getTermFreq();
        // 这里先设置了默认值  在writeProx 中可能会覆盖内部的属性
        postings.lastOffsets[termID] = 0;
        postings.lastPositions[termID] = 0;

        // 主要就是将attr的信息抽出来 写入到 BytePool中
        writeProx(postings, termID);
    }

    /**
     * 代表某个term 第二次出现
     * @param termID
     */
    @Override
    void addTerm(final int termID) {
        TermVectorsPostingsArray postings = termVectorsPostingsArray;

        // 这里开始加频率了  也就是在标准分词器中 实际上没有记录频率信息 而是转移到之类
        postings.freqs[termID] += getTermFreq();

        writeProx(postings, termID);
    }

    /**
     * 获取词的频率信息  在标准分词器中 并没有看到freq的相关逻辑
     * @return
     */
    private int getTermFreq() {
        // 默认情况下总是返回1   这个值代表着重复出现term时 freq的增值是多少
        int freq = termFreqAtt.getTermFrequency();
        if (freq != 1) {
            if (doVectorPositions) {
                throw new IllegalArgumentException("field \"" + fieldInfo.name + "\": cannot index term vector positions while using custom TermFrequencyAttribute");
            }
            if (doVectorOffsets) {
                throw new IllegalArgumentException("field \"" + fieldInfo.name + "\": cannot index term vector offsets while using custom TermFrequencyAttribute");
            }
        }

        return freq;
    }

    @Override
    public void newPostingsArray() {
        termVectorsPostingsArray = (TermVectorsPostingsArray) postingsArray;
    }

    /**
     * @param size
     * @return
     */
    @Override
    ParallelPostingsArray createPostingsArray(int size) {
        return new TermVectorsPostingsArray(size);
    }

    /**
     * 该容器在 父类的3种基础数组上又拓展了 3种数据
     */
    static final class TermVectorsPostingsArray extends ParallelPostingsArray {
        public TermVectorsPostingsArray(int size) {
            super(size);
            freqs = new int[size];
            lastOffsets = new int[size];
            lastPositions = new int[size];
        }

        // 频率信息
        int[] freqs;                                       // How many times this term occurred in the current doc
        /**
         * 记录上一次偏移量的 endOffset
         */
        int[] lastOffsets;                                 // Last offset we saw
        /**
         * 记录上一次逻辑偏移量的信息
         */
        int[] lastPositions;                               // Last position where this term occurred

        @Override
        ParallelPostingsArray newInstance(int size) {
            return new TermVectorsPostingsArray(size);
        }

        @Override
        void copyTo(ParallelPostingsArray toArray, int numToCopy) {
            assert toArray instanceof TermVectorsPostingsArray;
            TermVectorsPostingsArray to = (TermVectorsPostingsArray) toArray;

            super.copyTo(toArray, numToCopy);

            System.arraycopy(freqs, 0, to.freqs, 0, size);
            System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, size);
            System.arraycopy(lastPositions, 0, to.lastPositions, 0, size);
        }

        @Override
        int bytesPerPosting() {
            return super.bytesPerPosting() + 3 * Integer.BYTES;
        }
    }
}
