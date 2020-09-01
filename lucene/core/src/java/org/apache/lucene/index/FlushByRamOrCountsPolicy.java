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


/**
 * Default {@link FlushPolicy} implementation that flushes new segments based on
 * RAM used and document count depending on the IndexWriter's
 * {@link IndexWriterConfig}. It also applies pending deletes based on the
 * number of buffered delete terms.
 *
 * <ul>
 * <li>
 * {@link #onDelete(DocumentsWriterFlushControl, DocumentsWriterPerThread)}
 * - applies pending delete operations based on the global number of buffered
 * delete terms if the consumed memory is greater than {@link IndexWriterConfig#getRAMBufferSizeMB()}</li>.
 * <li>
 * {@link #onInsert(DocumentsWriterFlushControl, DocumentsWriterPerThread)}
 * - flushes either on the number of documents per
 * {@link DocumentsWriterPerThread} (
 * {@link DocumentsWriterPerThread#getNumDocsInRAM()}) or on the global active
 * memory consumption in the current indexing session iff
 * {@link IndexWriterConfig#getMaxBufferedDocs()} or
 * {@link IndexWriterConfig#getRAMBufferSizeMB()} is enabled respectively</li>
 * <li>
 * {@link #onUpdate(DocumentsWriterFlushControl, DocumentsWriterPerThread)}
 * - calls
 * {@link #onInsert(DocumentsWriterFlushControl, DocumentsWriterPerThread)}
 * and
 * {@link #onDelete(DocumentsWriterFlushControl, DocumentsWriterPerThread)}
 * in order</li>
 * </ul>
 * All {@link IndexWriterConfig} settings are used to mark
 * {@link DocumentsWriterPerThread} as flush pending during indexing with
 * respect to their live updates.
 * <p>
 * If {@link IndexWriterConfig#setRAMBufferSizeMB(double)} is enabled, the
 * largest ram consuming {@link DocumentsWriterPerThread} will be marked as
 * pending iff the global active RAM consumption is {@code >=} the configured max RAM
 * buffer.
 * 当前  FlushPolicy 只有这一个实现类
 * FlushByRamOrCountsPolicy 的含义就是根据此时占用的内存量或者doc数量决定将perThread标记成待刷盘的策略
 */
class FlushByRamOrCountsPolicy extends FlushPolicy {

    /**
     * 代表此时内存中的某些doc数据需要被删除
     *
     * @param control
     * @param perThread
     */
    @Override
    public void onDelete(DocumentsWriterFlushControl control, DocumentsWriterPerThread perThread) {
        // 此时 deleteQueue中维护了太多数据 会设置一个需要处理 deleteQueue数据的标识
        if ((flushOnRAM() && control.getDeleteBytesUsed() > 1024 * 1024 * indexWriterConfig.getRAMBufferSizeMB())) {
            // 设置 flushDeletes 标识
            control.setApplyAllDeletes();
            if (infoStream.isEnabled("FP")) {
                infoStream.message("FP", "force apply deletes bytesUsed=" + control.getDeleteBytesUsed() + " vs ramBufferMB=" + indexWriterConfig.getRAMBufferSizeMB());
            }
        }
    }

    /**
     * 代表此时解析了新的doc数据
     *
     * @param control
     * @param perThread
     */
    @Override
    public void onInsert(DocumentsWriterFlushControl control, DocumentsWriterPerThread perThread) {
        // 总计2个维度 都必须先确保开启了自动刷盘   第一个维度是perThread本身占用的bytes数超过了阈值 代表此时暂存在内存中的数据过多 需要标记成刷盘
        // 第二个维度是整个flushControl管理的所有 perThread总和超过了阈值  此时会将占用内存最大的perThread 标记成待刷盘

        if (flushOnDocCount()
                && perThread.getNumDocsInRAM() >= indexWriterConfig
                .getMaxBufferedDocs()) {
            // Flush this state by num docs   将 perThread标记成待刷盘状态
            control.setFlushPending(perThread);
        } else if (flushOnRAM()) {// flush by RAM
            // 将 MB 转换成 byte
            final long limit = (long) (indexWriterConfig.getRAMBufferSizeMB() * 1024.d * 1024.d);
            //  control.getDeleteBytesUsed() 对应的是deleteQueue占用的bytes数
            final long totalRam = control.activeBytes() + control.getDeleteBytesUsed();
            if (totalRam >= limit) {
                if (infoStream.isEnabled("FP")) {
                    infoStream.message("FP", "trigger flush: activeBytes=" + control.activeBytes() + " deleteBytes=" + control.getDeleteBytesUsed() + " vs limit=" + limit);
                }
                // 将占用内存最大的perThread 对象标记成待刷盘
                markLargestWriterPending(control, perThread);
            }
        }
    }

    /**
     * Marks the most ram consuming active {@link DocumentsWriterPerThread} flush
     * pending
     * 找到占用内存最多的perThread对象 并标记成待刷盘
     */
    protected void markLargestWriterPending(DocumentsWriterFlushControl control,
                                            DocumentsWriterPerThread perThread) {
        DocumentsWriterPerThread largestNonPendingWriter = findLargestNonPendingWriter(control, perThread);
        if (largestNonPendingWriter != null) {
            control.setFlushPending(largestNonPendingWriter);
        }
    }

    /**
     * Returns <code>true</code> if this {@link FlushPolicy} flushes on
     * {@link IndexWriterConfig#getMaxBufferedDocs()}, otherwise
     * <code>false</code>.
     * indexWriterConfig.getMaxBufferedDocs() 不为-1时 就代表perThread内的doc占用bytes数超过该值时 触发刷盘
     */
    protected boolean flushOnDocCount() {
        return indexWriterConfig.getMaxBufferedDocs() != IndexWriterConfig.DISABLE_AUTO_FLUSH;
    }

    /**
     * Returns <code>true</code> if this {@link FlushPolicy} flushes on
     * {@link IndexWriterConfig#getRAMBufferSizeMB()}, otherwise
     * <code>false</code>.
     */
    protected boolean flushOnRAM() {
        return indexWriterConfig.getRAMBufferSizeMB() != IndexWriterConfig.DISABLE_AUTO_FLUSH;
    }
}
