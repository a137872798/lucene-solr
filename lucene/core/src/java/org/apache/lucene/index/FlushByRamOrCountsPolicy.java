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
 */
class FlushByRamOrCountsPolicy extends FlushPolicy {

    /**
     * 代表 通过 DocumentsWriter 对象 触发了 delete 方法
     *
     * @param control
     * @param perThread
     */
    @Override
    public void onDelete(DocumentsWriterFlushControl control, DocumentsWriterPerThread perThread) {
        // 此时待删除的doc占用大量内存时
        if ((flushOnRAM() && control.getDeleteBytesUsed() > 1024 * 1024 * indexWriterConfig.getRAMBufferSizeMB())) {
            // 设置 flushDeletes 标识
            control.setApplyAllDeletes();
            if (infoStream.isEnabled("FP")) {
                infoStream.message("FP", "force apply deletes bytesUsed=" + control.getDeleteBytesUsed() + " vs ramBufferMB=" + indexWriterConfig.getRAMBufferSizeMB());
            }
        }
    }

    /**
     * 代表此时有某些doc生成的索引信息 写入到内存中了
     *
     * @param control
     * @param perThread
     */
    @Override
    public void onInsert(DocumentsWriterFlushControl control, DocumentsWriterPerThread perThread) {
        // flushOnDocCount() 代表当内存中的doc超过一定量值时 会推荐触发刷盘
        if (flushOnDocCount()
                && perThread.getNumDocsInRAM() >= indexWriterConfig
                .getMaxBufferedDocs()) {
            // Flush this state by num docs   将 perThread标记成待刷盘状态
            control.setFlushPending(perThread);
            // 这个应该就是判断此时占用的内存是否超过一定值  超过的话自动进行刷盘状态
        } else if (flushOnRAM()) {// flush by RAM
            // 将 MB 转换成 byte
            final long limit = (long) (indexWriterConfig.getRAMBufferSizeMB() * 1024.d * 1024.d);
            final long totalRam = control.activeBytes() + control.getDeleteBytesUsed();
            if (totalRam >= limit) {
                if (infoStream.isEnabled("FP")) {
                    infoStream.message("FP", "trigger flush: activeBytes=" + control.activeBytes() + " deleteBytes=" + control.getDeleteBytesUsed() + " vs limit=" + limit);
                }
                // 由于此时内存中数据过多导致的刷盘
                markLargestWriterPending(control, perThread);
            }
        }
    }

    /**
     * Marks the most ram consuming active {@link DocumentsWriterPerThread} flush
     * pending
     * 由于此时占用的内存比较多 从而触发的刷盘
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
