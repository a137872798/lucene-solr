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
package org.apache.lucene.util.fst;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;

/** Provides storage of finite state machine (FST),
 *  using byte array or byte store allocated on heap.
 *
 * @lucene.experimental
 * 代表一个基于heap的fst存储对象
 */
public final class OnHeapFSTStore implements FSTStore {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OnHeapFSTStore.class);

    /** A {@link BytesStore}, used during building, or during reading when
     *  the FST is very large (more than 1 GB).  If the FST is less than 1
     *  GB then bytesArray is set instead. */
    // 该对象内部实际存储 依靠 List<byte[]>
    private BytesStore bytes;

    /** Used at read time when the FST fits into a single byte[]. */
    private byte[] bytesArray;

    /**
     * 每个block 最多对应多少bit
     */
    private final int maxBlockBits;

    public OnHeapFSTStore(int maxBlockBits) {
        if (maxBlockBits < 1 || maxBlockBits > 30) {
            throw new IllegalArgumentException("maxBlockBits should be 1 .. 30; got " + maxBlockBits);
        }

        this.maxBlockBits = maxBlockBits;
    }

    /**
     * 通过一组输入流进行初始化
     * @param in
     * @param numBytes  代表需要占用多少个byte
     * @throws IOException
     */
    @Override
    public void init(DataInput in, long numBytes) throws IOException {
        // 代表单个block 就可以存储
        if (numBytes > 1 << this.maxBlockBits) {
            // FST is big: we need multiple pages
            // 将数据存入到 多个byte[] 中
            bytes = new BytesStore(in, numBytes, 1<<this.maxBlockBits);
        } else {
            // FST fits into a single block: use ByteArrayBytesStoreReader for less overhead
            // 创建等大的 byte[]
            bytesArray = new byte[(int) numBytes];
            // 用输入流中的数据填满 数组
            in.readBytes(bytesArray, 0, bytesArray.length);
        }
    }

    @Override
    public long size() {
        if (bytesArray != null) {
            return bytesArray.length;
        } else {
            return bytes.ramBytesUsed();
        }
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + size();
    }

    /**
     * 返回一个反向读取的reader
     * @return
     */
    @Override
    public FST.BytesReader getReverseBytesReader() {
        if (bytesArray != null) {
            return new ReverseBytesReader(bytesArray);
        } else {
            return bytes.getReverseReader();
        }
    }

    /**
     * 将当前对象内部的数据写入到输出流中
     * @param out
     * @throws IOException
     */
    @Override
    public void writeTo(DataOutput out) throws IOException {
        if (bytes != null) {
            long numBytes = bytes.getPosition();
            out.writeVLong(numBytes);
            bytes.writeTo(out);
        } else {
            assert bytesArray != null;
            // 写入长度 和数据
            out.writeVLong(bytesArray.length);
            out.writeBytes(bytesArray, 0, bytesArray.length);
        }
    }
}
