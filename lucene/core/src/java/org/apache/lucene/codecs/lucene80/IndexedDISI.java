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
package org.apache.lucene.codecs.lucene80;

import java.io.DataInput;
import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;

/**
 * Disk-based implementation of a {@link DocIdSetIterator} which can return
 * the index of the current document, i.e. the ordinal of the current document
 * among the list of documents that this iterator can return. This is useful
 * to implement sparse doc values by only having to encode values for documents
 * that actually have a value.
 * <p>Implementation-wise, this {@link DocIdSetIterator} is inspired of
 * {@link RoaringDocIdSet roaring bitmaps} and encodes ranges of {@code 65536}
 * documents independently and picks between 3 encodings depending on the
 * density of the range:<ul>
 * <li>{@code ALL} if the range contains 65536 documents exactly,
 * <li>{@code DENSE} if the range contains 4096 documents or more; in that
 * case documents are stored in a bit set,
 * <li>{@code SPARSE} otherwise, and the lower 16 bits of the doc IDs are
 * stored in a {@link DataInput#readShort() short}.
 * </ul>
 * <p>Only ranges that contain at least one value are encoded.
 * <p>This implementation uses 6 bytes per document in the worst-case, which happens
 * in the case that all ranges contain exactly one document.
 * <p>
 * <p>
 * To avoid O(n) lookup time complexity, with n being the number of documents, two lookup
 * tables are used: A lookup table for block offset and index, and a rank structure
 * for DENSE block index lookups.
 * <p>
 * The lookup table is an array of {@code int}-pairs, with a pair for each block. It allows for
 * direct jumping to the block, as opposed to iteration from the current position and forward
 * one block at a time.
 * <p>
 * Each int-pair entry consists of 2 logical parts:
 * <p>
 * The first 32 bit int holds the index (number of set bits in the blocks) up to just before the
 * wanted block. The maximum number of set bits is the maximum number of documents, which is < 2^31.
 * <p>
 * The next int holds the offset in bytes into the underlying slice. As there is a maximum of 2^16
 * blocks, it follows that the maximum size of any block must not exceed 2^15 bytes to avoid
 * overflow (2^16 bytes if the int is treated as unsigned). This is currently the case, with the
 * largest block being DENSE and using 2^13 + 36 bytes.
 * <p>
 * The cache overhead is numDocs/1024 bytes.
 * <p>
 * Note: There are 4 types of blocks: ALL, DENSE, SPARSE and non-existing (0 set bits).
 * In the case of non-existing blocks, the entry in the lookup table has index equal to the
 * previous entry and offset equal to the next non-empty block.
 * <p>
 * The block lookup table is stored at the end of the total block structure.
 * <p>
 * <p>
 * The rank structure for DENSE blocks is an array of byte-pairs with an entry for each
 * sub-block (default 512 bits) out of the 65536 bits in the outer DENSE block.
 * <p>
 * Each rank-entry states the number of set bits within the block up to the bit before the
 * bit positioned at the start of the sub-block.
 * Note that that the rank entry of the first sub-block is always 0 and that the last entry can
 * at most be 65536-2 = 65634 and thus will always fit into an byte-pair of 16 bits.
 * <p>
 * The rank structure for a given DENSE block is stored at the beginning of the DENSE block.
 * This ensures locality and keeps logistics simple.
 *
 * @lucene.internal 该对象负责将 零散的docValue 连接起来
 */
final class IndexedDISI extends DocIdSetIterator {

    // jump-table time/space trade-offs to consider:
    // The block offsets and the block indexes could be stored in more compressed form with
    // two PackedInts or two MonotonicDirectReaders.
    // The DENSE ranks (default 128 shorts = 256 bytes) could likewise be compressed. But as there is
    // at least 4096 set bits in DENSE blocks, there will be at least one rank with 2^12 bits, so it
    // is doubtful if there is much to gain here.

    private static final int BLOCK_SIZE = 65536;   // The number of docIDs that a single block represents

    private static final int DENSE_BLOCK_LONGS = BLOCK_SIZE / Long.SIZE; // 1024
    public static final byte DEFAULT_DENSE_RANK_POWER = 9; // Every 512 docIDs / 8 longs

    /**
     * 当某个block下doc总数小于该值 采用连续存储的方式
     */
    static final int MAX_ARRAY_LENGTH = (1 << 12) - 1;


    /**
     * 当此时的docId 切换到下一个block时 需要将之前的数据写入到 文件中
     * @param block          代表block的下标
     * @param buffer         存储数据的位图对象  长度固定为 65535  此时位图已经存储了划分到该block下的所有doc (只取低16位)
     * @param cardinality    当前block中总计写入了多少数据
     * @param denseRankPower rank因子对象  范围在 7~15之内 默认为9
     * @param out            数据将会持久化到该文件中
     * @throws IOException
     */
    private static void flush(
            int block, FixedBitSet buffer, int cardinality, byte denseRankPower, IndexOutput out) throws IOException {
        assert block >= 0 && block < 65536;

        // 写入此时是第几个block
        out.writeShort((short) block);
        assert cardinality > 0 && cardinality <= 65536;
        // !!! 注意这里写入的值 -1了
        out.writeShort((short) (cardinality - 1));
        // 这个是参考那个稀疏视图的实现 也就是对数据进行分级
        // 本次写入的数据超过了 某个阈值 可以采用分块存储的方式
        if (cardinality > MAX_ARRAY_LENGTH) {
            // 不会出现这种情况  在 Lucene80DocValuesConsumer中已经判断过了 所有doc都更新docValue的情况不会进入这里
            if (cardinality != 65536) { // all docs are set
                if (denseRankPower != -1) {
                    // denseRankPower 越大 rank[] 就越大 rank每2个值分别记录高8位和低8位 (有关这个rank内位图有多少有效值)
                    final byte[] rank = createRank(buffer, denseRankPower);
                    // 将rank数组信息写入到 out中
                    out.writeBytes(rank, rank.length);
                }
                // 有了上面的rank[] 能够知道每多少long内有多少有效值  同时可以直接定位目标doc大概在哪个rank内
                for (long word : buffer.getBits()) {
                    out.writeLong(word);
                }
            }
        } else {
            // 代表数据比较松散 那么直接追加数据就好
            BitSetIterator it = new BitSetIterator(buffer, cardinality);
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                out.writeShort((short) doc);
            }
        }
    }

    /**
     * Creates a DENSE rank-entry (the number of set bits up to a given point) for the buffer.
     * One rank-entry for every {@code 2^denseRankPower} bits, with each rank-entry using 2 bytes.
     * Represented as a byte[] for fast flushing and mirroring of the retrieval representation.
     * 创建rank[] 此时还没有填充数据
     *
     * @param buffer
     * @param denseRankPower  这个值越大 每个rank包含的long值越多 同时范围也越宽(rank越少)
     * @return
     */
    private static byte[] createRank(FixedBitSet buffer, byte denseRankPower) {
        // 对于位图的每多少个long 允许生成一个 rank
        // denseRankPower 最大值为15 也就是longsPerRank=512  最大就是2个rank rank[]长度为4
        final int longsPerRank = 1 << (denseRankPower - 6);
        // 获得掩码
        final int rankMark = longsPerRank - 1;
        // DENSE_BLOCK_LONGS >> rankIndexShift 用于计算有多少rank   rank[] 每次都是连续读取2个值的 也就是有效长度只有一半
        final int rankIndexShift = denseRankPower - 7; // 6 for the long (2^6) + 1 for 2 bytes/entry
        // rank[]的大小刚好是 longsPerRank 的2倍
        final byte[] rank = new byte[DENSE_BLOCK_LONGS >> rankIndexShift];
        // 以long[]的形式返回位图对象
        final long[] bits = buffer.getBits();
        int bitCount = 0;
        // 默认情况下 使用1024个long值来存储数据
        for (int word = 0; word < DENSE_BLOCK_LONGS; word++) {
            // 代表每多少个word 才允许写入一个到 rank中
            if ((word & rankMark) == 0) { // Every longsPerRank longs
                // 连续写入2个值 一个只记录高8位的值  一个只记录低8位
                // word >> rankIndexShift 就是计算 该word落在哪个 rank上
                rank[word >> rankIndexShift] = (byte) (bitCount >> 8);
                rank[(word >> rankIndexShift) + 1] = (byte) (bitCount & 0xFF);
            }
            // 当没有达到rank的限制时  将每个long 有多少有效位记录下来
            bitCount += Long.bitCount(bits[word]);
        }
        return rank;
    }

    /**
     * Writes the docIDs from it to out, in logical blocks, one for each 65536 docIDs in monotonically increasing
     * gap-less order. DENSE blocks uses {@link #DEFAULT_DENSE_RANK_POWER} of 9 (every 512 docIDs / 8 longs).
     * The caller must keep track of the number of jump-table entries (returned by this method) as well as the
     * denseRankPower (9 for this method) and provide them when constructing an IndexedDISI for reading.
     *
     * @param it  the document IDs.
     * @param out destination for the blocks.
     * @return the number of jump-table entries following the blocks, -1 for no entries.
     * This should be stored in meta and used when creating an instance of IndexedDISI.
     * @throws IOException if there was an error writing to out.
     */
    static short writeBitSet(DocIdSetIterator it, IndexOutput out) throws IOException {
        return writeBitSet(it, out, DEFAULT_DENSE_RANK_POWER);
    }

    /**
     * Writes the docIDs from it to out, in logical blocks, one for each 65536 docIDs in monotonically
     * increasing gap-less order.
     * The caller must keep track of the number of jump-table entries (returned by this method) as well as the
     * denseRankPower and provide them when constructing an IndexedDISI for reading.
     *
     * @param it             the document IDs.                                                                                      迭代本次所有docId   当某次要刷盘的doc索引文件中 有很多doc写入失败 那么docId必然是不连续的 这时就使用跳跃结构来节省空间
     * @param out            destination for the blocks.                                                                            存储结果的输出流
     * @param denseRankPower for {@link Method#DENSE} blocks, a rank will be written every {@code 2^denseRankPower} docIDs.
     *                       Values &lt; 7 (every 128 docIDs) or &gt; 15 (every 32768 docIDs) disables DENSE rank.
     *                       Recommended values are 8-12: Every 256-4096 docIDs or 4-64 longs.
     *                       {@link #DEFAULT_DENSE_RANK_POWER} is 9: Every 512 docIDs.                                              一种因子 用来确定某个block下存储多少docId  一般不推荐太大或太小 (小于7大于15)  默认值为9
     *                       This should be stored in meta and used when creating an instance of IndexedDISI.
     * @return the number of jump-table entries following the blocks, -1 for no entries.
     * This should be stored in meta and used when creating an instance of IndexedDISI.
     * @throws IOException if there was an error writing to out.
     */
    static short writeBitSet(DocIdSetIterator it, IndexOutput out, byte denseRankPower) throws IOException {
        // 获取当前写入的起点
        final long origo = out.getFilePointer(); // All jumps are relative to the origo
        if ((denseRankPower < 7 || denseRankPower > 15) && denseRankPower != -1) {
            throw new IllegalArgumentException("Acceptable values for denseRankPower are 7-15 (every 128-32768 docIDs). " +
                    "The provided power was " + denseRankPower + " (every " + (int) Math.pow(2, denseRankPower) + " docIDs)");
        }
        // 记录所有block 总计存储了多少数据
        int totalCardinality = 0;
        // 在切换到下一个块之前已经写入了多少数据了
        int blockCardinality = 0;
        // 65535/64 = 1024 也就是该位图由1024个long组成
        final FixedBitSet buffer = new FixedBitSet(1 << 16);

        // 如果连续每2个下标读取的值是一样的 就代表这些数据在同一个block中 而发生改变前（最后一个数组下标-1）/2 就是block的下标
        int[] jumps = new int[ArrayUtil.oversize(1, Integer.BYTES * 2)];

        // 记录上一个值被划分到哪个block
        int prevBlock = -1;
        int jumpBlockIndex = 0;

        // 遍历所有docId  一般会进入到该方法 那么docId 一定不是连续的
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            // 每个block的大小65535
            final int block = doc >>> 16;
            // 代表上一个block有效 并且本次的值被划分到与上次不同的block
            if (prevBlock != -1 && block != prevBlock) {
                // Track offset+index from previous block up to current
                // 将block相关信息写入到 jumps中
                jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, jumpBlockIndex, prevBlock + 1);
                jumpBlockIndex = prevBlock + 1;
                // Flush block  每当切换一个block时 将之前的数据刷盘
                flush(prevBlock, buffer, blockCardinality, denseRankPower, out);
                // Reset for next block
                // 清空相关数据
                buffer.clear(0, buffer.length());
                totalCardinality += blockCardinality;
                blockCardinality = 0;
            }
            // 仅保留最后 16位
            buffer.set(doc & 0xFFFF);
            blockCardinality++;
            prevBlock = block;
        }
        // 代表还有剩余的数据 单独写入一次
        if (blockCardinality > 0) {
            jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, jumpBlockIndex, prevBlock + 1);
            totalCardinality += blockCardinality;
            flush(prevBlock, buffer, blockCardinality, denseRankPower, out);
            buffer.clear(0, buffer.length());
            prevBlock++;
        }

        // 查找上一个写入的 block
        final int lastBlock = prevBlock == -1 ? 0 : prevBlock; // There will always be at least 1 block (NO_MORE_DOCS)
        // Last entry is a SPARSE with blockIndex == 32767 and the single entry 65535, which becomes the docID NO_MORE_DOCS
        // To avoid creating 65K jump-table entries, only a single entry is created pointing to the offset of the
        // NO_MORE_DOCS block, with the jumpBlockIndex set to the logical EMPTY block after all real blocks.
        // 针对最后一次写入 还需要单独生成 jumps
        jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, lastBlock, lastBlock + 1);

        // 这里又写入了一个 满位图
        buffer.set(DocIdSetIterator.NO_MORE_DOCS & 0xFFFF);
        // 最后一次写入 block为0
        flush(DocIdSetIterator.NO_MORE_DOCS >>> 16, buffer, 1, denseRankPower, out);
        // offset+index jump-table stored at the end
        // 将 jump信息持久化
        return flushBlockJumps(jumps, lastBlock + 1, out, origo);
    }

    /**
     * Adds entries to the offset & index jump-table for blocks
     * 构建一个 jumpTableEntry对象 并存储到 data文件中
     *
     * @param jumps      存储相关信息的容器
     * @param offset     距离调用writeBitSet时 data文件起始位置的偏移量
     * @param index      总计写入了多少docId
     * @param startBlock 连接到下标为多少的 block
     * @param endBlock   下一个block
     * @return
     */
    private static int[] addJumps(int[] jumps, long offset, int index, int startBlock, int endBlock) {
        assert offset < Integer.MAX_VALUE : "Logically the offset should not exceed 2^30 but was >= Integer.MAX_VALUE";
        jumps = ArrayUtil.grow(jumps, (endBlock + 1) * 2);
        // 记录每个block 内部写入多少数据 以及起点在哪
        for (int b = startBlock; b < endBlock; b++) {
            jumps[b * 2] = index;
            jumps[b * 2 + 1] = (int) offset;
        }
        return jumps;
    }

    // Flushes the offset & index jump-table for blocks. This should be the last data written to out
    // This method returns the blockCount for the blocks reachable for the jump_table or -1 for no jump-table
    // 写入 jump信息
    private static short flushBlockJumps(int[] jumps, int blockCount, IndexOutput out, long origo) throws IOException {
        // 代表只使用了 一个 block 因为最后还会针对NO_MORE_DOCS 写入一次数据  这种特殊情况直接将block置0
        if (blockCount == 2) { // Jumps with a single real entry + NO_MORE_DOCS is just wasted space so we ignore that
            blockCount = 0;
        }
        for (int i = 0; i < blockCount; i++) {
            out.writeInt(jumps[i * 2]); // index
            out.writeInt(jumps[i * 2 + 1]); // offset
        }
        // As there are at most 32k blocks, the count is a short
        // The jumpTableOffset will be at lastPos - (blockCount * Long.BYTES)
        return (short) blockCount;
    }

    // Members are pkg-private to avoid synthetic accessors when accessed from the `Method` enum

    /**
     * The slice that stores the {@link DocIdSetIterator}.
     */
    final IndexInput slice;
    final int jumpTableEntryCount;
    final byte denseRankPower;
    /**
     * jumpTable 本身是用于快速定位 block的起点位置
     */
    final RandomAccessInput jumpTable; // Skip blocks of 64K bits
    /**
     * 以紧凑模式读取数据时 存储数据的容器
     */
    final byte[] denseRankTable;
    final long cost;

    /**
     * This constructor always creates a new blockSlice and a new jumpTable from in, to ensure that operations are
     * independent from the caller.
     * See {@link #IndexedDISI(IndexInput, RandomAccessInput, int, byte, long)} for re-use of blockSlice and jumpTable.
     *
     * @param in                  backing data.       对应存储数据的输入流
     * @param offset              starting offset for blocks in the backing data.
     * @param length              the number of bytes holding blocks and jump-table in the backing data.
     * @param jumpTableEntryCount the number of blocks covered by the jump-table.
     *                            This must match the number returned by {@link #writeBitSet(DocIdSetIterator, IndexOutput, byte)}.
     * @param denseRankPower      the number of docIDs covered by each rank entry in DENSE blocks, expressed as {@code 2^denseRankPower}.
     *                            This must match the power given in {@link #writeBitSet(DocIdSetIterator, IndexOutput, byte)}
     * @param cost                normally the number of logical docIDs.
     */
    IndexedDISI(IndexInput in, long offset, long length, int jumpTableEntryCount, byte denseRankPower, long cost) throws IOException {
        // createBlockSlice，createJumpTable 分别从in中读取2部分数据
        this(createBlockSlice(in, "docs", offset, length, jumpTableEntryCount),
                createJumpTable(in, offset, length, jumpTableEntryCount),
                jumpTableEntryCount, denseRankPower, cost);
    }

    /**
     * This constructor allows to pass the slice and jumpTable directly in case it helps reuse.
     * see eg. Lucene80 norms producer's merge instance.
     *
     * @param blockSlice          data blocks, normally created by {@link #createBlockSlice}.     对应 block数据的in分片
     * @param jumpTable           table holding jump-data for block-skips, normally created by {@link #createJumpTable}.   对应 jumpTable数据的in分片
     * @param jumpTableEntryCount the number of blocks covered by the jump-table.
     *                            This must match the number returned by {@link #writeBitSet(DocIdSetIterator, IndexOutput, byte)}.
     * @param denseRankPower      the number of docIDs covered by each rank entry in DENSE blocks, expressed as {@code 2^denseRankPower}.
     *                            This must match the power given in {@link #writeBitSet(DocIdSetIterator, IndexOutput, byte)}   存储紧凑数据的容器大小
     * @param cost                normally the number of logical docIDs.
     */
    IndexedDISI(IndexInput blockSlice, RandomAccessInput jumpTable, int jumpTableEntryCount, byte denseRankPower, long cost) throws IOException {
        if ((denseRankPower < 7 || denseRankPower > 15) && denseRankPower != -1) {
            throw new IllegalArgumentException("Acceptable values for denseRankPower are 7-15 (every 128-32768 docIDs). " +
                    "The provided power was " + denseRankPower + " (every " + (int) Math.pow(2, denseRankPower) + " docIDs). ");
        }

        this.slice = blockSlice;
        this.jumpTable = jumpTable;
        this.jumpTableEntryCount = jumpTableEntryCount;
        this.denseRankPower = denseRankPower;
        // 2的7次是128
        final int rankIndexShift = denseRankPower - 7;
        this.denseRankTable = denseRankPower == -1 ? null : new byte[DENSE_BLOCK_LONGS >> rankIndexShift];
        this.cost = cost;
    }

    /**
     * Helper method for using {@link #IndexedDISI(IndexInput, RandomAccessInput, int, byte, long)}.
     * Creates a disiSlice for the IndexedDISI data blocks, without the jump-table.
     *
     * @param slice               backing data, holding both blocks and jump-table.    数据都存储在该输入流中
     * @param sliceDescription    human readable slice designation.
     * @param offset              relative to the backing data.     对应slice的某个偏移量
     * @param length              full length of the IndexedDISI, including blocks and jump-table data.    代表这个DISI 的总大小  包含了 block数据 以及 jump-table 内的数据
     * @param jumpTableEntryCount the number of blocks covered by the jump-table.
     * @return a jumpTable containing the block jump-data or null if no such table exists.
     * @throws IOException if a RandomAccessInput could not be created from slice.
     */
    public static IndexInput createBlockSlice(
            IndexInput slice, String sliceDescription, long offset, long length, int jumpTableEntryCount) throws IOException {
        // 这里在预估跳跃表的大小   每个块占用8 byte
        long jumpTableBytes = jumpTableEntryCount < 0 ? 0 : jumpTableEntryCount * Integer.BYTES * 2;
        // 这里为block的数据创建分片
        return slice.slice(sliceDescription, offset, length - jumpTableBytes);
    }

    /**
     * Helper method for using {@link #IndexedDISI(IndexInput, RandomAccessInput, int, byte, long)}.
     * Creates a RandomAccessInput covering only the jump-table data or null.
     *
     * @param slice               backing data, holding both blocks and jump-table.
     * @param offset              relative to the backing data.   指定data输入流中数据开始的位置
     * @param length              full length of the IndexedDISI, including blocks and jump-table data.
     * @param jumpTableEntryCount the number of blocks covered by the jump-table.
     * @return a jumpTable containing the block jump-data or null if no such table exists.
     * @throws IOException if a RandomAccessInput could not be created from slice.
     *                     根据从 data中 还原jumpTable 内部的数据
     */
    public static RandomAccessInput createJumpTable(
            IndexInput slice, long offset, long length, int jumpTableEntryCount) throws IOException {
        if (jumpTableEntryCount <= 0) {
            return null;
        } else {
            int jumpTableBytes = jumpTableEntryCount * Integer.BYTES * 2;
            // 看来在data的连续数据中 前面存储的是元数据  后面的是 jumpTable数据
            return slice.randomAccessSlice(offset + length - jumpTableBytes, jumpTableBytes);
        }
    }

    // 每个block 最大能存储 65535 个 docId 逻辑上的  然后根据docId 实际的数量再选择合适的数据结构存储数据 （这个是物理上的  所以通过一定算法可以减少实际分配的空间）

    /**
     * block 起始对应的docId
     */
    int block = -1;
    /**
     * 当前block 结尾的位置
     */
    long blockEnd;
    /**
     * 代表视图的起始偏移量 用于计算seek的位置
     */
    long denseBitmapOffset = -1; // Only used for DENSE blocks
    /**
     * 对应下一个block 起始的docIndex
     */
    int nextBlockIndex = -1;
    Method method;

    int doc = -1;
    /**
     * 当前读取到了block的哪个位置  一个block下有多个值
     */
    int index = -1;

    // SPARSE variables
    boolean exists;

    // DENSE variables
    long word;
    int wordIndex = -1;
    // number of one bits encountered so far, including those of `word`   这是一个累加值 代表该block 下所有word消耗的bit数总和
    int numberOfOnes;
    // Used with rank for jumps inside of DENSE as they are absolute instead of relative
    int denseOrigoIndex;

    // ALL variables
    int gap;

    /**
     * 返回当前 docId
     *
     * @return
     */
    @Override
    public int docID() {
        return doc;
    }

    /**
     * 切换到 目标值  如果没有则选择大于该目标值的最小docId
     *
     * @param target
     * @return
     * @throws IOException
     */
    @Override
    public int advance(int target) throws IOException {
        // 看来每个 block 的大小是  0x0000FFFF  (2的16次)
        final int targetBlock = target & 0xFFFF0000;
        // 代表需要切换到对应的 block的位置
        if (block < targetBlock) {
            advanceBlock(targetBlock);
        }
        // 在定位到 block后 尝试寻找有没有匹配的doc
        if (block == targetBlock) {
            if (method.advanceWithinBlock(this, target)) {
                return doc;
            }
            // 代表已经读取到该block的末尾了 切换到下一个block
            readBlockHeader();
        }
        boolean found = method.advanceWithinBlock(this, block);
        assert found;
        return doc;
    }

    /**
     * 代表精确查找
     *
     * @param target
     * @return
     * @throws IOException
     */
    public boolean advanceExact(int target) throws IOException {
        final int targetBlock = target & 0xFFFF0000;
        if (block < targetBlock) {
            advanceBlock(targetBlock);
        }
        boolean found = block == targetBlock && method.advanceExactWithinBlock(this, target);
        this.doc = target;
        return found;
    }

    /**
     * 转移到目标block
     *
     * @param targetBlock
     * @throws IOException
     */
    private void advanceBlock(int targetBlock) throws IOException {
        // 这里兑换成一个 block下标   (每个block大小为2的16次)
        final int blockIndex = targetBlock >> 16;
        // If the destination block is 2 blocks or more ahead, we use the jump-table.
        // 代表当前block与 目标block 相差2个block 及以上
        if (jumpTable != null && blockIndex >= (block >> 16) + 2) {
            // If the jumpTableEntryCount is exceeded, there are no further bits. Last entry is always NO_MORE_DOCS
            // 看来一个 jumpTableEntry 定位一个block的位置信息  （jumpTableEntryCount-1对应NO_MORE_DOCS）
            final int inRangeBlockIndex = blockIndex < jumpTableEntryCount ? blockIndex : jumpTableEntryCount - 1;
            // 定位到 8的位置 读取到12
            final int index = jumpTable.readInt(inRangeBlockIndex * Integer.BYTES * 2);
            // 12到16的位置对应 offset
            final int offset = jumpTable.readInt(inRangeBlockIndex * Integer.BYTES * 2 + Integer.BYTES);
            // 这个nextBlockIndex 主要是为了在之后为 index 赋值   总是当前读取的起始位置-1
            this.nextBlockIndex = index - 1; // -1 to compensate for the always-added 1 in readBlockHeader
            // 将block分片定位到对应的位置  block 内部存储的就是 docValue
            slice.seek(offset);
            // 通过解析 block头部 获取一些基础信息
            readBlockHeader();
            return;
        }

        // Fallback to iteration of blocks  挨个读取block
        do {
            slice.seek(blockEnd);
            readBlockHeader();
        } while (block < targetBlock);
    }

    /**
     * 这时 slice 已经定位到某个block的起始位置
     *
     * @throws IOException
     */
    private void readBlockHeader() throws IOException {
        // 通过 <<16 将下标还原成偏移量
        block = Short.toUnsignedInt(slice.readShort()) << 16;
        assert block >= 0;
        // 代表该block 下存储了多少docValue
        final int numValues = 1 + Short.toUnsignedInt(slice.readShort());
        index = nextBlockIndex;
        nextBlockIndex = index + numValues;
        // 这里以3种方式存储 对应那个叫啥的位图   当数据量本身就少的时候 采用稀疏的存储方式  也就是直接将doc连续存储在slice中
        if (numValues <= MAX_ARRAY_LENGTH) {
            method = Method.SPARSE;
            blockEnd = slice.getFilePointer() + (numValues << 1);
            // 65535 = 2<<15  也就是刚好一个block被填满
        } else if (numValues == 65536) {
            method = Method.ALL;
            blockEnd = slice.getFilePointer();
            // block 的起始值 代表 docId的起始值
            gap = block - index - 1;
        } else {
            method = Method.DENSE;
            denseBitmapOffset = slice.getFilePointer() + (denseRankTable == null ? 0 : denseRankTable.length);
            // 后面的 1<<13 属于占位符
            blockEnd = denseBitmapOffset + (1 << 13);
            // Performance consideration: All rank (default 128 * 16 bits) are loaded up front. This should be fast with the
            // reusable byte[] buffer, but it is still wasted if the DENSE block is iterated in small steps.
            // If this results in too great a performance regression, a heuristic strategy might work where the rank data
            // are loaded on first in-block advance, if said advance is > X docIDs. The hope being that a small first
            // advance means that subsequent advances will be small too.
            // Another alternative is to maintain an extra slice for DENSE rank, but IndexedDISI is already slice-heavy.
            // 先从分片中读取一部分数据
            if (denseRankPower != -1) {
                slice.readBytes(denseRankTable, 0, denseRankTable.length);
            }
            wordIndex = -1;
            numberOfOnes = index + 1;
            denseOrigoIndex = numberOfOnes;
        }
    }

    /**
     * 切换到下个doc
     *
     * @return
     * @throws IOException
     */
    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    public int index() {
        return index;
    }

    @Override
    public long cost() {
        return cost;
    }

    /**
     * 使用3种方式存储数据
     */
    enum Method {

        /**
         * 当某个block内部的数据比较少时
         */
        SPARSE {
            /**
             * 在当前block下寻找 doc
             * @param disi
             * @param target
             * @return
             * @throws IOException
             */
            @Override
            boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
                final int targetInBlock = target & 0xFFFF;
                // TODO: binary search
                for (; disi.index < disi.nextBlockIndex; ) {
                    // 该block下所有的docId 比较稀疏 直接连续存储
                    int doc = Short.toUnsignedInt(disi.slice.readShort());
                    disi.index++;
                    if (doc >= targetInBlock) {
                        // 这里的 doc 只是在该block下的相对偏移量 通过 disi.block| 运算 获得绝对偏移量
                        disi.doc = disi.block | doc;
                        disi.exists = true;
                        return true;
                    }
                }
                return false;
            }

            /**
             * 精确匹配
             * @param disi
             * @param target
             * @return
             * @throws IOException
             */
            @Override
            boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
                final int targetInBlock = target & 0xFFFF;
                // TODO: binary search
                if (target == disi.doc) {
                    return disi.exists;
                }
                for (; disi.index < disi.nextBlockIndex; ) {
                    int doc = Short.toUnsignedInt(disi.slice.readShort());
                    disi.index++;
                    if (doc >= targetInBlock) {
                        // 代表下个值超过了指定的doc
                        if (doc != targetInBlock) {
                            // 回指到上一个值
                            disi.index--;
                            disi.slice.seek(disi.slice.getFilePointer() - Short.BYTES);
                            break;
                        }
                        disi.exists = true;
                        return true;
                    }
                }
                disi.exists = false;
                return false;
            }
        },
        DENSE {
            /**
             * 找到大于等于 target的最小的值
             * @param disi
             * @param target
             * @return
             * @throws IOException
             */
            @Override
            boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
                // target 在该block下的起始位置
                final int targetInBlock = target & 0xFFFF;
                // 同样的套路 将 65535 通过位运算 拆解成2个值
                // 2<<6 对应64  也就是 64*1024     这个跟那个稀疏位图的实现是类似的      每个word 代表一个标识 代表它下属的64个docId中至少有一个值 总计1024个标识 可以表达65535个docId
                final int targetWordIndex = targetInBlock >>> 6;

                // If possible, skip ahead using the rank cache
                // If the distance between the current position and the target is < rank-longs
                // there is no sense in using rank
                // 每次应该是读取批量数据 并存储在 denseRank容器中 后面的  公式应该是计算大小之类的  也就是wordIndex的差距超过了容器大小 需要重新读取数据到容器中
                if (disi.denseRankPower != -1 && targetWordIndex - disi.wordIndex >= (1 << (disi.denseRankPower - 6))) {
                    rankSkip(disi, targetInBlock);
                }

                // 正常流程从这里开始  也就是 word 应该是连续设置的
                for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {
                    disi.word = disi.slice.readLong();
                    // 累加word消耗的总bit
                    disi.numberOfOnes += Long.bitCount(disi.word);
                }
                disi.wordIndex = targetWordIndex;

                // 左侧除了 目标值外 还有多少bit
                long leftBits = disi.word >>> target;
                if (leftBits != 0L) {
                    disi.doc = target + Long.numberOfTrailingZeros(leftBits);
                    disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
                    return true;
                }

                // There were no set bits at the wanted position. Move forward until one is reached
                while (++disi.wordIndex < 1024) {
                    // This could use the rank cache to skip empty spaces >= 512 bits, but it seems unrealistic
                    // that such blocks would be DENSE
                    disi.word = disi.slice.readLong();
                    if (disi.word != 0) {
                        disi.index = disi.numberOfOnes;
                        disi.numberOfOnes += Long.bitCount(disi.word);
                        disi.doc = disi.block | (disi.wordIndex << 6) | Long.numberOfTrailingZeros(disi.word);
                        return true;
                    }
                }
                // No set bits in the block at or after the wanted position.
                return false;
            }

            @Override
            boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
                final int targetInBlock = target & 0xFFFF;
                final int targetWordIndex = targetInBlock >>> 6;

                // If possible, skip ahead using the rank cache
                // If the distance between the current position and the target is < rank-longs
                // there is no sense in using rank
                if (disi.denseRankPower != -1 && targetWordIndex - disi.wordIndex >= (1 << (disi.denseRankPower - 6))) {
                    rankSkip(disi, targetInBlock);
                }

                for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {
                    disi.word = disi.slice.readLong();
                    disi.numberOfOnes += Long.bitCount(disi.word);
                }
                disi.wordIndex = targetWordIndex;

                long leftBits = disi.word >>> target;
                disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
                return (leftBits & 1L) != 0;
            }


        },
        /**
         * 代表该block被填满 所以数据一定存在
         */
        ALL {
            @Override
            boolean advanceWithinBlock(IndexedDISI disi, int target) {
                disi.doc = target;
                disi.index = target - disi.gap;
                return true;
            }

            @Override
            boolean advanceExactWithinBlock(IndexedDISI disi, int target) {
                disi.index = target - disi.gap;
                return true;
            }
        };

        /**
         * Advance to the first doc from the block that is equal to or greater than {@code target}.
         * Return true if there is such a doc and false otherwise.
         */
        abstract boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException;

        /**
         * Advance the iterator exactly to the position corresponding to the given {@code target}
         * and return whether this document exists.
         */
        abstract boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException;
    }

    /**
     * If the distance between the current position and the target is > 8 words, the rank cache will
     * be used to guarantee a worst-case of 1 rank-lookup and 7 word-read-and-count-bits operations.
     * Note: This does not guarantee a skip up to target, only up to nearest rank boundary. It is the
     * responsibility of the caller to iterate further to reach target.
     *
     * @param disi          standard DISI.
     * @param targetInBlock lower 16 bits of the target   docId >> block  也就是docId在该block的相对偏移量
     * @throws IOException if a DISI seek failed.
     */
    private static void rankSkip(IndexedDISI disi, int targetInBlock) throws IOException {
        assert disi.denseRankPower >= 0 : disi.denseRankPower;
        // Resolve the rank as close to targetInBlock as possible (maximum distance is 8 longs)
        // Note: rankOrigoOffset is tracked on block open, so it is absolute (e.g. don't add origo)
        // 看来数据存储还有不同的级别
        final int rankIndex = targetInBlock >> disi.denseRankPower; // Default is 9 (8 longs: 2^3 * 2^6 = 512 docIDs)

        // TODO 这里还不清楚在算什么
        final int rank =
                (disi.denseRankTable[rankIndex << 1] & 0xFF) << 8 |
                        (disi.denseRankTable[(rankIndex << 1) + 1] & 0xFF);

        // Position the counting logic just after the rank point
        final int rankAlignedWordIndex = rankIndex << disi.denseRankPower >> 6;
        disi.slice.seek(disi.denseBitmapOffset + rankAlignedWordIndex * Long.BYTES);
        long rankWord = disi.slice.readLong();
        int denseNOO = rank + Long.bitCount(rankWord);

        disi.wordIndex = rankAlignedWordIndex;
        disi.word = rankWord;
        disi.numberOfOnes = disi.denseOrigoIndex + denseNOO;
    }
}
