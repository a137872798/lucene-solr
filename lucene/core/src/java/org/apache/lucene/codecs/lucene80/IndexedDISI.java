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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;

import java.io.DataInput;
import java.io.IOException;

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
     *
     * @param block          本次 buffer数据对应的block
     * @param buffer         存储数据的位图对象  长度固定为 65535  此时位图已经存储了划分到该block下的所有doc
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
        // !!! 注意这里写入的值 -1了  可能是转换成了下标
        out.writeShort((short) (cardinality - 1));
        // 这个是参考那个稀疏视图的实现 也就是根据block内数据量分级  不同级别使用不同算法
        // 本次写入的数据超过了 某个阈值 可以采用分块存储的方式

        // 代表分到该block下的数据 较多 不挨个写入了
        if (cardinality > MAX_ARRAY_LENGTH) {
            // 当该block 刚好会被填满时  不写入任何数据  (全都有只要检测未写入 然后挨个读取65535个值就好)
            if (cardinality != 65536) { // all docs are set
                if (denseRankPower != -1) {
                    // 将一个block进一步拆解成一个rank[] rank的长度代表被拆分成多少段  内部的值代表每个段存储了多少doc
                    final byte[] rank = createRank(buffer, denseRankPower);
                    // 将rank数组信息写入到 out中
                    out.writeBytes(rank, rank.length);
                }
                // 这里直接将压缩的long值写入 使用者读取时自己进行解析   每个long 被当作一个 字    而 rank以每多少个字为单位 记录了目标位置下之前所有字中总计写入了多少docId
                for (long word : buffer.getBits()) {
                    out.writeLong(word);
                }
            }
        } else {
            // 代表数据量较少 直接存储到  out文件中
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
     * @param denseRankPower 取值范围为 7~15   该值越大代表 rank数组越小 每个rank 记录的word就越多
     * @return
     */
    private static byte[] createRank(FixedBitSet buffer, byte denseRankPower) {
        // 取值范围为 2~512       -6 代表换算成字  也就是每个rank下要存储多少个字
        final int longsPerRank = 1 << (denseRankPower - 6);
        // 获得掩码
        final int rankMark = longsPerRank - 1;
        // 取值范围为 0~8      -7 首先是 -6 代表总计要存储多少字 -1 代表每个字占用2个rank[] 槽
        final int rankIndexShift = denseRankPower - 7; // 6 for the long (2^6) + 1 for 2 bytes/entry
        // rank[]的大小范围  1024 >> 0 =1024 ~  1024 >> 8=4   当 rank长度为1024时  longsPerRank 为2   当rank长度为 4时 longsPerRank为 512   可以看到 rank.length*longsPerRank 的值总是2048
        // longsPerRank的意思是 每多少个long值 允许写入到一个rank中
        final byte[] rank = new byte[DENSE_BLOCK_LONGS >> rankIndexShift];
        // 以long[]的形式返回位图对象  代表该block下存储了哪些doc
        final long[] bits = buffer.getBits();
        int bitCount = 0;
        // 默认情况下 使用1024个long值来存储数据   也就是 bits的大小
        for (int word = 0; word < DENSE_BLOCK_LONGS; word++) {
            // 代表每多少个word 才允许写入一个到 rank中
            if ((word & rankMark) == 0) { // Every longsPerRank longs
                // 连续写入2个值 一个只记录高8位的值  一个只记录低8位
                // 下标的取值范围是 4~ 1024 刚好匹配
                // 当rank长度越大时  代表 记录的粒度更细     每个rank 代表之前所有字记录的docId 数量总和
                rank[word >> rankIndexShift] = (byte) (bitCount >> 8);
                rank[(word >> rankIndexShift) + 1] = (byte) (bitCount & 0xFF);
            }
            // 记录该long 下有多少值是有效的    当 longsPerRank为512时 该值最大值为32768  =  65536/2  代表可以用15位去表示  也就是2个long值 也就是占用2个rank的slot
            // 这个值是不断累加的
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
                // 第一次 prevBlock = 0 jumpBlockIndex = 0  第二次如果跳跃 比如此时block是1  startBlock还是0 endBlock 变成1  写入的是上一个 block的数据 jumpBlockIndex 变成了1
                // 第三次如果没跳跃  jumpBlockIndex =1  prevBlock = 1 block = 1
                // 第四次跳跃 jumpBlockIndex = 1 prevBlock = 1  block = 3    这时 startBlock 是 1 endBlock 是2    也就是每个block的信息 都使用一个 jumps的2个slot存储 一个slot存储index 一个slot存储offset
                jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, jumpBlockIndex, prevBlock + 1);
                jumpBlockIndex = prevBlock + 1;
                // flush方法的核心作用还是保存了 docId  当docId较少时 直接全部写入  当docId较多时 创建了一个rank[]数组 将block拆解成多个段记录每个段包含多少docId  之后直接将docId位图对应的long值写入
                // 由使用者在读取时自己解析
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

        // 查找上一个写入的 block   当一个docId都没有写入时 会选择下标为0的block
        final int lastBlock = prevBlock == -1 ? 0 : prevBlock; // There will always be at least 1 block (NO_MORE_DOCS)
        // Last entry is a SPARSE with blockIndex == 32767 and the single entry 65535, which becomes the docID NO_MORE_DOCS
        // To avoid creating 65K jump-table entries, only a single entry is created pointing to the offset of the
        // NO_MORE_DOCS block, with the jumpBlockIndex set to the logical EMPTY block after all real blocks.
        // 针对最后一次写入 还需要单独生成 jumps
        jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, lastBlock, lastBlock + 1);

        // 这里又写入了一个 满位图
        buffer.set(DocIdSetIterator.NO_MORE_DOCS & 0xFFFF);
        // 最后一次flush的block是个特殊值
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
     * @param index      之前所有block写入了多少docId
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
        // 如果实际上只使用了一个 block  + 最后特殊的 NO_MORE_DOCS 那么将block数量看作0
        if (blockCount == 2) { // Jumps with a single real entry + NO_MORE_DOCS is just wasted space so we ignore that
            blockCount = 0;
        }
        // 最后才将jumps信息写入 out
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
     * @param blockSlice          data blocks, normally created by {@link #createBlockSlice}.     对应block数据的in分片
     * @param jumpTable           table holding jump-data for block-skips, normally created by {@link #createJumpTable}.   对应 jumpTable数据的in分片
     * @param jumpTableEntryCount the number of blocks covered by the jump-table.
     *                            This must match the number returned by {@link #writeBitSet(DocIdSetIterator, IndexOutput, byte)}.          jump结构的数量
     * @param denseRankPower      the number of docIDs covered by each rank entry in DENSE blocks, expressed as {@code 2^denseRankPower}.
     *                            This must match the power given in {@link #writeBitSet(DocIdSetIterator, IndexOutput, byte)}     使用的收缩因子  当docId 采用特殊结构存储时 数组的大小受该因子影响
     * @param cost                normally the number of logical docIDs.            代表总计存储了多少 docId
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
        // 还原 rankIndexShift 属性  它决定了rank[] 的大小
        final int rankIndexShift = denseRankPower - 7;
        // 对应 rank[] 的长度信息
        this.denseRankTable = denseRankPower == -1 ? null : new byte[DENSE_BLOCK_LONGS >> rankIndexShift];
        this.cost = cost;
    }

    /**
     * Helper method for using {@link #IndexedDISI(IndexInput, RandomAccessInput, int, byte, long)}.
     * Creates a disiSlice for the IndexedDISI data blocks, without the jump-table.
     *
     * @param slice               backing data, holding both blocks and jump-table.    之前存储disi数据的容器
     * @param sliceDescription    human readable slice designation.
     * @param offset              relative to the backing data.          代表disi数据块在 slice的起始偏移量
     * @param length              full length of the IndexedDISI, including blocks and jump-table data.   代表disi数据的总长度
     * @param jumpTableEntryCount the number of blocks covered by the jump-table.                 生成的 jump数量
     * @return a jumpTable containing the block jump-data or null if no such table exists.
     * @throws IOException if a RandomAccessInput could not be created from slice.
     */
    public static IndexInput createBlockSlice(
            IndexInput slice, String sliceDescription, long offset, long length, int jumpTableEntryCount) throws IOException {
        // 每个 jump结构是由 2个int组成的 对应int[]中连续的2个值
        long jumpTableBytes = jumpTableEntryCount < 0 ? 0 : jumpTableEntryCount * Integer.BYTES * 2;
        // 创建一个更精准的分片    jump结构是最后写入的也就是 length - jumpTableBytes 对应的就是docId的信息
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
     *                     根据slice 创建一个仅定位到 jump[] 的数据分片
     */
    public static RandomAccessInput createJumpTable(
            IndexInput slice, long offset, long length, int jumpTableEntryCount) throws IOException {
        if (jumpTableEntryCount <= 0) {
            return null;
        } else {
            int jumpTableBytes = jumpTableEntryCount * Integer.BYTES * 2;
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
     * 对应  rank[] 数组后
     * for (long word : buffer.getBits()) {
     * out.writeLong(word);
     * }
     */
    long denseBitmapOffset = -1; // Only used for DENSE blocks
    /**
     * 对应下一个block 起始的docIndex
     */
    int nextBlockIndex = -1;
    Method method;

    int doc = -1;
    /**
     * index 代表当前读取到了全局范围内的第几个 doc
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
        // 先通过 去低16位 找到该docId 所在的 block
        final int targetBlock = target & 0xFFFF0000;
        // 代表需要切换到对应的 block的位置
        if (block < targetBlock) {
            // 切换到目标block
            advanceBlock(targetBlock);
        }
        // 在定位到 block后 尝试寻找有没有匹配的doc
        if (block == targetBlock) {
            // 只要当下block 有 >= target 的docId 就返回 true
            if (method.advanceWithinBlock(this, target)) {
                return doc;
            }
            // 代表该block下都是小于目标doc的  切换到下一个block 尝试检测是否有大doc的
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
     * 转移到目标block    每个block 按照docId的密集程度 使用3种结构存储docId
     *
     * @param targetBlock
     * @throws IOException
     */
    private void advanceBlock(int targetBlock) throws IOException {
        // 这里兑换成一个 block下标   (每个block大小为1的16次)
        final int blockIndex = targetBlock >> 16;
        // If the destination block is 2 blocks or more ahead, we use the jump-table.
        // 当本次换取的 block 与当前block的 差距在2 或以上  不采用遍历的方式 而是通过之前存储的 jump[] 直接跳跃到目标位置
        if (jumpTable != null && blockIndex >= (block >> 16) + 2) {
            // If the jumpTableEntryCount is exceeded, there are no further bits. Last entry is always NO_MORE_DOCS
            final int inRangeBlockIndex = blockIndex < jumpTableEntryCount ? blockIndex : jumpTableEntryCount - 1;
            // 之前的所有block写入了多少docId
            final int index = jumpTable.readInt(inRangeBlockIndex * Integer.BYTES * 2);
            // 距离起始位置的偏移量
            final int offset = jumpTable.readInt(inRangeBlockIndex * Integer.BYTES * 2 + Integer.BYTES);
            // 这个nextBlockIndex 主要是为了在之后为 index 赋值   总是当前读取的起始位置-1
            this.nextBlockIndex = index - 1; // -1 to compensate for the always-added 1 in readBlockHeader
            // 将block分片定位到对应的位置
            slice.seek(offset);
            // 解析 特殊结构的相关信息
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
        // 读取出来的值 代表此时是第几个 block
        block = Short.toUnsignedInt(slice.readShort()) << 16;
        assert block >= 0;
        // 写入的时候-1了  这里刚好+1 进行补偿  代表该block下写入了多少docId
        final int numValues = 1 + Short.toUnsignedInt(slice.readShort());

        // 之前的所有block写入了多少docId -1
        index = nextBlockIndex;
        // 计算下一个block 对应的 index 值
        nextBlockIndex = index + numValues;
        // 这里以3种方式存储 对应那个叫啥的位图   当数据量本身就少的时候 采用稀疏的存储方式  也就是直接将doc连续存储在slice中
        if (numValues <= MAX_ARRAY_LENGTH) {
            method = Method.SPARSE;
            // 因为每个 doc 以short的形式写入  所以这里 << 1
            blockEnd = slice.getFilePointer() + (numValues << 1);
            // 65535 = 2<<15  也就是刚好一个block被填满  这时就不写入任何docId了
        } else if (numValues == 65536) {
            method = Method.ALL;
            blockEnd = slice.getFilePointer();
            gap = block - index - 1;
        } else {
            // 代表采用特殊结构存储
            method = Method.DENSE;
            // 该结构 会先写入 rank[]
            denseBitmapOffset = slice.getFilePointer() + (denseRankTable == null ? 0 : denseRankTable.length);
            // 65535/64 = 1024 个long  然后要连续写入 1024个long 就是 8192 = 1 << 13
            blockEnd = denseBitmapOffset + (1 << 13);
            // Performance consideration: All rank (default 128 * 16 bits) are loaded up front. This should be fast with the
            // reusable byte[] buffer, but it is still wasted if the DENSE block is iterated in small steps.
            // If this results in too great a performance regression, a heuristic strategy might work where the rank data
            // are loaded on first in-block advance, if said advance is > X docIDs. The hope being that a small first
            // advance means that subsequent advances will be small too.
            // Another alternative is to maintain an extra slice for DENSE rank, but IndexedDISI is already slice-heavy.
            // 加载 rank[] 的数据
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
         * 当某个block内部的数据比较少时  这里是直接将所有docId 写入
         */
        SPARSE {
            /**
             * 在当前block下寻找 doc   只要能找到大于docId的值 就认为能找到
             * @param disi
             * @param target
             * @return
             * @throws IOException
             */
            @Override
            boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
                // 获取落在该block后的docId值
                final int targetInBlock = target & 0xFFFF;
                // TODO: binary search
                // nextBlockIndex - index 就代表该block下写入了多少 doc
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
                // 代表此时正在遍历到某个doc  exists 代表是否存在该值
                if (target == disi.doc) {
                    return disi.exists;
                }
                for (; disi.index < disi.nextBlockIndex; ) {
                    int doc = Short.toUnsignedInt(disi.slice.readShort());
                    disi.index++;
                    if (doc >= targetInBlock) {
                        // 代表下个值超过了指定的doc
                        if (doc != targetInBlock) {
                            // 回指到上一个小于target的值
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
             * @param target  这个应该是从0开始把 所以 >>> 后会至少保留一位不为0
             * @return
             * @throws IOException
             */
            @Override
            boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
                // target 在该block下的起始位置
                final int targetInBlock = target & 0xFFFF;
                // 找到 word 的下标
                final int targetWordIndex = targetInBlock >>> 6;

                // If possible, skip ahead using the rank cache
                // If the distance between the current position and the target is < rank-longs
                // there is no sense in using rank
                // 2个word 的差距刚好满足一个 rank的大小 就直接通过读取下一个rank的值来快速查询
                if (disi.denseRankPower != -1 && targetWordIndex - disi.wordIndex >= (1 << (disi.denseRankPower - 6))) {
                    rankSkip(disi, targetInBlock);
                }

                // 正常流程从这里开始 挨个读取word的值 并计算当前总的 docId数
                for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {
                    disi.word = disi.slice.readLong();
                    // 更新当前总的doc数量
                    disi.numberOfOnes += Long.bitCount(disi.word);
                }
                // 对应 word的下标  代表读取到第几个word
                disi.wordIndex = targetWordIndex;

                // 左侧除了 目标值外 还有多少bit    target会变成64的余数
                long leftBits = disi.word >>> target;
                // 此时代表一定能大于target的值
                if (leftBits != 0L) {
                    // 有多少0 就代表比目标值大多少
                    disi.doc = target + Long.numberOfTrailingZeros(leftBits);
                    // 剩余的部分 代表不被处理的 docId数量  将总的-不被处理的 就是当前已经遍历到的
                    disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
                    return true;
                }

                // There were no set bits at the wanted position. Move forward until one is reached
                // 当为满足 1024个字  也就是还没有到 block的末尾时 就不断读取
                while (++disi.wordIndex < 1024) {
                    // This could use the rank cache to skip empty spaces >= 512 bits, but it seems unrealistic
                    // that such blocks would be DENSE
                    disi.word = disi.slice.readLong();
                    // 代表内部有 docId  如果是空的位图就要跳过(空的long不记录任何docId )
                    if (disi.word != 0) {
                        // 当发现了下一个有效的值时  更新index
                        disi.index = disi.numberOfOnes;
                        disi.numberOfOnes += Long.bitCount(disi.word);
                        // 计算 docId的值
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
                // 因为只要定位到该block 后  doc一定存在 所以直接读取数据
                disi.doc = target;
                //              gap = block - index - 1;
                disi.index = target - disi.gap;
                return true;
            }

            @Override
            boolean advanceExactWithinBlock(IndexedDISI disi, int target) {
                //               gap = block - index - 1;
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
     * @param targetInBlock lower 16 bits of the target
     * @throws IOException if a DISI seek failed.
     */
    private static void rankSkip(IndexedDISI disi, int targetInBlock) throws IOException {
        assert disi.denseRankPower >= 0 : disi.denseRankPower;
        // Resolve the rank as close to targetInBlock as possible (maximum distance is 8 longs)
        // Note: rankOrigoOffset is tracked on block open, so it is absolute (e.g. don't add origo)
        // 先确定目标位置应该被 划分正在 rank[] 的下标   这里与写入是对应的  每个word 对应rank[]中2个位置  word  =  block/64  = block/ 1<<6
        // rank 的计算又是通过  rankIndexShift = denseRankPower - 7
        // 整个后  rankIndex = denseRankPower - 7 + 6
        final int rankIndex = targetInBlock >> disi.denseRankPower; // Default is 9 (8 longs: 2^3 * 2^6 = 512 docIDs)

        // 在这里又多了 << 1 就与写入是一样的
        // 这里就是还原划分到整个小范围内 写入了多少docId
        final int rank =
                (disi.denseRankTable[rankIndex << 1] & 0xFF) << 8 |
                        (disi.denseRankTable[(rankIndex << 1) + 1] & 0xFF);

        // Position the counting logic just after the rank point
        // 计算此时是第几个 word
        final int rankAlignedWordIndex = rankIndex << disi.denseRankPower >> 6;
        disi.slice.seek(disi.denseBitmapOffset + rankAlignedWordIndex * Long.BYTES);
        // 这里就记录了 该word下 有多少docId
        long rankWord = disi.slice.readLong();
        // 之前所有 rank内记录的 docId总数 加上本次的 才是当前总的 docId 数
        int denseNOO = rank + Long.bitCount(rankWord);

        disi.wordIndex = rankAlignedWordIndex;
        disi.word = rankWord;
        disi.numberOfOnes = disi.denseOrigoIndex + denseNOO;
    }
}
