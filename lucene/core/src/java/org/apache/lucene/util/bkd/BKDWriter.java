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
package org.apache.lucene.util.bkd;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;

// TODO
//   - allow variable length byte[] (across docs and dims), but this is quite a bit more hairy
//   - we could also index "auto-prefix terms" here, and use better compression, and maybe only use for the "fully contained" case so we'd
//     only index docIDs
//   - the index could be efficiently encoded as an FST, so we don't have wasteful
//     (monotonic) long[] leafBlockFPs; or we could use MonotonicLongValues ... but then
//     the index is already plenty small: 60M OSM points --> 1.1 MB with 128 points
//     per leaf, and you can reduce that by putting more points per leaf
//   - we could use threads while building; the higher nodes are very parallelizable

/**
 * Recursively builds a block KD-tree to assign all incoming points in N-dim space to smaller
 * and smaller N-dim rectangles (cells) until the number of points in a given
 * rectangle is &lt;= <code>maxPointsInLeafNode</code>.  The tree is
 * partially balanced, which means the leaf nodes will have
 * the requested <code>maxPointsInLeafNode</code> except one that might have less.
 * Leaf nodes may straddle the two bottom levels of the binary tree.
 * Values that fall exactly on a cell boundary may be in either cell.
 *
 * <p>The number of dimensions can be 1 to 8, but every byte[] value is fixed length.
 *
 * <p>This consumes heap during writing: it allocates a <code>Long[numLeaves]</code>,
 * a <code>byte[numLeaves*(1+bytesPerDim)]</code> and then uses up to the specified
 * {@code maxMBSortInHeap} heap space for writing.
 *
 * <p>
 * <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> / bytesPerDim
 * total points.
 *
 * @lucene.experimental
 */

public class BKDWriter implements Closeable {

    public static final String CODEC_NAME = "BKD";
    public static final int VERSION_START = 4; // version used by Lucene 7.0
    //public static final int VERSION_CURRENT = VERSION_START;
    public static final int VERSION_LEAF_STORES_BOUNDS = 5;
    public static final int VERSION_SELECTIVE_INDEXING = 6;
    public static final int VERSION_LOW_CARDINALITY_LEAVES = 7;
    public static final int VERSION_CURRENT = VERSION_LOW_CARDINALITY_LEAVES;

    /**
     * How many bytes each docs takes in the fixed-width offline format
     */
    private final int bytesPerDoc;

    /**
     * Default maximum number of point in each leaf block
     * 每个叶子block 最多存储 512个维度的值
     */
    public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 512;

    /**
     * Default maximum heap to use, before spilling to (slower) disk
     * 默认使用的最大堆
     */
    public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f;

    /**
     * Maximum number of index dimensions (2 * max index dimensions)
     */
    public static final int MAX_DIMS = 16;

    /**
     * Maximum number of index dimensions
     */
    public static final int MAX_INDEX_DIMS = 8;

    /**
     * Number of splits before we compute the exact bounding box of an inner node.
     */
    private static final int SPLITS_BEFORE_EXACT_BOUNDS = 4;

    /**
     * How many dimensions we are storing at the leaf (data) nodes
     */
    protected final int numDataDims;

    /**
     * How many dimensions we are indexing in the internal nodes
     */
    protected final int numIndexDims;

    /**
     * How many bytes each value in each dimension takes.
     */
    protected final int bytesPerDim;

    /**
     * numDataDims * bytesPerDim
     */
    protected final int packedBytesLength;

    /**
     * numIndexDims * bytesPerDim
     */
    protected final int packedIndexBytesLength;

    final TrackingDirectoryWrapper tempDir;
    final String tempFileNamePrefix;
    final double maxMBSortInHeap;

    final byte[] scratchDiff;
    final byte[] scratch1;
    final byte[] scratch2;
    final BytesRef scratchBytesRef1 = new BytesRef();
    final BytesRef scratchBytesRef2 = new BytesRef();
    final int[] commonPrefixLengths;

    /**
     * 存储docId的 位图
     */
    protected final FixedBitSet docsSeen;

    private PointWriter pointWriter;
    private boolean finished;

    private IndexOutput tempInput;
    protected final int maxPointsInLeafNode;
    private final int maxPointsSortInHeap;

    /**
     * Minimum per-dim values, packed
     */
    protected final byte[] minPackedValue;

    /**
     * Maximum per-dim values, packed
     */
    protected final byte[] maxPackedValue;

    /**
     * 总计写入了多少value
     */
    protected long pointCount;

    /**
     * An upper bound on how many points the caller will add (includes deletions)
     */
    private final long totalPointCount;

    private final int maxDoc;

    /**
     *
     * @param maxDoc   代表该段下总计要写入多少doc
     * @param tempDir   写入的目标目录
     * @param tempFileNamePrefix   段名字  也作为临时文件的前缀
     * @param numDataDims   维度数量
     * @param numIndexDims  需要被索引的维度数量
     * @param bytesPerDim  每个维度的数据占用多少byte
     * @param maxPointsInLeafNode  每个leaf node 拥有多少维度的数据
     * @param maxMBSortInHeap   默认使用的堆大小
     * @param totalPointCount  有多少个 value (未按照维度拆解前)
     * @throws IOException
     */
    public BKDWriter(int maxDoc, Directory tempDir, String tempFileNamePrefix, int numDataDims, int numIndexDims, int bytesPerDim,
                     int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount) throws IOException {
        verifyParams(numDataDims, numIndexDims, maxPointsInLeafNode, maxMBSortInHeap, totalPointCount);
        // We use tracking dir to deal with removing files on exception, so each place that
        // creates temp files doesn't need crazy try/finally/sucess logic:
        this.tempDir = new TrackingDirectoryWrapper(tempDir);
        this.tempFileNamePrefix = tempFileNamePrefix;
        this.maxPointsInLeafNode = maxPointsInLeafNode;
        this.numDataDims = numDataDims;
        this.numIndexDims = numIndexDims;
        this.bytesPerDim = bytesPerDim;
        this.totalPointCount = totalPointCount;
        this.maxDoc = maxDoc;
        // 根据docId 构建位图对象
        docsSeen = new FixedBitSet(maxDoc);
        // 计算总计需要多少bytes
        packedBytesLength = numDataDims * bytesPerDim;
        packedIndexBytesLength = numIndexDims * bytesPerDim;

        // 存储单个维度数据的临时数组
        scratchDiff = new byte[bytesPerDim];
        scratch1 = new byte[packedBytesLength];
        scratch2 = new byte[packedBytesLength];
        commonPrefixLengths = new int[numDataDims];

        minPackedValue = new byte[packedIndexBytesLength];
        maxPackedValue = new byte[packedIndexBytesLength];

        // dimensional values (numDims * bytesPerDim) + docID (int)
        // 存储每个doc下对应 多维度数据需要的byte数  就是原本多维度数据消耗的 byte + docId 消耗的byte
        bytesPerDoc = packedBytesLength + Integer.BYTES;

        // Maximum number of points we hold in memory at any time
        // 每个heap 能存储多少个doc的数据
        maxPointsSortInHeap = (int) ((maxMBSortInHeap * 1024 * 1024) / (bytesPerDoc));

        // Finally, we must be able to hold at least the leaf node in heap during build:
        if (maxPointsSortInHeap < maxPointsInLeafNode) {
            throw new IllegalArgumentException("maxMBSortInHeap=" + maxMBSortInHeap + " only allows for maxPointsSortInHeap=" + maxPointsSortInHeap + ", but this is less than maxPointsInLeafNode=" + maxPointsInLeafNode + "; either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
        }

        this.maxMBSortInHeap = maxMBSortInHeap;
    }

    public static void verifyParams(int numDims, int numIndexDims, int maxPointsInLeafNode, double maxMBSortInHeap, long totalPointCount) {
        // We encode dim in a single byte in the splitPackedValues, but we only expose 4 bits for it now, in case we want to use
        // remaining 4 bits for another purpose later
        if (numDims < 1 || numDims > MAX_DIMS) {
            throw new IllegalArgumentException("numDims must be 1 .. " + MAX_DIMS + " (got: " + numDims + ")");
        }
        if (numIndexDims < 1 || numIndexDims > MAX_INDEX_DIMS) {
            throw new IllegalArgumentException("numIndexDims must be 1 .. " + MAX_INDEX_DIMS + " (got: " + numIndexDims + ")");
        }
        if (numIndexDims > numDims) {
            throw new IllegalArgumentException("numIndexDims cannot exceed numDims (" + numDims + ") (got: " + numIndexDims + ")");
        }
        if (maxPointsInLeafNode <= 0) {
            throw new IllegalArgumentException("maxPointsInLeafNode must be > 0; got " + maxPointsInLeafNode);
        }
        if (maxPointsInLeafNode > ArrayUtil.MAX_ARRAY_LENGTH) {
            throw new IllegalArgumentException("maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxPointsInLeafNode);
        }
        if (maxMBSortInHeap < 0.0) {
            throw new IllegalArgumentException("maxMBSortInHeap must be >= 0.0 (got: " + maxMBSortInHeap + ")");
        }
        if (totalPointCount < 0) {
            throw new IllegalArgumentException("totalPointCount must be >=0 (got: " + totalPointCount + ")");
        }
    }

    private void initPointWriter() throws IOException {
        assert pointWriter == null : "Point writer is already initialized";
        //total point count is an estimation but the final point count must be equal or lower to that number.
        if (totalPointCount > maxPointsSortInHeap) {
            pointWriter = new OfflinePointWriter(tempDir, tempFileNamePrefix, packedBytesLength, "spill", 0);
            tempInput = ((OfflinePointWriter) pointWriter).out;
        } else {
            pointWriter = new HeapPointWriter(Math.toIntExact(totalPointCount), packedBytesLength);
        }
    }


    public void add(byte[] packedValue, int docID) throws IOException {
        if (packedValue.length != packedBytesLength) {
            throw new IllegalArgumentException("packedValue should be length=" + packedBytesLength + " (got: " + packedValue.length + ")");
        }
        if (pointCount >= totalPointCount) {
            throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + (pointCount + 1) + " values");
        }
        if (pointCount == 0) {
            initPointWriter();
            System.arraycopy(packedValue, 0, minPackedValue, 0, packedIndexBytesLength);
            System.arraycopy(packedValue, 0, maxPackedValue, 0, packedIndexBytesLength);
        } else {
            for (int dim = 0; dim < numIndexDims; dim++) {
                int offset = dim * bytesPerDim;
                if (Arrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0) {
                    System.arraycopy(packedValue, offset, minPackedValue, offset, bytesPerDim);
                } else if (Arrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0) {
                    System.arraycopy(packedValue, offset, maxPackedValue, offset, bytesPerDim);
                }
            }
        }
        pointWriter.append(packedValue, docID);
        pointCount++;
        docsSeen.set(docID);
    }

    /**
     * How many points have been added so far
     */
    public long getPointCount() {
        return pointCount;
    }

    private static class MergeReader {
        final BKDReader bkd;
        final BKDReader.IntersectState state;
        final MergeState.DocMap docMap;

        /**
         * Current doc ID
         */
        public int docID;

        /**
         * Which doc in this block we are up to
         */
        private int docBlockUpto;

        /**
         * How many docs in the current block
         */
        private int docsInBlock;

        /**
         * Which leaf block we are up to
         */
        private int blockID;

        private final byte[] packedValues;

        public MergeReader(BKDReader bkd, MergeState.DocMap docMap) throws IOException {
            this.bkd = bkd;
            state = new BKDReader.IntersectState(bkd.in.clone(),
                    bkd.numDataDims,
                    bkd.packedBytesLength,
                    bkd.packedIndexBytesLength,
                    bkd.maxPointsInLeafNode,
                    null,
                    null);
            this.docMap = docMap;
            state.in.seek(bkd.getMinLeafBlockFP());
            this.packedValues = new byte[bkd.maxPointsInLeafNode * bkd.packedBytesLength];
        }

        public boolean next() throws IOException {
            //System.out.println("MR.next this=" + this);
            while (true) {
                if (docBlockUpto == docsInBlock) {
                    if (blockID == bkd.leafNodeOffset) {
                        //System.out.println("  done!");
                        return false;
                    }
                    //System.out.println("  new block @ fp=" + state.in.getFilePointer());
                    docsInBlock = bkd.readDocIDs(state.in, state.in.getFilePointer(), state.scratchIterator);
                    assert docsInBlock > 0;
                    docBlockUpto = 0;
                    bkd.visitDocValues(state.commonPrefixLengths, state.scratchDataPackedValue, state.scratchMinIndexPackedValue, state.scratchMaxIndexPackedValue, state.in, state.scratchIterator, docsInBlock, new IntersectVisitor() {
                        int i = 0;

                        @Override
                        public void visit(int docID) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void visit(int docID, byte[] packedValue) {
                            assert docID == state.scratchIterator.docIDs[i];
                            System.arraycopy(packedValue, 0, packedValues, i * bkd.packedBytesLength, bkd.packedBytesLength);
                            i++;
                        }

                        @Override
                        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                            return Relation.CELL_CROSSES_QUERY;
                        }

                    });

                    blockID++;
                }

                final int index = docBlockUpto++;
                int oldDocID = state.scratchIterator.docIDs[index];

                int mappedDocID;
                if (docMap == null) {
                    mappedDocID = oldDocID;
                } else {
                    mappedDocID = docMap.get(oldDocID);
                }

                if (mappedDocID != -1) {
                    // Not deleted!
                    docID = mappedDocID;
                    System.arraycopy(packedValues, index * bkd.packedBytesLength, state.scratchDataPackedValue, 0, bkd.packedBytesLength);
                    return true;
                }
            }
        }
    }

    private static class BKDMergeQueue extends PriorityQueue<MergeReader> {
        private final int bytesPerDim;

        public BKDMergeQueue(int bytesPerDim, int maxSize) {
            super(maxSize);
            this.bytesPerDim = bytesPerDim;
        }

        @Override
        public boolean lessThan(MergeReader a, MergeReader b) {
            assert a != b;

            int cmp = Arrays.compareUnsigned(a.state.scratchDataPackedValue, 0, bytesPerDim, b.state.scratchDataPackedValue, 0, bytesPerDim);
            if (cmp < 0) {
                return true;
            } else if (cmp > 0) {
                return false;
            }

            // Tie break by sorting smaller docIDs earlier:
            return a.docID < b.docID;
        }
    }

    /**
     * flat representation of a kd-tree
     * 代表多个叶子节点的组合
     */
    private interface BKDTreeLeafNodes {
        /**
         * number of leaf nodes
         */
        int numLeaves();

        /**
         * pointer to the leaf node previously written. Leaves are order from
         * left to right, so leaf at {@code index} 0 is the leftmost leaf and
         * the the leaf at {@code numleaves()} -1 is the rightmost leaf
         */
        long getLeafLP(int index);

        /**
         * split value between two leaves. The split value at position n corresponds to the
         * leaves at (n -1) and n.
         */
        BytesRef getSplitValue(int index);

        /**
         * split dimension between two leaves. The split dimension at position n corresponds to the
         * leaves at (n -1) and n.
         */
        int getSplitDimension(int index);
    }

    /**
     * Write a field from a {@link MutablePointValues}. This way of writing
     * points is faster than regular writes with {@link BKDWriter#add} since
     * there is opportunity for reordering points before writing them to
     * disk. This method does not use transient disk in order to reorder points.
     * 将 MutablePointValues 内部数据写入到 BKD 树中
     */
    public long writeField(IndexOutput out, String fieldName, MutablePointValues reader) throws IOException {
        if (numDataDims == 1) {
            // 代表单维度数据 直接写入就好
            return writeField1Dim(out, fieldName, reader);
        } else {
            return writeFieldNDims(out, fieldName, reader);
        }
    }

    private void computePackedValueBounds(MutablePointValues values, int from, int to, byte[] minPackedValue, byte[] maxPackedValue, BytesRef scratch) {
        if (from == to) {
            return;
        }
        values.getValue(from, scratch);
        System.arraycopy(scratch.bytes, scratch.offset, minPackedValue, 0, packedIndexBytesLength);
        System.arraycopy(scratch.bytes, scratch.offset, maxPackedValue, 0, packedIndexBytesLength);
        for (int i = from + 1; i < to; ++i) {
            values.getValue(i, scratch);
            for (int dim = 0; dim < numIndexDims; dim++) {
                final int startOffset = dim * bytesPerDim;
                final int endOffset = startOffset + bytesPerDim;
                if (Arrays.compareUnsigned(scratch.bytes, scratch.offset + startOffset, scratch.offset + endOffset, minPackedValue, startOffset, endOffset) < 0) {
                    System.arraycopy(scratch.bytes, scratch.offset + startOffset, minPackedValue, startOffset, bytesPerDim);
                } else if (Arrays.compareUnsigned(scratch.bytes, scratch.offset + startOffset, scratch.offset + endOffset, maxPackedValue, startOffset, endOffset) > 0) {
                    System.arraycopy(scratch.bytes, scratch.offset + startOffset, maxPackedValue, startOffset, bytesPerDim);
                }
            }
        }
    }

    /* In the 2+D case, we recursively pick the split dimension, compute the
     * median value and partition other values around it. */
    private long writeFieldNDims(IndexOutput out, String fieldName, MutablePointValues values) throws IOException {
        if (pointCount != 0) {
            throw new IllegalStateException("cannot mix add and writeField");
        }

        // Catch user silliness:
        if (finished == true) {
            throw new IllegalStateException("already finished");
        }

        // Mark that we already finished:
        finished = true;

        pointCount = values.size();

        final int numLeaves = Math.toIntExact((pointCount + maxPointsInLeafNode - 1) / maxPointsInLeafNode);
        final int numSplits = numLeaves - 1;

        checkMaxLeafNodeCount(numLeaves);

        final byte[] splitPackedValues = new byte[numSplits * bytesPerDim];
        final byte[] splitDimensionValues = new byte[numSplits];
        final long[] leafBlockFPs = new long[numLeaves];

        // compute the min/max for this slice
        computePackedValueBounds(values, 0, Math.toIntExact(pointCount), minPackedValue, maxPackedValue, scratchBytesRef1);
        for (int i = 0; i < Math.toIntExact(pointCount); ++i) {
            docsSeen.set(values.getDocID(i));
        }

        final int[] parentSplits = new int[numIndexDims];
        build(0, numLeaves, values, 0, Math.toIntExact(pointCount), out,
                minPackedValue.clone(), maxPackedValue.clone(), parentSplits,
                splitPackedValues, splitDimensionValues, leafBlockFPs,
                new int[maxPointsInLeafNode]);
        assert Arrays.equals(parentSplits, new int[numIndexDims]);

        scratchBytesRef1.length = bytesPerDim;
        scratchBytesRef1.bytes = splitPackedValues;

        BKDTreeLeafNodes leafNodes = new BKDTreeLeafNodes() {
            @Override
            public long getLeafLP(int index) {
                return leafBlockFPs[index];
            }

            @Override
            public BytesRef getSplitValue(int index) {
                scratchBytesRef1.offset = index * bytesPerDim;
                return scratchBytesRef1;
            }

            @Override
            public int getSplitDimension(int index) {
                return splitDimensionValues[index] & 0xff;
            }

            @Override
            public int numLeaves() {
                return leafBlockFPs.length;
            }
        };

        long indexFP = out.getFilePointer();
        writeIndex(out, maxPointsInLeafNode, leafNodes);
        return indexFP;
    }

    /* In the 1D case, we can simply sort points in ascending order and use the
     * same writing logic as we use at merge time. */
    // 只存储单维度数据
    private long writeField1Dim(IndexOutput out, String fieldName, MutablePointValues reader) throws IOException {
        // 将数据按照 value 大小进行排序
        MutablePointsReaderUtils.sort(maxDoc, packedIndexBytesLength, reader, 0, Math.toIntExact(reader.size()));

        final OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(out);

        // 该方法会遍历内部所有value 并调用 visit
        reader.intersect(
                // 通过该对象读取value的值 并按照维度数进行拆解
                new IntersectVisitor() {

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                oneDimWriter.add(packedValue, docID);
            }

            @Override
            public void visit(int docID) throws IOException {
                throw new IllegalStateException();
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return Relation.CELL_CROSSES_QUERY;
            }
        });

        return oneDimWriter.finish();
    }

    /**
     * More efficient bulk-add for incoming {@link BKDReader}s.  This does a merge sort of the already
     * sorted values and currently only works when numDims==1.  This returns -1 if all documents containing
     * dimensional values were deleted.
     */
    public long merge(IndexOutput out, List<MergeState.DocMap> docMaps, List<BKDReader> readers) throws IOException {
        assert docMaps == null || readers.size() == docMaps.size();

        BKDMergeQueue queue = new BKDMergeQueue(bytesPerDim, readers.size());

        for (int i = 0; i < readers.size(); i++) {
            BKDReader bkd = readers.get(i);
            MergeState.DocMap docMap;
            if (docMaps == null) {
                docMap = null;
            } else {
                docMap = docMaps.get(i);
            }
            MergeReader reader = new MergeReader(bkd, docMap);
            if (reader.next()) {
                queue.add(reader);
            }
        }

        OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(out);

        while (queue.size() != 0) {
            MergeReader reader = queue.top();
            // System.out.println("iter reader=" + reader);

            oneDimWriter.add(reader.state.scratchDataPackedValue, reader.docID);

            if (reader.next()) {
                queue.updateTop();
            } else {
                // This segment was exhausted
                queue.pop();
            }
        }

        return oneDimWriter.finish();
    }

    // Reused when writing leaf blocks
    private final ByteBuffersDataOutput scratchOut = ByteBuffersDataOutput.newResettableInstance();

    /**
     * 写单维度数据的 bkd树
     */
    private class OneDimensionBKDWriter {

        /**
         * 就是索引文件
         */
        final IndexOutput out;
        /**
         * 每当填满一个 leaf的数据时 记录写入file 前文件的偏移量
         */
        final List<Long> leafBlockFPs = new ArrayList<>();
        /**
         * 对应每个leaf 的第一个值
         */
        final List<byte[]> leafBlockStartValues = new ArrayList<>();

        /**
         * 每个 leaf的数据都被存储到这个大的 byte[] 中
         */
        final byte[] leafValues = new byte[maxPointsInLeafNode * packedBytesLength];
        /**
         * 存储每个叶数据对应的docId
         */
        final int[] leafDocs = new int[maxPointsInLeafNode];
        /**
         * 记录整个BKD树此时有多少 多维数据 相当于是所有leafCount的总和
         */
        private long valueCount;

        /**
         * 此时叶数量
         */
        private int leafCount;
        /**
         * 只记录不重复的值
         */
        private int leafCardinality;

        OneDimensionBKDWriter(IndexOutput out) {
            if (numIndexDims != 1) {
                throw new UnsupportedOperationException("numIndexDims must be 1 but got " + numIndexDims);
            }
            // 该值默认情况是0
            if (pointCount != 0) {
                throw new IllegalStateException("cannot mix add and merge");
            }

            // Catch user silliness:
            // 该对象是以 field为单位创建的
            if (finished == true) {
                throw new IllegalStateException("already finished");
            }

            // Mark that we already finished:
            finished = true;

            this.out = out;

            lastPackedValue = new byte[packedBytesLength];
        }

        // for asserts
        final byte[] lastPackedValue;
        private int lastDocID;

        /**
         * 将数据写入到 bkd树种
         * @param packedValue   单维度数据
         * @param docID   对应的docId
         * @throws IOException
         */
        void add(byte[] packedValue, int docID) throws IOException {
            assert valueInOrder(valueCount + leafCount,
                    0, lastPackedValue, packedValue, 0, docID, lastDocID);

            // Arrays.mismatch 找到2个byte 比较的部分中首个不同的元素对应的下标  -1 代表完全相同
            // 也就是只要有元素不相同 就将叶的基数 + 1  叶的基数 也就是该叶下不重复的值
            if (leafCount == 0 || Arrays.mismatch(leafValues, (leafCount - 1) * bytesPerDim, leafCount * bytesPerDim, packedValue, 0, bytesPerDim) != -1) {
                leafCardinality++;
            }
            // 将 packedValue的数据 拷贝到 leafValues 中   重复的值也会写入到 leafValues中
            // 同时也会覆盖上一轮写入到leafValues 的值
            System.arraycopy(packedValue, 0, leafValues, leafCount * packedBytesLength, packedBytesLength);
            // 存储id 同时也会覆盖上一轮的数据
            leafDocs[leafCount] = docID;
            docsSeen.set(docID);
            leafCount++;

            if (valueCount + leafCount > totalPointCount) {
                throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + (valueCount + leafCount) + " values");
            }

            // 代表此时一个leaf的数据已经填满 需要写入到 树中
            if (leafCount == maxPointsInLeafNode) {
                // We write a block once we hit exactly the max count ... this is different from
                // when we write N > 1 dimensional points where we write between max/2 and max per leaf block
                writeLeafBlock(leafCardinality);
                // 清空数据
                leafCardinality = 0;
                leafCount = 0;
            }

            assert (lastDocID = docID) >= 0; // only assign when asserts are enabled
        }

        /**
         * 当所有数据都以 leaf为单位 并写入到 bkd 后触发
         * @return
         * @throws IOException
         */
        public long finish() throws IOException {
            // 代表还有部分数据需要写入到 out中
            if (leafCount > 0) {
                writeLeafBlock(leafCardinality);
                leafCardinality = 0;
                leafCount = 0;
            }

            // 代表本次没有写入任何数据  返回-1
            if (valueCount == 0) {
                return -1;
            }

            // 记录总计写入了多少数据
            pointCount = valueCount;

            long indexFP = out.getFilePointer();

            scratchBytesRef1.length = bytesPerDim;
            scratchBytesRef1.offset = 0;
            assert leafBlockStartValues.size() + 1 == leafBlockFPs.size();

            // 生成 bkd 树
            BKDTreeLeafNodes leafNodes = new BKDTreeLeafNodes() {

                /**
                 * 获取某个叶 在文件中的偏移量
                 * @param index
                 * @return
                 */
                @Override
                public long getLeafLP(int index) {
                    return leafBlockFPs.get(index);
                }

                /**
                 * 切换到每个叶的第一个值
                 * @param index
                 * @return
                 */
                @Override
                public BytesRef getSplitValue(int index) {
                    scratchBytesRef1.bytes = leafBlockStartValues.get(index);
                    return scratchBytesRef1;
                }

                @Override
                public int getSplitDimension(int index) {
                    return 0;
                }

                /**
                 * 返回叶的数量
                 * @return
                 */
                @Override
                public int numLeaves() {
                    return leafBlockFPs.size();
                }
            };
            writeIndex(out, maxPointsInLeafNode, leafNodes);
            return indexFP;
        }

        /**
         * 将叶中的数据转移到 树中
         * @param leafCardinality  叶的基数  也就是该叶下不重复的值有多少
         * @throws IOException
         */
        private void writeLeafBlock(int leafCardinality) throws IOException {
            assert leafCount != 0;
            // 代表首次
            if (valueCount == 0) {
                // 拷贝最小值
                System.arraycopy(leafValues, 0, minPackedValue, 0, packedIndexBytesLength);
            }
            // 因为之前已经排序过了 所以这里只写入最大的值  (更新最大值)
            System.arraycopy(leafValues, (leafCount - 1) * packedBytesLength, maxPackedValue, 0, packedIndexBytesLength);

            // 此时树中总数据数
            valueCount += leafCount;

            // 初始状态为 0
            if (leafBlockFPs.size() > 0) {
                // Save the first (minimum) value in each leaf block except the first, to build the split value index in the end:
                leafBlockStartValues.add(ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength));
            }
            // 记录文件偏移量
            leafBlockFPs.add(out.getFilePointer());
            // 避免节点过多的 检测 可忽略
            checkMaxLeafNodeCount(leafBlockFPs.size());

            // Find per-dim common prefix:
            // 记录此时总偏移量
            int offset = (leafCount - 1) * packedBytesLength;
            // 找到前缀的偏移量  这里只读取单个维度的长度 是因为该对象确定了每个值维度的数量是1吗
            int prefix = Arrays.mismatch(leafValues, 0, bytesPerDim, leafValues, offset, offset + bytesPerDim);
            // 因为是按照从小到大的顺序 如果首尾值相同 等同于该leaf下所有值相同
            if (prefix == -1) {
                // 代表该维度的bytes都相同
                prefix = bytesPerDim;
            }

            // 这个下标实际上是对应维度数  因为当前对象是单维度 所以只使用0
            commonPrefixLengths[0] = prefix;

            assert scratchOut.size() == 0;
            // 写入docId
            writeLeafBlockDocs(scratchOut, leafDocs, 0, leafCount);
            // 写入当前leaf的公共前缀 到临时容器
            writeCommonPrefixes(scratchOut, commonPrefixLengths, leafValues);

            // 初始化 ref对象 该对象包装了 byte[] 只是单独维护了一份指针 用于读取内部的数据
            // 代表一个多维度值的长度  确保每次只能读取一个 多维度值
            scratchBytesRef1.length = packedBytesLength;
            scratchBytesRef1.bytes = leafValues;

            final IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
                @Override
                public BytesRef apply(int i) {
                    // 更新偏移量后返回 这样读取的值就会变化
                    scratchBytesRef1.offset = packedBytesLength * i;
                    return scratchBytesRef1;
                }
            };
            assert valuesInOrderAndBounds(leafCount, 0, ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength),
                    ArrayUtil.copyOfSubArray(leafValues, (leafCount - 1) * packedBytesLength, leafCount * packedBytesLength),
                    packedValues, leafDocs, 0);
            // 写入到临时out 中
            writeLeafBlockPackedValues(scratchOut, commonPrefixLengths, leafCount, 0, packedValues, leafCardinality);
            // 将数据转移到索引文件中
            scratchOut.copyTo(out);
            scratchOut.reset();
        }
    }

    /**
     *
     * @param numLeaves   此时有多少叶
     * @return
     */
    private int getNumLeftLeafNodes(int numLeaves) {
        assert numLeaves > 1 : "getNumLeftLeaveNodes() called with " + numLeaves;
        // return the level that can be filled with this number of leaves
        // Integer.numberOfLeadingZeros(numLeaves) 高位有多少个0
        // 能进入到这里代表叶的数量大于1  那么需要的位数 至少是2   Integer.numberOfLeadingZeros(numLeaves) 最多是30
        int lastFullLevel = 31 - Integer.numberOfLeadingZeros(numLeaves);
        // 这几个值不知道咋用 ???
        // how many leaf nodes are in the full level
        int leavesFullLevel = 1 << lastFullLevel;
        // half of the leaf nodes from the full level goes to the left
        int numLeftLeafNodes = leavesFullLevel / 2;
        // leaf nodes that do not fit in the full level
        int unbalancedLeafNodes = numLeaves - leavesFullLevel;
        // distribute unbalanced leaf nodes
        numLeftLeafNodes += Math.min(unbalancedLeafNodes, numLeftLeafNodes);
        // we should always place unbalanced leaf nodes on the left
        assert numLeftLeafNodes >= numLeaves - numLeftLeafNodes && numLeftLeafNodes <= 2L * (numLeaves - numLeftLeafNodes);
        return numLeftLeafNodes;
    }

    // TODO: if we fixed each partition step to just record the file offset at the "split point", we could probably handle variable length
    // encoding and not have our own ByteSequencesReader/Writer

    // useful for debugging:
  /*
  private void printPathSlice(String desc, PathSlice slice, int dim) throws IOException {
    System.out.println("    " + desc + " dim=" + dim + " count=" + slice.count + ":");    
    try(PointReader r = slice.writer.getReader(slice.start, slice.count)) {
      int count = 0;
      while (r.next()) {
        byte[] v = r.packedValue();
        System.out.println("      " + count + ": " + new BytesRef(v, dim*bytesPerDim, bytesPerDim));
        count++;
        if (count == slice.count) {
          break;
        }
      }
    }
  }
  */

    private void checkMaxLeafNodeCount(int numLeaves) {
        if (bytesPerDim * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH) {
            throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + maxPointsInLeafNode + ") and reindex");
        }
    }

    /**
     * Writes the BKD tree to the provided {@link IndexOutput} and returns the file offset where index was written.
     */
    public long finish(IndexOutput out) throws IOException {
        // System.out.println("\nBKDTreeWriter.finish pointCount=" + pointCount + " out=" + out + " heapWriter=" + heapPointWriter);

        // TODO: specialize the 1D case?  it's much faster at indexing time (no partitioning on recurse...)

        // Catch user silliness:
        if (finished == true) {
            throw new IllegalStateException("already finished");
        }

        if (pointCount == 0) {
            throw new IllegalStateException("must index at least one point");
        }

        //mark as finished
        finished = true;

        pointWriter.close();
        BKDRadixSelector.PathSlice points = new BKDRadixSelector.PathSlice(pointWriter, 0, pointCount);
        //clean up pointers
        tempInput = null;
        pointWriter = null;

        final int numLeaves = Math.toIntExact((pointCount + maxPointsInLeafNode - 1) / maxPointsInLeafNode);
        final int numSplits = numLeaves - 1;

        checkMaxLeafNodeCount(numLeaves);

        // NOTE: we could save the 1+ here, to use a bit less heap at search time, but then we'd need a somewhat costly check at each
        // step of the recursion to recompute the split dim:

        // Indexed by nodeID, but first (root) nodeID is 1.  We do 1+ because the lead byte at each recursion says which dim we split on.
        byte[] splitPackedValues = new byte[Math.toIntExact(numSplits * bytesPerDim)];
        byte[] splitDimensionValues = new byte[numSplits];

        // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1 (e.g. 7)
        long[] leafBlockFPs = new long[numLeaves];

        // Make sure the math above "worked":
        assert pointCount / numLeaves <= maxPointsInLeafNode : "pointCount=" + pointCount + " numLeaves=" + numLeaves + " maxPointsInLeafNode=" + maxPointsInLeafNode;

        //We re-use the selector so we do not need to create an object every time.
        BKDRadixSelector radixSelector = new BKDRadixSelector(numDataDims, numIndexDims, bytesPerDim, maxPointsSortInHeap, tempDir, tempFileNamePrefix);

        boolean success = false;
        try {

            final int[] parentSplits = new int[numIndexDims];
            build(0, numLeaves, points,
                    out, radixSelector,
                    minPackedValue.clone(), maxPackedValue.clone(),
                    parentSplits,
                    splitPackedValues,
                    splitDimensionValues,
                    leafBlockFPs,
                    new int[maxPointsInLeafNode]);
            assert Arrays.equals(parentSplits, new int[numIndexDims]);

            // If no exception, we should have cleaned everything up:
            assert tempDir.getCreatedFiles().isEmpty();
            //long t2 = System.nanoTime();
            //System.out.println("write time: " + ((t2-t1)/1000000.0) + " msec");

            success = true;
        } finally {
            if (success == false) {
                IOUtils.deleteFilesIgnoringExceptions(tempDir, tempDir.getCreatedFiles());
            }
        }

        scratchBytesRef1.bytes = splitPackedValues;
        scratchBytesRef1.length = bytesPerDim;
        BKDTreeLeafNodes leafNodes = new BKDTreeLeafNodes() {
            @Override
            public long getLeafLP(int index) {
                return leafBlockFPs[index];
            }

            @Override
            public BytesRef getSplitValue(int index) {
                scratchBytesRef1.offset = index * bytesPerDim;
                return scratchBytesRef1;
            }

            @Override
            public int getSplitDimension(int index) {
                return splitDimensionValues[index] & 0xff;
            }

            @Override
            public int numLeaves() {
                return leafBlockFPs.length;
            }
        };

        // Write index:
        long indexFP = out.getFilePointer();
        writeIndex(out, maxPointsInLeafNode, leafNodes);
        return indexFP;
    }

    /**
     * Packs the two arrays, representing a semi-balanced binary tree, into a compact byte[] structure.
     * 生成索引结构
     */
    private byte[] packIndex(BKDTreeLeafNodes leafNodes) throws IOException {
        /** Reused while packing the index */
        ByteBuffersDataOutput writeBuffer = ByteBuffersDataOutput.newResettableInstance();

        // This is the "file" we append the byte[] to:
        List<byte[]> blocks = new ArrayList<>();
        // length 等同于 一个 value
        byte[] lastSplitValues = new byte[bytesPerDim * numIndexDims];
        //System.out.println("\npack index");
        int totalSize = recursePackIndex(writeBuffer, leafNodes, 0l, blocks, lastSplitValues, new boolean[numIndexDims], false,
                0, leafNodes.numLeaves());

        // Compact the byte[] blocks into single byte index:
        byte[] index = new byte[totalSize];
        int upto = 0;
        for (byte[] block : blocks) {
            System.arraycopy(block, 0, index, upto, block.length);
            upto += block.length;
        }
        assert upto == totalSize;

        return index;
    }

    /**
     * Appends the current contents of writeBuffer as another block on the growing in-memory file
     */
    private int appendBlock(ByteBuffersDataOutput writeBuffer, List<byte[]> blocks) throws IOException {
        byte[] block = writeBuffer.toArrayCopy();
        blocks.add(block);
        writeBuffer.reset();
        return block.length;
    }

    /**
     * lastSplitValues is per-dimension split value previously seen; we use this to prefix-code the split byte[] on each inner node
     * @param writeBuffer  写入数据的临时容器
     * @param leafNodes bkd 树结构
     * @param minBlockFP 现在看到就是0
     * @param blocks  空ArrayList
     * @param lastSplitValues  大小刚好等同于一个 value
     * @param negativeDeltas  大小与 维度数相同
     * @param isLeft  默认为false
     * @param leavesOffset  默认为0
     * @param numLeaves  有多少叶
     */
    private int recursePackIndex(ByteBuffersDataOutput writeBuffer, BKDTreeLeafNodes leafNodes, long minBlockFP, List<byte[]> blocks,
                                 byte[] lastSplitValues, boolean[] negativeDeltas, boolean isLeft, int leavesOffset, int numLeaves) throws IOException {
        // 如果此时只有一个叶
        if (numLeaves == 1) {
            if (isLeft) {
                assert leafNodes.getLeafLP(leavesOffset) - minBlockFP == 0;
                // 返回0
                return 0;
            } else {
                // 首个叶对应的 文件偏移量
                long delta = leafNodes.getLeafLP(leavesOffset) - minBlockFP;
                assert leafNodes.numLeaves() == numLeaves || delta > 0 : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
                // 先写入首个叶对应的文件偏移量
                writeBuffer.writeVLong(delta);
                // 将 writerBuffer的数据转换成 byte[] 并追加到 blocks 中
                return appendBlock(writeBuffer, blocks);
            }
        } else {
            // 代表此时有多个 leaf
            long leftBlockFP;
            // TODO 先忽略这种情况
            if (isLeft) {
                // The left tree's left most leaf block FP is always the minimal FP:
                assert leafNodes.getLeafLP(leavesOffset) == minBlockFP;
                leftBlockFP = minBlockFP;
            } else {
                // 找到第一个叶对应的filePoint
                leftBlockFP = leafNodes.getLeafLP(leavesOffset);
                // 目前就是 filePoint - 0
                long delta = leftBlockFP - minBlockFP;
                assert leafNodes.numLeaves() == numLeaves || delta > 0 : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
                // 将增量信息写入到 writerBuffer 中
                writeBuffer.writeVLong(delta);
            }

            int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
            final int rightOffset = leavesOffset + numLeftLeafNodes;
            final int splitOffset = rightOffset - 1;

            int splitDim = leafNodes.getSplitDimension(splitOffset);
            BytesRef splitValue = leafNodes.getSplitValue(splitOffset);
            int address = splitValue.offset;

            //System.out.println("recursePack inner nodeID=" + nodeID + " splitDim=" + splitDim + " splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

            // find common prefix with last split value in this dim:
            int prefix = Arrays.mismatch(splitValue.bytes, address, address + bytesPerDim, lastSplitValues,
                    splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim);
            if (prefix == -1) {
                prefix = bytesPerDim;
            }

            //System.out.println("writeNodeData nodeID=" + nodeID + " splitDim=" + splitDim + " numDims=" + numDims + " bytesPerDim=" + bytesPerDim + " prefix=" + prefix);

            int firstDiffByteDelta;
            if (prefix < bytesPerDim) {
                //System.out.println("  delta byte cur=" + Integer.toHexString(splitPackedValues[address+prefix]&0xFF) + " prev=" + Integer.toHexString(lastSplitValues[splitDim * bytesPerDim + prefix]&0xFF) + " negated?=" + negativeDeltas[splitDim]);
                firstDiffByteDelta = (splitValue.bytes[address + prefix] & 0xFF) - (lastSplitValues[splitDim * bytesPerDim + prefix] & 0xFF);
                if (negativeDeltas[splitDim]) {
                    firstDiffByteDelta = -firstDiffByteDelta;
                }
                //System.out.println("  delta=" + firstDiffByteDelta);
                assert firstDiffByteDelta > 0;
            } else {
                firstDiffByteDelta = 0;
            }

            // pack the prefix, splitDim and delta first diff byte into a single vInt:
            int code = (firstDiffByteDelta * (1 + bytesPerDim) + prefix) * numIndexDims + splitDim;

            //System.out.println("  code=" + code);
            //System.out.println("  splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

            writeBuffer.writeVInt(code);

            // write the split value, prefix coded vs. our parent's split value:
            int suffix = bytesPerDim - prefix;
            byte[] savSplitValue = new byte[suffix];
            if (suffix > 1) {
                writeBuffer.writeBytes(splitValue.bytes, address + prefix + 1, suffix - 1);
            }

            byte[] cmp = lastSplitValues.clone();

            System.arraycopy(lastSplitValues, splitDim * bytesPerDim + prefix, savSplitValue, 0, suffix);

            // copy our split value into lastSplitValues for our children to prefix-code against
            System.arraycopy(splitValue.bytes, address + prefix, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

            int numBytes = appendBlock(writeBuffer, blocks);

            // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into the right sub-tree we can
            // quickly seek to its starting point
            int idxSav = blocks.size();
            blocks.add(null);

            boolean savNegativeDelta = negativeDeltas[splitDim];
            negativeDeltas[splitDim] = true;


            int leftNumBytes = recursePackIndex(writeBuffer, leafNodes, leftBlockFP, blocks, lastSplitValues, negativeDeltas, true,
                    leavesOffset, numLeftLeafNodes);

            if (numLeftLeafNodes != 1) {
                writeBuffer.writeVInt(leftNumBytes);
            } else {
                assert leftNumBytes == 0 : "leftNumBytes=" + leftNumBytes;
            }

            byte[] bytes2 = writeBuffer.toArrayCopy();
            writeBuffer.reset();
            // replace our placeholder:
            blocks.set(idxSav, bytes2);

            negativeDeltas[splitDim] = false;
            int rightNumBytes = recursePackIndex(writeBuffer, leafNodes, leftBlockFP, blocks, lastSplitValues, negativeDeltas, false,
                    rightOffset, numLeaves - numLeftLeafNodes);

            negativeDeltas[splitDim] = savNegativeDelta;

            // restore lastSplitValues to what caller originally passed us:
            System.arraycopy(savSplitValue, 0, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

            assert Arrays.equals(lastSplitValues, cmp);

            return numBytes + bytes2.length + leftNumBytes + rightNumBytes;
        }
    }

    /**
     *
     * @param out
     * @param countPerLeaf  每个叶有多少数据
     * @param leafNodes  此时已经生成的 bkd树
     * @throws IOException
     */
    private void writeIndex(IndexOutput out, int countPerLeaf, BKDTreeLeafNodes leafNodes) throws IOException {
        byte[] packedIndex = packIndex(leafNodes);
        writeIndex(out, countPerLeaf, leafNodes.numLeaves(), packedIndex);
    }

    private void writeIndex(IndexOutput out, int countPerLeaf, int numLeaves, byte[] packedIndex) throws IOException {

        CodecUtil.writeHeader(out, CODEC_NAME, VERSION_CURRENT);
        out.writeVInt(numDataDims);
        out.writeVInt(numIndexDims);
        out.writeVInt(countPerLeaf);
        out.writeVInt(bytesPerDim);

        assert numLeaves > 0;
        out.writeVInt(numLeaves);
        out.writeBytes(minPackedValue, 0, packedIndexBytesLength);
        out.writeBytes(maxPackedValue, 0, packedIndexBytesLength);

        out.writeVLong(pointCount);
        out.writeVInt(docsSeen.cardinality());
        out.writeVInt(packedIndex.length);
        out.writeBytes(packedIndex, 0, packedIndex.length);
    }

    /**
     * 将该叶下所有docId 写入
     * @param out  临时容器
     * @param docIDs
     * @param start
     * @param count
     * @throws IOException
     */
    private void writeLeafBlockDocs(DataOutput out, int[] docIDs, int start, int count) throws IOException {
        assert count > 0 : "maxPointsInLeafNode=" + maxPointsInLeafNode;
        // 写入 docId 总数
        out.writeVInt(count);
        // 将docId 写入到临时容器
        DocIdsWriter.writeDocIds(docIDs, start, count, out);
    }

    /**
     * 将某个leaf的数据写入到 临时输出流中
     * @param out
     * @param commonPrefixLengths   记录每个维度的公共前缀长度
     * @param count      当前叶下有多少数据
     * @param sortedDim     默认为0
     * @param packedValues   该函数会切换 scratchBytesRef1 的offset 这样才能读取到下一个多维度值
     * @param leafCardinality   该leaf下不重复的值的数量
     * @throws IOException
     */
    private void writeLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues, int leafCardinality) throws IOException {
        // 这么多维度合起来的公共前缀长度是多少
        int prefixLenSum = Arrays.stream(commonPrefixLengths).sum();
        // 代表所有值都是一样的 写入 -1
        if (prefixLenSum == packedBytesLength) {
            // all values in this block are equal
            out.writeByte((byte) -1);
        } else {
            assert commonPrefixLengths[sortedDim] < bytesPerDim;
            // estimate if storing the values with cardinality is cheaper than storing all values.
            // 先看看 sorted 为0的情况   这里就变成了 公共前缀结束的位置作为起始偏移量  也意味着从这里开始后面的数据要压缩
            int compressedByteOffset = sortedDim * bytesPerDim + commonPrefixLengths[sortedDim];
            int highCardinalityCost;
            int lowCardinalityCost;
            // 代表数据都是不一致的
            if (count == leafCardinality) {
                // all values in this block are different
                highCardinalityCost = 0;
                lowCardinalityCost = 1;
            } else {
                // compute cost of runLen compression
                // 代表有部分数据是一致的
                int numRunLens = 0;   // 代表执行了多少次 runLen   每执行一次就代表 有多少value的第一个byte是相同的 或者说拆分成了几组 这种情况是没考虑前缀相同的value超过255的
                                      // 如果超过了 numRunLen也要增加
                for (int i = 0; i < count; ) {
                    // do run-length compression on the byte at compressedByteOffset
                    // 0xff = 255
                    // 代表这么多值的 第一个byte是相同的
                    int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
                    assert runLen <= 0xff;
                    numRunLens++;
                    i += runLen;
                }

                // 思考一下 当leaf中有大量数据一致时  lowCardinalityCost 必然小于上面的值  因为 leafCardinality 远小于 count
                // Add cost of runLen compression
                highCardinalityCost = count * (packedBytesLength - prefixLenSum - 1) + 2 * numRunLens;
                // +1 is the byte needed for storing the cardinality
                lowCardinalityCost = leafCardinality * (packedBytesLength - prefixLenSum + 1);
            }
            // 采用上面的方式 也就是只存储不重复的值
            if (lowCardinalityCost <= highCardinalityCost) {
                out.writeByte((byte) -2);
                writeLowCardinalityLeafBlockPackedValues(out, commonPrefixLengths, count, packedValues);
            } else {
                // 所有数据都不一致时 不可能挨个存储所有值了 就要通过压缩手段  这里就是只按照第一个byte相同的进行分组 并写入 那么复用的部分大幅减小
                out.writeByte((byte) sortedDim); // sortedDim 默认为0
                writeHighCardinalityLeafBlockPackedValues(out, commonPrefixLengths, count, sortedDim, packedValues, compressedByteOffset);
            }
        }
    }

    /**
     * 当 lowCardinalityCost <= highCardinalityCost 时 采用该方法写入数据
     * @param out   临时输出流
     * @param commonPrefixLengths   对应每个维度的 共享前缀长度
     * @param count    leafCount
     * @param packedValues   调用时 会修改内部读取 leafValue的偏移量
     * @throws IOException
     */
    private void writeLowCardinalityLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, IntFunction<BytesRef> packedValues) throws IOException {
        if (numIndexDims != 1) {
            // 以每个leafCount值对应的后缀 在所有等长后缀的 min/max 写入到out中
            writeActualBounds(out, commonPrefixLengths, count, packedValues);
        }
        // 重置偏移量
        BytesRef value = packedValues.apply(0);
        // 将某个 value 存储到 临时容器中
        System.arraycopy(value.bytes, value.offset, scratch1, 0, packedBytesLength);

        // 代表有多少个value的多维度值前缀完全相同  1代表不相同（仅有自己）
        int cardinality = 1;
        for (int i = 1; i < count; i++) {
            // 遍历每个 value
            value = packedValues.apply(i);
            // 挨个比较每个维度
            // 根据维度数 读取前缀并处理  先假设维度为1
            for (int dim = 0; dim < numDataDims; dim++) {
                // 找到不共享的起始位置
                final int start = dim * bytesPerDim + commonPrefixLengths[dim];
                // 对应该value的结尾位置  看来多维度数据本身要求内部每个维度占用的 byte数是一致的
                final int end = dim * bytesPerDim + bytesPerDim;
                // 比较相邻的2个值共享前缀的位置   只要有某个维度的值不相同 在下面的for循环中 就会将所有维度 按照bytesPerDim - commonPrefixLengths[j] 的部分写入到out中
                if (Arrays.mismatch(value.bytes, value.offset + start, value.offset + end, scratch1, start, end) != -1) {
                    // 代表有多少个value的多维度值全部相同 默认为1 代表不相同
                    out.writeVInt(cardinality);
                    for (int j = 0; j < numDataDims; j++) {
                        // 将非共享的部分写入临时容器  实际上可能有部分数据是共享的  commonPrefixLengths 仅记录首个value与最后一个value共享的部分
                        out.writeBytes(scratch1, j * bytesPerDim + commonPrefixLengths[j], bytesPerDim - commonPrefixLengths[j]);
                    }
                    // 更新 scratch1的值
                    System.arraycopy(value.bytes, value.offset, scratch1, 0, packedBytesLength);
                    cardinality = 1;
                    break;
                // 代表此前所有的维度都相等 并且此时是最后一个维度  将    cardinality +1
                } else if (dim == numDataDims - 1) {
                    cardinality++;
                }
            }
        }
        out.writeVInt(cardinality);
        // 因为最后一个value的值还没有写入,这里再写入   也就是共享的值 只写入一次 即使所有值都相同也要至少写入一次
        for (int i = 0; i < numDataDims; i++) {
            out.writeBytes(scratch1, i * bytesPerDim + commonPrefixLengths[i], bytesPerDim - commonPrefixLengths[i]);
        }
    }

    /**
     *
     * @param out   数据写入的临时输出流
     * @param commonPrefixLengths  每个维度对应的 公共前缀长度
     * @param count  leafCount
     * @param sortedDim    先考虑为0的情况 这时compressedByteOffset 为commonPrefixLengths[sortedDim] 也就对应第一维数据的 共享前缀长度
     * @param packedValues   用于切换下一个leaf中的下一个值
     * @param compressedByteOffset   代表从哪里开始压缩数据
     * @throws IOException
     */
    private void writeHighCardinalityLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues, int compressedByteOffset) throws IOException {
        if (numIndexDims != 1) {
            writeActualBounds(out, commonPrefixLengths, count, packedValues);
        }
        // 将最后一个排序的前缀长度加1
        commonPrefixLengths[sortedDim]++;
        for (int i = 0; i < count; ) {
            // do run-length compression on the byte at compressedByteOffset
            // 按照第一个byte是否相同进行分组  runLen 代表该组下有多少值的第一个byte是相同的
            int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
            assert runLen <= 0xff;
            // 切换到对应的值
            BytesRef first = packedValues.apply(i);
            // 获取那个第一个byte
            byte prefixByte = first.bytes[first.offset + compressedByteOffset];
            // 将该byte 以及有多少个leafCount 对应的第一个byte就是该byte 写入
            out.writeByte(prefixByte);
            out.writeByte((byte) runLen);
            // 将范围数据写入 到out中
            writeLeafBlockPackedValuesRange(out, commonPrefixLengths, i, i + runLen, packedValues);
            i += runLen;
            assert i <= count;
        }
    }

    /**
     * 当多维数据的维度不为1 时调用该方法   将每个值对应的后缀长度在所有 value取相同后缀长度的 min/max 写入到out   什么乱七八糟的这么绕
     * @param out
     * @param commonPrefixLengths
     * @param count
     * @param packedValues
     * @throws IOException
     */
    private void writeActualBounds(DataOutput out, int[] commonPrefixLengths, int count, IntFunction<BytesRef> packedValues) throws IOException {
        for (int dim = 0; dim < numIndexDims; ++dim) {
            // 对应维度的前缀长度
            int commonPrefixLength = commonPrefixLengths[dim];
            // 计算不同的后缀长度
            int suffixLength = bytesPerDim - commonPrefixLength;
            if (suffixLength > 0) {
                // 以当前后缀  与 leafCount 中所有后缀长度的数据比较 获取最大值与最小值
                BytesRef[] minMax = computeMinMax(count, packedValues, dim * bytesPerDim + commonPrefixLength, suffixLength);
                BytesRef min = minMax[0];
                BytesRef max = minMax[1];
                out.writeBytes(min.bytes, min.offset, min.length);
                out.writeBytes(max.bytes, max.offset, max.length);
            }
        }
    }

    /**
     * Return an array that contains the min and max values for the [offset, offset+length] interval
     * of the given {@link BytesRef}s.
     * @param offset 对应每个维度的后缀的起始位置
     * 计算最大值和最小值
     */
    private static BytesRef[] computeMinMax(int count, IntFunction<BytesRef> packedValues, int offset, int length) {
        assert length > 0;
        // 创建存储 min/max 的2个容器
        BytesRefBuilder min = new BytesRefBuilder();
        BytesRefBuilder max = new BytesRefBuilder();
        // 切换到第一个值
        BytesRef first = packedValues.apply(0);
        // 切换到当前维度 后缀的起始偏移量
        min.copyBytes(first.bytes, first.offset + offset, length);
        max.copyBytes(first.bytes, first.offset + offset, length);
        for (int i = 1; i < count; ++i) {
            // 获取后面的值 并比较 后缀的大小
            BytesRef candidate = packedValues.apply(i);
            if (Arrays.compareUnsigned(min.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) > 0) {
                min.copyBytes(candidate.bytes, candidate.offset + offset, length);
            } else if (Arrays.compareUnsigned(max.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) < 0) {
                max.copyBytes(candidate.bytes, candidate.offset + offset, length);
            }
        }
        return new BytesRef[]{min.get(), max.get()};
    }

    /**
     *
     * @param out
     * @param commonPrefixLengths  每个维度对应的前缀长度   sortDim对应的前缀长度要+1
     * @param start  此时正读取到的 leafCount
     * @param end   start + runLen
     * @param packedValues
     * @throws IOException
     */
    private void writeLeafBlockPackedValuesRange(DataOutput out, int[] commonPrefixLengths, int start, int end, IntFunction<BytesRef> packedValues) throws IOException {
        for (int i = start; i < end; ++i) {
            // 挨个读取这些共享第一个 byte 的所有值
            BytesRef ref = packedValues.apply(i);
            assert ref.length == packedBytesLength;

            // 按照维度将他们拆解
            for (int dim = 0; dim < numDataDims; dim++) {
                // 获取每个维度对应的共享前缀长度
                int prefix = commonPrefixLengths[dim];
                // 只写入后缀部分
                out.writeBytes(ref.bytes, ref.offset + dim * bytesPerDim + prefix, bytesPerDim - prefix);
            }
        }
    }

    /**
     *
     * @param packedValues  该对象调用时会切换scratchBytesRef1的offset
     * @param start  遍历 leafCount时当前的值
     * @param end   Math.min(i + 0xff, count)    i = start   也就是最小值是  i + 255
     * @param byteOffset   压缩的起始偏移量    之前计算共享前缀的长度的时候  是直接用了 第一个value 与最后一个value做匹配 所以中间的一些value 可能会与首个value有共享的地方
     * @return
     */
    private static int runLen(IntFunction<BytesRef> packedValues, int start, int end, int byteOffset) {
        // 切换偏移量  首次调用时  start为0 将offset也切换成0 了
        BytesRef first = packedValues.apply(start);
        // 从首个值开始往后读取 非共享位置对应的 byte
        byte b = first.bytes[first.offset + byteOffset];
        for (int i = start + 1; i < end; ++i) {
            // 切换到下一个 value
            BytesRef ref = packedValues.apply(i);
            // 每次只读取第一个 byte
            byte b2 = ref.bytes[ref.offset + byteOffset];
            assert Byte.toUnsignedInt(b2) >= Byte.toUnsignedInt(b);
            if (b != b2) {
                // 代表从哪里开始 第一个byte无法匹配
                return i - start;
            }
        }
        // 代表所有的value 对应的第一个byte是相同的
        return end - start;
    }

    /**
     * 写入 当前leaf的公共前缀
     * @param out   临时容器
     * @param commonPrefixes
     * @param packedValue   存储叶数据的容器
     * @throws IOException
     */
    private void writeCommonPrefixes(DataOutput out, int[] commonPrefixes, byte[] packedValue) throws IOException {
        for (int dim = 0; dim < numDataDims; dim++) {
            // 根据维度数 分别写入公共前缀长度
            out.writeVInt(commonPrefixes[dim]);
            //System.out.println(commonPrefixes[dim] + " of " + bytesPerDim);
            // 相当于将第一个多维度值 按照维度数进行拆解  将每个维度对应的前缀存储起来
            out.writeBytes(packedValue, dim * bytesPerDim, commonPrefixes[dim]);
        }
    }

    @Override
    public void close() throws IOException {
        finished = true;
        if (tempInput != null) {
            // NOTE: this should only happen on exception, e.g. caller calls close w/o calling finish:
            try {
                tempInput.close();
            } finally {
                tempDir.deleteFile(tempInput.getName());
                tempInput = null;
            }
        }
    }

    /**
     * Called on exception, to check whether the checksum is also corrupt in this source, and add that
     * information (checksum matched or didn't) as a suppressed exception.
     */
    private Error verifyChecksum(Throwable priorException, PointWriter writer) throws IOException {
        assert priorException != null;

        // TODO: we could improve this, to always validate checksum as we recurse, if we shared left and
        // right reader after recursing to children, and possibly within recursed children,
        // since all together they make a single pass through the file.  But this is a sizable re-org,
        // and would mean leaving readers (IndexInputs) open for longer:
        if (writer instanceof OfflinePointWriter) {
            // We are reading from a temp file; go verify the checksum:
            String tempFileName = ((OfflinePointWriter) writer).name;
            if (tempDir.getCreatedFiles().contains(tempFileName)) {
                try (ChecksumIndexInput in = tempDir.openChecksumInput(tempFileName, IOContext.READONCE)) {
                    CodecUtil.checkFooter(in, priorException);
                }
            }
        }

        // We are reading from heap; nothing to add:
        throw IOUtils.rethrowAlways(priorException);
    }

    /**
     * Called only in assert
     */
    private boolean valueInBounds(BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue) {
        for (int dim = 0; dim < numIndexDims; dim++) {
            int offset = bytesPerDim * dim;
            if (Arrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDim, minPackedValue, offset, offset + bytesPerDim) < 0) {
                return false;
            }
            if (Arrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) > 0) {
                return false;
            }
        }

        return true;
    }

    /**
     * Pick the next dimension to split.
     *
     * @param minPackedValue the min values for all dimensions
     * @param maxPackedValue the max values for all dimensions
     * @param parentSplits   how many times each dim has been split on the parent levels
     * @return the dimension to split
     */
    protected int split(byte[] minPackedValue, byte[] maxPackedValue, int[] parentSplits) {
        // First look at whether there is a dimension that has split less than 2x less than
        // the dim that has most splits, and return it if there is such a dimension and it
        // does not only have equals values. This helps ensure all dimensions are indexed.
        int maxNumSplits = 0;
        for (int numSplits : parentSplits) {
            maxNumSplits = Math.max(maxNumSplits, numSplits);
        }
        for (int dim = 0; dim < numIndexDims; ++dim) {
            final int offset = dim * bytesPerDim;
            if (parentSplits[dim] < maxNumSplits / 2 &&
                    Arrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, maxPackedValue, offset, offset + bytesPerDim) != 0) {
                return dim;
            }
        }

        // Find which dim has the largest span so we can split on it:
        int splitDim = -1;
        for (int dim = 0; dim < numIndexDims; dim++) {
            NumericUtils.subtract(bytesPerDim, dim, maxPackedValue, minPackedValue, scratchDiff);
            if (splitDim == -1 || Arrays.compareUnsigned(scratchDiff, 0, bytesPerDim, scratch1, 0, bytesPerDim) > 0) {
                System.arraycopy(scratchDiff, 0, scratch1, 0, bytesPerDim);
                splitDim = dim;
            }
        }

        //System.out.println("SPLIT: " + splitDim);
        return splitDim;
    }

    /**
     * Pull a partition back into heap once the point count is low enough while recursing.
     */
    private HeapPointWriter switchToHeap(PointWriter source) throws IOException {
        int count = Math.toIntExact(source.count());
        try (PointReader reader = source.getReader(0, source.count());
             HeapPointWriter writer = new HeapPointWriter(count, packedBytesLength)) {
            for (int i = 0; i < count; i++) {
                boolean hasNext = reader.next();
                assert hasNext;
                writer.append(reader.pointValue());
            }
            source.destroy();
            return writer;
        } catch (Throwable t) {
            throw verifyChecksum(t, source);
        }
    }

    /* Recursively reorders the provided reader and writes the bkd-tree on the fly; this method is used
     * when we are writing a new segment directly from IndexWriter's indexing buffer (MutablePointsReader). */
    private void build(int leavesOffset, int numLeaves,
                       MutablePointValues reader, int from, int to,
                       IndexOutput out,
                       byte[] minPackedValue, byte[] maxPackedValue,
                       int[] parentSplits,
                       byte[] splitPackedValues,
                       byte[] splitDimensionValues,
                       long[] leafBlockFPs,
                       int[] spareDocIds) throws IOException {

        if (numLeaves == 1) {
            // leaf node
            final int count = to - from;
            assert count <= maxPointsInLeafNode;

            // Compute common prefixes
            Arrays.fill(commonPrefixLengths, bytesPerDim);
            reader.getValue(from, scratchBytesRef1);
            for (int i = from + 1; i < to; ++i) {
                reader.getValue(i, scratchBytesRef2);
                for (int dim = 0; dim < numDataDims; dim++) {
                    final int offset = dim * bytesPerDim;
                    int dimensionPrefixLength = commonPrefixLengths[dim];
                    commonPrefixLengths[dim] = Arrays.mismatch(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset,
                            scratchBytesRef1.offset + offset + dimensionPrefixLength,
                            scratchBytesRef2.bytes, scratchBytesRef2.offset + offset,
                            scratchBytesRef2.offset + offset + dimensionPrefixLength);
                    if (commonPrefixLengths[dim] == -1) {
                        commonPrefixLengths[dim] = dimensionPrefixLength;
                    }
                }
            }

            // Find the dimension that has the least number of unique bytes at commonPrefixLengths[dim]
            FixedBitSet[] usedBytes = new FixedBitSet[numDataDims];
            for (int dim = 0; dim < numDataDims; ++dim) {
                if (commonPrefixLengths[dim] < bytesPerDim) {
                    usedBytes[dim] = new FixedBitSet(256);
                }
            }
            for (int i = from + 1; i < to; ++i) {
                for (int dim = 0; dim < numDataDims; dim++) {
                    if (usedBytes[dim] != null) {
                        byte b = reader.getByteAt(i, dim * bytesPerDim + commonPrefixLengths[dim]);
                        usedBytes[dim].set(Byte.toUnsignedInt(b));
                    }
                }
            }
            int sortedDim = 0;
            int sortedDimCardinality = Integer.MAX_VALUE;
            for (int dim = 0; dim < numDataDims; ++dim) {
                if (usedBytes[dim] != null) {
                    final int cardinality = usedBytes[dim].cardinality();
                    if (cardinality < sortedDimCardinality) {
                        sortedDim = dim;
                        sortedDimCardinality = cardinality;
                    }
                }
            }

            // sort by sortedDim
            MutablePointsReaderUtils.sortByDim(numDataDims, numIndexDims, sortedDim, bytesPerDim, commonPrefixLengths,
                    reader, from, to, scratchBytesRef1, scratchBytesRef2);

            BytesRef comparator = scratchBytesRef1;
            BytesRef collector = scratchBytesRef2;
            reader.getValue(from, comparator);
            int leafCardinality = 1;
            for (int i = from + 1; i < to; ++i) {
                reader.getValue(i, collector);
                for (int dim = 0; dim < numDataDims; dim++) {
                    final int start = dim * bytesPerDim + commonPrefixLengths[dim];
                    final int end = dim * bytesPerDim + bytesPerDim;
                    if (Arrays.mismatch(collector.bytes, collector.offset + start, collector.offset + end,
                            comparator.bytes, comparator.offset + start, comparator.offset + end) != -1) {
                        leafCardinality++;
                        BytesRef scratch = collector;
                        collector = comparator;
                        comparator = scratch;
                        break;
                    }
                }
            }
            // Save the block file pointer:
            leafBlockFPs[leavesOffset] = out.getFilePointer();

            assert scratchOut.size() == 0;

            // Write doc IDs
            int[] docIDs = spareDocIds;
            for (int i = from; i < to; ++i) {
                docIDs[i - from] = reader.getDocID(i);
            }
            //System.out.println("writeLeafBlock pos=" + out.getFilePointer());
            writeLeafBlockDocs(scratchOut, docIDs, 0, count);

            // Write the common prefixes:
            reader.getValue(from, scratchBytesRef1);
            System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset, scratch1, 0, packedBytesLength);
            writeCommonPrefixes(scratchOut, commonPrefixLengths, scratch1);

            // Write the full values:
            IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
                @Override
                public BytesRef apply(int i) {
                    reader.getValue(from + i, scratchBytesRef1);
                    return scratchBytesRef1;
                }
            };
            assert valuesInOrderAndBounds(count, sortedDim, minPackedValue, maxPackedValue, packedValues,
                    docIDs, 0);
            writeLeafBlockPackedValues(scratchOut, commonPrefixLengths, count, sortedDim, packedValues, leafCardinality);
            scratchOut.copyTo(out);
            scratchOut.reset();
        } else {
            // inner node

            final int splitDim;
            // compute the split dimension and partition around it
            if (numIndexDims == 1) {
                splitDim = 0;
            } else {
                // for dimensions > 2 we recompute the bounds for the current inner node to help the algorithm choose best
                // split dimensions. Because it is an expensive operation, the frequency we recompute the bounds is given
                // by SPLITS_BEFORE_EXACT_BOUNDS.
                if (numLeaves != leafBlockFPs.length && numIndexDims > 2 && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
                    computePackedValueBounds(reader, from, to, minPackedValue, maxPackedValue, scratchBytesRef1);
                }
                splitDim = split(minPackedValue, maxPackedValue, parentSplits);
            }

            // How many leaves will be in the left tree:
            int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
            // How many points will be in the left tree:
            final int mid = from + numLeftLeafNodes * maxPointsInLeafNode;

            int commonPrefixLen = Arrays.mismatch(minPackedValue, splitDim * bytesPerDim,
                    splitDim * bytesPerDim + bytesPerDim, maxPackedValue, splitDim * bytesPerDim,
                    splitDim * bytesPerDim + bytesPerDim);
            if (commonPrefixLen == -1) {
                commonPrefixLen = bytesPerDim;
            }

            MutablePointsReaderUtils.partition(numDataDims, numIndexDims, maxDoc, splitDim, bytesPerDim, commonPrefixLen,
                    reader, from, to, mid, scratchBytesRef1, scratchBytesRef2);

            final int rightOffset = leavesOffset + numLeftLeafNodes;
            final int splitOffset = rightOffset - 1;
            // set the split value
            final int address = splitOffset * bytesPerDim;
            splitDimensionValues[splitOffset] = (byte) splitDim;
            reader.getValue(mid, scratchBytesRef1);
            System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * bytesPerDim, splitPackedValues, address, bytesPerDim);

            byte[] minSplitPackedValue = ArrayUtil.copyOfSubArray(minPackedValue, 0, packedIndexBytesLength);
            byte[] maxSplitPackedValue = ArrayUtil.copyOfSubArray(maxPackedValue, 0, packedIndexBytesLength);
            System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * bytesPerDim,
                    minSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);
            System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * bytesPerDim,
                    maxSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);

            // recurse
            parentSplits[splitDim]++;
            build(leavesOffset, numLeftLeafNodes, reader, from, mid, out,
                    minPackedValue, maxSplitPackedValue, parentSplits,
                    splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);
            build(rightOffset, numLeaves - numLeftLeafNodes, reader, mid, to, out,
                    minSplitPackedValue, maxPackedValue, parentSplits,
                    splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);
            parentSplits[splitDim]--;
        }
    }


    private void computePackedValueBounds(BKDRadixSelector.PathSlice slice, byte[] minPackedValue, byte[] maxPackedValue) throws IOException {
        try (PointReader reader = slice.writer.getReader(slice.start, slice.count)) {
            if (reader.next() == false) {
                return;
            }
            BytesRef value = reader.pointValue().packedValue();
            System.arraycopy(value.bytes, value.offset, minPackedValue, 0, packedIndexBytesLength);
            System.arraycopy(value.bytes, value.offset, maxPackedValue, 0, packedIndexBytesLength);
            while (reader.next()) {
                value = reader.pointValue().packedValue();
                for (int dim = 0; dim < numIndexDims; dim++) {
                    final int startOffset = dim * bytesPerDim;
                    final int endOffset = startOffset + bytesPerDim;
                    if (Arrays.compareUnsigned(value.bytes, value.offset + startOffset, value.offset + endOffset, minPackedValue, startOffset, endOffset) < 0) {
                        System.arraycopy(value.bytes, value.offset + startOffset, minPackedValue, startOffset, bytesPerDim);
                    } else if (Arrays.compareUnsigned(value.bytes, value.offset + startOffset, value.offset + endOffset, maxPackedValue, startOffset, endOffset) > 0) {
                        System.arraycopy(value.bytes, value.offset + startOffset, maxPackedValue, startOffset, bytesPerDim);
                    }
                }
            }
        }
    }

    /**
     * The point writer contains the data that is going to be splitted using radix selection.
     * /*  This method is used when we are merging previously written segments, in the numDims > 1 case.
     */
    private void build(int leavesOffset, int numLeaves,
                       BKDRadixSelector.PathSlice points,
                       IndexOutput out,
                       BKDRadixSelector radixSelector,
                       byte[] minPackedValue, byte[] maxPackedValue,
                       int[] parentSplits,
                       byte[] splitPackedValues,
                       byte[] splitDimensionValues,
                       long[] leafBlockFPs,
                       int[] spareDocIds) throws IOException {

        if (numLeaves == 1) {

            // Leaf node: write block
            // We can write the block in any order so by default we write it sorted by the dimension that has the
            // least number of unique bytes at commonPrefixLengths[dim], which makes compression more efficient
            HeapPointWriter heapSource;
            if (points.writer instanceof HeapPointWriter == false) {
                // Adversarial cases can cause this, e.g. merging big segments with most of the points deleted
                heapSource = switchToHeap(points.writer);
            } else {
                heapSource = (HeapPointWriter) points.writer;
            }

            int from = Math.toIntExact(points.start);
            int to = Math.toIntExact(points.start + points.count);
            //we store common prefix on scratch1
            computeCommonPrefixLength(heapSource, scratch1, from, to);

            int sortedDim = 0;
            int sortedDimCardinality = Integer.MAX_VALUE;
            FixedBitSet[] usedBytes = new FixedBitSet[numDataDims];
            for (int dim = 0; dim < numDataDims; ++dim) {
                if (commonPrefixLengths[dim] < bytesPerDim) {
                    usedBytes[dim] = new FixedBitSet(256);
                }
            }
            //Find the dimension to compress
            for (int dim = 0; dim < numDataDims; dim++) {
                int prefix = commonPrefixLengths[dim];
                if (prefix < bytesPerDim) {
                    int offset = dim * bytesPerDim;
                    for (int i = from; i < to; ++i) {
                        PointValue value = heapSource.getPackedValueSlice(i);
                        BytesRef packedValue = value.packedValue();
                        int bucket = packedValue.bytes[packedValue.offset + offset + prefix] & 0xff;
                        usedBytes[dim].set(bucket);
                    }
                    int cardinality = usedBytes[dim].cardinality();
                    if (cardinality < sortedDimCardinality) {
                        sortedDim = dim;
                        sortedDimCardinality = cardinality;
                    }
                }
            }

            // sort the chosen dimension
            radixSelector.heapRadixSort(heapSource, from, to, sortedDim, commonPrefixLengths[sortedDim]);
            // compute cardinality
            int leafCardinality = heapSource.computeCardinality(from, to, numDataDims, bytesPerDim, commonPrefixLengths);

            // Save the block file pointer:
            leafBlockFPs[leavesOffset] = out.getFilePointer();
            //System.out.println("  write leaf block @ fp=" + out.getFilePointer());

            // Write docIDs first, as their own chunk, so that at intersect time we can add all docIDs w/o
            // loading the values:
            int count = to - from;
            assert count > 0 : "numLeaves=" + numLeaves + " leavesOffset=" + leavesOffset;
            assert count <= spareDocIds.length : "count=" + count + " > length=" + spareDocIds.length;
            // Write doc IDs
            int[] docIDs = spareDocIds;
            for (int i = 0; i < count; i++) {
                docIDs[i] = heapSource.getPackedValueSlice(from + i).docID();
            }
            writeLeafBlockDocs(out, docIDs, 0, count);

            // TODO: minor opto: we don't really have to write the actual common prefixes, because BKDReader on recursing can regenerate it for us
            // from the index, much like how terms dict does so from the FST:

            // Write the common prefixes:
            writeCommonPrefixes(out, commonPrefixLengths, scratch1);

            // Write the full values:
            IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
                final BytesRef scratch = new BytesRef();

                {
                    scratch.length = packedBytesLength;
                }

                @Override
                public BytesRef apply(int i) {
                    PointValue value = heapSource.getPackedValueSlice(from + i);
                    return value.packedValue();
                }
            };
            assert valuesInOrderAndBounds(count, sortedDim, minPackedValue, maxPackedValue, packedValues,
                    docIDs, 0);
            writeLeafBlockPackedValues(out, commonPrefixLengths, count, sortedDim, packedValues, leafCardinality);

        } else {
            // Inner node: partition/recurse

            final int splitDim;
            if (numIndexDims == 1) {
                splitDim = 0;
            } else {
                // for dimensions > 2 we recompute the bounds for the current inner node to help the algorithm choose best
                // split dimensions. Because it is an expensive operation, the frequency we recompute the bounds is given
                // by SPLITS_BEFORE_EXACT_BOUNDS.
                if (numLeaves != leafBlockFPs.length && numIndexDims > 2 && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
                    computePackedValueBounds(points, minPackedValue, maxPackedValue);
                }
                splitDim = split(minPackedValue, maxPackedValue, parentSplits);
            }

            assert numLeaves <= leafBlockFPs.length : "numLeaves=" + numLeaves + " leafBlockFPs.length=" + leafBlockFPs.length;

            // How many leaves will be in the left tree:
            final int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
            // How many points will be in the left tree:
            final long leftCount = numLeftLeafNodes * maxPointsInLeafNode;

            BKDRadixSelector.PathSlice[] slices = new BKDRadixSelector.PathSlice[2];

            int commonPrefixLen = Arrays.mismatch(minPackedValue, splitDim * bytesPerDim,
                    splitDim * bytesPerDim + bytesPerDim, maxPackedValue, splitDim * bytesPerDim,
                    splitDim * bytesPerDim + bytesPerDim);
            if (commonPrefixLen == -1) {
                commonPrefixLen = bytesPerDim;
            }

            byte[] splitValue = radixSelector.select(points, slices, points.start, points.start + points.count, points.start + leftCount, splitDim, commonPrefixLen);

            final int rightOffset = leavesOffset + numLeftLeafNodes;
            final int splitValueOffset = rightOffset - 1;

            splitDimensionValues[splitValueOffset] = (byte) splitDim;
            int address = splitValueOffset * bytesPerDim;
            System.arraycopy(splitValue, 0, splitPackedValues, address, bytesPerDim);

            byte[] minSplitPackedValue = new byte[packedIndexBytesLength];
            System.arraycopy(minPackedValue, 0, minSplitPackedValue, 0, packedIndexBytesLength);

            byte[] maxSplitPackedValue = new byte[packedIndexBytesLength];
            System.arraycopy(maxPackedValue, 0, maxSplitPackedValue, 0, packedIndexBytesLength);

            System.arraycopy(splitValue, 0, minSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);
            System.arraycopy(splitValue, 0, maxSplitPackedValue, splitDim * bytesPerDim, bytesPerDim);

            parentSplits[splitDim]++;
            // Recurse on left tree:
            build(leavesOffset, numLeftLeafNodes, slices[0],
                    out, radixSelector, minPackedValue, maxSplitPackedValue,
                    parentSplits, splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);

            // Recurse on right tree:
            build(rightOffset, numLeaves - numLeftLeafNodes, slices[1],
                    out, radixSelector, minSplitPackedValue, maxPackedValue,
                    parentSplits, splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);

            parentSplits[splitDim]--;
        }
    }

    private void computeCommonPrefixLength(HeapPointWriter heapPointWriter, byte[] commonPrefix, int from, int to) {
        Arrays.fill(commonPrefixLengths, bytesPerDim);
        PointValue value = heapPointWriter.getPackedValueSlice(from);
        BytesRef packedValue = value.packedValue();
        for (int dim = 0; dim < numDataDims; dim++) {
            System.arraycopy(packedValue.bytes, packedValue.offset + dim * bytesPerDim, commonPrefix, dim * bytesPerDim, bytesPerDim);
        }
        for (int i = from + 1; i < to; i++) {
            value = heapPointWriter.getPackedValueSlice(i);
            packedValue = value.packedValue();
            for (int dim = 0; dim < numDataDims; dim++) {
                if (commonPrefixLengths[dim] != 0) {
                    int j = Arrays.mismatch(commonPrefix, dim * bytesPerDim, dim * bytesPerDim + commonPrefixLengths[dim], packedValue.bytes, packedValue.offset + dim * bytesPerDim, packedValue.offset + dim * bytesPerDim + commonPrefixLengths[dim]);
                    if (j != -1) {
                        commonPrefixLengths[dim] = j;
                    }
                }
            }
        }
    }

    // only called from assert
    private boolean valuesInOrderAndBounds(int count, int sortedDim, byte[] minPackedValue, byte[] maxPackedValue,
                                           IntFunction<BytesRef> values, int[] docs, int docsOffset) throws IOException {
        byte[] lastPackedValue = new byte[packedBytesLength];
        int lastDoc = -1;
        for (int i = 0; i < count; i++) {
            BytesRef packedValue = values.apply(i);
            assert packedValue.length == packedBytesLength;
            assert valueInOrder(i, sortedDim, lastPackedValue, packedValue.bytes, packedValue.offset,
                    docs[docsOffset + i], lastDoc);
            lastDoc = docs[docsOffset + i];

            // Make sure this value does in fact fall within this leaf cell:
            assert valueInBounds(packedValue, minPackedValue, maxPackedValue);
        }
        return true;
    }

    // only called from assert
    private boolean valueInOrder(long ord, int sortedDim, byte[] lastPackedValue, byte[] packedValue, int packedValueOffset,
                                 int doc, int lastDoc) {
        int dimOffset = sortedDim * bytesPerDim;
        if (ord > 0) {
            int cmp = Arrays.compareUnsigned(lastPackedValue, dimOffset, dimOffset + bytesPerDim, packedValue, packedValueOffset + dimOffset, packedValueOffset + dimOffset + bytesPerDim);
            if (cmp > 0) {
                throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, packedBytesLength) + " ord=" + ord);
            }
            if (cmp == 0 && numDataDims > numIndexDims) {
                int dataOffset = numIndexDims * bytesPerDim;
                cmp = Arrays.compareUnsigned(lastPackedValue, dataOffset, packedBytesLength, packedValue, packedValueOffset + dataOffset, packedValueOffset + packedBytesLength);
                if (cmp > 0) {
                    throw new AssertionError("data values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, packedBytesLength) + " ord=" + ord);
                }
            }
            if (cmp == 0 && doc < lastDoc) {
                throw new AssertionError("docs out of order: last doc=" + lastDoc + " current doc=" + doc + " ord=" + ord);
            }
        }
        System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, packedBytesLength);
        return true;
    }
}
