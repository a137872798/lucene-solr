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


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.RamUsageEstimator;

import static org.apache.lucene.util.fst.FST.Arc.BitTable;

// TODO: break this into WritableFST and ReadOnlyFST.. then
// we can have subclasses of ReadOnlyFST to handle the
// different byte[] level encodings (packed or
// not)... and things like nodeCount, arcCount are read only

// TODO: if FST is pure prefix trie we can do a more compact
// job, ie, once we are at a 'suffix only', just store the
// completion labels as a string not as a series of arcs.

// NOTE: while the FST is able to represent a non-final
// dead-end state (NON_FINAL_END_NODE=0), the layers above
// (FSTEnum, Util) have problems with this!!

/**
 * Represents an finite state machine (FST), using a
 * compact byte[] format.
 * <p> The format is similar to what's used by Morfologik
 * (https://github.com/morfologik/morfologik-stemming).
 *
 * <p> See the {@link org.apache.lucene.util.fst package
 * documentation} for some simple examples.
 *
 * @lucene.experimental 有限状态机
 */
public final class FST<T> implements Accountable {

    /**
     * Specifies allowed range of each int input label for
     * this FST.
     */
    // 只允许写入 1byte  2byte  4byte
    public enum INPUT_TYPE {BYTE1, BYTE2, BYTE4}

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FST.class);
    private static final long ARC_SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Arc.class);

    private static final int BIT_FINAL_ARC = 1 << 0;
    static final int BIT_LAST_ARC = 1 << 1;
    static final int BIT_TARGET_NEXT = 1 << 2;

    // TODO: we can free up a bit if we can nuke this:
    private static final int BIT_STOP_NODE = 1 << 3;

    /**
     * This flag is set if the arc has an output.
     */
    public static final int BIT_ARC_HAS_OUTPUT = 1 << 4;

    private static final int BIT_ARC_HAS_FINAL_OUTPUT = 1 << 5;

    /**
     * Value of the arc flags to declare a node with fixed length arcs
     * designed for binary search.
     */
    // We use this as a marker because this one flag is illegal by itself.
    public static final byte ARCS_FOR_BINARY_SEARCH = BIT_ARC_HAS_FINAL_OUTPUT;

    /**
     * Value of the arc flags to declare a node with fixed length arcs
     * and bit table designed for direct addressing.
     */
    static final byte ARCS_FOR_DIRECT_ADDRESSING = 1 << 6;

    /**
     * @see #shouldExpandNodeWithFixedLengthArcs
     */
    static final int FIXED_LENGTH_ARC_SHALLOW_DEPTH = 3; // 0 => only root node.

    /**
     * @see #shouldExpandNodeWithFixedLengthArcs
     */
    static final int FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS = 5;

    /**
     * @see #shouldExpandNodeWithFixedLengthArcs
     */
    static final int FIXED_LENGTH_ARC_DEEP_NUM_ARCS = 10;

    /**
     * Maximum oversizing factor allowed for direct addressing compared to binary search when expansion
     * credits allow the oversizing. This factor prevents expansions that are obviously too costly even
     * if there are sufficient credits.
     *
     * @see #shouldExpandNodeWithDirectAddressing
     */
    private static final float DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR = 1.66f;

    // Increment version to change it
    private static final String FILE_FORMAT_NAME = "FST";
    private static final int VERSION_START = 6;
    private static final int VERSION_CURRENT = 7;

    // Never serialized; just used to represent the virtual
    // final node w/ no arcs:
    private static final long FINAL_END_NODE = -1;

    // Never serialized; just used to represent the virtual
    // non-final node w/ no arcs:
    private static final long NON_FINAL_END_NODE = 0;

    /**
     * If arc has this label then that arc is final/accepted
     */
    public static final int END_LABEL = -1;

    final INPUT_TYPE inputType;

    // if non-null, this FST accepts the empty string and
    // produces this output
    T emptyOutput;

    /**
     * A {@link BytesStore}, used during building, or during reading when
     * the FST is very large (more than 1 GB).  If the FST is less than 1
     * GB then bytesArray is set instead.
     */
    // FST 的数据 实际上都是写入到该对象   该对象同时提供了正向遍历和反向遍历的功能
    final BytesStore bytes;

    private final FSTStore fstStore;

    private long startNode = -1;

    public final Outputs<T> outputs;

    /**
     * Represents a single arc.
     * arc上携带了 数据的ascii码 以及权重信息
     */
    public static final class Arc<T> {

        //*** Arc fields.

        private int label;

        private T output;

        /**
         * 指向下一个 CompileNode的地址
         */
        private long target;

        private byte flags;

        /**
         * 下一个节点对应的权重信息
         */
        private T nextFinalOutput;

        private long nextArc;

        private byte nodeFlags;

        //*** Fields for arcs belonging to a node with fixed length arcs.
        // So only valid when bytesPerArc != 0.
        // nodeFlags == ARCS_FOR_BINARY_SEARCH || nodeFlags == ARCS_FOR_DIRECT_ADDRESSING.

        /**
         * 如果arc以固定长度存储时 才会设置这个值
         */
        private int bytesPerArc;

        private long posArcsStart;

        private int arcIdx;

        private int numArcs;

        //*** Fields for a direct addressing node. nodeFlags == ARCS_FOR_DIRECT_ADDRESSING.

        /**
         * Start position in the {@link FST.BytesReader} of the presence bits for a direct addressing node, aka the bit-table
         */
        private long bitTableStart;

        /**
         * First label of a direct addressing node.
         */
        private int firstLabel;

        /**
         * Index of the current label of a direct addressing node. While {@link #arcIdx} is the current index in the label
         * range, {@link #presenceIndex} is its corresponding index in the list of actually present labels. It is equal
         * to the number of bits set before the bit at {@link #arcIdx} in the bit-table. This field is a cache to avoid
         * to count bits set repeatedly when iterating the next arcs.
         */
        private int presenceIndex;

        /**
         * Returns this
         */
        public Arc<T> copyFrom(Arc<T> other) {
            label = other.label();
            target = other.target();
            flags = other.flags();
            output = other.output();
            nextFinalOutput = other.nextFinalOutput();
            nextArc = other.nextArc();
            nodeFlags = other.nodeFlags();
            bytesPerArc = other.bytesPerArc();

            // Fields for arcs belonging to a node with fixed length arcs.
            // We could avoid copying them if bytesPerArc() == 0 (this was the case with previous code, and the current code
            // still supports that), but it may actually help external uses of FST to have consistent arc state, and debugging
            // is easier.
            posArcsStart = other.posArcsStart();
            arcIdx = other.arcIdx();
            numArcs = other.numArcs();
            bitTableStart = other.bitTableStart;
            firstLabel = other.firstLabel();
            presenceIndex = other.presenceIndex;

            return this;
        }

        boolean flag(int flag) {
            return FST.flag(flags, flag);
        }

        /**
         * 判断是否携带了 last标识
         * @return
         */
        public boolean isLast() {
            return flag(BIT_LAST_ARC);
        }

        /**
         * 检测是否携带了  final 标识
         * @return
         */
        public boolean isFinal() {
            return flag(BIT_FINAL_ARC);
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append(" target=").append(target());
            b.append(" label=0x").append(Integer.toHexString(label()));
            if (flag(BIT_FINAL_ARC)) {
                b.append(" final");
            }
            if (flag(BIT_LAST_ARC)) {
                b.append(" last");
            }
            if (flag(BIT_TARGET_NEXT)) {
                b.append(" targetNext");
            }
            if (flag(BIT_STOP_NODE)) {
                b.append(" stop");
            }
            if (flag(BIT_ARC_HAS_OUTPUT)) {
                b.append(" output=").append(output());
            }
            if (flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
                b.append(" nextFinalOutput=").append(nextFinalOutput());
            }
            if (bytesPerArc() != 0) {
                b.append(" arcArray(idx=").append(arcIdx()).append(" of ").append(numArcs()).append(")")
                        .append("(").append(nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING ? "da" : "bs").append(")");
            }
            return b.toString();
        }

        public int label() {
            return label;
        }

        public T output() {
            return output;
        }

        /**
         * Ord/address to target node.
         */
        public long target() {
            return target;
        }

        public byte flags() {
            return flags;
        }

        public T nextFinalOutput() {
            return nextFinalOutput;
        }

        /**
         * Address (into the byte[]) of the next arc - only for list of variable length arc.
         * Or ord/address to the next node if label == {@link #END_LABEL}.
         */
        long nextArc() {
            return nextArc;
        }

        /**
         * Where we are in the array; only valid if bytesPerArc != 0.
         */
        public int arcIdx() {
            return arcIdx;
        }

        /**
         * Node header flags. Only meaningful to check if the value is either
         * {@link #ARCS_FOR_BINARY_SEARCH} or {@link #ARCS_FOR_DIRECT_ADDRESSING}
         * (other value when bytesPerArc == 0).
         */
        public byte nodeFlags() {
            return nodeFlags;
        }

        /**
         * Where the first arc in the array starts; only valid if bytesPerArc != 0
         */
        public long posArcsStart() {
            return posArcsStart;
        }

        /**
         * Non-zero if this arc is part of a node with fixed length arcs, which means all
         * arcs for the node are encoded with a fixed number of bytes so
         * that we binary search or direct address. We do when there are enough
         * arcs leaving one node. It wastes some bytes but gives faster lookups.
         */
        public int bytesPerArc() {
            return bytesPerArc;
        }

        /**
         * How many arcs; only valid if bytesPerArc != 0 (fixed length arcs).
         * For a node designed for binary search this is the array size.
         * For a node designed for direct addressing, this is the label range.
         */
        public int numArcs() {
            return numArcs;
        }

        /**
         * First label of a direct addressing node.
         * Only valid if nodeFlags == {@link #ARCS_FOR_DIRECT_ADDRESSING}.
         */
        int firstLabel() {
            return firstLabel;
        }

        /**
         * Helper methods to read the bit-table of a direct addressing node.
         * Only valid for {@link Arc} with {@link Arc#nodeFlags()} == {@code ARCS_FOR_DIRECT_ADDRESSING}.
         */
        static class BitTable {

            /**
             * See {@link BitTableUtil#isBitSet(int, FST.BytesReader)}.
             */
            static boolean isBitSet(int bitIndex, Arc<?> arc, FST.BytesReader in) throws IOException {
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                in.setPosition(arc.bitTableStart);
                return BitTableUtil.isBitSet(bitIndex, in);
            }

            /**
             * See {@link BitTableUtil#countBits(int, FST.BytesReader)}.
             * The count of bit set is the number of arcs of a direct addressing node.
             */
            static int countBits(Arc<?> arc, FST.BytesReader in) throws IOException {
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                in.setPosition(arc.bitTableStart);
                return BitTableUtil.countBits(getNumPresenceBytes(arc.numArcs()), in);
            }

            /**
             * See {@link BitTableUtil#countBitsUpTo(int, FST.BytesReader)}.
             */
            static int countBitsUpTo(int bitIndex, Arc<?> arc, FST.BytesReader in) throws IOException {
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                in.setPosition(arc.bitTableStart);
                return BitTableUtil.countBitsUpTo(bitIndex, in);
            }

            /**
             * See {@link BitTableUtil#nextBitSet(int, int, FST.BytesReader)}.
             */
            static int nextBitSet(int bitIndex, Arc<?> arc, FST.BytesReader in) throws IOException {
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                in.setPosition(arc.bitTableStart);
                return BitTableUtil.nextBitSet(bitIndex, getNumPresenceBytes(arc.numArcs()), in);
            }

            /**
             * See {@link BitTableUtil#previousBitSet(int, FST.BytesReader)}.
             */
            static int previousBitSet(int bitIndex, Arc<?> arc, FST.BytesReader in) throws IOException {
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                in.setPosition(arc.bitTableStart);
                return BitTableUtil.previousBitSet(bitIndex, in);
            }

            /**
             * Asserts the bit-table of the provided {@link Arc} is valid.
             */
            static boolean assertIsValid(Arc<?> arc, FST.BytesReader in) throws IOException {
                assert arc.bytesPerArc() > 0;
                assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                // First bit must be set.
                assert isBitSet(0, arc, in);
                // Last bit must be set.
                assert isBitSet(arc.numArcs() - 1, arc, in);
                // No bit set after the last arc.
                assert nextBitSet(arc.numArcs() - 1, arc, in) == -1;
                return true;
            }
        }
    }

    private static boolean flag(int flags, int bit) {
        return (flags & bit) != 0;
    }

    /**
     * make a new empty FST, for building; Builder invokes this
     *
     * @param inputType   代表输出类型
     * @param outputs   用于构建FST的特殊输出流 一般是ByteSequenceOutputs
     * @param bytesPageBits  每个bit 对应多大的 byte[] 数据页  默认值为15
     */
    FST(INPUT_TYPE inputType, Outputs<T> outputs, int bytesPageBits) {
        this.inputType = inputType;
        this.outputs = outputs;
        fstStore = null;
        // 根据指定的大小创建 store对象
        bytes = new BytesStore(bytesPageBits);
        // pad: ensure no node gets address 0 which is reserved to mean
        // the stop state w/ no arcs
        // 初始化时写入一个0  这也代表着一个FST的终止标识
        bytes.writeByte((byte) 0);
        emptyOutput = null;
    }

    /**
     * 一般情况下认为 一个block最多占30个bit
     */
    private static final int DEFAULT_MAX_BLOCK_BITS = Constants.JRE_IS_64BIT ? 30 : 28;

    /**
     * Load a previously saved FST.
     */
    // 使用相关参数进行初始化
    public FST(DataInput in, Outputs<T> outputs) throws IOException {
        this(in, outputs, new OnHeapFSTStore(DEFAULT_MAX_BLOCK_BITS));
    }

    /**
     * Load a previously saved FST; maxBlockBits allows you to
     * control the size of the byte[] pages used to hold the FST bytes.
     * 基于一个已有的输入流还原 FST
     */
    public FST(DataInput in, Outputs<T> outputs, FSTStore fstStore) throws IOException {
        bytes = null;
        this.fstStore = fstStore;
        this.outputs = outputs;

        // NOTE: only reads formats VERSION_START up to VERSION_CURRENT; we don't have
        // back-compat promise for FSTs (they are experimental), but we are sometimes able to offer it
        // 这里在检验输入流是否合法
        CodecUtil.checkHeader(in, FILE_FORMAT_NAME, VERSION_START, VERSION_CURRENT);
        // 对应save()存入的数据  先忽略 emptyBytes的逻辑
        if (in.readByte() == 1) {
            // accepts empty string
            // 1 KB blocks:
            // 1 << 10 = 1024   1kb = 1024byte
            BytesStore emptyBytes = new BytesStore(10);
            // 读取内部的数据长度
            int numBytes = in.readVInt();
            // 读取指定长度的数据 填充到 store中
            emptyBytes.copyBytes(in, numBytes);

            // De-serialize empty-string output:
            // 生成一个反向读取数据的reader
            BytesReader reader = emptyBytes.getReverseReader();
            // NoOutputs uses 0 bytes when writing its output,
            // so we have to check here else BytesStore gets
            // angry:
            if (numBytes > 0) {
                // 指向reader 的默认 这样该对象才能正常使用
                reader.setPosition(numBytes - 1);
            }
            emptyOutput = outputs.readFinalOutput(reader);
        } else {
            emptyOutput = null;
        }
        // 这里是数据类型
        final byte t = in.readByte();
        switch (t) {
            case 0:
                inputType = INPUT_TYPE.BYTE1;
                break;
            case 1:
                inputType = INPUT_TYPE.BYTE2;
                break;
            case 2:
                inputType = INPUT_TYPE.BYTE4;
                break;
            default:
                throw new IllegalStateException("invalid input type " + t);
        }
        // fst的起点偏移量  因为要反向读取
        startNode = in.readVLong();

        // 对应 BytesStore的大小
        long numBytes = in.readVLong();
        this.fstStore.init(in, numBytes);
    }

    @Override
    public long ramBytesUsed() {
        long size = BASE_RAM_BYTES_USED;
        if (this.fstStore != null) {
            size += this.fstStore.ramBytesUsed();
        } else {
            size += bytes.ramBytesUsed();
        }

        return size;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(input=" + inputType + ",output=" + outputs;
    }

    /**
     * 当调用 FSTCompiler.compiler() 时 会从后往前冻结所有节点 然后通过root.node(存储在fst BytesStore的地址) 来触发该方法
     * @param newStartNode
     * @throws IOException
     */
    void finish(long newStartNode) throws IOException {
        assert newStartNode <= bytes.getPosition();
        if (startNode != -1) {
            throw new IllegalStateException("already finished");
        }
        if (newStartNode == FINAL_END_NODE && emptyOutput != null) {
            newStartNode = 0;
        }
        // 此时 startNode 记录的就是最后一个arc对应flag的位置
        startNode = newStartNode;
        bytes.finish();
    }

    public T getEmptyOutput() {
        return emptyOutput;
    }

    /**
     * 当输入为 "" 时对应的权重
     * @param v
     */
    void setEmptyOutput(T v) {
        if (emptyOutput != null) {
            emptyOutput = outputs.merge(emptyOutput, v);
        } else {
            emptyOutput = v;
        }
    }

    /**
     * 将内部数据转移到 out中
     * @param out
     * @throws IOException
     */
    public void save(DataOutput out) throws IOException {
        if (startNode == -1) {
            throw new IllegalStateException("call finish first");
        }
        // 写入 FST 前缀 以及对应的版本号
        CodecUtil.writeHeader(out, FILE_FORMAT_NAME, VERSION_CURRENT);
        // TODO: really we should encode this as an arc, arriving
        // to the root node, instead of special casing here:
        // 如果 "" 对应了某个权重,走这个逻辑  不过有限制条件应该不会出现这种情况
        if (emptyOutput != null) {
            // Accepts empty string
            // 代表允许出现空的字符串
            out.writeByte((byte) 1);

            // Serialize empty-string output:
            ByteBuffersDataOutput ros = new ByteBuffersDataOutput();
            // 这里outputs 只是定义了写入的动作
            outputs.writeFinalOutput(emptyOutput, ros);
            byte[] emptyOutputBytes = ros.toArrayCopy();
            int emptyLen = emptyOutputBytes.length;

            // reverse
            // 将数据反向写入
            final int stopAt = emptyLen / 2;
            int upto = 0;
            while (upto < stopAt) {
                final byte b = emptyOutputBytes[upto];
                emptyOutputBytes[upto] = emptyOutputBytes[emptyLen - upto - 1];
                emptyOutputBytes[emptyLen - upto - 1] = b;
                upto++;
            }
            // 将empty string 对应的 output写入到 索引文件中
            out.writeVInt(emptyLen);
            out.writeBytes(emptyOutputBytes, 0, emptyLen);
        } else {
            // 当empty string 没有权重时 写入0
            out.writeByte((byte) 0);
        }
        final byte t;
        // 这里写入fst输入的类型
        if (inputType == INPUT_TYPE.BYTE1) {
            t = 0;
        } else if (inputType == INPUT_TYPE.BYTE2) {
            t = 1;
        } else {
            t = 2;
        }
        out.writeByte(t);
        // 记录fst起点的偏移量
        out.writeVLong(startNode);
        // fst中编译完成的节点数据就是存储在 bytes中的
        if (bytes != null) {
            long numBytes = bytes.getPosition();
            out.writeVLong(numBytes);
            // 将bytes的数据全部写入到out 中
            bytes.writeTo(out);
        } else {
            assert fstStore != null;
            fstStore.writeTo(out);
        }
    }

    /**
     * Writes an automaton to a file.
     * 将数据写入某个文件中
     */
    public void save(final Path path) throws IOException {
        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(path))) {
            save(new OutputStreamDataOutput(os));
        }
    }

    /**
     * Reads an automaton from a file.
     * 从某个目标文件读取数据 并还原成一个FST
     */
    public static <T> FST<T> read(Path path, Outputs<T> outputs) throws IOException {
        try (InputStream is = Files.newInputStream(path)) {
            return new FST<>(new InputStreamDataInput(new BufferedInputStream(is)), outputs);
        }
    }

    /**
     * 将 label 信息写入到输出流中
     *
     * @param out
     * @param v
     * @throws IOException
     */
    private void writeLabel(DataOutput out, int v) throws IOException {
        assert v >= 0 : "v=" + v;
        // 根据输入类型 确定写入的长度
        // byte 和 short 都是正常写入  遇到int这种比较大的值 就采用  VInt 进行写入
        if (inputType == INPUT_TYPE.BYTE1) {
            assert v <= 255 : "v=" + v;
            out.writeByte((byte) v);
        } else if (inputType == INPUT_TYPE.BYTE2) {
            assert v <= 65535 : "v=" + v;
            out.writeShort((short) v);
        } else {
            out.writeVInt(v);
        }
    }

    /**
     * Reads one BYTE1/2/4 label from the provided {@link DataInput}.
     * 读取 label 还需要考虑 inputType
     */
    public int readLabel(DataInput in) throws IOException {
        final int v;
        if (inputType == INPUT_TYPE.BYTE1) {
            // Unsigned byte:
            v = in.readByte() & 0xFF;
        } else if (inputType == INPUT_TYPE.BYTE2) {
            // Unsigned short:
            v = in.readShort() & 0xFFFF;
        } else {
            v = in.readVInt();
        }
        return v;
    }

    /**
     * returns true if the node at this address has any
     * outgoing arcs
     */
    public static <T> boolean targetHasArcs(Arc<T> arc) {
        return arc.target() > 0;
    }


    /**
     * serializes new node by appending its bytes to the end
     * of the current byte[]
     * @param fstCompiler  往fst中插入数据的是哪个 compiler
     * @param nodeIn   本次插入的节点 从frontier的最后一个空节点开始
     * @return
     * @throws IOException
     * 将某个未编译的节点添加到 fst中
     */
    long addNode(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn) throws IOException {
        // 这里先获取一个空的输出  一般情况下 outputs 就是 ByteSequenceOutputs   那么这里就会返回一个 NO_OUTPUT
        T NO_OUTPUT = outputs.getNoOutput();

        //System.out.println("FST.addNode pos=" + bytes.getPosition() + " numArcs=" + nodeIn.numArcs);
        // 代表这个节点没有任何出度 一般就是最后一个节点
        if (nodeIn.numArcs == 0) {
            // 这里返回的值会作为 上个arc的 target
            if (nodeIn.isFinal) {
                return FINAL_END_NODE;
            } else {
                return NON_FINAL_END_NODE;
            }
        }

        // 当目标节点的arc不为0时  实际上非末尾节点都会存在arc 就是input在对应下标的 ascii码
        final long startAddress = fstCompiler.bytes.getPosition();
        //System.out.println("  startAddr=" + startAddress);

        // 是否以固定长度存储 arcs  当某个node下挂载的arc特别多的情况 可以采用固定长度存储 这样可以利用二分查找   TODO 这个逻辑就先不看吧
        final boolean doFixedLengthArcs = shouldExpandNodeWithFixedLengthArcs(fstCompiler, nodeIn);
        // 代表允许以固定长度存储 arc  TODO 先忽略这里 因为一般情况下node只会使用到一个arc
        if (doFixedLengthArcs) {
            //System.out.println("  fixed length arcs");
            // 如果当前长度小于 目标节点的出度
            if (fstCompiler.numBytesPerArc.length < nodeIn.numArcs) {
                // 对数组进行扩容   后面的参数用于估算使用的RAM 大小
                fstCompiler.numBytesPerArc = new int[ArrayUtil.oversize(nodeIn.numArcs, Integer.BYTES)];
                // 更新该数组  该数组长度与numBytesPerArc 保持一致
                fstCompiler.numLabelBytesPerArc = new int[fstCompiler.numBytesPerArc.length];
            }
        }

        // 累计arc的值
        fstCompiler.arcCount += nodeIn.numArcs;

        // 获取最后一个arc的下标
        final int lastArc = nodeIn.numArcs - 1;

        // 获取此时的起点位置  准备写入数据
        long lastArcStart = fstCompiler.bytes.getPosition();

        int maxBytesPerArc = 0;
        int maxBytesPerArcWithoutLabel = 0;
        // 这里在遍历所有的 arc 从0开始 最后又会将数据反向写入
        for (int arcIdx = 0; arcIdx < nodeIn.numArcs; arcIdx++) {
            final FSTCompiler.Arc<T> arc = nodeIn.arcs[arcIdx];
            // 因为从后往前处理 后面的节点在编译完成后 会作为target 设置到上一个为编译node的lastArc中
            final FSTCompiler.CompiledNode target = (FSTCompiler.CompiledNode) arc.target;

            // 下面根据节点情况 生成标记位
            int flags = 0;
            //System.out.println("  arc " + arcIdx + " label=" + arc.label + " -> target=" + target.node);

            // 代表此时遍历的是最后一个arc
            if (arcIdx == lastArc) {
                // 为标识值 加了一个 特殊值
                flags += BIT_LAST_ARC;
            }

            // 代表上一个冻结的节点就是本次arc指向的节点  这样就可以不存储next的节点对应的下标了  一般就是最后一个arc才出现这种情况
            if (fstCompiler.lastFrozenNode == target.node && !doFixedLengthArcs) {
                // TODO: for better perf (but more RAM used) we
                // could avoid this except when arc is "near" the
                // last arc:
                flags += BIT_TARGET_NEXT;
            }

            // 代表该arc刚好是某个input的结尾
            if (arc.isFinal) {
                flags += BIT_FINAL_ARC;
                // 代表该节点上 存储了权重信息 比如 do15 dog 2 那么权重差值13 就会存储在o的节点上 用于还原do的权重信息
                // 比如 do2 dog15 那么 o 后面的节点final 也是true 但是这时就不需要额外的权重了
                if (arc.nextFinalOutput != NO_OUTPUT) {
                    flags += BIT_ARC_HAS_FINAL_OUTPUT;
                }
            } else {
                // 如果下个节点不是 final 节点 那么该值必然不会被设置
                assert arc.nextFinalOutput == NO_OUTPUT;
            }

            // 代表下游节点有效  并不是空节点    返回的是存储在ByteStore的地址   如果整条链路中最后一个arc 它的target就是 <= 0
            boolean targetHasArcs = target.node > 0;

            // 代表此时到达某条链路的结尾
            if (!targetHasArcs) {
                flags += BIT_STOP_NODE;
            }

            // 代表当前arc 有权重信息
            if (arc.output != NO_OUTPUT) {
                flags += BIT_ARC_HAS_OUTPUT;
            }

            // 将标识位信息 先写入到 BytesStore中
            fstCompiler.bytes.writeByte((byte) flags);
            // 此时写入的位置已经更新了
            long labelStart = fstCompiler.bytes.getPosition();
            // 将 ascii信息写入
            writeLabel(fstCompiler.bytes, arc.label);
            // 计算数据的长度信息
            int numLabelBytes = (int) (fstCompiler.bytes.getPosition() - labelStart);

            // System.out.println("  write arc: label=" + (char) arc.label + " flags=" + flags + " target=" + target.node + " pos=" + bytes.getPosition() + " output=" + outputs.outputToString(arc.output));

            // 如果携带了权重信息 那么将权重信息写入
            if (arc.output != NO_OUTPUT) {
                // outputs.write 会先将数据的长度信息写入 之后才写入数据 因为有可能权重会超过一个byte才能表示
                outputs.write(arc.output, fstCompiler.bytes);
                //System.out.println("    write output");
            }

            // 存储节点上的权重信息
            if (arc.nextFinalOutput != NO_OUTPUT) {
                //System.out.println("    write final output");
                outputs.writeFinalOutput(arc.nextFinalOutput, fstCompiler.bytes);
            }

            // 代表子节点不为空节点    并且上一个冻结的节点 不是本次指向的节点
            if (targetHasArcs && (flags & BIT_TARGET_NEXT) == 0) {
                assert target.node > 0;
                //System.out.println("    write target");
                // 将子节点的地址写入
                fstCompiler.bytes.writeVLong(target.node);
            }

            // just write the arcs "like normal" on first pass, but record how many bytes each one took
            // and max byte size:
            // TODO 先忽略 固定长度 arc的写入
            if (doFixedLengthArcs) {
                // 记录本次被遍历的 arc 的长度
                int numArcBytes = (int) (fstCompiler.bytes.getPosition() - lastArcStart);
                // 该数组用于暂存长度  注意是按照 arc的下标来存的
                fstCompiler.numBytesPerArc[arcIdx] = numArcBytes;
                fstCompiler.numLabelBytesPerArc[arcIdx] = numLabelBytes;
                lastArcStart = fstCompiler.bytes.getPosition();
                // 记录最长的 arc长度
                maxBytesPerArc = Math.max(maxBytesPerArc, numArcBytes);
                maxBytesPerArcWithoutLabel = Math.max(maxBytesPerArcWithoutLabel, numArcBytes - numLabelBytes);
                //System.out.println("    arcBytes=" + numArcBytes + " labelBytes=" + numLabelBytes);
            }
        }

        // TODO: try to avoid wasteful cases: disable doFixedLengthArcs in that case
    /* 
     * 
     * LUCENE-4682: what is a fair heuristic here?
     * It could involve some of these:
     * 1. how "busy" the node is: nodeIn.inputCount relative to frontier[0].inputCount?
     * 2. how much binSearch saves over scan: nodeIn.numArcs
     * 3. waste: numBytes vs numBytesExpanded
     * 
     * the one below just looks at #3
    if (doFixedLengthArcs) {
      // rough heuristic: make this 1.25 "waste factor" a parameter to the phd ctor????
      int numBytes = lastArcStart - startAddress;
      int numBytesExpanded = maxBytesPerArc * nodeIn.numArcs;
      if (numBytesExpanded > numBytes*1.25) {
        doFixedLengthArcs = false;
      }
    }
    */


        // TODO 忽略固定长度
        if (doFixedLengthArcs) {
            assert maxBytesPerArc > 0;
            // 2nd pass just "expands" all arcs to take up a fixed byte size

            // 同一个节点上 最小的 label 与最大的 label 之间的跨度
            int labelRange = nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label + 1;
            assert labelRange > 0;
            // 如果有很多 arc 那么不方便将每个arc都往后移 这样比较耗时  TODO  先忽略该情况
            if (shouldExpandNodeWithDirectAddressing(fstCompiler, nodeIn, maxBytesPerArc, maxBytesPerArcWithoutLabel, labelRange)) {
                writeNodeForDirectAddressing(fstCompiler, nodeIn, startAddress, maxBytesPerArcWithoutLabel, labelRange);
                fstCompiler.directAddressingNodeCount++;
            // 使用二分查找提高效率
            } else {
                // 这个 startAddress 就是一开始bytes的position
                writeNodeForBinarySearch(fstCompiler, nodeIn, startAddress, maxBytesPerArc);
                // 增加二分查找的计数
                fstCompiler.binarySearchNodeCount++;
            }
        }

        // 这个偏移量就是当前节点的地址  可以看到是一个末尾的地址
        final long thisNodeAddress = fstCompiler.bytes.getPosition() - 1;
        // 这里反转内部存储的数据
        fstCompiler.bytes.reverse(startAddress, thisNodeAddress);
        // 增加当前已经编译完成的节点数
        fstCompiler.nodeCount++;
        return thisNodeAddress;
    }

    /**
     * Returns whether the given node should be expanded with fixed length arcs.
     * Nodes will be expanded depending on their depth (distance from the root node) and their number
     * of arcs.
     * <p>
     * Nodes with fixed length arcs use more space, because they encode all arcs with a fixed number
     * of bytes, but they allow either binary search or direct addressing on the arcs (instead of linear
     * scan) on lookup by arc label.
     * 判断目标节点下的arc数据是否应该以相同长度存储
     */
    private boolean shouldExpandNodeWithFixedLengthArcs(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> node) {
        // 首先需要 compiler 打开这个开关
        return fstCompiler.allowFixedLengthArcs &&
                // depth 就是 frontier 的下标     当前处理的node下标要在3以内  且arc数量超过5
                ((node.depth <= FIXED_LENGTH_ARC_SHALLOW_DEPTH && node.numArcs >= FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS) ||
                        // 如果arc数量超过10就可以无视深度
                        node.numArcs >= FIXED_LENGTH_ARC_DEEP_NUM_ARCS);
    }

    /**
     * Returns whether the given node should be expanded with direct addressing instead of binary search.
     * <p>
     * Prefer direct addressing for performance if it does not oversize binary search byte size too much,
     * so that the arcs can be directly addressed by label.
     *
     * @see FSTCompiler#getDirectAddressingMaxOversizingFactor()
     * 使用要基于地址直接进行扩充
     */
    private boolean shouldExpandNodeWithDirectAddressing(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn,
                                                         int numBytesPerArc, int maxBytesPerArcWithoutLabel, int labelRange) {
        // Anticipate precisely the size of the encodings.
        int sizeForBinarySearch = numBytesPerArc * nodeIn.numArcs;
        int sizeForDirectAddressing = getNumPresenceBytes(labelRange) + fstCompiler.numLabelBytesPerArc[0]
                + maxBytesPerArcWithoutLabel * nodeIn.numArcs;

        // Determine the allowed oversize compared to binary search.
        // This is defined by a parameter of FST Builder (default 1: no oversize).
        int allowedOversize = (int) (sizeForBinarySearch * fstCompiler.getDirectAddressingMaxOversizingFactor());
        int expansionCost = sizeForDirectAddressing - allowedOversize;

        // Select direct addressing if either:
        // - Direct addressing size is smaller than binary search.
        //   In this case, increment the credit by the reduced size (to use it later).
        // - Direct addressing size is larger than binary search, but the positive credit allows the oversizing.
        //   In this case, decrement the credit by the oversize.
        // In addition, do not try to oversize to a clearly too large node size
        // (this is the DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR parameter).
        if (expansionCost <= 0 || (fstCompiler.directAddressingExpansionCredit >= expansionCost
                && sizeForDirectAddressing <= allowedOversize * DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR)) {
            fstCompiler.directAddressingExpansionCredit -= expansionCost;
            return true;
        }
        return false;
    }

    /**
     * 基于二分查找
     *
     * @param fstCompiler
     * @param nodeIn
     * @param startAddress
     * @param maxBytesPerArc
     */
    private void writeNodeForBinarySearch(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn, long startAddress, int maxBytesPerArc) {
        // Build the header in a buffer.
        // It is a false/special arc which is in fact a node header with node flags followed by node metadata.
        fstCompiler.fixedLengthArcsBuffer
                .resetPosition()
                // 写入一个特殊标记
                .writeByte(ARCS_FOR_BINARY_SEARCH)
                // 预备写入 buffer中的arc数量
                .writeVInt(nodeIn.numArcs)
                // 写入arc的长度
                .writeVInt(maxBytesPerArc);
        // 返回当前写入的长度
        int headerLen = fstCompiler.fixedLengthArcsBuffer.getPosition();

        // Expand the arcs in place, backwards.
        long srcPos = fstCompiler.bytes.getPosition();
        // 这里定位到一个新的位置
        long destPos = startAddress + headerLen + nodeIn.numArcs * maxBytesPerArc;
        assert destPos >= srcPos;
        if (destPos > srcPos) {
            // 跳跃到 destPos的位置
            fstCompiler.bytes.skipBytes((int) (destPos - srcPos));
            // 开始从后往前倒退
            for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
                destPos -= maxBytesPerArc;
                int arcLen = fstCompiler.numBytesPerArc[arcIdx];
                srcPos -= arcLen;
                if (srcPos != destPos) {
                    assert destPos > srcPos : "destPos=" + destPos + " srcPos=" + srcPos + " arcIdx=" + arcIdx + " maxBytesPerArc=" + maxBytesPerArc + " arcLen=" + arcLen + " nodeIn.numArcs=" + nodeIn.numArcs;
                    fstCompiler.bytes.copyBytes(srcPos, destPos, arcLen);
                }
            }
        }

        // Write the header.
        // 从最初的位置开始 写入头部数据
        fstCompiler.bytes.writeBytes(startAddress, fstCompiler.fixedLengthArcsBuffer.getBytes(), 0, headerLen);
    }

    private void writeNodeForDirectAddressing(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn, long startAddress, int maxBytesPerArcWithoutLabel, int labelRange) {
        // Expand the arcs backwards in a buffer because we remove the labels.
        // So the obtained arcs might occupy less space. This is the reason why this
        // whole method is more complex.
        // Drop the label bytes since we can infer the label based on the arc index,
        // the presence bits, and the first label. Keep the first label.
        int headerMaxLen = 11;
        int numPresenceBytes = getNumPresenceBytes(labelRange);
        long srcPos = fstCompiler.bytes.getPosition();
        int totalArcBytes = fstCompiler.numLabelBytesPerArc[0] + nodeIn.numArcs * maxBytesPerArcWithoutLabel;
        int bufferOffset = headerMaxLen + numPresenceBytes + totalArcBytes;
        byte[] buffer = fstCompiler.fixedLengthArcsBuffer.ensureCapacity(bufferOffset).getBytes();
        // Copy the arcs to the buffer, dropping all labels except first one.
        for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
            bufferOffset -= maxBytesPerArcWithoutLabel;
            int srcArcLen = fstCompiler.numBytesPerArc[arcIdx];
            srcPos -= srcArcLen;
            int labelLen = fstCompiler.numLabelBytesPerArc[arcIdx];
            // Copy the flags.
            fstCompiler.bytes.copyBytes(srcPos, buffer, bufferOffset, 1);
            // Skip the label, copy the remaining.
            int remainingArcLen = srcArcLen - 1 - labelLen;
            if (remainingArcLen != 0) {
                fstCompiler.bytes.copyBytes(srcPos + 1 + labelLen, buffer, bufferOffset + 1, remainingArcLen);
            }
            if (arcIdx == 0) {
                // Copy the label of the first arc only.
                bufferOffset -= labelLen;
                fstCompiler.bytes.copyBytes(srcPos + 1, buffer, bufferOffset, labelLen);
            }
        }
        assert bufferOffset == headerMaxLen + numPresenceBytes;

        // Build the header in the buffer.
        // It is a false/special arc which is in fact a node header with node flags followed by node metadata.
        fstCompiler.fixedLengthArcsBuffer
                .resetPosition()
                .writeByte(ARCS_FOR_DIRECT_ADDRESSING)
                .writeVInt(labelRange) // labelRange instead of numArcs.
                .writeVInt(maxBytesPerArcWithoutLabel); // maxBytesPerArcWithoutLabel instead of maxBytesPerArc.
        int headerLen = fstCompiler.fixedLengthArcsBuffer.getPosition();

        // Prepare the builder byte store. Enlarge or truncate if needed.
        long nodeEnd = startAddress + headerLen + numPresenceBytes + totalArcBytes;
        long currentPosition = fstCompiler.bytes.getPosition();
        if (nodeEnd >= currentPosition) {
            fstCompiler.bytes.skipBytes((int) (nodeEnd - currentPosition));
        } else {
            fstCompiler.bytes.truncate(nodeEnd);
        }
        assert fstCompiler.bytes.getPosition() == nodeEnd;

        // Write the header.
        long writeOffset = startAddress;
        fstCompiler.bytes.writeBytes(writeOffset, fstCompiler.fixedLengthArcsBuffer.getBytes(), 0, headerLen);
        writeOffset += headerLen;

        // Write the presence bits
        writePresenceBits(fstCompiler, nodeIn, writeOffset, numPresenceBytes);
        writeOffset += numPresenceBytes;

        // Write the first label and the arcs.
        fstCompiler.bytes.writeBytes(writeOffset, fstCompiler.fixedLengthArcsBuffer.getBytes(), bufferOffset, totalArcBytes);
    }

    private void writePresenceBits(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn, long dest, int numPresenceBytes) {
        long bytePos = dest;
        byte presenceBits = 1; // The first arc is always present.
        int presenceIndex = 0;
        int previousLabel = nodeIn.arcs[0].label;
        for (int arcIdx = 1; arcIdx < nodeIn.numArcs; arcIdx++) {
            int label = nodeIn.arcs[arcIdx].label;
            assert label > previousLabel;
            presenceIndex += label - previousLabel;
            while (presenceIndex >= Byte.SIZE) {
                fstCompiler.bytes.writeByte(bytePos++, presenceBits);
                presenceBits = 0;
                presenceIndex -= Byte.SIZE;
            }
            // Set the bit at presenceIndex to flag that the corresponding arc is present.
            presenceBits |= 1 << presenceIndex;
            previousLabel = label;
        }
        assert presenceIndex == (nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label) % 8;
        assert presenceBits != 0; // The last byte is not 0.
        assert (presenceBits & (1 << presenceIndex)) != 0; // The last arc is always present.
        fstCompiler.bytes.writeByte(bytePos++, presenceBits);
        assert bytePos - dest == numPresenceBytes;
    }

    /**
     * Gets the number of bytes required to flag the presence of each arc in the given label range, one bit per arc.
     */
    private static int getNumPresenceBytes(int labelRange) {
        assert labelRange >= 0;
        return (labelRange + 7) >> 3;
    }

    /**
     * Reads the presence bits of a direct-addressing node.
     * Actually we don't read them here, we just keep the pointer to the bit-table start and we skip them.
     */
    private void readPresenceBytes(Arc<T> arc, BytesReader in) throws IOException {
        assert arc.bytesPerArc() > 0;
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        arc.bitTableStart = in.getPosition();
        in.skipBytes(getNumPresenceBytes(arc.numArcs()));
    }

    /**
     * Fills virtual 'start' arc, ie, an empty incoming arc to the FST's start node
     * 读取第一个arc的数据  这是一个startArc 并没有存储实际数据 他指向编译完成的fst的 最后一个arc的地址
     */
    public Arc<T> getFirstArc(Arc<T> arc) {
        T NO_OUTPUT = outputs.getNoOutput();

        // TODO
        if (emptyOutput != null) {
            arc.flags = BIT_FINAL_ARC | BIT_LAST_ARC;
            arc.nextFinalOutput = emptyOutput;
            // 当某arc  isFinal == false  时  且nextFinalOutput不为空  在flag中追加 BIT_ARC_HAS_FINAL_OUTPUT
            if (emptyOutput != NO_OUTPUT) {
                arc.flags = (byte) (arc.flags() | BIT_ARC_HAS_FINAL_OUTPUT);
            }
        } else {
            // 正常情况下 首个初始化的arc 会被标记成 arc[]的最后一个   同时output 和nextOutput 都是 NO_OUTPUT
            arc.flags = BIT_LAST_ARC;
            arc.nextFinalOutput = NO_OUTPUT;
        }
        arc.output = NO_OUTPUT;

        // If there are no nodes, ie, the FST only accepts the
        // empty string, then startNode is 0
        // 首个arc 指向的target是 startNode
        arc.target = startNode;
        return arc;
    }

    /**
     * Follows the <code>follow</code> arc and reads the last
     * arc of its target; this changes the provided
     * <code>arc</code> (2nd arg) in-place and returns it.
     *
     * @return Returns the second argument
     * (<code>arc</code>).
     * 读取follow下游 finalArc 的信息 并填充到arc中
     */
    Arc<T> readLastTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
        //System.out.println("readLast");
        // 如果下游没有arc
        if (!targetHasArcs(follow)) {
            //System.out.println("  end node");
            // 创建一个 FSTEnum的终止arc
            assert follow.isFinal();
            arc.label = END_LABEL;
            arc.target = FINAL_END_NODE;
            arc.output = follow.nextFinalOutput();
            arc.flags = BIT_LAST_ARC;
            arc.nodeFlags = arc.flags;
            return arc;
        } else {
            // 存在的话 从输入流中定位到对应的节点
            in.setPosition(follow.target());
            // 读取数据并设置到 入参arc上
            byte flags = arc.nodeFlags = in.readByte();
            // TODO 忽略等长arc
            if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
                // Special arc which is actually a node header for fixed length arcs.
                // Jump straight to end to find the last arc.
                arc.numArcs = in.readVInt();
                arc.bytesPerArc = in.readVInt();
                //System.out.println("  array numArcs=" + arc.numArcs + " bpa=" + arc.bytesPerArc);
                if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
                    readPresenceBytes(arc, in);
                    arc.firstLabel = readLabel(in);
                    arc.posArcsStart = in.getPosition();
                    readLastArcByDirectAddressing(arc, in);
                } else {
                    arc.arcIdx = arc.numArcs() - 2;
                    arc.posArcsStart = in.getPosition();
                    readNextRealArc(arc, in);
                }
            } else {
                arc.flags = flags;
                // non-array: linear scan
                arc.bytesPerArc = 0;
                //System.out.println("  scan");
                // 只要此时 arc不是last 就切换到同一个node下的下一个arc  也就是连续读取  因为同一个节点下的多个arc是连续存储的
                while (!arc.isLast()) {
                    // skip this arc:
                    readLabel(in);
                    if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
                        outputs.skipOutput(in);
                    }
                    if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
                        outputs.skipFinalOutput(in);
                    }
                    if (arc.flag(BIT_STOP_NODE)) {
                    } else if (arc.flag(BIT_TARGET_NEXT)) {
                    } else {
                        // 应该就是target 是一个long值 且 BIT_TARGET_NEXT 为0
                        readUnpackedNodeTarget(in);
                    }
                    // 读取下一个arc的flags
                    arc.flags = in.readByte();
                }
                // Undo the byte flags we read:
                // 定位当前 flag 并读取arc数据
                in.skipBytes(-1);
                arc.nextArc = in.getPosition();
                readNextRealArc(arc, in);
            }
            assert arc.isLast();
            return arc;
        }
    }

    private long readUnpackedNodeTarget(BytesReader in) throws IOException {
        return in.readVLong();
    }

    /**
     * Follow the <code>follow</code> arc and read the first arc of its target;
     * this changes the provided <code>arc</code> (2nd arg) in-place and returns
     * it.
     *
     * @param follow  上一个arc的数据
     * @param arc    通过上一个arc读取到的数据会被填充到该arc内
     * @param in  用于反向读取fst内部已经编译完成的数据
     * @return Returns the second argument (<code>arc</code>).
     */
    public Arc<T> readFirstTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
        //int pos = address;
        //System.out.println("    readFirstTarget follow.target=" + follow.target + " isFinal=" + follow.isFinal());
        // 代表此时arc是某个input的结尾
        if (follow.isFinal()) {
            // Insert "fake" final first arc:
            // 这时就返回一个 特殊的arc提示已经读取完毕
            arc.label = END_LABEL;
            // 将follow原本连接的下一个节点的output回填到 arc上  此时计算总的output就是这些arc.output的总和
            arc.output = follow.nextFinalOutput();
            // 标记该arc代表了某个input的结尾
            arc.flags = BIT_FINAL_ARC;
            //  当出现比如 at/2  att/5 这种情况时  在迭代的时候会优先读取 at 之后读取 att 因为写入是反向的 tta 读取的时候会先发现第二个t上有终止符 就先获取了 at作为结果

            // 在addNode 中传入某个下游arc为0的node时 会返回 0/-1 作为上个arc的 target 也就代表走到链尾
            // 原本只有当该arc 是某个节点下最后一个arc时 才在addNode方法中设置该标识的
            // 对应 att的情况
            if (follow.target() <= 0) {
                arc.flags |= BIT_LAST_ARC;
            } else {
                // NOTE: nextArc is a node (not an address!) in this case:
                // 代表还未走到当前arc链的链尾  但是中途遇到了终止的节点  对应at 这时 就会设置最后一个t的地址
                arc.nextArc = follow.target();
            }
            // 因为本节点已经结束了 所以target 填了 一个 空值
            arc.target = FINAL_END_NODE;
            arc.nodeFlags = arc.flags;
            //System.out.println("    insert isFinal; nextArc=" + follow.target + " isLast=" + arc.isLast() + " output=" + outputs.outputToString(arc.output));
            return arc;
        } else {
            // 该方法总是以 target作为下一个读取的arc 与 readNext不同
            // 根据follow的信息读取下一个arc数据  in是负责反向读取 编译完成的 FST的输入流
            return readFirstRealTargetArc(follow.target(), arc, in);
        }
    }

    /**
     * @param nodeAddress
     * @param arc
     * @param in
     * @return
     * @throws IOException
     */
    public Arc<T> readFirstRealTargetArc(long nodeAddress, Arc<T> arc, final BytesReader in) throws IOException {
        // 定位到目标位置  如果此时 arc是 firstArc 此时指向的地址就是 最后一个写入的arc对应的flag的下标
        in.setPosition(nodeAddress);
        //System.out.println("   flags=" + arc.flags);

        // 读取flag信息
        byte flags = arc.nodeFlags = in.readByte();
        // TODO 只有当 arc以等长形式写入时 才会设置这2个值 先忽略
        if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
            //System.out.println("  fixed length arc");
            // Special arc which is actually a node header for fixed length arcs.
            arc.numArcs = in.readVInt();
            arc.bytesPerArc = in.readVInt();
            arc.arcIdx = -1;
            if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
                readPresenceBytes(arc, in);
                arc.firstLabel = readLabel(in);
                arc.presenceIndex = -1;
            }
            arc.posArcsStart = in.getPosition();
            //System.out.println("  bytesPer=" + arc.bytesPerArc + " numArcs=" + arc.numArcs + " arcsStart=" + pos);
        } else {
            // arc.nextArc 是用来定位自身 flag位置的么
            arc.nextArc = nodeAddress;
            arc.bytesPerArc = 0;
        }

        return readNextRealArc(arc, in);
    }

    /**
     * Returns whether <code>arc</code>'s target points to a node in expanded format (fixed length arcs).
     */
    boolean isExpandedTarget(Arc<T> follow, BytesReader in) throws IOException {
        if (!targetHasArcs(follow)) {
            return false;
        } else {
            in.setPosition(follow.target());
            byte flags = in.readByte();
            return flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING;
        }
    }

    /**
     * In-place read; returns the arc.
     * 将arc的数据替换成同一上游节点上 下一个arc的数据
     */
    public Arc<T> readNextArc(Arc<T> arc, BytesReader in) throws IOException {
        // 代表本次传入的是终止arc(或者说 fake arc)  能够进入到这里时 nextArc 就对应下个arc边的flag位置
        if (arc.label() == END_LABEL) {
            // This was a fake inserted "final" arc
            if (arc.nextArc() <= 0) {
                throw new IllegalArgumentException("cannot readNextArc when arc.isLast()=true");
            }
            // 根据flag位置读取arc的数据覆盖回传入的终止arc上
            return readFirstRealTargetArc(arc.nextArc(), arc, in);
        } else {
            // 传入某条 非 isLast 的边 某个节点下 isLast = false的arc数据是连续的 他们的下游arc数据才是通过target进行跳跃
            // 核心就是将 arc.nextArc 作为下个arc的地址
            return readNextRealArc(arc, in);
        }
    }

    /**
     * Peeks at next arc's label; does not alter arc.  Do
     * not call this if arc.isLast()!
     */
    int readNextArcLabel(Arc<T> arc, BytesReader in) throws IOException {
        assert !arc.isLast();

        if (arc.label() == END_LABEL) {
            //System.out.println("    nextArc fake " + arc.nextArc);
            // Next arc is the first arc of a node.
            // Position to read the first arc label.

            in.setPosition(arc.nextArc());
            byte flags = in.readByte();
            if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
                //System.out.println("    nextArc fixed length arc");
                // Special arc which is actually a node header for fixed length arcs.
                int numArcs = in.readVInt();
                in.readVInt(); // Skip bytesPerArc.
                if (flags == ARCS_FOR_BINARY_SEARCH) {
                    in.readByte(); // Skip arc flags.
                } else {
                    in.skipBytes(getNumPresenceBytes(numArcs));
                }
            }
        } else {
            if (arc.bytesPerArc() != 0) {
                //System.out.println("    nextArc real array");
                // Arcs have fixed length.
                if (arc.nodeFlags() == ARCS_FOR_BINARY_SEARCH) {
                    // Point to next arc, -1 to skip arc flags.
                    in.setPosition(arc.posArcsStart() - (1 + arc.arcIdx()) * arc.bytesPerArc() - 1);
                } else {
                    assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
                    // Direct addressing node. The label is not stored but rather inferred
                    // based on first label and arc index in the range.
                    assert BitTable.assertIsValid(arc, in);
                    assert BitTable.isBitSet(arc.arcIdx(), arc, in);
                    int nextIndex = BitTable.nextBitSet(arc.arcIdx(), arc, in);
                    assert nextIndex != -1;
                    return arc.firstLabel() + nextIndex;
                }
            } else {
                // Arcs have variable length.
                //System.out.println("    nextArc real list");
                // Position to next arc, -1 to skip flags.
                in.setPosition(arc.nextArc() - 1);
            }
        }
        return readLabel(in);
    }

    public Arc<T> readArcByIndex(Arc<T> arc, final BytesReader in, int idx) throws IOException {
        assert arc.bytesPerArc() > 0;
        assert arc.nodeFlags() == ARCS_FOR_BINARY_SEARCH;
        assert idx >= 0 && idx < arc.numArcs();
        in.setPosition(arc.posArcsStart() - idx * arc.bytesPerArc());
        arc.arcIdx = idx;
        arc.flags = in.readByte();
        return readArc(arc, in);
    }

    /**
     * Reads a present direct addressing node arc, with the provided index in the label range.
     *
     * @param rangeIndex The index of the arc in the label range. It must be present.
     *                   The real arc offset is computed based on the presence bits of
     *                   the direct addressing node.
     */
    public Arc<T> readArcByDirectAddressing(Arc<T> arc, final BytesReader in, int rangeIndex) throws IOException {
        assert BitTable.assertIsValid(arc, in);
        assert rangeIndex >= 0 && rangeIndex < arc.numArcs();
        assert BitTable.isBitSet(rangeIndex, arc, in);
        int presenceIndex = BitTable.countBitsUpTo(rangeIndex, arc, in);
        return readArcByDirectAddressing(arc, in, rangeIndex, presenceIndex);
    }

    /**
     * Reads a present direct addressing node arc, with the provided index in the label range and its corresponding
     * presence index (which is the count of presence bits before it).
     */
    private Arc<T> readArcByDirectAddressing(Arc<T> arc, final BytesReader in, int rangeIndex, int presenceIndex) throws IOException {
        in.setPosition(arc.posArcsStart() - presenceIndex * arc.bytesPerArc());
        arc.arcIdx = rangeIndex;
        arc.presenceIndex = presenceIndex;
        arc.flags = in.readByte();
        return readArc(arc, in);
    }

    /**
     * Reads the last arc of a direct addressing node.
     * This method is equivalent to call {@link #readArcByDirectAddressing(Arc, BytesReader, int)} with {@code rangeIndex}
     * equal to {@code arc.numArcs() - 1}, but it is faster.
     */
    public Arc<T> readLastArcByDirectAddressing(Arc<T> arc, final BytesReader in) throws IOException {
        assert BitTable.assertIsValid(arc, in);
        int presenceIndex = BitTable.countBits(arc, in) - 1;
        return readArcByDirectAddressing(arc, in, arc.numArcs() - 1, presenceIndex);
    }

    /**
     * Never returns null, but you should never call this if
     * arc.isLast() is true.
     * 根据此时arc的描述信息 读取数据并填充
     */
    public Arc<T> readNextRealArc(Arc<T> arc, final BytesReader in) throws IOException {

        // TODO: can't assert this because we call from readFirstArc
        // assert !flag(arc.flags, BIT_LAST_ARC);

        // 解析flag信息
        switch (arc.nodeFlags()) {

            case ARCS_FOR_BINARY_SEARCH:
                assert arc.bytesPerArc() > 0;
                arc.arcIdx++;
                assert arc.arcIdx() >= 0 && arc.arcIdx() < arc.numArcs();
                in.setPosition(arc.posArcsStart() - arc.arcIdx() * arc.bytesPerArc());
                arc.flags = in.readByte();
                break;

            case ARCS_FOR_DIRECT_ADDRESSING:
                assert BitTable.assertIsValid(arc, in);
                assert arc.arcIdx() == -1 || BitTable.isBitSet(arc.arcIdx(), arc, in);
                int nextIndex = BitTable.nextBitSet(arc.arcIdx(), arc, in);
                return readArcByDirectAddressing(arc, in, nextIndex, arc.presenceIndex + 1);

                // 先忽略上面的2种情况 他们是为了快捷遍历某个node下所有arc的 对于功能实现本身没有影响
            default:
                // Variable length arcs - linear search.
                assert arc.bytesPerArc() == 0;
                // 还原flag信息 因为这种情况下 nextArc 存储的还是当前arc对应的flag下标
                in.setPosition(arc.nextArc());
                arc.flags = in.readByte();
        }
        return readArc(arc, in);
    }

    /**
     * Reads an arc.
     * <br>Precondition: The arc flags byte has already been read and set;
     * the given BytesReader is positioned just after the arc flags byte.
     * 这里不需要考虑是否使用了 二分查找 或者 directAddress 查找
     */
    private Arc<T> readArc(Arc<T> arc, BytesReader in) throws IOException {
        if (arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING) {
            arc.label = arc.firstLabel() + arc.arcIdx();
        } else {
            // 继 flag后下一个值就是字面量
            arc.label = readLabel(in);
        }

        // 如果有携带 output 继续读取
        if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
            arc.output = outputs.read(in);
        } else {
            // 否则设置一个空值
            arc.output = outputs.getNoOutput();
        }

        // 代表此时arc刚好是某个词的结束位置
        if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
            arc.nextFinalOutput = outputs.readFinalOutput(in);
        } else {
            arc.nextFinalOutput = outputs.getNoOutput();
        }

        // BIT_STOP_NODE 代表此时走到了某条arc链的尾部
        if (arc.flag(BIT_STOP_NODE)) {
            // 代表此时是某个input的终止位置
            if (arc.flag(BIT_FINAL_ARC)) {
                arc.target = FINAL_END_NODE;
            } else {
                // 虽然到了end 但是该input还没有结束 这种情况现在还没发现
                arc.target = NON_FINAL_END_NODE;
            }
            // 此时的nextArc 对应该arc的末尾位置  继续读取byte 就可以得到在 bytesStore中直接相连的下个arc.flag数据
            // 注意 readNextRealArc 就是通过nextArc 来读取当前非isLast arc的下一条arc的
            arc.nextArc = in.getPosition(); // Only useful for list.
        // 如果携带了该标识 代表arc没有存储target信息  直接连接到下个arc.flag
        } else if (arc.flag(BIT_TARGET_NEXT)) {
            // 同上
            arc.nextArc = in.getPosition(); // Only useful for list.
            // TODO: would be nice to make this lazy -- maybe
            // caller doesn't need the target and is scanning arcs...
            // 目前BIT_TARGET_NEXT 应该与 BIT_LAST_ARC 是一起设置的  只有当此时处理的arc是该node的最后一个arc 它的上一个arc才会与它相邻存储  可能固定长度存储的arc做了特殊处理吧 先忽略
            if (!arc.flag(BIT_LAST_ARC)) {
                if (arc.bytesPerArc() == 0) {
                    // must scan
                    seekToNextNode(in);
                } else {
                    int numArcs = arc.nodeFlags == ARCS_FOR_DIRECT_ADDRESSING ? BitTable.countBits(arc, in) : arc.numArcs();
                    in.setPosition(arc.posArcsStart() - arc.bytesPerArc() * numArcs);
                }
            }
            // 还是会设置target值  使用方式就是先通过 in.setPosition(target) 定位后 再读取数据 就是flag信息
            arc.target = in.getPosition();
        } else {
            // 就是读取一个long值 该long就是下一个target的起点位置
            arc.target = readUnpackedNodeTarget(in);
            arc.nextArc = in.getPosition(); // Only useful for list.
        }
        return arc;
    }

    static <T> Arc<T> readEndArc(Arc<T> follow, Arc<T> arc) {
        if (follow.isFinal()) {
            if (follow.target() <= 0) {
                arc.flags = FST.BIT_LAST_ARC;
            } else {
                arc.flags = 0;
                // NOTE: nextArc is a node (not an address!) in this case:
                arc.nextArc = follow.target();
            }
            arc.output = follow.nextFinalOutput();
            arc.label = FST.END_LABEL;
            return arc;
        } else {
            return null;
        }
    }

    // TODO: could we somehow [partially] tableize arc lookups
    // like automaton?

    /**
     * Finds an arc leaving the incoming arc, replacing the arc in place.
     * This returns null if the arc was not found, else the incoming arc.
     * @param follow 上游arc  从它出发
     * 找到与 label 匹配的arc 并将属性转移到  入参arc上
     */
    public Arc<T> findTargetArc(int labelToMatch, Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {

        // 代表已经读取到末尾了
        if (labelToMatch == END_LABEL) {
            // 代表当 label到末尾时 arc 刚好也到了末尾 也就是完全匹配 这时返回终止节点
            if (follow.isFinal()) {
                if (follow.target() <= 0) {
                    arc.flags = BIT_LAST_ARC;
                } else {
                    arc.flags = 0;
                    // NOTE: nextArc is a node (not an address!) in this case:
                    // 对应 at att的情况 这时找到了 at 但是还没有走完链路 所以存储了通往最后一个t的地址  该终止arc确保了 FSTEnum的 doNext可以正常工作
                    arc.nextArc = follow.target();
                }
                arc.output = follow.nextFinalOutput();
                arc.label = END_LABEL;
                arc.nodeFlags = arc.flags;
                return arc;
            } else {
                // 代表此时arc还没有结束 也就是没有完全匹配label 所以返回null
                return null;
            }
        }

        // 没有下游arc了 返回null 代表找不到符合条件的arc
        if (!targetHasArcs(follow)) {
            return null;
        }

        in.setPosition(follow.target());

        // System.out.println("fta label=" + (char) labelToMatch);

        // TODO 忽略等长arc的逻辑
        byte flags = arc.nodeFlags = in.readByte();
        if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
            arc.numArcs = in.readVInt(); // This is in fact the label range.
            arc.bytesPerArc = in.readVInt();
            readPresenceBytes(arc, in);
            arc.firstLabel = readLabel(in);
            arc.posArcsStart = in.getPosition();

            int arcIndex = labelToMatch - arc.firstLabel();
            if (arcIndex < 0 || arcIndex >= arc.numArcs()) {
                return null; // Before or after label range.
            } else if (!BitTable.isBitSet(arcIndex, arc, in)) {
                return null; // Arc missing in the range.
            }
            return readArcByDirectAddressing(arc, in, arcIndex);
        } else if (flags == ARCS_FOR_BINARY_SEARCH) {
            arc.numArcs = in.readVInt();
            arc.bytesPerArc = in.readVInt();
            arc.posArcsStart = in.getPosition();

            // Array is sparse; do binary search:
            int low = 0;
            int high = arc.numArcs() - 1;
            while (low <= high) {
                //System.out.println("    cycle");
                int mid = (low + high) >>> 1;
                // +1 to skip over flags
                in.setPosition(arc.posArcsStart() - (arc.bytesPerArc() * mid + 1));
                int midLabel = readLabel(in);
                final int cmp = midLabel - labelToMatch;
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    arc.arcIdx = mid - 1;
                    //System.out.println("    found!");
                    return readNextRealArc(arc, in);
                }
            }
            return null;
        }

        // 忽略 等长arc的逻辑
        // Linear scan
        readFirstRealTargetArc(follow.target(), arc, in);

        while (true) {
            //System.out.println("  non-bs cycle");
            // TODO: we should fix this code to not have to create
            // object for the output of every arc we scan... only
            // for the matching arc, if found
            if (arc.label() == labelToMatch) {
                //System.out.println("    found!");
                return arc;
            } else if (arc.label() > labelToMatch) {
                return null;
            } else if (arc.isLast()) {
                return null;
            } else {
                readNextRealArc(arc, in);
            }
        }
    }

    private void seekToNextNode(BytesReader in) throws IOException {

        while (true) {

            final int flags = in.readByte();
            readLabel(in);

            if (flag(flags, BIT_ARC_HAS_OUTPUT)) {
                outputs.skipOutput(in);
            }

            if (flag(flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
                outputs.skipFinalOutput(in);
            }

            if (!flag(flags, BIT_STOP_NODE) && !flag(flags, BIT_TARGET_NEXT)) {
                readUnpackedNodeTarget(in);
            }

            if (flag(flags, BIT_LAST_ARC)) {
                return;
            }
        }
    }

    /**
     * Returns a {@link BytesReader} for this FST, positioned at
     * position 0.
     * FST 有2种初始化方式 一种根据 BytesStore 一种根据  FSTStore
     */
    public BytesReader getBytesReader() {
        if (this.fstStore != null) {
            return this.fstStore.getReverseBytesReader();
        } else {
            return bytes.getReverseReader();
        }
    }

    /**
     * Reads bytes stored in an FST.
     * 该对象负责从 FST 中读取数据   DataInput 定义了一系列模板 只有readBytes 交由子类实现
     */
    public static abstract class BytesReader extends DataInput {
        /**
         * Get current read position.
         */
        public abstract long getPosition();

        /**
         * Set current read position.
         */
        public abstract void setPosition(long pos);

        /**
         * Returns true if this reader uses reversed bytes
         * under-the-hood. 判断数据是否采用反向读取的方式
         */
        public abstract boolean reversed();
    }
}
