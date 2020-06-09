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


import java.io.IOException;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST.INPUT_TYPE; // javadoc

// TODO: could we somehow stream an FST to disk while we
// build it?

/**
 * Builds a minimal FST (maps an IntsRef term to an arbitrary
 * output) from pre-sorted terms with outputs.  The FST
 * becomes an FSA if you use NoOutputs.  The FST is written
 * on-the-fly into a compact serialized format byte array, which can
 * be saved to / loaded from a Directory or used directly
 * for traversal.  The FST is always finite (no cycles).
 *
 * <p>NOTE: The algorithm is described at
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698</p>
 *
 * <p>The parameterized type T is the output type.  See the
 * subclasses of {@link Outputs}.
 *
 * <p>FSTs larger than 2.1GB are now possible (as of Lucene
 * 4.2).  FSTs containing more than 2.1B nodes are also now
 * possible, however they cannot be packed.
 *
 * @lucene.experimental 用于构建 FST 的核心类
 */

public class FSTCompiler<T> {

    static final float DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR = 1f;

    /**
     * 该对象存储节点的hash值 主要是复用节点 以节省内存
     */
    private final NodeHash<T> dedupHash;
    /**
     * 该对象最终会构建出 fst对象
     */
    final FST<T> fst;
    private final T NO_OUTPUT;

    // private static final boolean DEBUG = true;

    // simplistic pruning: we prune node (and all following
    // nodes) if less than this number of terms go through it:
    private final int minSuffixCount1;

    // better pruning: we prune node (and all following
    // nodes) if the prior node has less than this number of
    // terms go through it:
    private final int minSuffixCount2;

    private final boolean doShareNonSingletonNodes;
    private final int shareMaxTailLength;

    private final IntsRefBuilder lastInput = new IntsRefBuilder();

    // NOTE: cutting this over to ArrayList instead loses ~6%
    // in build performance on 9.8M Wikipedia terms; so we
    // left this as an array:
    // current "frontier"
    // 每次插入数据时 都会被封装成 UnCompiledNode  每当确定前面的数据不会改变时 会调用freeze 将node固化
    // Compiler 被创建时 使用10个对象填充该数组
    // frontier[0] 是根节点 此时还没有任何数据  当插入第一个数据时 根节点会往下延申 通过 arc关联到下一个节点
    private UnCompiledNode<T>[] frontier;

    // Used for the BIT_TARGET_NEXT optimization (whereby
    // instead of storing the address of the target node for
    // a given arc, we mark a single bit noting that the next
    // node in the byte[] is the target node):
    // 代表上一个冻结的节点
    long lastFrozenNode;

    // Reused temporarily while building the FST:
    int[] numBytesPerArc = new int[4];
    int[] numLabelBytesPerArc = new int[numBytesPerArc.length];
    final FixedLengthArcsBuffer fixedLengthArcsBuffer = new FixedLengthArcsBuffer();

    long arcCount;
    long nodeCount;
    long binarySearchNodeCount;
    long directAddressingNodeCount;

    final boolean allowFixedLengthArcs;
    final float directAddressingMaxOversizingFactor;
    long directAddressingExpansionCredit;

    /**
     * 在该对象中包含一个 current[]数组
     */
    final BytesStore bytes;

    /**
     * Instantiates an FST/FSA builder with default settings and pruning options turned off.
     * For more tuning and tweaking, see {@link Builder}.
     */
    public FSTCompiler(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
        this(inputType, 0, 0, true, true, Integer.MAX_VALUE, outputs, true, 15, 1f);
    }

    private FSTCompiler(FST.INPUT_TYPE inputType, int minSuffixCount1, int minSuffixCount2, boolean doShareSuffix,
                        boolean doShareNonSingletonNodes, int shareMaxTailLength, Outputs<T> outputs,
                        boolean allowFixedLengthArcs, int bytesPageBits, float directAddressingMaxOversizingFactor) {
        this.minSuffixCount1 = minSuffixCount1;
        this.minSuffixCount2 = minSuffixCount2;
        this.doShareNonSingletonNodes = doShareNonSingletonNodes;
        this.shareMaxTailLength = shareMaxTailLength;
        this.allowFixedLengthArcs = allowFixedLengthArcs;
        this.directAddressingMaxOversizingFactor = directAddressingMaxOversizingFactor;
        fst = new FST<>(inputType, outputs, bytesPageBits);
        bytes = fst.bytes;
        assert bytes != null;
        // 如果支持后缀共享的话 初始化一个hash桶对象用于复用结尾对象
        if (doShareSuffix) {
            dedupHash = new NodeHash<>(fst, bytes.getReverseReader(false));
        } else {
            dedupHash = null;
        }
        // 设置空对象
        NO_OUTPUT = outputs.getNoOutput();

        // 刚创建时 开辟10个对象的空间
        @SuppressWarnings({"rawtypes", "unchecked"}) final UnCompiledNode<T>[] f =
                (UnCompiledNode<T>[]) new UnCompiledNode[10];
        frontier = f;
        for (int idx = 0; idx < frontier.length; idx++) {
            frontier[idx] = new UnCompiledNode<>(this, idx);
        }
    }

    /**
     * Fluent-style constructor for FST {@link FSTCompiler}.
     * <p>
     * Creates an FST/FSA builder with all the possible tuning and construction tweaks.
     * Read parameter documentation carefully.
     */
    public static class Builder<T> {

        private final INPUT_TYPE inputType;
        private final Outputs<T> outputs;
        private int minSuffixCount1;
        private int minSuffixCount2;
        private boolean shouldShareSuffix = true;
        private boolean shouldShareNonSingletonNodes = true;
        private int shareMaxTailLength = Integer.MAX_VALUE;
        private boolean allowFixedLengthArcs = true;
        private int bytesPageBits = 15;
        private float directAddressingMaxOversizingFactor = DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR;

        /**
         * @param inputType The input type (transition labels). Can be anything from {@link INPUT_TYPE}
         *                  enumeration. Shorter types will consume less memory. Strings (character sequences) are
         *                  represented as {@link INPUT_TYPE#BYTE4} (full unicode codepoints).
         * @param outputs   The output type for each input sequence. Applies only if building an FST. For
         *                  FSA, use {@link NoOutputs#getSingleton()} and {@link NoOutputs#getNoOutput()} as the
         *                  singleton output object.
         */
        public Builder(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
            this.inputType = inputType;
            this.outputs = outputs;
        }

        /**
         * If pruning the input graph during construction, this threshold is used for telling if a node is kept
         * or pruned. If transition_count(node) &gt;= minSuffixCount1, the node is kept.
         * <p>
         * Default = 0.
         */
        public Builder<T> minSuffixCount1(int minSuffixCount1) {
            this.minSuffixCount1 = minSuffixCount1;
            return this;
        }

        /**
         * Better pruning: we prune node (and all following nodes) if the prior node has less than this number
         * of terms go through it.
         * <p>
         * Default = 0.
         */
        public Builder<T> minSuffixCount2(int minSuffixCount2) {
            this.minSuffixCount2 = minSuffixCount2;
            return this;
        }

        /**
         * If {@code true}, the shared suffixes will be compacted into unique paths.
         * This requires an additional RAM-intensive hash map for lookups in memory. Setting this parameter to
         * {@code false} creates a single suffix path for all input sequences. This will result in a larger
         * FST, but requires substantially less memory and CPU during building.
         * <p>
         * Default = {@code true}.
         */
        public Builder<T> shouldShareSuffix(boolean shouldShareSuffix) {
            this.shouldShareSuffix = shouldShareSuffix;
            return this;
        }

        /**
         * Only used if {@code shouldShareSuffix} is true. Set this to true to ensure FST is fully minimal,
         * at cost of more CPU and more RAM during building.
         * <p>
         * Default = {@code true}.
         */
        public Builder<T> shouldShareNonSingletonNodes(boolean shouldShareNonSingletonNodes) {
            this.shouldShareNonSingletonNodes = shouldShareNonSingletonNodes;
            return this;
        }

        /**
         * Only used if {@code shouldShareSuffix} is true. Set this to Integer.MAX_VALUE to ensure FST is
         * fully minimal, at cost of more CPU and more RAM during building.
         * <p>
         * Default = {@link Integer#MAX_VALUE}.
         */
        public Builder<T> shareMaxTailLength(int shareMaxTailLength) {
            this.shareMaxTailLength = shareMaxTailLength;
            return this;
        }

        /**
         * Pass {@code false} to disable the fixed length arc optimization (binary search or direct addressing)
         * while building the FST; this will make the resulting FST smaller but slower to traverse.
         * <p>
         * Default = {@code true}.
         */
        public Builder<T> allowFixedLengthArcs(boolean allowFixedLengthArcs) {
            this.allowFixedLengthArcs = allowFixedLengthArcs;
            return this;
        }

        /**
         * How many bits wide to make each byte[] block in the BytesStore; if you know the FST
         * will be large then make this larger.  For example 15 bits = 32768 byte pages.
         * <p>
         * Default = 15.
         */
        public Builder<T> bytesPageBits(int bytesPageBits) {
            this.bytesPageBits = bytesPageBits;
            return this;
        }

        /**
         * Overrides the default the maximum oversizing of fixed array allowed to enable direct addressing
         * of arcs instead of binary search.
         * <p>
         * Setting this factor to a negative value (e.g. -1) effectively disables direct addressing,
         * only binary search nodes will be created.
         * <p>
         * This factor does not determine whether to encode a node with a list of variable length arcs or with
         * fixed length arcs. It only determines the effective encoding of a node that is already known to be
         * encoded with fixed length arcs.
         * <p>
         * Default = 1.
         */
        public Builder<T> directAddressingMaxOversizingFactor(float factor) {
            this.directAddressingMaxOversizingFactor = factor;
            return this;
        }

        /**
         * Creates a new {@link FSTCompiler}.
         */
        public FSTCompiler<T> build() {
            FSTCompiler<T> fstCompiler = new FSTCompiler<>(inputType, minSuffixCount1, minSuffixCount2, shouldShareSuffix,
                    shouldShareNonSingletonNodes, shareMaxTailLength, outputs, allowFixedLengthArcs, bytesPageBits,
                    directAddressingMaxOversizingFactor);
            return fstCompiler;
        }
    }

    public float getDirectAddressingMaxOversizingFactor() {
        return directAddressingMaxOversizingFactor;
    }

    public long getTermCount() {
        return frontier[0].inputCount;
    }

    public long getNodeCount() {
        // 1+ in order to count the -1 implicit final node
        return 1 + nodeCount;
    }

    public long getArcCount() {
        return arcCount;
    }

    public long getMappedStateCount() {
        return dedupHash == null ? 0 : nodeCount;
    }

    private CompiledNode compileNode(UnCompiledNode<T> nodeIn, int tailLength) throws IOException {
        final long node;
        long bytesPosStart = bytes.getPosition();
        if (dedupHash != null && (doShareNonSingletonNodes || nodeIn.numArcs <= 1) && tailLength <= shareMaxTailLength) {
            if (nodeIn.numArcs == 0) {
                node = fst.addNode(this, nodeIn);
                lastFrozenNode = node;
            } else {
                node = dedupHash.add(this, nodeIn);
            }
        } else {
            node = fst.addNode(this, nodeIn);
        }
        assert node != -2;

        long bytesPosEnd = bytes.getPosition();
        if (bytesPosEnd != bytesPosStart) {
            // The FST added a new node:
            assert bytesPosEnd > bytesPosStart;
            lastFrozenNode = node;
        }

        nodeIn.clear();

        final CompiledNode fn = new CompiledNode();
        fn.node = node;
        return fn;
    }

    /**
     * 数据在插入前已经完成排序 所以每次排除掉相同前缀的部分后  上次剩余的部分必然不会再变化 那么就可以进行冻结
     * @param prefixLenPlus1
     * @throws IOException
     */
    private void freezeTail(int prefixLenPlus1) throws IOException {
        //System.out.println("  compileTail " + prefixLenPlus1);
        final int downTo = Math.max(1, prefixLenPlus1);
        for (int idx = lastInput.length(); idx >= downTo; idx--) {

            boolean doPrune = false;
            boolean doCompile = false;

            final UnCompiledNode<T> node = frontier[idx];
            final UnCompiledNode<T> parent = frontier[idx - 1];

            if (node.inputCount < minSuffixCount1) {
                doPrune = true;
                doCompile = true;
            } else if (idx > prefixLenPlus1) {
                // prune if parent's inputCount is less than suffixMinCount2
                if (parent.inputCount < minSuffixCount2 || (minSuffixCount2 == 1 && parent.inputCount == 1 && idx > 1)) {
                    // my parent, about to be compiled, doesn't make the cut, so
                    // I'm definitely pruned

                    // if minSuffixCount2 is 1, we keep only up
                    // until the 'distinguished edge', ie we keep only the
                    // 'divergent' part of the FST. if my parent, about to be
                    // compiled, has inputCount 1 then we are already past the
                    // distinguished edge.  NOTE: this only works if
                    // the FST outputs are not "compressible" (simple
                    // ords ARE compressible).
                    doPrune = true;
                } else {
                    // my parent, about to be compiled, does make the cut, so
                    // I'm definitely not pruned
                    doPrune = false;
                }
                doCompile = true;
            } else {
                // if pruning is disabled (count is 0) we can always
                // compile current node
                doCompile = minSuffixCount2 == 0;
            }

            //System.out.println("    label=" + ((char) lastInput.ints[lastInput.offset+idx-1]) + " idx=" + idx + " inputCount=" + frontier[idx].inputCount + " doCompile=" + doCompile + " doPrune=" + doPrune);

            if (node.inputCount < minSuffixCount2 || (minSuffixCount2 == 1 && node.inputCount == 1 && idx > 1)) {
                // drop all arcs
                for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
                    @SuppressWarnings({"rawtypes", "unchecked"}) final UnCompiledNode<T> target =
                            (UnCompiledNode<T>) node.arcs[arcIdx].target;
                    target.clear();
                }
                node.numArcs = 0;
            }

            if (doPrune) {
                // this node doesn't make it -- deref it
                node.clear();
                parent.deleteLast(lastInput.intAt(idx - 1), node);
            } else {

                if (minSuffixCount2 != 0) {
                    compileAllTargets(node, lastInput.length() - idx);
                }
                final T nextFinalOutput = node.output;

                // We "fake" the node as being final if it has no
                // outgoing arcs; in theory we could leave it
                // as non-final (the FST can represent this), but
                // FSTEnum, Util, etc., have trouble w/ non-final
                // dead-end states:
                final boolean isFinal = node.isFinal || node.numArcs == 0;

                if (doCompile) {
                    // this node makes it and we now compile it.  first,
                    // compile any targets that were previously
                    // undecided:
                    parent.replaceLast(lastInput.intAt(idx - 1),
                            compileNode(node, 1 + lastInput.length() - idx),
                            nextFinalOutput,
                            isFinal);
                } else {
                    // replaceLast just to install
                    // nextFinalOutput/isFinal onto the arc
                    parent.replaceLast(lastInput.intAt(idx - 1),
                            node,
                            nextFinalOutput,
                            isFinal);
                    // this node will stay in play for now, since we are
                    // undecided on whether to prune it.  later, it
                    // will be either compiled or pruned, so we must
                    // allocate a new node:
                    frontier[idx] = new UnCompiledNode<>(this, idx);
                }
            }
        }
    }

    // for debugging
  /*
  private String toString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      return b.toString();
    }
  }
  */

    /**
     * Add the next input/output pair.  The provided input
     * must be sorted after the previous one according to
     * {@link IntsRef#compareTo}.  It's also OK to add the same
     * input twice in a row with different outputs, as long
     * as {@link Outputs} implements the {@link Outputs#merge}
     * method. Note that input is fully consumed after this
     * method is returned (so caller is free to reuse), but
     * output is not.  So if your outputs are changeable (eg
     * {@link ByteSequenceOutputs} or {@link
     * IntSequenceOutputs}) then you cannot reuse across
     * calls.
     * @param input  一组以 ascii写入的字符  每个元素会写入到node中
     */
    // 开始往 compiler中插入一个节点
    public void add(IntsRef input, T output) throws IOException {
    /*
    if (DEBUG) {
      BytesRef b = new BytesRef(input.length);
      for(int x=0;x<input.length;x++) {
        b.bytes[x] = (byte) input.ints[x];
      }
      b.length = input.length;
      if (output == NO_OUTPUT) {
        System.out.println("\nFST ADD: input=" + toString(b) + " " + b);
      } else {
        System.out.println("\nFST ADD: input=" + toString(b) + " " + b + " output=" + fst.outputs.outputToString(output));
      }
    }
    */

        // De-dup NO_OUTPUT since it must be a singleton:
        // 指向单例对象  释放引用
        if (output.equals(NO_OUTPUT)) {
            output = NO_OUTPUT;
        }

        assert lastInput.length() == 0 || input.compareTo(lastInput.get()) >= 0 : "inputs are added out of order lastInput=" + lastInput.get() + " vs input=" + input;
        assert validOutput(output);

        //System.out.println("\nadd: " + input);
        // 因为本身构建 FST 的数据必须满足 已经完成排序  所以只有第一次允许写入一个空值   并且写入的位置 自然对应了 UnCompiledNode的末尾
        if (input.length == 0) {
            // empty input: only allowed as first input.  we have
            // to special case this because the packed FST
            // format cannot represent the empty input since
            // 'finalness' is stored on the incoming arc, not on
            // the node
            // 代表该节点被多少字符(链路)共享
            // frontier[0] 是根节点  会被所有的链路共享    匹配前缀是从根节点出发的  通过根节点延申的边  到下游节点
            frontier[0].inputCount++;
            // 该节点可以直接作为一个终止节点
            frontier[0].isFinal = true;
            // 当整个FST 链路中没有其他节点时  传入的 output 会作为 emptyOutput 代表整个FST 为空结构时  使用的output值
            fst.setEmptyOutput(output);
            return;
        }

        // compare shared prefix length
        // 找到共享前缀的偏移量
        int pos1 = 0;
        int pos2 = input.offset;
        // 将本次插入的数据与上次插入的数据做比较 计算出相同的前缀长度 (这里有一个前提就是构建fst时 插入的数据就是顺序的)
        final int pos1Stop = Math.min(lastInput.length(), input.length);
        while (true) {
            // 代表这个节点被共享的次数
            frontier[pos1].inputCount++;
            //System.out.println("  incr " + pos1 + " ct=" + frontier[pos1].inputCount + " n=" + frontier[pos1]);
            // 当前缀不相同时 退出循环  或者当前遍历的位置已经超过了 较小的数据的长度
            if (pos1 >= pos1Stop || lastInput.intAt(pos1) != input.ints[pos2]) {
                break;
            }
            pos1++;
            pos2++;
        }
        // 指向相同前缀+2的位置
        final int prefixLenPlus1 = pos1 + 1;

        // 如果存储为完成节点的数组容量不够大 需要先进行扩容    这里额外空出了一个位置 可能是用于存储 标记finial的
        if (frontier.length < input.length + 1) {
            final UnCompiledNode<T>[] next = ArrayUtil.grow(frontier, input.length + 1);
            for (int idx = frontier.length; idx < next.length; idx++) {
                next[idx] = new UnCompiledNode<>(this, idx);
            }
            frontier = next;
        }

        // minimize/compile states from previous input's
        // orphan'd suffix
        freezeTail(prefixLenPlus1);

        // init tail states for current input
        for (int idx = prefixLenPlus1; idx <= input.length; idx++) {
            frontier[idx - 1].addArc(input.ints[input.offset + idx - 1],
                    frontier[idx]);
            frontier[idx].inputCount++;
        }

        final UnCompiledNode<T> lastNode = frontier[input.length];
        if (lastInput.length() != input.length || prefixLenPlus1 != input.length + 1) {
            lastNode.isFinal = true;
            lastNode.output = NO_OUTPUT;
        }

        // push conflicting outputs forward, only as far as
        // needed
        for (int idx = 1; idx < prefixLenPlus1; idx++) {
            final UnCompiledNode<T> node = frontier[idx];
            final UnCompiledNode<T> parentNode = frontier[idx - 1];

            final T lastOutput = parentNode.getLastOutput(input.ints[input.offset + idx - 1]);
            assert validOutput(lastOutput);

            final T commonOutputPrefix;
            final T wordSuffix;

            if (lastOutput != NO_OUTPUT) {
                commonOutputPrefix = fst.outputs.common(output, lastOutput);
                assert validOutput(commonOutputPrefix);
                wordSuffix = fst.outputs.subtract(lastOutput, commonOutputPrefix);
                assert validOutput(wordSuffix);
                parentNode.setLastOutput(input.ints[input.offset + idx - 1], commonOutputPrefix);
                node.prependOutput(wordSuffix);
            } else {
                commonOutputPrefix = wordSuffix = NO_OUTPUT;
            }

            output = fst.outputs.subtract(output, commonOutputPrefix);
            assert validOutput(output);
        }

        if (lastInput.length() == input.length && prefixLenPlus1 == 1 + input.length) {
            // same input more than 1 time in a row, mapping to
            // multiple outputs
            lastNode.output = fst.outputs.merge(lastNode.output, output);
        } else {
            // this new arc is private to this new input; set its
            // arc output to the leftover output:
            frontier[prefixLenPlus1 - 1].setLastOutput(input.ints[input.offset + prefixLenPlus1 - 1], output);
        }

        // save last input
        lastInput.copyInts(input);

        //System.out.println("  count[0]=" + frontier[0].inputCount);
    }

    private boolean validOutput(T output) {
        return output == NO_OUTPUT || !output.equals(NO_OUTPUT);
    }

    /**
     * Returns final FST.  NOTE: this will return null if
     * nothing is accepted by the FST.
     */
    public FST<T> compile() throws IOException {

        final UnCompiledNode<T> root = frontier[0];

        // minimize nodes in the last word's suffix
        freezeTail(0);
        if (root.inputCount < minSuffixCount1 || root.inputCount < minSuffixCount2 || root.numArcs == 0) {
            if (fst.emptyOutput == null) {
                return null;
            } else if (minSuffixCount1 > 0 || minSuffixCount2 > 0) {
                // empty string got pruned
                return null;
            }
        } else {
            if (minSuffixCount2 != 0) {
                compileAllTargets(root, lastInput.length());
            }
        }
        //if (DEBUG) System.out.println("  builder.finish root.isFinal=" + root.isFinal + " root.output=" + root.output);
        fst.finish(compileNode(root, lastInput.length()).node);

        return fst;
    }

    private void compileAllTargets(UnCompiledNode<T> node, int tailLength) throws IOException {
        for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
            final Arc<T> arc = node.arcs[arcIdx];
            if (!arc.target.isCompiled()) {
                // not yet compiled
                @SuppressWarnings({"rawtypes", "unchecked"}) final UnCompiledNode<T> n = (UnCompiledNode<T>) arc.target;
                if (n.numArcs == 0) {
                    //System.out.println("seg=" + segment + "        FORCE final arc=" + (char) arc.label);
                    arc.isFinal = n.isFinal = true;
                }
                arc.target = compileNode(n, tailLength - 1);
            }
        }
    }

    /**
     * Expert: holds a pending (seen but not yet serialized) arc.
     */
    // 该对象负责存储数据  并且关联到一个node对象  node实现了链表的功能
    // 每个node下会挂载一个 Arc[]     arc可以理解成FST 结构中的边
    static class Arc<T> {
        int label;                             // really an "unsigned" byte   这个arc所代表的值 使用ascii 代替byte
        Node target;   // 连接的下个节点  也就是先从一个node出发 它包含了一个 arc[] 然后每个arc又会连接到下一个node
        boolean isFinal;
        T output;     // out也就是类似权重的附加值了  FST的特性是每个字符串有自己的权重 而不会重复
        T nextFinalOutput; // 等同于 target.output
    }

    // NOTE: not many instances of Node or CompiledNode are in
    // memory while the FST is being built; it's only the
    // current "frontier":

    interface Node {
        boolean isCompiled();  // 代表是否编译完成 或者说是否是终止节点
    }

    public long fstRamBytesUsed() {
        return fst.ramBytesUsed();
    }

    /**
     * 代表某个 arc信息已经写入到 current[] 中
     */
    static final class CompiledNode implements Node {
        long node;

        @Override
        public boolean isCompiled() {
            return true;
        }
    }

    /**
     * Expert: holds a pending (seen but not yet serialized) Node.
     */
    // 非终止节点 可以挂载一个Arc[]   他这个是用来替代 trie树的 map的 我们知道在trie树下每个节点可以挂载一个map 代表以该节点的字符为起点 下一个字符的可能性
    // 在node链中 UnCompiledNode 代表尚未将arc信息存放到current[] 中
    static final class UnCompiledNode<T> implements Node {
        final FSTCompiler<T> owner;
        int numArcs;
        // arc数量为0的节点 将不会写入 FST
        Arc<T>[] arcs;
        // TODO: instead of recording isFinal/output on the
        // node, maybe we should use -1 arc to mean "end" (like
        // we do when reading the FST).  Would simplify much
        // code here...
        T output;
        // 当该节点是 终点时  arcs[] 是空数组  并且此时 output才有值    每个节点最终都会写入到FST中
        boolean isFinal;
        // 该节点被多少链路共享   比如 acc acd  当前节点作为连接 ac 的节点 被2条链路共享  (a,c 这种对应的是 arc.label 所以 连接ac的才是节点)
        long inputCount;

        /**
         * This node's depth, starting from the automaton root.
         */
        final int depth;

        /**
         * @param depth The node's depth starting from the automaton root. Needed for
         *              LUCENE-2934 (node expansion based on conditions other than the
         *              fanout size).
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        UnCompiledNode(FSTCompiler<T> owner, int depth) {
            this.owner = owner;
            arcs = (Arc<T>[]) new Arc[1];
            arcs[0] = new Arc<>();
            output = owner.NO_OUTPUT;
            this.depth = depth;
        }

        @Override
        public boolean isCompiled() {
            return false;
        }

        void clear() {
            numArcs = 0;
            isFinal = false;
            output = owner.NO_OUTPUT;
            inputCount = 0;

            // We don't clear the depth here because it never changes
            // for nodes on the frontier (even when reused).
        }

        T getLastOutput(int labelToMatch) {
            assert numArcs > 0;
            assert arcs[numArcs - 1].label == labelToMatch;
            return arcs[numArcs - 1].output;
        }

        void addArc(int label, Node target) {
            assert label >= 0;
            assert numArcs == 0 || label > arcs[numArcs - 1].label : "arc[numArcs-1].label=" + arcs[numArcs - 1].label + " new label=" + label + " numArcs=" + numArcs;
            if (numArcs == arcs.length) {
                final Arc<T>[] newArcs = ArrayUtil.grow(arcs, numArcs + 1);
                for (int arcIdx = numArcs; arcIdx < newArcs.length; arcIdx++) {
                    newArcs[arcIdx] = new Arc<>();
                }
                arcs = newArcs;
            }
            final Arc<T> arc = arcs[numArcs++];
            arc.label = label;
            arc.target = target;
            arc.output = arc.nextFinalOutput = owner.NO_OUTPUT;
            arc.isFinal = false;
        }

        void replaceLast(int labelToMatch, Node target, T nextFinalOutput, boolean isFinal) {
            assert numArcs > 0;
            final Arc<T> arc = arcs[numArcs - 1];
            assert arc.label == labelToMatch : "arc.label=" + arc.label + " vs " + labelToMatch;
            arc.target = target;
            //assert target.node != -2;
            arc.nextFinalOutput = nextFinalOutput;
            arc.isFinal = isFinal;
        }

        void deleteLast(int label, Node target) {
            assert numArcs > 0;
            assert label == arcs[numArcs - 1].label;
            assert target == arcs[numArcs - 1].target;
            numArcs--;
        }

        void setLastOutput(int labelToMatch, T newOutput) {
            assert owner.validOutput(newOutput);
            assert numArcs > 0;
            final Arc<T> arc = arcs[numArcs - 1];
            assert arc.label == labelToMatch;
            arc.output = newOutput;
        }

        // pushes an output prefix forward onto all arcs
        void prependOutput(T outputPrefix) {
            assert owner.validOutput(outputPrefix);

            for (int arcIdx = 0; arcIdx < numArcs; arcIdx++) {
                arcs[arcIdx].output = owner.fst.outputs.add(outputPrefix, arcs[arcIdx].output);
                assert owner.validOutput(arcs[arcIdx].output);
            }

            if (isFinal) {
                output = owner.fst.outputs.add(outputPrefix, output);
                assert owner.validOutput(output);
            }
        }
    }

    /**
     * Reusable buffer for building nodes with fixed length arcs (binary search or direct addressing).
     */
    static class FixedLengthArcsBuffer {

        // Initial capacity is the max length required for the header of a node with fixed length arcs:
        // header(byte) + numArcs(vint) + numBytes(vint)
        private byte[] bytes = new byte[11];
        private final ByteArrayDataOutput bado = new ByteArrayDataOutput(bytes);

        /**
         * Ensures the capacity of the internal byte array. Enlarges it if needed.
         */
        FixedLengthArcsBuffer ensureCapacity(int capacity) {
            if (bytes.length < capacity) {
                bytes = new byte[ArrayUtil.oversize(capacity, Byte.BYTES)];
                bado.reset(bytes);
            }
            return this;
        }

        FixedLengthArcsBuffer resetPosition() {
            bado.reset(bytes);
            return this;
        }

        FixedLengthArcsBuffer writeByte(byte b) {
            bado.writeByte(b);
            return this;
        }

        FixedLengthArcsBuffer writeVInt(int i) {
            try {
                bado.writeVInt(i);
            } catch (IOException e) { // Never thrown.
                throw new RuntimeException(e);
            }
            return this;
        }

        int getPosition() {
            return bado.getPosition();
        }

        /**
         * Gets the internal byte array.
         */
        byte[] getBytes() {
            return bytes;
        }
    }
}
