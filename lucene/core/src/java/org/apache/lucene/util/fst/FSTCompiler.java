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
     * 就是一个hash桶 主要是复用节点 以节省内存
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

    /**
     * 每当某个节点转移到 fst中 将目标节点挂载的 arc加到该值上
     */
    long arcCount;
    long nodeCount;
    long binarySearchNodeCount;
    long directAddressingNodeCount;

    /**
     * 是否开启了 每个arc 使用相同长度的开关
     */
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

    /**
     * 使用相关属性初始化  FSTCompiler 对象
     * @param inputType
     * @param minSuffixCount1
     * @param minSuffixCount2
     * @param doShareSuffix
     * @param doShareNonSingletonNodes
     * @param shareMaxTailLength
     * @param outputs
     * @param allowFixedLengthArcs
     * @param bytesPageBits
     * @param directAddressingMaxOversizingFactor
     */
    private FSTCompiler(FST.INPUT_TYPE inputType, int minSuffixCount1, int minSuffixCount2, boolean doShareSuffix,
                        boolean doShareNonSingletonNodes, int shareMaxTailLength, Outputs<T> outputs,
                        boolean allowFixedLengthArcs, int bytesPageBits, float directAddressingMaxOversizingFactor) {
        this.minSuffixCount1 = minSuffixCount1;
        this.minSuffixCount2 = minSuffixCount2;
        this.doShareNonSingletonNodes = doShareNonSingletonNodes;
        this.shareMaxTailLength = shareMaxTailLength;
        this.allowFixedLengthArcs = allowFixedLengthArcs;
        this.directAddressingMaxOversizingFactor = directAddressingMaxOversizingFactor;
        // 基于当前属性创建了一个  fst对象
        fst = new FST<>(inputType, outputs, bytesPageBits);
        // 当创建完 fst后 内部会初始化一个 bytesStore对象
        bytes = fst.bytes;
        assert bytes != null;
        // 注意 只有在共享后缀的基础上才会创建这个hash对象
        if (doShareSuffix) {
            // 这里传入一个 反向读取 bytesStore内部的数据的 reader   该reader对象在setPosition之前无法正常使用
            dedupHash = new NodeHash<>(fst, bytes.getReverseReader(false));
        } else {
            dedupHash = null;
        }
        NO_OUTPUT = outputs.getNoOutput();

        @SuppressWarnings({"rawtypes", "unchecked"}) final UnCompiledNode<T>[] f =
                (UnCompiledNode<T>[]) new UnCompiledNode[10];
        frontier = f;
        for (int idx = 0; idx < frontier.length; idx++) {
            // 这里下标作为depth
            frontier[idx] = new UnCompiledNode<>(this, idx);
        }
    }

    /**
     * Fluent-style constructor for FST {@link FSTCompiler}.
     * <p>
     * Creates an FST/FSA builder with all the possible tuning and construction tweaks.
     * Read parameter documentation carefully.
     * 就是以流式api 方便设置 FSTCompiler对象内部的属性
     */
    public static class Builder<T> {

        /**
         * 这里标明了输入的类型  (byte1,byte2,byte4)
         */
        private final INPUT_TYPE inputType;
        /**
         * 该对象是构建 fst的辅助类  比如判别插入的字符串有公共前缀之类的
         */
        private final Outputs<T> outputs;
        /**
         * 最小的后缀数量  1
         */
        private int minSuffixCount1;
        /**
         * 最小的后缀数量  2
         */
        private int minSuffixCount2;
        /**
         * 默认情况下 会共享相同的后缀  (可以节省空间)     fst本身用于存储大量的文本 所以对于内存消耗比较注重  会尽可能的压缩内存
         */
        private boolean shouldShareSuffix = true;
        /**
         * ???
         */
        private boolean shouldShareNonSingletonNodes = true;
        /**
         * 允许共享的最大尾缀长度
         */
        private int shareMaxTailLength = Integer.MAX_VALUE;
        /**
         * 是否分配固定长度的 arc
         */
        private boolean allowFixedLengthArcs = true;
        private int bytesPageBits = 15;
        /**
         * 直接地址 修改长度的因子???
         */
        private float directAddressingMaxOversizingFactor = DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR;

        /**
         * @param inputType The input type (transition labels). Can be anything from {@link INPUT_TYPE}
         *                  enumeration. Shorter types will consume less memory. Strings (character sequences) are
         *                  represented as {@link INPUT_TYPE#BYTE4} (full unicode codepoints).
         * @param outputs   The output type for each input sequence. Applies only if building an FST. For
         *                  FSA, use {@link NoOutputs#getSingleton()} and {@link NoOutputs#getNoOutput()} as the
         *                  singleton output object.
         *                  初始化时会指定数据类型  以及用以鉴别公共前缀的 outputs
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
         * 基于这些固有的属性返回一个 FSTCompiler 对象
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

    /**
     * 总是返回第一个 unCompiledNode 的 inputCount 作为 TermCount
     * @return
     */
    public long getTermCount() {
        return frontier[0].inputCount;
    }

    /**
     * 这里好像额外考虑了一个 final节点
     * @return
     */
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

    /**
     * 将某个节点变成编译后的状态
     * @param nodeIn
     * @param tailLength
     * @return
     * @throws IOException
     */
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
     * 冻结指定前缀后面的末尾  冻结后的数据就会存入到 fst中
     * @param prefixLenPlus1
     * @throws IOException
     */
    private void freezeTail(int prefixLenPlus1) throws IOException {
        //System.out.println("  compileTail " + prefixLenPlus1);
        final int downTo = Math.max(1, prefixLenPlus1);
        // 遍历上一次输入的值 从共享的前缀之后的数据开始
        for (int idx = lastInput.length(); idx >= downTo; idx--) {

            // 是否已经修剪
            boolean doPrune = false;
            boolean doCompile = false;

            // 获取当前节点 以及它的父节点
            final UnCompiledNode<T> node = frontier[idx];
            final UnCompiledNode<T> parent = frontier[idx - 1];

            // 如果当前节点的 inputCount 小于最小的后缀数量 那么就是已经修剪过的
            if (node.inputCount < minSuffixCount1) {
                doPrune = true;
                doCompile = true;
            // 第一次 idx = prefixLenPlus1
            } else if (idx > prefixLenPlus1) {
                // prune if parent's inputCount is less than suffixMinCount2
                // 对父对象进行修剪
                if (parent.inputCount < minSuffixCount2 || (minSuffixCount2 == 1 && parent.inputCount == 1 && idx > 1)) {
                    // my parent, about to be compiled, doesn't make the cut, so
                    // I'm definitely pruned
                    // 父对象被编译完 不符合要求 本对象一定已经被裁剪了

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
                    // 父对象已经编译完成  符合要求 所以本对象不需要被裁剪
                    doPrune = false;
                }
                doCompile = true;
            } else {
                // if pruning is disabled (count is 0) we can always
                // compile current node
                // 判断本节点是否编译完成就是看 后缀Count2 是否为0
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
     * 这里往 FST 中追加一个元素
     *
     * @param input  这个应该就是插入的数据携带的一组 权重一样的东西  长度 跟output会占用的节点数一样
     * @param output 要插入的数据
     */
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
        // 节省内存开销 释放引用
        if (output.equals(NO_OUTPUT)) {
            output = NO_OUTPUT;
        }

        assert lastInput.length() == 0 || input.compareTo(lastInput.get()) >= 0 : "inputs are added out of order lastInput=" + lastInput.get() + " vs input=" + input;
        assert validOutput(output);

        //System.out.println("\nadd: " + input);
        // TODO 特殊情况 先忽略
        if (input.length == 0) {
            // empty input: only allowed as first input.  we have
            // to special case this because the packed FST
            // format cannot represent the empty input since
            // 'finalness' is stored on the incoming arc, not on
            // the node
            frontier[0].inputCount++;
            frontier[0].isFinal = true;
            fst.setEmptyOutput(output);
            return;
        }

        // compare shared prefix length
        // 这里在寻找与上一个插入的 input 相同的后缀的位置
        int pos1 = 0;
        int pos2 = input.offset;
        final int pos1Stop = Math.min(lastInput.length(), input.length);
        while (true) {
            // 增加该元素上挂载的 input
            frontier[pos1].inputCount++;
            //System.out.println("  incr " + pos1 + " ct=" + frontier[pos1].inputCount + " n=" + frontier[pos1]);
            if (pos1 >= pos1Stop || lastInput.intAt(pos1) != input.ints[pos2]) {
                break;
            }
            pos1++;
            pos2++;
        }
        // 找到本次插入的input 和 lastInput 公共前缀的位置  这里额外加了1
        final int prefixLenPlus1 = pos1 + 1;

        // 用于存储 UnCompiledNode 的数组长度不够  需要进行扩容
        if (frontier.length < input.length + 1) {
            // 存在2种节点 一种叫 UnCompiledNode  也就是还未被冻结的节点  可能会产生新的子节点
            // 另一种是确定不会再变化的节点
            final UnCompiledNode<T>[] next = ArrayUtil.grow(frontier, input.length + 1);
            for (int idx = frontier.length; idx < next.length; idx++) {
                next[idx] = new UnCompiledNode<>(this, idx);
            }
            frontier = next;
        }

        // minimize/compile states from previous input's
        // orphan'd suffix
        // 每次插入新的数据时 无法被共享的后缀就要被冻结
        freezeTail(prefixLenPlus1);

        // init tail states for current input
        for (int idx = prefixLenPlus1; idx <= input.length; idx++) {
            // 从无法被共享的位置开始  将数据 挨个插入到 UnCompiledNode
            frontier[idx - 1].addArc(input.ints[input.offset + idx - 1],
                    frontier[idx]);
            // 增加计数器
            frontier[idx].inputCount++;
        }

        // 找到最后一个节点  如果该节点与之前的节点不同 就需要设置成末尾节点
        final UnCompiledNode<T> lastNode = frontier[input.length];
        if (lastInput.length() != input.length || prefixLenPlus1 != input.length + 1) {
            lastNode.isFinal = true;
            lastNode.output = NO_OUTPUT;
        }

        // push conflicting outputs forward, only as far as
        // needed
        // 如果有冲突的话 重新分配 output值
        for (int idx = 1; idx < prefixLenPlus1; idx++) {
            // 从最前面开始 直到共享前缀的末尾
            final UnCompiledNode<T> node = frontier[idx];
            final UnCompiledNode<T> parentNode = frontier[idx - 1];

            // 返回父节点挂载的 多个 arc的最后一个
            final T lastOutput = parentNode.getLastOutput(input.ints[input.offset + idx - 1]);
            assert validOutput(lastOutput);

            final T commonOutputPrefix;
            final T wordSuffix;

            if (lastOutput != NO_OUTPUT) {
                // 计算公共前缀
                commonOutputPrefix = fst.outputs.common(output, lastOutput);
                assert validOutput(commonOutputPrefix);
                // 计算新的后缀
                wordSuffix = fst.outputs.subtract(lastOutput, commonOutputPrefix);
                assert validOutput(wordSuffix);
                // 把最后一个节点的 output 更新成了 公共后缀
                parentNode.setLastOutput(input.ints[input.offset + idx - 1], commonOutputPrefix);
                // 为当前节点下所有arc 都追加一个固定的前缀
                node.prependOutput(wordSuffix);
            } else {
                // 如果最后一个 output 是 NO_OUTPUT
                commonOutputPrefix = wordSuffix = NO_OUTPUT;
            }

            // 将输出数据变成 2个数据的交集
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
    // 每个node下会挂载一个 Arc[]
    static class Arc<T> {
        int label;                             // really an "unsigned" byte   这个arc所代表的值 使用ascii 代替byte
        Node target;   // 连接的下个节点  也就是先从一个node出发 它包含了一个 arc[] 然后每个arc又会连接到下一个node
        boolean isFinal;  // 是否是结尾
        T output;     // out也就是类似权重的附加值了  FST的特性是每个字符串有自己的权重 而不会重复
        T nextFinalOutput;
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
     * 对应一个已经被 freeze的节点
     */
    static final class CompiledNode implements Node {
        /**
         * 地址信息
         */
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
    // 该对象内部的api 只是简单的设置 arc属性
    static final class UnCompiledNode<T> implements Node {
        /**
         * 代表该节点属于哪个 compiler
         */
        final FSTCompiler<T> owner;
        /**
         * 下面挂载的 arc数组元素数量
         */
        int numArcs;
        /**
         * arc数组
         */
        Arc<T>[] arcs;

        // output 和 isFinal 被转移到了node???
        // TODO: instead of recording isFinal/output on the
        // node, maybe we should use -1 arc to mean "end" (like
        // we do when reading the FST).  Would simplify much
        // code here...
        T output;
        boolean isFinal;
        long inputCount;

        /**
         * This node's depth, starting from the automaton root.
         */
        final int depth;

        /**
         * @param depth The node's depth starting from the automaton root. Needed for
         *              LUCENE-2934 (node expansion based on conditions other than the
         *              fanout size)
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        UnCompiledNode(FSTCompiler<T> owner, int depth) {
            this.owner = owner;
            // 刚创建的情况下 arc包含一个空对象
            arcs = (Arc<T>[]) new Arc[1];
            arcs[0] = new Arc<>();
            // 一个空对象  占位符
            output = owner.NO_OUTPUT;
            this.depth = depth;
        }

        @Override
        public boolean isCompiled() {
            return false;
        }

        /**
         * 注意该 方法没有释放内部的arc 只是将指针重置了
         */
        void clear() {
            numArcs = 0;
            isFinal = false;
            output = owner.NO_OUTPUT;
            inputCount = 0;

            // We don't clear the depth here because it never changes
            // for nodes on the frontier (even when reused).
        }

        // 返回 挂载的 arc[] 中 最后一个arc的数据
        T getLastOutput(int labelToMatch) {
            assert numArcs > 0;
            assert arcs[numArcs - 1].label == labelToMatch;
            return arcs[numArcs - 1].output;
        }

        /**
         * 添加一个新的arc   此时添加的节点 不是作为final节点
         * @param label   权重值
         * @param target  arc连接的节点
         */
        void addArc(int label, Node target) {
            assert label >= 0;
            assert numArcs == 0 || label > arcs[numArcs - 1].label : "arc[numArcs-1].label=" + arcs[numArcs - 1].label + " new label=" + label + " numArcs=" + numArcs;
            // 初始化时  numArcs 为0   而arcs.length 为1
            // 当需要往该节点插入第二个节点时
            if (numArcs == arcs.length) {
                // 对arc进行扩容
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

        /**
         * 修改最后一个arc 内部的属性
         * @param labelToMatch
         * @param target
         * @param nextFinalOutput
         * @param isFinal
         */
        void replaceLast(int labelToMatch, Node target, T nextFinalOutput, boolean isFinal) {
            assert numArcs > 0;
            // 先找到最后一个arc
            final Arc<T> arc = arcs[numArcs - 1];
            assert arc.label == labelToMatch : "arc.label=" + arc.label + " vs " + labelToMatch;
            arc.target = target;
            //assert target.node != -2;
            arc.nextFinalOutput = nextFinalOutput;
            arc.isFinal = isFinal;
        }

        /**
         * 删除最后一个节点  实际上 只是减小指针 而没有释放内存
         * @param label
         * @param target
         */
        void deleteLast(int label, Node target) {
            assert numArcs > 0;
            assert label == arcs[numArcs - 1].label;
            assert target == arcs[numArcs - 1].target;
            numArcs--;
        }

        /**
         * 设置最后一个节点的 output
         * @param labelToMatch
         * @param newOutput
         */
        void setLastOutput(int labelToMatch, T newOutput) {
            assert owner.validOutput(newOutput);
            assert numArcs > 0;
            final Arc<T> arc = arcs[numArcs - 1];
            assert arc.label == labelToMatch;
            arc.output = newOutput;
        }

        // pushes an output prefix forward onto all arcs
        // 将一个前缀设置到所有的arc中
        void prependOutput(T outputPrefix) {
            assert owner.validOutput(outputPrefix);

            for (int arcIdx = 0; arcIdx < numArcs; arcIdx++) {
                // 找到所有 arc.output
                arcs[arcIdx].output = owner.fst.outputs.add(outputPrefix, arcs[arcIdx].output);
                assert owner.validOutput(arcs[arcIdx].output);
            }

            // 如果当前节点是最后一个节点  还会额外为该对象的 output赋值
            if (isFinal) {
                output = owner.fst.outputs.add(outputPrefix, output);
                assert owner.validOutput(output);
            }
        }
    }

    /**
     * Reusable buffer for building nodes with fixed length arcs (binary search or direct addressing).
     * 该对象负责存储固定长度的 arc
     */
    static class FixedLengthArcsBuffer {

        // Initial capacity is the max length required for the header of a node with fixed length arcs:
        // header(byte) + numArcs(vint) + numBytes(vint)
        // 固定长度为11
        private byte[] bytes = new byte[11];
        // byte[] 的包装类
        private final ByteArrayDataOutput bado = new ByteArrayDataOutput(bytes);

        /**
         * Ensures the capacity of the internal byte array. Enlarges it if needed.
         * 判断当前是否有足够的空间 不足的话进行扩容
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

        /**
         * 写入一个VInt  VInt的特性就是会先判断该值实际占用多少位   会以7位为单位拆解写入
         * @param i
         * @return
         */
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
