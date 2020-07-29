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


import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;

import java.io.IOException;

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
    /**
     * 最多允许共享的长度
     */
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
    // 描述上一个节点在 bytesStore的地址   如果是最后那个空节点 那么该值为0
    long lastFrozenNode;

    // Reused temporarily while building the FST:
    // 临时数组 当构建FST 时 可能要暂存一些数据
    int[] numBytesPerArc = new int[4];
    int[] numLabelBytesPerArc = new int[numBytesPerArc.length];
    /**
     * 该对象 负责以固定的长度存储arc
     */
    final FixedLengthArcsBuffer fixedLengthArcsBuffer = new FixedLengthArcsBuffer();

    /**
     * 只有操作 FST对象时 会间接修改该值  每当有节点被冻结时 目标节点的arc值会累加到该值上
     */
    long arcCount;
    /**
     * 当前已经编译完成的节点数
     */
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
     *
     * @param inputType                           代表输出的数据类型
     * @param minSuffixCount1
     * @param minSuffixCount2
     * @param doShareSuffix
     * @param doShareNonSingletonNodes
     * @param shareMaxTailLength
     * @param outputs
     * @param allowFixedLengthArcs
     * @param bytesPageBits                       默认值为15  也就代表 BytesStore中每个block的大小为 1<<15
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
        fst = new FST<>(inputType, outputs, bytesPageBits);
        // 获取fst的数据仓库
        bytes = fst.bytes;
        assert bytes != null;
        // 注意 只有在共享后缀的基础上才会创建这个hash对象
        if (doShareSuffix) {
            // allowSingle 代表是否支持处理一个block的情况
            dedupHash = new NodeHash<>(fst, bytes.getReverseReader(false));
        } else {
            dedupHash = null;
        }
        // 初始化成一个空的 输出流
        NO_OUTPUT = outputs.getNoOutput();

        @SuppressWarnings({"rawtypes", "unchecked"}) final UnCompiledNode<T>[] f =
                (UnCompiledNode<T>[]) new UnCompiledNode[10];
        frontier = f;
        // 初始化10个未编译的节点
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
         *
         */
        private float directAddressingMaxOversizingFactor = DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR;

        /**
         * @param inputType The input type (transition labels). Can be anything from {@link INPUT_TYPE}
         *                  enumeration. Shorter types will consume less memory. Strings (character sequences) are
         *                  represented as {@link INPUT_TYPE#BYTE4} (full unicode codepoints).   较少的类型 消耗的内存会更小
         * @param outputs   The output type for each input sequence. Applies only if building an FST. For
         *                  FSA, use {@link NoOutputs#getSingleton()} and {@link NoOutputs#getNoOutput()} as the
         *                  singleton output object.  该输出流就是专门用于构建FST的 一般就是 ByteSequenceOutputs.getSingleton
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
         * 代表裁剪输入图的最小节点数
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
         * true 代表后缀将会被共享 也就是构建更小的FST 如果为false 则会为每个输入的后缀单独创建一个路径 这样构建的FST 会更大 但是在构建时需要的内存和CPU会小很多
         * (更小的CPU开销可以理解 但是内存怎么也会减小呢)
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
         * 如果将该标识设置为true 会使得生成的 FST 最小  同时会消耗更多的 CPU 和 内存资源
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
         * 当开启共享后缀后 允许的后缀最大长度
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
         * false 代表禁用固定长度的 arc优化 (使用二分查找或者直接寻址)  这将会使得创建的FST更小 但是遍历速度更慢
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
         * 用于规定在 BytesStore中  每个block 有多少位
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
         * 当将该值设置为负数时 会禁止直接寻址 只创建二进制搜索节点
         * 该因子无法确定 arc 采用变长还是固定长度的链    仅确定已知的有效编码是固定长度
         */
        public Builder<T> directAddressingMaxOversizingFactor(float factor) {
            this.directAddressingMaxOversizingFactor = factor;
            return this;
        }

        /**
         * Creates a new {@link FSTCompiler}.
         * 基于这些属性返回一个 FSTCompiler 对象
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
     *
     * @return
     */
    public long getTermCount() {
        return frontier[0].inputCount;
    }

    /**
     * 这里好像额外考虑了一个 final节点
     *
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
     * 编译某个节点
     *
     * @param nodeIn   从frontier 为input预留的node 开始 不断往前编译  直到首个不再共享前缀的节点(parent)的下一个节点(node)
     * @param tailLength   从1开始增大 直到 后缀长度+1 (lastInput.length-prefixLenPlus1+1)
     * @return
     * @throws IOException
     */
    private CompiledNode compileNode(UnCompiledNode<T> nodeIn, int tailLength) throws IOException {
        // 对应 bytesStore的地址
        final long node;
        // 找到数据仓库此时的偏移量
        long bytesPosStart = bytes.getPosition();
        // dedupHash != null 代表开启了共享后缀
        // 这里还要求尾部长度 不能超过限制值
        // TODO doShareNonSingletonNodes 啥意思???  该值默认为true
        if (dedupHash != null && (doShareNonSingletonNodes || nodeIn.numArcs <= 1) && tailLength <= shareMaxTailLength) {
            // 如果是最后一个节点 arc应该就是0 那么此时就可以加入到 fst中
            if (nodeIn.numArcs == 0) {
                // 如果是最后一个节点 返回的node 就是0 (NON_FINAL_END_NODE)
                node = fst.addNode(this, nodeIn);
                // 更新上一个被冻结的节点
                lastFrozenNode = node;
            } else {
                // 除了最后一个节点之外 其他节点应该是携带 arc信息的 将信息写入到 nodeHash中 （store内部也会调用 fst.addNode）
                node = dedupHash.add(this, nodeIn);
            }
        } else {
            // 这里不使用缓存 所以直接写入到fst中
            node = fst.addNode(this, nodeIn);
        }
        assert node != -2;

        // 检测store的偏移量是否发生了变化
        long bytesPosEnd = bytes.getPosition();
        // 代表确实写入了数据
        if (bytesPosEnd != bytesPosStart) {
            // The FST added a new node:
            assert bytesPosEnd > bytesPosStart;
            // 将此时最新的地址信息写入到 lastNode上
            lastFrozenNode = node;
        }

        // 将该节点属性清除 这样就可以作为一个普通节点 等待下一个input填充数据
        nodeIn.clear();

        // 返回一个编译完的节点 内部的node 就是 bytesStore的地址
        final CompiledNode fn = new CompiledNode();
        fn.node = node;
        return fn;
    }

    /**
     *
     * @param prefixLenPlus1 相同前缀长度+1 (原本 prefixLen 是数组下标)
     * @throws IOException
     */
    private void freezeTail(int prefixLenPlus1) throws IOException {
        //System.out.println("  compileTail " + prefixLenPlus1);
        final int downTo = Math.max(1, prefixLenPlus1);
        // downTo 就是从末尾开始直到第一个不一样的元素  也就是遍历不相同的部分
        for (int idx = lastInput.length(); idx >= downTo; idx--) {

            boolean doPrune = false;
            boolean doCompile = false;

            // frontier[idx] 是取下一个元素 frontier会确保比当前最长的input长度多1个单位
            final UnCompiledNode<T> node = frontier[idx];
            // 这个值才对应此时处理的后缀不同的节点
            final UnCompiledNode<T> parent = frontier[idx - 1];

            // minSuffixCount1 该值默认是0 先忽略第一个分支
            if (node.inputCount < minSuffixCount1) {
                doPrune = true;
                doCompile = true;
            // 处理第一个不同的后缀时 不会进入这个分支 当后缀长度超过1时 走第三个分支
            } else if (idx > prefixLenPlus1) {
                // prune if parent's inputCount is less than suffixMinCount2
                // minSuffixCount2 默认为0 这个分支也可以忽略了
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
                    // 代表不需要被裁剪
                    doPrune = false;
                }
                // 需要被编译
                doCompile = true;
            } else {
                // if pruning is disabled (count is 0) we can always
                // compile current node
                doCompile = minSuffixCount2 == 0;
            }

            // 因为minSuffixCount1, minSuffixCount2 默认是0 所以上面几条分支最后得到的结果是一致的  都是 doCompile=true doPrune = false

            //System.out.println("    label=" + ((char) lastInput.ints[lastInput.offset+idx-1]) + " idx=" + idx + " inputCount=" + frontier[idx].inputCount + " doCompile=" + doCompile + " doPrune=" + doPrune);
            // 因为 minSuffixCount2 为0 忽略这里的逻辑
            if (node.inputCount < minSuffixCount2 || (minSuffixCount2 == 1 && node.inputCount == 1 && idx > 1)) {
                // drop all arcs
                // 找到应该被冻结的节点 对下面所有的 arc 进行丢弃
                for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
                    @SuppressWarnings({"rawtypes", "unchecked"}) final UnCompiledNode<T> target =
                            (UnCompiledNode<T>) node.arcs[arcIdx].target;
                    // 将该节点下的 numArcs 置0
                    target.clear();
                }
                node.numArcs = 0;
            }

            // 忽略
            if (doPrune) {
                // this node doesn't make it -- deref it
                // 清除当前节点下所有的 arc
                node.clear();
                // 父节点删除该节点   这个 index会比input的下标大1 这里又-1 进行还原  (该方法内部就是 numArc -1 )
                parent.deleteLast(lastInput.intAt(idx - 1), node);
            } else {

                // 先假定该值就是0  跳过该方法
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

                // 默认进入第一个分支
                if (doCompile) {
                    // this node makes it and we now compile it.  first,
                    // compile any targets that were previously
                    // undecided:
                    // 找到本次处理的不匹配的字符与node节点 进行处理
                    // replaceLast 会替换parent的最后一个arc对象相关属性
                    parent.replaceLast(lastInput.intAt(idx - 1),
                            // 第一次处理 node 对应frontier 最后一个空节点 (每次frontier 都会比写入的input大1)
                            // idx最大值为 lastInput.length()  也就是第二个参数最小值为 1
                            // 从compileNode 可以看出 大体是将 UncompileNode 的数据转存到 bytesStore中 然后 clear node之前的数据
                            // 并将 写入 bytesStore数据后返回的地址赋值到一个新创建的CompileNode对象中
                            compileNode(node, 1 + lastInput.length() - idx),
                            nextFinalOutput,
                            isFinal);
                } else {
                    // replaceLast just to install
                    // nextFinalOutput/isFinal onto the arc
                    // 这里并没有将节点变成编译状态  只是更新了 nextFinalOutput 和 isFinal
                    parent.replaceLast(lastInput.intAt(idx - 1),
                            node,
                            nextFinalOutput,
                            isFinal);
                    // this node will stay in play for now, since we are
                    // undecided on whether to prune it.  later, it
                    // will be either compiled or pruned, so we must
                    // allocate a new node:
                    // 将目标为设置成一个新的节点
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
     * 首先确定该方法的调用时机
     * 在 BlockTreeTermsWriter 每当写入一些term时  当发现他们在连续使用了一个相同的前缀后 突然不再使用了 这时 将相同的前缀部分通过该对象保存起来
     *
     * @param input  就是这组term共享的前缀 如果该批数据分为多次写入 那么这里会存储上一个写入的entry对应的floorLeadByte  首个entry的 floorLeadByte为-1
     * @param output 这个输出流中包含了 分批处理的多个pendingBlock数据
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
        // 当output为空的时候 指向一个单例对象  应该是之后某个地方可以反复读取 所以想通过单例节省开销吧???
        if (output.equals(NO_OUTPUT)) {
            output = NO_OUTPUT;
        }

        assert lastInput.length() == 0 || input.compareTo(lastInput.get()) >= 0 : "inputs are added out of order lastInput=" + lastInput.get() + " vs input=" + input;
        assert validOutput(output);

        //System.out.println("\nadd: " + input);
        // TODO 共享前缀长度是0的时候 首先要求 那个minItem的限制先去除吧 否则应该是不会出现这种情况的
        // TODO 场景应该是这样 第一个term 传入一个长度为0的值 之后传入一个新值 这时代表 长度为0的值不再被共享 同时满足 minItem的限制 才会进入到这里
        if (input.length == 0) {
            // empty input: only allowed as first input.  we have
            // to special case this because the packed FST
            // format cannot represent the empty input since
            // 'finalness' is stored on the incoming arc, not on
            // the node
            // 这个时候 FST的构建是最简单的 增加终止节点的共享数 并标记以完成 退出   数组下标也对应着深度的概念
            frontier[0].inputCount++;  // frontier[0] 是特殊的 arc 所有字符串都会共享该节点 同时 inputCount 就是共享计数
            frontier[0].isFinal = true;
            // TODO 待处理
            fst.setEmptyOutput(output);
            return;
        }

        // compare shared prefix length
        // 这里在寻找与上一个插入的 input 相同的后缀的位置
        int pos1 = 0;
        int pos2 = input.offset;
        // 首个block input的长度就是 共享前缀的长度  之后的block 长度+1 在原有的基础上增加一个floorLeadLabel   再之后同一批block的长度应该都是一样的
        // TODO 也可能长度差会超过1吧
        final int pos1Stop = Math.min(lastInput.length(), input.length);
        while (true) {
            // 增加经过该节点的链路数量  (这个时候应该只存在一种链路  一旦出现了不一样的节点的时候 应该就会将不同的节点编译掉)
            frontier[pos1].inputCount++;
            // 注意 当达到某个输入流的末尾 或者 当2者不同时 退出循环
            if (pos1 >= pos1Stop || lastInput.intAt(pos1) != input.ints[pos2]) {
                break;
            }
            pos1++;
            pos2++;
        }

        // 前缀长度+1   pos1是数组下标
        final int prefixLenPlus1 = pos1 + 1;

        // 本次输入的字符可能长度超过了之前创建的 node 这里针对 frontier进行扩容
        // 注意这里额外创建了一个node  ( < input.length + 1)
        if (frontier.length < input.length + 1) {
            final UnCompiledNode<T>[] next = ArrayUtil.grow(frontier, input.length + 1);
            for (int idx = frontier.length; idx < next.length; idx++) {
                next[idx] = new UnCompiledNode<>(this, idx);
            }
            frontier = next;
        }

        // minimize/compile states from previous input's
        // orphan'd suffix
        // 因为传入了新的值 所以之前不同的后缀部分可以被冻结了
        /**
         * 这里只有2种情况
         * 1: prefixLenPlus1 超过了之前存储的数据 那么就不需要冻结
         * 2: prefixLenPlus1 <= 之前存储的数据   超出的部分要进行冻结
         */
        freezeTail(prefixLenPlus1);

        // init tail states for current input
        // 开始将 后面没有重复的部分写入到 frontier 中
        for (int idx = prefixLenPlus1; idx <= input.length; idx++) {
            // 因为 prefixLenPlus1 = idx+1 所以下面要减回来
            frontier[idx - 1].addArc(input.ints[input.offset + idx - 1],
                    // 这里生成了一个新的 arc 同时下游连接的是  frontier[idx]
                    frontier[idx]);
            // 子节点会挨个增加计数
            frontier[idx].inputCount++;
        }

        // 这是本次插入的节点
        final UnCompiledNode<T> lastNode = frontier[input.length];
        // 代表本次输入字符与上次不一样  那么将本次插入的节点更新成 final节点  同时将 output设置为一个特殊值 意味着该节点是末尾节点
        if (lastInput.length() != input.length || prefixLenPlus1 != input.length + 1) {
            lastNode.isFinal = true;
            lastNode.output = NO_OUTPUT;
        }

        // push conflicting outputs forward, only as far as
        // needed
        // 这里是在设置 output  默认情况下 首次插入的 term  所有output会集中在首个  arc.output 上
        // 之后添加新的 term 通过计算它们 的公共前缀 将output进行拆分  相同的部分 存储在共享前缀中 然后后面不同的部分 单独设置 output
        for (int idx = 1; idx < prefixLenPlus1; idx++) {
            // 当idx 为1 时  parentNode 就是 那个特殊节点 frontier[0]
            final UnCompiledNode<T> node = frontier[idx];
            final UnCompiledNode<T> parentNode = frontier[idx - 1];

            // 这里的每一次计算都会对下一次产生影响 因为每次改动的node节点会作为下一个parent  节点

            // 返回该节点上挂载的最后一个arc 对应的output
            final T lastOutput = parentNode.getLastOutput(input.ints[input.offset + idx - 1]);
            assert validOutput(lastOutput);

            final T commonOutputPrefix;
            final T wordSuffix;

            if (lastOutput != NO_OUTPUT) {
                // 开始计算公共前缀
                commonOutputPrefix = fst.outputs.common(output, lastOutput);
                assert validOutput(commonOutputPrefix);
                wordSuffix = fst.outputs.subtract(lastOutput, commonOutputPrefix);
                assert validOutput(wordSuffix);
                // 将父节点 的 arc 修改成  commonOutputPrefix
                // 同时将本节点的arc 对应的output 修改成 wordSuffix
                parentNode.setLastOutput(input.ints[input.offset + idx - 1], commonOutputPrefix);
                // 为当前节点下所有arc 都追加一个固定的前缀   （output下沉）
                node.prependOutput(wordSuffix);
            } else {
                // 本次不需要修改 output
                commonOutputPrefix = wordSuffix = NO_OUTPUT;
            }

            // 相当于传到下一轮的 output本身也共用了parent的 commonOutputPrefix  所以下面output要减小
            output = fst.outputs.subtract(output, commonOutputPrefix);
            assert validOutput(output);
        }

        // 代表2次输入的数据一致
        if (lastInput.length() == input.length && prefixLenPlus1 == 1 + input.length) {
            // same input more than 1 time in a row, mapping to
            // multiple outputs
            // ascii一致 与 output是否一致 没有必然联系吧???
            lastNode.output = fst.outputs.merge(lastNode.output, output);
        } else {
            // this new arc is private to this new input; set its
            // arc output to the leftover output:
            // 从分叉点开始更新 lastOutput
            frontier[prefixLenPlus1 - 1].setLastOutput(input.ints[input.offset + prefixLenPlus1 - 1], output);
        }

        // save last input
        // 更新 lastInput
        lastInput.copyInts(input);

        //System.out.println("  count[0]=" + frontier[0].inputCount);
    }

    private boolean validOutput(T output) {
        return output == NO_OUTPUT || !output.equals(NO_OUTPUT);
    }

    /**
     * Returns final FST.  NOTE: this will return null if
     * nothing is accepted by the FST.
     * 完成 fst的构建
     */
    public FST<T> compile() throws IOException {

        // 找到根节点
        final UnCompiledNode<T> root = frontier[0];

        // minimize nodes in the last word's suffix
        // 从后往前 将所有节点写入 fst
        freezeTail(0);
        // 这种情况应该可以忽略 代表根节点此时没有挂载任何 arc
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
        // 最后就是触发 fst.finish
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
        int label;                             // really an "unsigned" byte   这个arc所代表的值 使用ascii 代替byte  （这条边附带的文本信息）
        Node target;   // 连接的下个节点  也就是先从一个node出发 它包含了一个 arc[] 然后每个arc又会连接到下一个node
        boolean isFinal;  // 当该表示为true 时  target 为无效节点 意味着到达末尾了
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
         * 数据之前被写入到 bytesStore中
         * 这里就是写入时返回的地址信息
         */
        long node;

        @Override
        public boolean isCompiled() {
            return true;
        }
    }

    /**
     * Expert: holds a pending (seen but not yet serialized) Node.
     * 存储还未编译完成的临时节点
     */
    static final class UnCompiledNode<T> implements Node {
        /**
         * 该节点是由哪个compiler对象创建的
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

        // TODO: instead of recording isFinal/output on the
        // node, maybe we should use -1 arc to mean "end" (like
        // we do when reading the FST).  Would simplify much
        // code here...
        // 该属性一开始为 NO_OUTPUT
        T output;

        /**
         * 代表这个节点处理完毕了
         */
        boolean isFinal;
        /**
         * 该节点被多少链路共享
         */
        long inputCount;

        /**
         * This node's depth, starting from the automaton root.
         * 对应 front数组的下标
         */
        final int depth;

        /**
         * @param depth The node's depth starting from the automaton root. Needed for
         *              LUCENE-2934 (node expansion based on conditions other than the
         *              fanout size)
         *              描述节点的深度信息
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        UnCompiledNode(FSTCompiler<T> owner, int depth) {
            this.owner = owner;
            // 刚创建的情况下 arc包含一个空对象
            arcs = (Arc<T>[]) new Arc[1];
            arcs[0] = new Arc<>();
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
         * @param label  这个int其实就是fst实际挂载的数据   因为采用了 ascii 码进行编码 所以不使用byte来标识
         * @param target arc连接的下游节点
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
            // 这里会触发 numArcs ++
            final Arc<T> arc = arcs[numArcs++];
            arc.label = label;
            arc.target = target;
            arc.output = arc.nextFinalOutput = owner.NO_OUTPUT;
            arc.isFinal = false;
        }

        /**
         * 替换最后一个 arc下挂载的节点
         * TODO arc在什么时机创建 以及它是做什么的???
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
         *
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
         *
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
                // 更新之前所有arc 的output (将本次入参作为前缀 追加到之前arc的output上)
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
         *
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
