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

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PagedGrowableWriter;

// Used to dedup states (lookup already-frozen states)
// 存储数据 这样在构建fst时 如果发现了hash相同的数据则可以共享数据 (避免重复创建 节省内存)
final class NodeHash<T> {

    /**
     * 实际存储数据的桶
     */
    private PagedGrowableWriter table;
    /**
     * 记录此时hash结构中一共存储了多少数据
     */
    private long count;
    private long mask;
    /**
     * 代表该对象挂载在哪个fst上
     */
    private final FST<T> fst;

    /**
     * 该对象负责暂存数据 以及读取数据
     */
    private final FST.Arc<T> scratchArc = new FST.Arc<>();
    /**
     * 以反向读取对象来包装 bytesStore 内部的数据
     */
    private final FST.BytesReader in;

    /**
     * 通过 fst 以及反向读取 fst内部数据的 BytesStore的reader对象进行初始化
     * @param fst
     * @param in
     */
    public NodeHash(FST<T> fst, FST.BytesReader in) {
        // 该对象包含扩容的api 并且 每当写入的 元素需要的 bitPerValue 变大时 就将原本的数据取出来 按照新的位数 写入到一个新容器中
        table = new PagedGrowableWriter(16, 1 << 27, 8, PackedInts.COMPACT);
        mask = 15;
        this.fst = fst;
        this.in = in;
    }


    /**
     * 检测fst上目标地址对应的node与传入的node是否完全一致  TODO 待看
     * @param node
     * @param address
     * @return
     * @throws IOException
     */
    private boolean nodesEqual(FSTCompiler.UnCompiledNode<T> node, long address) throws IOException {
        // 将数据读取到目标容器
        fst.readFirstRealTargetArc(address, scratchArc, in);

        // Fail fast for a node with fixed length arcs.
        if (scratchArc.bytesPerArc() != 0) {
            // 只有当 flag是 用于二分查找时 才直接使用numArc做判断
            if (scratchArc.nodeFlags() == FST.ARCS_FOR_BINARY_SEARCH) {
                if (node.numArcs != scratchArc.numArcs()) {
                    return false;
                }
            } else {
                assert scratchArc.nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING;
                if ((node.arcs[node.numArcs - 1].label - node.arcs[0].label + 1) != scratchArc.numArcs()
                        || node.numArcs != FST.Arc.BitTable.countBits(scratchArc, in)) {
                    return false;
                }
            }
        }

        for (int arcUpto = 0; arcUpto < node.numArcs; arcUpto++) {
            final FSTCompiler.Arc<T> arc = node.arcs[arcUpto];
            if (arc.label != scratchArc.label() ||
                    !arc.output.equals(scratchArc.output()) ||
                    ((FSTCompiler.CompiledNode) arc.target).node != scratchArc.target() ||
                    !arc.nextFinalOutput.equals(scratchArc.nextFinalOutput()) ||
                    arc.isFinal != scratchArc.isFinal()) {
                return false;
            }

            if (scratchArc.isLast()) {
                if (arcUpto == node.numArcs - 1) {
                    return true;
                } else {
                    return false;
                }
            }
            fst.readNextRealArc(scratchArc, in);
        }

        return false;
    }

    // hash code for an unfrozen node.  This must be identical
    // to the frozen case (below)!!   计算一个未冻结的节点的hash值
    private long hash(FSTCompiler.UnCompiledNode<T> node) {
        final int PRIME = 31;
        //System.out.println("hash unfrozen");
        long h = 0;
        // TODO: maybe if number of arcs is high we can safely subsample?
        // 遍历该节点的 arc
        for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
            final FSTCompiler.Arc<T> arc = node.arcs[arcIdx];
            //System.out.println("  label=" + arc.label + " target=" + ((Builder.CompiledNode) arc.target).node + " h=" + h + " output=" + fst.outputs.outputToString(arc.output) + " isFinal?=" + arc.isFinal);
            h = PRIME * h + arc.label;
            long n = ((FSTCompiler.CompiledNode) arc.target).node;
            h = PRIME * h + (int) (n ^ (n >> 32));
            h = PRIME * h + arc.output.hashCode();
            h = PRIME * h + arc.nextFinalOutput.hashCode();
            if (arc.isFinal) {
                h += 17;
            }
        }
        //System.out.println("  ret " + (h&Integer.MAX_VALUE));
        return h & Long.MAX_VALUE;
    }

    /**
     * 计算某个地址的hash值
     *
     * @param node
     * @return
     * @throws IOException
     */
    private long hash(long node) throws IOException {
        final int PRIME = 31;
        //System.out.println("hash frozen node=" + node);
        long h = 0;
        // 将数据读取到 scratchArc 中
        fst.readFirstRealTargetArc(node, scratchArc, in);
        // 从目标位置开始 直到读取到末尾 并根据中途读取到的数据生成hash
        while (true) {
            // System.out.println("  label=" + scratchArc.label + " target=" + scratchArc.target + " h=" + h + " output=" + fst.outputs.outputToString(scratchArc.output) + " next?=" + scratchArc.flag(4) + " final?=" + scratchArc.isFinal() + " pos=" + in.getPosition());
            h = PRIME * h + scratchArc.label();
            h = PRIME * h + (int) (scratchArc.target() ^ (scratchArc.target() >> 32));
            h = PRIME * h + scratchArc.output().hashCode();
            h = PRIME * h + scratchArc.nextFinalOutput().hashCode();
            if (scratchArc.isFinal()) {
                h += 17;
            }
            if (scratchArc.isLast()) {
                break;
            }
            fst.readNextRealArc(scratchArc, in);
        }
        //System.out.println("  ret " + (h&Integer.MAX_VALUE));
        return h & Long.MAX_VALUE;
    }

    /**
     * 将某个节点数据写入到 缓存 以及 fst中
     * @param fstCompiler    本次写入的节点所属的 compiler
     * @param nodeIn      被加入的是某个未完成的节点
     * @return
     * @throws IOException
     */
    public long add(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn) throws IOException {
        //System.out.println("hash: add count=" + count + " vs " + table.size() + " mask=" + mask);
        // 通过散列函数后计算出hash值
        final long h = hash(nodeIn);
        // 取余计算 下标
        long pos = h & mask;
        int c = 0;
        while (true) {
            // 从桶中找到对应下标的数据
            final long v = table.get(pos);
            // 代表该位置还没有设置值
            if (v == 0) {
                // freeze & add
                // 将节点冻结后存储到fst中 实际上就是存储到ByteStore中
                final long node = fst.addNode(fstCompiler, nodeIn);
                //System.out.println("  now freeze node=" + node);
                assert hash(node) == h : "frozenHash=" + hash(node) + " vs h=" + h;
                count++;
                // 将存储到fst后返回的地址信息保存起来
                table.set(pos, node);
                // Rehash at 2/3 occupancy:
                // 当前桶中的数据如果超过了 2/3  进行重hash
                if (count > 2 * table.size() / 3) {
                    rehash();
                }
                // 成功添加到bucket 后 返回node   对应nodeIn的地址
                return node;
            // 如果已经设置到桶中了   直接返回在fst中的地址
            } else if (nodesEqual(nodeIn, v)) {
                // same node is already here
                return v;
            }

            // quadratic probe
            // 线性探测法
            pos = (pos + (++c)) & mask;
        }
    }

    // called only by rehash
    // 当发生重 hash时 将某个地址加入到新table 中
    private void addNew(long address) throws IOException {
        // 计算新下标
        long pos = hash(address) & mask;
        int c = 0;
        while (true) {
            if (table.get(pos) == 0) {
                table.set(pos, address);
                break;
            }

            // quadratic probe
            // 线性探测法解决冲突
            pos = (pos + (++c)) & mask;
        }
    }

    /**
     * 扩容 并转移桶中的数据
     * @throws IOException
     */
    private void rehash() throws IOException {
        final PagedGrowableWriter oldTable = table;

        // 前几次分配 桶的大小远远不及 pageSize  之后 桶中的总元素会越来越大  使用的页也会越来越多
        table = new PagedGrowableWriter(2 * oldTable.size(), 1 << 30, PackedInts.bitsRequired(count), PackedInts.COMPACT);
        mask = table.size() - 1;
        for (long idx = 0; idx < oldTable.size(); idx++) {
            // 从旧桶中将数据读取出来 重新加入到桶中
            final long address = oldTable.get(idx);
            if (address != 0) {
                addNew(address);
            }
        }
    }

}
