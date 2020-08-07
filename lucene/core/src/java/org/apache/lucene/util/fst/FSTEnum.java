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


import static org.apache.lucene.util.fst.FST.Arc.BitTable;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;

/**
 * Can next() and advance() through the terms in an FST
 *
 * @lucene.experimental 在 lucene中 迭代器都被叫做 Enum
 * 该对象是从fst中读取数据
 */

abstract class FSTEnum<T> {

    /**
     * 数据所在的fst
     */
    protected final FST<T> fst;

    /**
     * 负责读取某个node下的arc信息  0 存储的是firstArc 代表一个空的arc
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected FST.Arc<T>[] arcs = new FST.Arc[10];
    // outputs are cumulative
    /**
     * 每个arc对应的权重信息 0存储的是空值
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected T[] output = (T[]) new Object[10];

    protected final T NO_OUTPUT;
    /**
     * 该对象负责从 fst中读取数据   (一般是反向读取的容器)
     */
    protected final FST.BytesReader fstReader;

    /**
     * 对应迭代器光标  0存储的是firstArc 没有实际意义 数据从1开始读取
     */
    protected int upto;
    int targetLength;

    /**
     * doFloor controls the behavior of advance: if it's true
     * doFloor is true, advance positions to the biggest
     * term before target.
     */
    FSTEnum(FST<T> fst) {
        this.fst = fst;
        // 反向读取对象
        fstReader = fst.getBytesReader();
        NO_OUTPUT = fst.outputs.getNoOutput();
        // 此时读取的是一个空的arc  它的target指向 编译完成的fst的首个arc的地址
        fst.getFirstArc(getArc(0));
        output[0] = NO_OUTPUT;
    }

    protected abstract int getTargetLabel();

    protected abstract int getCurrentLabel();

    protected abstract void setCurrentLabel(int label);

    protected abstract void grow();

    /**
     * Rewinds enum state to match the shared prefix between
     * current term and target term
     */
    // 尽可能的先匹配前缀  目标就是至少此时current的值要比target小
    private void rewindPrefix() throws IOException {
        if (upto == 0) {
            //System.out.println("  init");
            upto = 1;
            // 代表准备工作完成
            fst.readFirstTargetArc(getArc(0), getArc(1), fstReader);
            return;
        }
        //System.out.println("  rewind upto=" + upto + " vs targetLength=" + targetLength);

        final int currentLimit = upto;
        upto = 1;

        // 这里是在匹配此时的前缀值  如果此时已经有部分匹配的前缀 那么就以这个前缀作为起点往下查询 否则就从头开始查询
        // upto 要去掉首位2个特殊的arc 才是真正有效的arc长度
        while (upto < currentLimit && upto <= targetLength + 1) {
            final int cmp = getCurrentLabel() - getTargetLabel();
            // current < target的时候 认为处理完毕了
            if (cmp < 0) {
                // seek forward
                //System.out.println("    seek fwd");
                break;
            } else if (cmp > 0) {
                // seek backwards -- reset this arc to the first arc
                final FST.Arc<T> arc = getArc(upto);
                // 重新从 下游的首个arc开始  注意此时后面的arc很可能还是不一致的  但是后面的部分可以忽略 因为current是 ByteRef类型 也就是可以从外部设置长度
                fst.readFirstTargetArc(getArc(upto - 1), arc, fstReader);
                //System.out.println("    seek first arc");
                break;
            }
            upto++;
        }
        //System.out.println("  fall through upto=" + upto);
    }

    /**
     * 从 fst中读取下一个数据
     *
     * @throws IOException
     */
    protected void doNext() throws IOException {
        //System.out.println("FE: next upto=" + upto);
        if (upto == 0) {
            //System.out.println("  init");
            upto = 1;
            // 开始读取第一个有实际意义的数据
            fst.readFirstTargetArc(getArc(0), getArc(1), fstReader);
        } else {
            // pop
            //System.out.println("  check pop curArc target=" + arcs[upto].target + " label=" + arcs[upto].label + " isLast?=" + arcs[upto].isLast());
            // 2种情况会出现 isLast为true 第一种 当前arc为node的最后一条边 第二种 当前arc是某个arc链的末尾 他后面的node.arcNum为0
            // 比如 at/2 att/5 在反向读取fst的数据时 会先读取 at的数据 此时arcs[]为 起始arc，a，t，终止arc
            // 这时 终止arc对应的target不是0(因为后面还有个t 所以不需要往回读 直接将终止arc 修改成t) 之后在 pushFirst 发现t携带final标识 补上一个终止arc 最后就变成
            // 起始arc，a，t，t, 终止arc 并且当前终止arc上会携带 isLast 之后 t,t都是isLast 所以回退到某条非isLast 边 (在迭代过程中 都是从非isLast边往下 直到某条isLast边 )
            while (arcs[upto].isLast()) {

                // 代表此时arc的上游是第一个node 且也已经是最后一条边了  中途没有出现任何的分叉 所以最后一条边的数据也读取完了
                upto--;
                if (upto == 0) {
                    //System.out.println("  eof");
                    return;
                }
            }
            fst.readNextArc(arcs[upto], fstReader);
        }

        // 当调用完该方法时 此时已经读取了一个完整的词  同时 output[] 内 upto 对应的值就是该term的权重  (或者说文档id)
        pushFirst();
    }

    // TODO: should we return a status here (SEEK_FOUND / SEEK_NOT_FOUND /
    // SEEK_END)?  saves the eq check above?

    /**
     * Seeks to smallest term that's &gt;= target.
     */
    // 查找大于等于 target 的词
    protected void doSeekCeil() throws IOException {

        //System.out.println("    advance len=" + target.length + " curlen=" + current.length);

        // TODO: possibly caller could/should provide common
        // prefix length?  ie this work may be redundant if
        // caller is in fact intersecting against its own
        // automaton

        //System.out.println("FE.seekCeil upto=" + upto);

        // Save time by starting at the end of the shared prefix
        // b/w our current term & the target:
        // 做一些前缀匹配的准备工作 至少要让此时的arc.label 比target.label 小
        rewindPrefix();
        //System.out.println("  after rewind upto=" + upto);

        // 从该值开始出现不一致
        FST.Arc<T> arc = getArc(upto);
        //System.out.println("  init targetLabel=" + targetLabel);

        // Now scan forward, matching the new suffix of the target
        while (arc != null) {
            int targetLabel = getTargetLabel();
            // System.out.println("  cycle upto=" + upto + " arc.label=" + arc.label + " (" + (char) arc.label + ") vs targetLabel=" + targetLabel);
            // TODO 代表采用了 arc等长存储 跳过这部分 对理解FST的读取没有影响
            if (arc.bytesPerArc() != 0 && arc.label() != FST.END_LABEL) {
                // Arcs are in an array
                final FST.BytesReader in = fst.getBytesReader();
                if (arc.nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING) {
                    arc = doSeekCeilArrayDirectAddressing(arc, targetLabel, in);
                } else {
                    assert arc.nodeFlags() == FST.ARCS_FOR_BINARY_SEARCH;
                    arc = doSeekCeilArrayPacked(arc, targetLabel, in);
                }
            } else {
                arc = doSeekCeilList(arc, targetLabel);
            }
        }
    }

    private FST.Arc<T> doSeekCeilArrayDirectAddressing(final FST.Arc<T> arc, final int targetLabel, final FST.BytesReader in) throws IOException {
        // The array is addressed directly by label, with presence bits to compute the actual arc offset.

        int targetIndex = targetLabel - arc.firstLabel();
        if (targetIndex >= arc.numArcs()) {
            // Target is beyond the last arc, out of label range.
            // Dead end (target is after the last arc);
            // rollback to last fork then push
            upto--;
            while (true) {
                if (upto == 0) {
                    return null;
                }
                final FST.Arc<T> prevArc = getArc(upto);
                //System.out.println("  rollback upto=" + upto + " arc.label=" + prevArc.label + " isLast?=" + prevArc.isLast());
                if (!prevArc.isLast()) {
                    fst.readNextArc(prevArc, fstReader);
                    pushFirst();
                    return null;
                }
                upto--;
            }
        } else {
            if (targetIndex < 0) {
                targetIndex = -1;
            } else if (BitTable.isBitSet(targetIndex, arc, in)) {
                fst.readArcByDirectAddressing(arc, in, targetIndex);
                assert arc.label() == targetLabel;
                // found -- copy pasta from below
                output[upto] = fst.outputs.add(output[upto - 1], arc.output());
                if (targetLabel == FST.END_LABEL) {
                    return null;
                }
                setCurrentLabel(arc.label());
                incr();
                return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
            }
            // Not found, return the next arc (ceil).
            int ceilIndex = BitTable.nextBitSet(targetIndex, arc, in);
            assert ceilIndex != -1;
            fst.readArcByDirectAddressing(arc, in, ceilIndex);
            assert arc.label() > targetLabel;
            pushFirst();
            return null;
        }
    }

    private FST.Arc<T> doSeekCeilArrayPacked(final FST.Arc<T> arc, final int targetLabel, final FST.BytesReader in) throws IOException {
        // The array is packed -- use binary search to find the target.
        int idx = Util.binarySearch(fst, arc, targetLabel);
        if (idx >= 0) {
            // Match
            fst.readArcByIndex(arc, in, idx);
            assert arc.arcIdx() == idx;
            assert arc.label() == targetLabel : "arc.label=" + arc.label() + " vs targetLabel=" + targetLabel + " mid=" + idx;
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            if (targetLabel == FST.END_LABEL) {
                return null;
            }
            setCurrentLabel(arc.label());
            incr();
            return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
        }
        idx = -1 - idx;
        if (idx == arc.numArcs()) {
            // Dead end
            fst.readArcByIndex(arc, in, idx - 1);
            assert arc.isLast();
            // Dead end (target is after the last arc);
            // rollback to last fork then push
            upto--;
            while (true) {
                if (upto == 0) {
                    return null;
                }
                final FST.Arc<T> prevArc = getArc(upto);
                //System.out.println("  rollback upto=" + upto + " arc.label=" + prevArc.label + " isLast?=" + prevArc.isLast());
                if (!prevArc.isLast()) {
                    fst.readNextArc(prevArc, fstReader);
                    pushFirst();
                    return null;
                }
                upto--;
            }
        } else {
            // Ceiling - arc with least higher label
            fst.readArcByIndex(arc, in, idx);
            assert arc.label() > targetLabel;
            pushFirst();
            return null;
        }
    }

    /**
     * @param arc         从arc开始往后 直到与 targetLabel 匹配 或者大于targetLabel的最小值
     * @param targetLabel
     * @return
     * @throws IOException
     */
    private FST.Arc<T> doSeekCeilList(final FST.Arc<T> arc, final int targetLabel) throws IOException {
        // Arcs are not array'd -- must do linear scan:
        // 此时完全匹配 开始读取下一个值
        if (arc.label() == targetLabel) {
            // recurse
            // 刷新权重值 因为之前调用 rewindPrefix 的时候 没有替换掉旧的权重
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            // 代表此时是读取到末尾了  忽略
            if (targetLabel == FST.END_LABEL) {
                return null;
            }
            setCurrentLabel(arc.label());
            incr();
            // 读取下一个arc  在外层做了while循环 所以会不断匹配
            return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
            // 代表此时就是 大于target的最小值 替换后面的arc 并返回null (返回null后在外层的while会退出循环)
        } else if (arc.label() > targetLabel) {
            pushFirst();
            return null;
            // 代表此时 arc 已经达到末尾了 此时应该就是要获取下一个arc 并检测是否大于 target 如果大于则使用大的值退出循环  否则 使用新的arc 继续匹配
        } else if (arc.isLast()) {
            // Dead end (target is after the last arc);
            // rollback to last fork then push
            upto--;
            while (true) {
                if (upto == 0) {
                    return null;
                }
                final FST.Arc<T> prevArc = getArc(upto);
                //System.out.println("  rollback upto=" + upto + " arc.label=" + prevArc.label + " isLast?=" + prevArc.isLast());
                // 在之前的arc都匹配的情况下 发现读取到末尾了 这时最近的一个分叉点的下一个arc 必然就是大于目标arc的最小arc
                if (!prevArc.isLast()) {
                    // 使用同一个节点的下一个arc 并刷新后面的arc
                    fst.readNextArc(prevArc, fstReader);
                    pushFirst();
                    return null;
                }
                upto--;
            }
        } else {
            // 小于的情况 读取下一个arc
            // keep scanning
            //System.out.println("    next scan");
            fst.readNextArc(arc, fstReader);
        }
        return arc;
    }

    // Todo: should we return a status here (SEEK_FOUND / SEEK_NOT_FOUND /
    // SEEK_END)?  saves the eq check above?

    /**
     * Seeks to largest term that's &lt;= target.
     */
    // 找到 小于等于target的最大值
    void doSeekFloor() throws IOException {

        // TODO: possibly caller could/should provide common
        // prefix length?  ie this work may be redundant if
        // caller is in fact intersecting against its own
        // automaton
        //System.out.println("FE: seek floor upto=" + upto);

        // Save CPU by starting at the end of the shared prefix
        // b/w our current term & the target:
        // 此时尽可能匹配前缀 并且不同的地方 arc为上一个节点的第一个arc
        rewindPrefix();

        //System.out.println("FE: after rewind upto=" + upto);

        FST.Arc<T> arc = getArc(upto);

        //System.out.println("FE: init targetLabel=" + targetLabel);

        // Now scan forward, matching the new suffix of the target
        while (arc != null) {
            //System.out.println("  cycle upto=" + upto + " arc.label=" + arc.label + " (" + (char) arc.label + ") targetLabel=" + targetLabel + " isLast?=" + arc.isLast() + " bba=" + arc.bytesPerArc);
            int targetLabel = getTargetLabel();

            if (arc.bytesPerArc() != 0 && arc.label() != FST.END_LABEL) {
                // Arcs are in an array
                final FST.BytesReader in = fst.getBytesReader();
                if (arc.nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING) {
                    arc = doSeekFloorArrayDirectAddressing(arc, targetLabel, in);
                } else {
                    assert arc.nodeFlags() == FST.ARCS_FOR_BINARY_SEARCH;
                    arc = doSeekFloorArrayPacked(arc, targetLabel, in);
                }
            } else {
                arc = doSeekFloorList(arc, targetLabel);
            }
        }
    }

    private FST.Arc<T> doSeekFloorArrayDirectAddressing(FST.Arc<T> arc, int targetLabel, FST.BytesReader in) throws IOException {
        // The array is addressed directly by label, with presence bits to compute the actual arc offset.

        int targetIndex = targetLabel - arc.firstLabel();
        if (targetIndex < 0) {
            // Before first arc.
            return backtrackToFloorArc(arc, targetLabel, in);
        } else if (targetIndex >= arc.numArcs()) {
            // After last arc.
            fst.readLastArcByDirectAddressing(arc, in);
            assert arc.label() < targetLabel;
            assert arc.isLast();
            pushLast();
            return null;
        } else {
            // Within label range.
            if (BitTable.isBitSet(targetIndex, arc, in)) {
                fst.readArcByDirectAddressing(arc, in, targetIndex);
                assert arc.label() == targetLabel;
                // found -- copy pasta from below
                output[upto] = fst.outputs.add(output[upto - 1], arc.output());
                if (targetLabel == FST.END_LABEL) {
                    return null;
                }
                setCurrentLabel(arc.label());
                incr();
                return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
            }
            // Scan backwards to find a floor arc.
            int floorIndex = BitTable.previousBitSet(targetIndex, arc, in);
            assert floorIndex != -1;
            fst.readArcByDirectAddressing(arc, in, floorIndex);
            assert arc.label() < targetLabel;
            assert arc.isLast() || fst.readNextArcLabel(arc, in) > targetLabel;
            pushLast();
            return null;
        }
    }

    /**
     * Backtracks until it finds a node which first arc is before our target label.`
     * Then on the node, finds the arc just before the targetLabel.
     *
     * @return null to continue the seek floor recursion loop.
     */
    private FST.Arc<T> backtrackToFloorArc(FST.Arc<T> arc, int targetLabel, final FST.BytesReader in) throws IOException {
        while (true) {
            // First, walk backwards until we find a node which first arc is before our target label.
            fst.readFirstTargetArc(getArc(upto - 1), arc, fstReader);
            if (arc.label() < targetLabel) {
                // Then on this node, find the arc just before the targetLabel.
                if (!arc.isLast()) {
                    if (arc.bytesPerArc() != 0 && arc.label() != FST.END_LABEL) {
                        if (arc.nodeFlags() == FST.ARCS_FOR_BINARY_SEARCH) {
                            findNextFloorArcBinarySearch(arc, targetLabel, in);
                        } else {
                            assert arc.nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING;
                            findNextFloorArcDirectAddressing(arc, targetLabel, in);
                        }
                    } else {
                        while (!arc.isLast() && fst.readNextArcLabel(arc, in) < targetLabel) {
                            fst.readNextArc(arc, fstReader);
                        }
                    }
                }
                assert arc.label() < targetLabel;
                assert arc.isLast() || fst.readNextArcLabel(arc, in) >= targetLabel;
                pushLast();
                return null;
            }
            upto--;
            if (upto == 0) {
                return null;
            }
            targetLabel = getTargetLabel();
            arc = getArc(upto);
        }
    }

    /**
     * Finds and reads an arc on the current node which label is strictly less than the given label.
     * Skips the first arc, finds next floor arc; or none if the floor arc is the first
     * arc itself (in this case it has already been read).
     * <p>
     * Precondition: the given arc is the first arc of the node.
     */
    private void findNextFloorArcDirectAddressing(FST.Arc<T> arc, int targetLabel, final FST.BytesReader in) throws IOException {
        assert arc.nodeFlags() == FST.ARCS_FOR_DIRECT_ADDRESSING;
        assert arc.label() != FST.END_LABEL;
        assert arc.label() == arc.firstLabel();
        if (arc.numArcs() > 1) {
            int targetIndex = targetLabel - arc.firstLabel();
            assert targetIndex >= 0;
            if (targetIndex >= arc.numArcs()) {
                // Beyond last arc. Take last arc.
                fst.readLastArcByDirectAddressing(arc, in);
            } else {
                // Take the preceding arc, even if the target is present.
                int floorIndex = BitTable.previousBitSet(targetIndex, arc, in);
                if (floorIndex > 0) {
                    fst.readArcByDirectAddressing(arc, in, floorIndex);
                }
            }
        }
    }

    /**
     * Same as {@link #findNextFloorArcDirectAddressing} for binary search node.
     */
    private void findNextFloorArcBinarySearch(FST.Arc<T> arc, int targetLabel, FST.BytesReader in) throws IOException {
        assert arc.nodeFlags() == FST.ARCS_FOR_BINARY_SEARCH;
        assert arc.label() != FST.END_LABEL;
        assert arc.arcIdx() == 0;
        if (arc.numArcs() > 1) {
            int idx = Util.binarySearch(fst, arc, targetLabel);
            assert idx != -1;
            if (idx > 1) {
                fst.readArcByIndex(arc, in, idx - 1);
            } else if (idx < -2) {
                fst.readArcByIndex(arc, in, -2 - idx);
            }
        }
    }

    private FST.Arc<T> doSeekFloorArrayPacked(FST.Arc<T> arc, int targetLabel, final FST.BytesReader in) throws IOException {
        // Arcs are fixed array -- use binary search to find the target.
        int idx = Util.binarySearch(fst, arc, targetLabel);

        if (idx >= 0) {
            // Match -- recurse
            //System.out.println("  match!  arcIdx=" + idx);
            fst.readArcByIndex(arc, in, idx);
            assert arc.arcIdx() == idx;
            assert arc.label() == targetLabel : "arc.label=" + arc.label() + " vs targetLabel=" + targetLabel + " mid=" + idx;
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            if (targetLabel == FST.END_LABEL) {
                return null;
            }
            setCurrentLabel(arc.label());
            incr();
            return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
        } else if (idx == -1) {
            // Before first arc.
            return backtrackToFloorArc(arc, targetLabel, in);
        } else {
            // There is a floor arc; idx will be (-1 - (floor + 1)).
            fst.readArcByIndex(arc, in, -2 - idx);
            assert arc.isLast() || fst.readNextArcLabel(arc, in) > targetLabel;
            assert arc.label() < targetLabel : "arc.label=" + arc.label() + " vs targetLabel=" + targetLabel;
            pushLast();
            return null;
        }
    }

    /**
     * 找到<= 目标值的第一个arc
     *
     * @param arc
     * @param targetLabel
     * @return
     * @throws IOException
     */
    private FST.Arc<T> doSeekFloorList(FST.Arc<T> arc, int targetLabel) throws IOException {
        if (arc.label() == targetLabel) {
            // Match -- recurse
            // 更新权重值
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            if (targetLabel == FST.END_LABEL) {
                return null;
            }
            setCurrentLabel(arc.label());
            incr();
            return fst.readFirstTargetArc(arc, getArc(upto), fstReader);
            // 代表超过目标值了 需要跳跃到上一个node 的多个arc 并找到首个小于target的值
        } else if (arc.label() > targetLabel) {
            // TODO: if each arc could somehow read the arc just
            // before, we can save this re-scan.  The ceil case
            // doesn't need this because it reads the next arc
            // instead:
            while (true) {
                // First, walk backwards until we find a first arc
                // that's before our target label:
                fst.readFirstTargetArc(getArc(upto - 1), arc, fstReader);
                if (arc.label() < targetLabel) {
                    // Then, scan forwards to the arc just before
                    // the targetLabel:
                    // 在读取到最后一个arc之前 不断读取更大的arc 但同时要确保 < target
                    while (!arc.isLast() && fst.readNextArcLabel(arc, fstReader) < targetLabel) {
                        fst.readNextArc(arc, fstReader);
                    }
                    // pushLast 是往下每次都读取 最后一个arc  与 pushFirst相反
                    pushLast();
                    return null;
                }
                // 如果上一个多arc的最小值 还是超过了target 就继续往上减小
                upto--;
                if (upto == 0) {
                    return null;
                }
                targetLabel = getTargetLabel();
                arc = getArc(upto);
            }
            // 这就是小于的情况 并且不是末尾
        } else if (!arc.isLast()) {
            //System.out.println("  check next label=" + fst.readNextArcLabel(arc) + " (" + (char) fst.readNextArcLabel(arc) + ")");
            // 如果下一个值就比 target大了 就按照当前的arc 读取后面的数据
            if (fst.readNextArcLabel(arc, fstReader) > targetLabel) {
                // 注意这里调用的是 pushLast
                pushLast();
                return null;
            } else {
                // keep scanning
                return fst.readNextArc(arc, fstReader);
            }
        } else {
            // 都是用 finalArc 填充后面的槽
            pushLast();
            return null;
        }
    }

    /**
     * Seeks to exactly target term.
     */
    // 从FST中找到精确匹配的词
    boolean doSeekExact() throws IOException {

        // TODO: possibly caller could/should provide common
        // prefix length?  ie this work may be redundant if
        // caller is in fact intersecting against its own
        // automaton

        //System.out.println("FE: seek exact upto=" + upto);

        // Save time by starting at the end of the shared prefix
        // b/w our current term & the target:
        rewindPrefix();

        //System.out.println("FE: after rewind upto=" + upto);
        // 从最后一个匹配的arc开始处理
        FST.Arc<T> arc = getArc(upto - 1);
        int targetLabel = getTargetLabel();

        final FST.BytesReader fstReader = fst.getBytesReader();

        while (true) {
            //System.out.println("  cycle target=" + (targetLabel == -1 ? "-1" : (char) targetLabel));
            // 精确匹配目标 arc
            final FST.Arc<T> nextArc = fst.findTargetArc(targetLabel, arc, getArc(upto), fstReader);
            // 代表没有找到匹配的arc
            if (nextArc == null) {
                // short circuit
                //upto--;
                //upto = 0;
                // 此时将最后一个upto 修改成 关联node下首个arc
                fst.readFirstTargetArc(arc, getArc(upto), fstReader);
                //System.out.println("  no match upto=" + upto);
                return false;
            }
            // Match -- recurse:
            // 代表该label 已经匹配成功继续往下遍历 匹配剩余的部分
            output[upto] = fst.outputs.add(output[upto - 1], nextArc.output());
            // 完全匹配
            if (targetLabel == FST.END_LABEL) {
                //System.out.println("  return found; upto=" + upto + " output=" + output[upto] + " nextArc=" + nextArc.isLast());
                return true;
            }
            setCurrentLabel(targetLabel);
            incr();
            targetLabel = getTargetLabel();
            arc = nextArc;
        }
    }

    /**
     * 进行扩容
     */
    private void incr() {
        upto++;
        grow();
        if (arcs.length <= upto) {
            @SuppressWarnings({"rawtypes", "unchecked"}) final FST.Arc<T>[] newArcs =
                    new FST.Arc[ArrayUtil.oversize(1 + upto, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
            System.arraycopy(arcs, 0, newArcs, 0, arcs.length);
            arcs = newArcs;
        }
        if (output.length <= upto) {
            @SuppressWarnings({"rawtypes", "unchecked"}) final T[] newOutput =
                    (T[]) new Object[ArrayUtil.oversize(1 + upto, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
            System.arraycopy(output, 0, newOutput, 0, output.length);
            output = newOutput;
        }
    }

    // Appends current arc, and then recurses from its target,
    // appending first arc all the way to the final node
    // 根据此时arc的数据 读取词信息
    private void pushFirst() throws IOException {

        // 找到通过readFirstTargetArc 设置的数据
        FST.Arc<T> arc = arcs[upto];
        assert arc != null;

        // 计算权重总和 权重
        while (true) {
            // 以上一个权重值作为基准 开始累加权重
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            // 代表此时已经读取到末尾  这个arc 也加入到数组中了 但是它的字面量没有设置到 label数组中
            if (arc.label() == FST.END_LABEL) {
                // Final node
                break;
            }
            //System.out.println("  pushFirst label=" + (char) arc.label + " upto=" + upto + " output=" + fst.outputs.outputToString(output[upto]));
            // 设置此时的字面量
            setCurrentLabel(arc.label());
            // 对存储字面量的数组 和 权重的数组大小进行检测 是否需要扩容
            incr();

            // 在 incr() 中 upto已经前移了 这里获取一个新的 arc
            final FST.Arc<T> nextArc = getArc(upto);
            // 根据上一个arc的信息 读取数据并填充到新的arc中
            fst.readFirstTargetArc(arc, nextArc, fstReader);
            arc = nextArc;
        }
    }

    // Recurses from current arc, appending last arc all the
    // way to the first final node
    // 每次都先读取 lastArc
    private void pushLast() throws IOException {

        FST.Arc<T> arc = arcs[upto];
        assert arc != null;

        while (true) {
            setCurrentLabel(arc.label());
            output[upto] = fst.outputs.add(output[upto - 1], arc.output());
            if (arc.label() == FST.END_LABEL) {
                // Final node
                break;
            }
            incr();

            arc = fst.readLastTargetArc(arc, getArc(upto), fstReader);
        }
    }

    /**
     * 获取对应的arc 不存在则创建
     *
     * @param idx
     * @return
     */
    private FST.Arc<T> getArc(int idx) {
        if (arcs[idx] == null) {
            arcs[idx] = new FST.Arc<>();
        }
        return arcs[idx];
    }

}
