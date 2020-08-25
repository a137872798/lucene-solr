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
package org.apache.lucene.codecs.blocktree;


import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Iterates through terms in this field.
 * 该对象负责遍历 目标field下所有的term
 */
final class SegmentTermsEnum extends BaseTermsEnum {

    // Lazy init:
    // 该对象负责读取数据
    IndexInput in;

    /**
     * 除了第一个frame外 每当遇到一个新的blockNode 就要增加一个frame代表嵌套了一层
     */
    private SegmentTermsEnumFrame[] stack;
    private final SegmentTermsEnumFrame staticFrame;
    /**
     * 当前指向的 frame
     */
    SegmentTermsEnumFrame currentFrame;

    /**
     * 代表此时 term已经填充了一个完整的term
     */
    boolean termExists;
    // 文件流
    final FieldReader fr;

    private int targetBeforeCurrentLength;

    //static boolean DEBUG = BlockTreeTermsWriter.DEBUG;

    /**
     * 该对象使用一个 byte[] 模拟一个输入流
     */
    private final ByteArrayDataInput scratchReader = new ByteArrayDataInput();

    // What prefix of the current term was present in the index; when we only next() through the index, this stays at 0.  It's only set when
    // we seekCeil/Exact:
    private int validIndexPrefix;

    // assert only:
    // 是否读取到末尾
    private boolean eof;

    /**
     * frame 负责读取数据 并将此时的term 回填到这里
     */
    final BytesRefBuilder term = new BytesRefBuilder();
    private final FST.BytesReader fstReader;

    /**
     * 他应该就是动态拓展的数组 每个元素对应 fst的一条边 多条边 就构成了一个term
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private FST.Arc<BytesRef>[] arcs = new FST.Arc[1];

    /**
     * 该对象通过 fst进行初始化 负责迭代 term
     *
     * @param fr
     * @throws IOException
     */
    public SegmentTermsEnum(FieldReader fr) throws IOException {
        this.fr = fr;

        // if (DEBUG) {
        //   System.out.println("BTTR.init seg=" + fr.parent.segment);
        // }
        stack = new SegmentTermsEnumFrame[0];

        // Used to hold seek by TermState, or cached seek
        // 生成一个用于存储 field下某个term 相关信息的对象
        staticFrame = new SegmentTermsEnumFrame(this, -1);

        // 代表该field 下term的词典
        if (fr.index == null) {
            fstReader = null;
        } else {
            fstReader = fr.index.getBytesReader();
        }

        // Init w/ root block; don't use index since it may
        // not (and need not) have been loaded
        for (int arcIdx = 0; arcIdx < arcs.length; arcIdx++) {
            arcs[arcIdx] = new FST.Arc<>();
        }

        currentFrame = staticFrame;
        // 生成初始节点  在 FST的迭代器中可以看到存在2个虚拟节点 这个就对应开始的那个虚拟节点
        final FST.Arc<BytesRef> arc;
        if (fr.index != null) {
            arc = fr.index.getFirstArc(arcs[0]);
            // Empty string prefix must have an output in the index!
            assert arc.isFinal();
        } else {
            arc = null;
        }
        //currentFrame = pushFrame(arc, rootCode, 0);
        //currentFrame.loadBlock();
        validIndexPrefix = 0;
        // if (DEBUG) {
        //   System.out.println("init frame state " + currentFrame.ord);
        //   printSeekState();
        // }

        //System.out.println();
        // computeBlockStats().print(System.out);
    }

    // Not private to avoid synthetic access$NNN methods
    // 延迟初始化 就是设置 termIn
    void initIndexInput() {
        if (this.in == null) {
            this.in = fr.parent.termsIn.clone();
        }
    }

    /**
     * Runs next() through the entire terms dict,
     * computing aggregate statistics.
     */
    public Stats computeBlockStats() throws IOException {

        Stats stats = new Stats(fr.parent.segment, fr.fieldInfo.name);
        if (fr.index != null) {
            stats.indexNumBytes = fr.index.ramBytesUsed();
        }

        currentFrame = staticFrame;
        FST.Arc<BytesRef> arc;
        if (fr.index != null) {
            arc = fr.index.getFirstArc(arcs[0]);
            // Empty string prefix must have an output in the index!
            assert arc.isFinal();
        } else {
            arc = null;
        }

        // Empty string prefix must have an output in the
        // index!
        currentFrame = pushFrame(arc, fr.rootCode, 0);
        currentFrame.fpOrig = currentFrame.fp;
        currentFrame.loadBlock();
        validIndexPrefix = 0;

        stats.startBlock(currentFrame, !currentFrame.isLastInFloor);

        allTerms:
        while (true) {

            // Pop finished blocks
            while (currentFrame.nextEnt == currentFrame.entCount) {
                stats.endBlock(currentFrame);
                if (!currentFrame.isLastInFloor) {
                    // Advance to next floor block
                    currentFrame.loadNextFloorBlock();
                    stats.startBlock(currentFrame, true);
                    break;
                } else {
                    if (currentFrame.ord == 0) {
                        break allTerms;
                    }
                    final long lastFP = currentFrame.fpOrig;
                    currentFrame = stack[currentFrame.ord - 1];
                    assert lastFP == currentFrame.lastSubFP;
                    // if (DEBUG) {
                    //   System.out.println("  reset validIndexPrefix=" + validIndexPrefix);
                    // }
                }
            }

            while (true) {
                if (currentFrame.next()) {
                    // Push to new block:
                    currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length());
                    currentFrame.fpOrig = currentFrame.fp;
                    // This is a "next" frame -- even if it's
                    // floor'd we must pretend it isn't so we don't
                    // try to scan to the right floor frame:
                    currentFrame.loadBlock();
                    stats.startBlock(currentFrame, !currentFrame.isLastInFloor);
                } else {
                    stats.term(term.get());
                    break;
                }
            }
        }

        stats.finish();

        // Put root frame back:
        currentFrame = staticFrame;
        if (fr.index != null) {
            arc = fr.index.getFirstArc(arcs[0]);
            // Empty string prefix must have an output in the index!
            assert arc.isFinal();
        } else {
            arc = null;
        }
        currentFrame = pushFrame(arc, fr.rootCode, 0);
        currentFrame.rewind();
        currentFrame.loadBlock();
        validIndexPrefix = 0;
        term.clear();

        return stats;
    }

    /**
     * 根据 ord 生成一个新的 frame 对象
     * @param ord
     * @return
     * @throws IOException
     */
    private SegmentTermsEnumFrame getFrame(int ord) throws IOException {
        // 代表需要扩容
        if (ord >= stack.length) {
            final SegmentTermsEnumFrame[] next = new SegmentTermsEnumFrame[ArrayUtil.oversize(1 + ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
            System.arraycopy(stack, 0, next, 0, stack.length);
            for (int stackOrd = stack.length; stackOrd < next.length; stackOrd++) {
                next[stackOrd] = new SegmentTermsEnumFrame(this, stackOrd);
            }
            stack = next;
        }
        assert stack[ord].ord == ord;
        return stack[ord];
    }

    private FST.Arc<BytesRef> getArc(int ord) {
        if (ord >= arcs.length) {
            @SuppressWarnings({"rawtypes", "unchecked"}) final FST.Arc<BytesRef>[] next =
                    new FST.Arc[ArrayUtil.oversize(1 + ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
            System.arraycopy(arcs, 0, next, 0, arcs.length);
            for (int arcOrd = arcs.length; arcOrd < next.length; arcOrd++) {
                next[arcOrd] = new FST.Arc<>();
            }
            arcs = next;
        }
        return arcs[ord];
    }

    /**
     * Pushes a frame we seek'd to
     * 首次调用 frameData 是 rootCode  也就是最后写入 fst的值对应的出度  最后写入的值应该是 "" 对应的出度
     * 首次调用 length为0
     */
    SegmentTermsEnumFrame pushFrame(FST.Arc<BytesRef> arc, BytesRef frameData, int length) throws IOException {
        // 使用临时引用对象
        scratchReader.reset(frameData.bytes, frameData.offset, frameData.length);
        final long code = scratchReader.readVLong();
        // 对应 blockTreeTermsWriter.encodeOutput()
        // fpSeek 写入本blockNode 信息前 termOut 的偏移量
        final long fpSeek = code >>> BlockTreeTermsReader.OUTPUT_FLAGS_NUM_BITS;
        // 相当于获取当前frame的下一个 frame  首次调用返回的是 ord为0的 frame  因为初始状态 currentFrame.ord 为-1
        final SegmentTermsEnumFrame f = getFrame(1 + currentFrame.ord);
        // 生成本批block的所有node 中是否包含 termNode
        f.hasTerms = (code & BlockTreeTermsReader.OUTPUT_FLAG_HAS_TERMS) != 0;
        f.hasTermsOrig = f.hasTerms;
        // 本次所有node 是否存储在一个block中
        f.isFloor = (code & BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR) != 0;
        // 代表不在一个block中  读取 小前缀的值
        if (f.isFloor) {
            f.setFloorData(scratchReader, frameData);
        }
        // 将数据填充到 frame上
        pushFrame(arc, fpSeek, length);

        return f;
    }

    /**
     * Pushes next'd frame or seek'd frame; we later
     * lazy-load the frame only when needed
     *
     * @param arc
     * @param fp
     * @param length
     * @return
     * @throws IOException
     */
    SegmentTermsEnumFrame pushFrame(FST.Arc<BytesRef> arc, long fp, int length) throws IOException {
        // 找到本次要设置的 frame
        final SegmentTermsEnumFrame f = getFrame(1 + currentFrame.ord);
        // 设置对应的边
        f.arc = arc;
        // nextEnt 默认为0  fpOrig 初始状态应该是0  首次应该进入下面的分支
        if (f.fpOrig == fp && f.nextEnt != -1) {
            //if (DEBUG) System.out.println("      push reused frame ord=" + f.ord + " fp=" + f.fp + " isFloor?=" + f.isFloor + " hasTerms=" + f.hasTerms + " pref=" + term + " nextEnt=" + f.nextEnt + " targetBeforeCurrentLength=" + targetBeforeCurrentLength + " term.length=" + term.length + " vs prefix=" + f.prefix);
            //if (f.prefix > targetBeforeCurrentLength) {
            if (f.ord > targetBeforeCurrentLength) {
                f.rewind();
            } else {
                // if (DEBUG) {
                //   System.out.println("        skip rewind!");
                // }
            }
            assert length == f.prefix;
        } else {
            // 这里应该是初始化相关属性
            f.nextEnt = -1;
            f.prefix = length;
            f.state.termBlockOrd = 0;
            f.fpOrig = f.fp = fp;
            f.lastSubFP = -1;
            // if (DEBUG) {
            //   final int sav = term.length;
            //   term.length = length;
            //   System.out.println("      push new frame ord=" + f.ord + " fp=" + f.fp + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " pref=" + brToString(term));
            //   term.length = sav;
            // }
        }

        return f;
    }

    // asserts only
    private boolean clearEOF() {
        eof = false;
        return true;
    }

    // asserts only
    private boolean setEOF() {
        eof = true;
        return true;
    }

  /*
  // for debugging
  @SuppressWarnings("unused")
  static String brToString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      // If BytesRef isn't actually UTF8, or it's eg a
      // prefix of UTF8 that ends mid-unicode-char, we
      // fallback to hex:
      return b.toString();
    }
  }

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(BytesRefBuilder b) {
    return brToString(b.get());
  }
  */

    /**
     * 精准匹配某个term
     * @param target
     * @return
     * @throws IOException
     */
    @Override
    public boolean seekExact(BytesRef target) throws IOException {

        if (fr.index == null) {
            throw new IllegalStateException("terms index was not loaded");
        }

        if (fr.size() > 0 && (target.compareTo(fr.getMin()) < 0 || target.compareTo(fr.getMax()) > 0)) {
            return false;
        }

        term.grow(1 + target.length);

        assert clearEOF();

        // if (DEBUG) {
        //   System.out.println("\nBTTR.seekExact seg=" + fr.parent.segment + " target=" + fr.fieldInfo.name + ":" + brToString(target) + " current=" + brToString(term) + " (exists?=" + termExists + ") validIndexPrefix=" + validIndexPrefix);
        //   printSeekState(System.out);
        // }

        FST.Arc<BytesRef> arc;
        int targetUpto;
        BytesRef output;

        targetBeforeCurrentLength = currentFrame.ord;

        if (currentFrame != staticFrame) {

            // We are already seek'd; find the common
            // prefix of new seek term vs current term and
            // re-use the corresponding seek state.  For
            // example, if app first seeks to foobar, then
            // seeks to foobaz, we can re-use the seek state
            // for the first 5 bytes.

            // if (DEBUG) {
            //   System.out.println("  re-use current seek state validIndexPrefix=" + validIndexPrefix);
            // }

            arc = arcs[0];
            assert arc.isFinal();
            output = arc.output();
            targetUpto = 0;

            SegmentTermsEnumFrame lastFrame = stack[0];
            assert validIndexPrefix <= term.length();

            final int targetLimit = Math.min(target.length, validIndexPrefix);

            int cmp = 0;

            // TODO: reverse vLong byte order for better FST
            // prefix output sharing

            // First compare up to valid seek frames:
            while (targetUpto < targetLimit) {
                cmp = (term.byteAt(targetUpto) & 0xFF) - (target.bytes[target.offset + targetUpto] & 0xFF);
                // if (DEBUG) {
                //    System.out.println("    cycle targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")"   + " arc.output=" + arc.output + " output=" + output);
                // }
                if (cmp != 0) {
                    break;
                }
                arc = arcs[1 + targetUpto];
                assert arc.label() == (target.bytes[target.offset + targetUpto] & 0xFF) : "arc.label=" + (char) arc.label() + " targetLabel=" + (char) (target.bytes[target.offset + targetUpto] & 0xFF);
                if (arc.output() != BlockTreeTermsReader.NO_OUTPUT) {
                    output = BlockTreeTermsReader.FST_OUTPUTS.add(output, arc.output());
                }
                if (arc.isFinal()) {
                    lastFrame = stack[1 + lastFrame.ord];
                }
                targetUpto++;
            }

            if (cmp == 0) {
                final int targetUptoMid = targetUpto;

                // Second compare the rest of the term, but
                // don't save arc/output/frame; we only do this
                // to find out if the target term is before,
                // equal or after the current term
                final int targetLimit2 = Math.min(target.length, term.length());
                while (targetUpto < targetLimit2) {
                    cmp = (term.byteAt(targetUpto) & 0xFF) - (target.bytes[target.offset + targetUpto] & 0xFF);
                    // if (DEBUG) {
                    //    System.out.println("    cycle2 targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")");
                    // }
                    if (cmp != 0) {
                        break;
                    }
                    targetUpto++;
                }

                if (cmp == 0) {
                    cmp = term.length() - target.length;
                }
                targetUpto = targetUptoMid;
            }

            if (cmp < 0) {
                // Common case: target term is after current
                // term, ie, app is seeking multiple terms
                // in sorted order
                // if (DEBUG) {
                //   System.out.println("  target is after current (shares prefixLen=" + targetUpto + "); frame.ord=" + lastFrame.ord);
                // }
                currentFrame = lastFrame;

            } else if (cmp > 0) {
                // Uncommon case: target term
                // is before current term; this means we can
                // keep the currentFrame but we must rewind it
                // (so we scan from the start)
                targetBeforeCurrentLength = lastFrame.ord;
                // if (DEBUG) {
                //   System.out.println("  target is before current (shares prefixLen=" + targetUpto + "); rewind frame ord=" + lastFrame.ord);
                // }
                currentFrame = lastFrame;
                currentFrame.rewind();
            } else {
                // Target is exactly the same as current term
                assert term.length() == target.length;
                if (termExists) {
                    // if (DEBUG) {
                    //   System.out.println("  target is same as current; return true");
                    // }
                    return true;
                } else {
                    // if (DEBUG) {
                    //   System.out.println("  target is same as current but term doesn't exist");
                    // }
                }
                //validIndexPrefix = currentFrame.depth;
                //term.length = target.length;
                //return termExists;
            }

        } else {

            targetBeforeCurrentLength = -1;
            arc = fr.index.getFirstArc(arcs[0]);

            // Empty string prefix must have an output (block) in the index!
            assert arc.isFinal();
            assert arc.output() != null;

            // if (DEBUG) {
            //   System.out.println("    no seek state; push root frame");
            // }

            output = arc.output();

            currentFrame = staticFrame;

            //term.length = 0;
            targetUpto = 0;
            currentFrame = pushFrame(arc, BlockTreeTermsReader.FST_OUTPUTS.add(output, arc.nextFinalOutput()), 0);
        }

        // if (DEBUG) {
        //   System.out.println("  start index loop targetUpto=" + targetUpto + " output=" + output + " currentFrame.ord=" + currentFrame.ord + " targetBeforeCurrentLength=" + targetBeforeCurrentLength);
        // }

        // We are done sharing the common prefix with the incoming target and where we are currently seek'd; now continue walking the index:
        while (targetUpto < target.length) {

            final int targetLabel = target.bytes[target.offset + targetUpto] & 0xFF;

            final FST.Arc<BytesRef> nextArc = fr.index.findTargetArc(targetLabel, arc, getArc(1 + targetUpto), fstReader);

            if (nextArc == null) {

                // Index is exhausted
                // if (DEBUG) {
                //   System.out.println("    index: index exhausted label=" + ((char) targetLabel) + " " + toHex(targetLabel));
                // }

                validIndexPrefix = currentFrame.prefix;
                //validIndexPrefix = targetUpto;

                currentFrame.scanToFloorFrame(target);

                if (!currentFrame.hasTerms) {
                    termExists = false;
                    term.setByteAt(targetUpto, (byte) targetLabel);
                    term.setLength(1 + targetUpto);
                    // if (DEBUG) {
                    //   System.out.println("  FAST NOT_FOUND term=" + brToString(term));
                    // }
                    return false;
                }

                currentFrame.loadBlock();

                final SeekStatus result = currentFrame.scanToTerm(target, true);
                if (result == SeekStatus.FOUND) {
                    // if (DEBUG) {
                    //   System.out.println("  return FOUND term=" + term.utf8ToString() + " " + term);
                    // }
                    return true;
                } else {
                    // if (DEBUG) {
                    //   System.out.println("  got " + result + "; return NOT_FOUND term=" + brToString(term));
                    // }
                    return false;
                }
            } else {
                // Follow this arc
                arc = nextArc;
                term.setByteAt(targetUpto, (byte) targetLabel);
                // Aggregate output as we go:
                assert arc.output() != null;
                if (arc.output() != BlockTreeTermsReader.NO_OUTPUT) {
                    output = BlockTreeTermsReader.FST_OUTPUTS.add(output, arc.output());
                }

                // if (DEBUG) {
                //   System.out.println("    index: follow label=" + toHex(target.bytes[target.offset + targetUpto]&0xff) + " arc.output=" + arc.output + " arc.nfo=" + arc.nextFinalOutput);
                // }
                targetUpto++;

                if (arc.isFinal()) {
                    //if (DEBUG) System.out.println("    arc is final!");
                    currentFrame = pushFrame(arc, BlockTreeTermsReader.FST_OUTPUTS.add(output, arc.nextFinalOutput()), targetUpto);
                    //if (DEBUG) System.out.println("    curFrame.ord=" + currentFrame.ord + " hasTerms=" + currentFrame.hasTerms);
                }
            }
        }

        //validIndexPrefix = targetUpto;
        validIndexPrefix = currentFrame.prefix;

        currentFrame.scanToFloorFrame(target);

        // Target term is entirely contained in the index:
        if (!currentFrame.hasTerms) {
            termExists = false;
            term.setLength(targetUpto);
            // if (DEBUG) {
            //   System.out.println("  FAST NOT_FOUND term=" + brToString(term));
            // }
            return false;
        }

        currentFrame.loadBlock();

        final SeekStatus result = currentFrame.scanToTerm(target, true);
        if (result == SeekStatus.FOUND) {
            // if (DEBUG) {
            //   System.out.println("  return FOUND term=" + term.utf8ToString() + " " + term);
            // }
            return true;
        } else {
            // if (DEBUG) {
            //   System.out.println("  got result " + result + "; return NOT_FOUND term=" + term.utf8ToString());
            // }

            return false;
        }
    }

    @Override
    public SeekStatus seekCeil(BytesRef target) throws IOException {

        if (fr.index == null) {
            throw new IllegalStateException("terms index was not loaded");
        }

        term.grow(1 + target.length);

        assert clearEOF();

        // if (DEBUG) {
        //   System.out.println("\nBTTR.seekCeil seg=" + fr.parent.segment + " target=" + fr.fieldInfo.name + ":" + brToString(target) + " " + target + " current=" + brToString(term) + " (exists?=" + termExists + ") validIndexPrefix=  " + validIndexPrefix);
        //   printSeekState(System.out);
        // }

        FST.Arc<BytesRef> arc;
        int targetUpto;
        BytesRef output;

        targetBeforeCurrentLength = currentFrame.ord;

        if (currentFrame != staticFrame) {

            // We are already seek'd; find the common
            // prefix of new seek term vs current term and
            // re-use the corresponding seek state.  For
            // example, if app first seeks to foobar, then
            // seeks to foobaz, we can re-use the seek state
            // for the first 5 bytes.

            //if (DEBUG) {
            //System.out.println("  re-use current seek state validIndexPrefix=" + validIndexPrefix);
            //}

            arc = arcs[0];
            assert arc.isFinal();
            output = arc.output();
            targetUpto = 0;

            SegmentTermsEnumFrame lastFrame = stack[0];
            assert validIndexPrefix <= term.length();

            final int targetLimit = Math.min(target.length, validIndexPrefix);

            int cmp = 0;

            // TODO: we should write our vLong backwards (MSB
            // first) to get better sharing from the FST

            // First compare up to valid seek frames:
            while (targetUpto < targetLimit) {
                cmp = (term.byteAt(targetUpto) & 0xFF) - (target.bytes[target.offset + targetUpto] & 0xFF);
                //if (DEBUG) {
                //System.out.println("    cycle targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.byteAt(targetUpto)) + ")"   + " arc.output=" + arc.output + " output=" + output);
                //}
                if (cmp != 0) {
                    break;
                }
                arc = arcs[1 + targetUpto];
                assert arc.label() == (target.bytes[target.offset + targetUpto] & 0xFF) : "arc.label=" + (char) arc.label() + " targetLabel=" + (char) (target.bytes[target.offset + targetUpto] & 0xFF);
                // TODO: we could save the outputs in local
                // byte[][] instead of making new objs ever
                // seek; but, often the FST doesn't have any
                // shared bytes (but this could change if we
                // reverse vLong byte order)
                if (arc.output() != BlockTreeTermsReader.NO_OUTPUT) {
                    output = BlockTreeTermsReader.FST_OUTPUTS.add(output, arc.output());
                }
                if (arc.isFinal()) {
                    lastFrame = stack[1 + lastFrame.ord];
                }
                targetUpto++;
            }


            if (cmp == 0) {
                final int targetUptoMid = targetUpto;
                // Second compare the rest of the term, but
                // don't save arc/output/frame:
                final int targetLimit2 = Math.min(target.length, term.length());
                while (targetUpto < targetLimit2) {
                    cmp = (term.byteAt(targetUpto) & 0xFF) - (target.bytes[target.offset + targetUpto] & 0xFF);
                    //if (DEBUG) {
                    //System.out.println("    cycle2 targetUpto=" + targetUpto + " (vs limit=" + targetLimit + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) + " vs termLabel=" + (char) (term.byteAt(targetUpto)) + ")");
                    //}
                    if (cmp != 0) {
                        break;
                    }
                    targetUpto++;
                }

                if (cmp == 0) {
                    cmp = term.length() - target.length;
                }
                targetUpto = targetUptoMid;
            }

            if (cmp < 0) {
                // Common case: target term is after current
                // term, ie, app is seeking multiple terms
                // in sorted order
                //if (DEBUG) {
                //System.out.println("  target is after current (shares prefixLen=" + targetUpto + "); clear frame.scanned ord=" + lastFrame.ord);
                //}
                currentFrame = lastFrame;

            } else if (cmp > 0) {
                // Uncommon case: target term
                // is before current term; this means we can
                // keep the currentFrame but we must rewind it
                // (so we scan from the start)
                targetBeforeCurrentLength = 0;
                //if (DEBUG) {
                //System.out.println("  target is before current (shares prefixLen=" + targetUpto + "); rewind frame ord=" + lastFrame.ord);
                //}
                currentFrame = lastFrame;
                currentFrame.rewind();
            } else {
                // Target is exactly the same as current term
                assert term.length() == target.length;
                if (termExists) {
                    //if (DEBUG) {
                    //System.out.println("  target is same as current; return FOUND");
                    //}
                    return SeekStatus.FOUND;
                } else {
                    //if (DEBUG) {
                    //System.out.println("  target is same as current but term doesn't exist");
                    //}
                }
            }

        } else {

            targetBeforeCurrentLength = -1;
            arc = fr.index.getFirstArc(arcs[0]);

            // Empty string prefix must have an output (block) in the index!
            assert arc.isFinal();
            assert arc.output() != null;

            //if (DEBUG) {
            //System.out.println("    no seek state; push root frame");
            //}

            output = arc.output();

            currentFrame = staticFrame;

            //term.length = 0;
            targetUpto = 0;
            currentFrame = pushFrame(arc, BlockTreeTermsReader.FST_OUTPUTS.add(output, arc.nextFinalOutput()), 0);
        }

        //if (DEBUG) {
        //System.out.println("  start index loop targetUpto=" + targetUpto + " output=" + output + " currentFrame.ord+1=" + currentFrame.ord + " targetBeforeCurrentLength=" + targetBeforeCurrentLength);
        //}

        // We are done sharing the common prefix with the incoming target and where we are currently seek'd; now continue walking the index:
        while (targetUpto < target.length) {

            final int targetLabel = target.bytes[target.offset + targetUpto] & 0xFF;

            final FST.Arc<BytesRef> nextArc = fr.index.findTargetArc(targetLabel, arc, getArc(1 + targetUpto), fstReader);

            if (nextArc == null) {

                // Index is exhausted
                // if (DEBUG) {
                //   System.out.println("    index: index exhausted label=" + ((char) targetLabel) + " " + targetLabel);
                // }

                validIndexPrefix = currentFrame.prefix;
                //validIndexPrefix = targetUpto;

                currentFrame.scanToFloorFrame(target);

                currentFrame.loadBlock();

                //if (DEBUG) System.out.println("  now scanToTerm");
                final SeekStatus result = currentFrame.scanToTerm(target, false);
                if (result == SeekStatus.END) {
                    term.copyBytes(target);
                    termExists = false;

                    if (next() != null) {
                        //if (DEBUG) {
                        //System.out.println("  return NOT_FOUND term=" + brToString(term));
                        //}
                        return SeekStatus.NOT_FOUND;
                    } else {
                        //if (DEBUG) {
                        //System.out.println("  return END");
                        //}
                        return SeekStatus.END;
                    }
                } else {
                    //if (DEBUG) {
                    //System.out.println("  return " + result + " term=" + brToString(term));
                    //}
                    return result;
                }
            } else {
                // Follow this arc
                term.setByteAt(targetUpto, (byte) targetLabel);
                arc = nextArc;
                // Aggregate output as we go:
                assert arc.output() != null;
                if (arc.output() != BlockTreeTermsReader.NO_OUTPUT) {
                    output = BlockTreeTermsReader.FST_OUTPUTS.add(output, arc.output());
                }

                //if (DEBUG) {
                //System.out.println("    index: follow label=" + (target.bytes[target.offset + targetUpto]&0xff) + " arc.output=" + arc.output + " arc.nfo=" + arc.nextFinalOutput);
                //}
                targetUpto++;

                if (arc.isFinal()) {
                    //if (DEBUG) System.out.println("    arc is final!");
                    currentFrame = pushFrame(arc, BlockTreeTermsReader.FST_OUTPUTS.add(output, arc.nextFinalOutput()), targetUpto);
                    //if (DEBUG) System.out.println("    curFrame.ord=" + currentFrame.ord + " hasTerms=" + currentFrame.hasTerms);
                }
            }
        }

        //validIndexPrefix = targetUpto;
        validIndexPrefix = currentFrame.prefix;

        currentFrame.scanToFloorFrame(target);

        currentFrame.loadBlock();

        final SeekStatus result = currentFrame.scanToTerm(target, false);

        if (result == SeekStatus.END) {
            term.copyBytes(target);
            termExists = false;
            if (next() != null) {
                //if (DEBUG) {
                //System.out.println("  return NOT_FOUND term=" + term.get().utf8ToString() + " " + term);
                //}
                return SeekStatus.NOT_FOUND;
            } else {
                //if (DEBUG) {
                //System.out.println("  return END");
                //}
                return SeekStatus.END;
            }
        } else {
            return result;
        }
    }

    @SuppressWarnings("unused")
    private void printSeekState(PrintStream out) throws IOException {
        if (currentFrame == staticFrame) {
            out.println("  no prior seek");
        } else {
            out.println("  prior seek state:");
            int ord = 0;
            boolean isSeekFrame = true;
            while (true) {
                SegmentTermsEnumFrame f = getFrame(ord);
                assert f != null;
                final BytesRef prefix = new BytesRef(term.get().bytes, 0, f.prefix);
                if (f.nextEnt == -1) {
                    out.println("    frame " + (isSeekFrame ? "(seek)" : "(next)") + " ord=" + ord + " fp=" + f.fp + (f.isFloor ? (" (fpOrig=" + f.fpOrig + ")") : "") + " prefixLen=" + f.prefix + " prefix=" + prefix + (f.nextEnt == -1 ? "" : (" (of " + f.entCount + ")")) + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " code=" + ((f.fp << BlockTreeTermsReader.OUTPUT_FLAGS_NUM_BITS) + (f.hasTerms ? BlockTreeTermsReader.OUTPUT_FLAG_HAS_TERMS : 0) + (f.isFloor ? BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR : 0)) + " isLastInFloor=" + f.isLastInFloor + " mdUpto=" + f.metaDataUpto + " tbOrd=" + f.getTermBlockOrd());
                } else {
                    out.println("    frame " + (isSeekFrame ? "(seek, loaded)" : "(next, loaded)") + " ord=" + ord + " fp=" + f.fp + (f.isFloor ? (" (fpOrig=" + f.fpOrig + ")") : "") + " prefixLen=" + f.prefix + " prefix=" + prefix + " nextEnt=" + f.nextEnt + (f.nextEnt == -1 ? "" : (" (of " + f.entCount + ")")) + " hasTerms=" + f.hasTerms + " isFloor=" + f.isFloor + " code=" + ((f.fp << BlockTreeTermsReader.OUTPUT_FLAGS_NUM_BITS) + (f.hasTerms ? BlockTreeTermsReader.OUTPUT_FLAG_HAS_TERMS : 0) + (f.isFloor ? BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR : 0)) + " lastSubFP=" + f.lastSubFP + " isLastInFloor=" + f.isLastInFloor + " mdUpto=" + f.metaDataUpto + " tbOrd=" + f.getTermBlockOrd());
                }
                if (fr.index != null) {
                    assert !isSeekFrame || f.arc != null : "isSeekFrame=" + isSeekFrame + " f.arc=" + f.arc;
                    if (f.prefix > 0 && isSeekFrame && f.arc.label() != (term.byteAt(f.prefix - 1) & 0xFF)) {
                        out.println("      broken seek state: arc.label=" + (char) f.arc.label() + " vs term byte=" + (char) (term.byteAt(f.prefix - 1) & 0xFF));
                        throw new RuntimeException("seek state is broken");
                    }
                    BytesRef output = Util.get(fr.index, prefix);
                    if (output == null) {
                        out.println("      broken seek state: prefix is not final in index");
                        throw new RuntimeException("seek state is broken");
                    } else if (isSeekFrame && !f.isFloor) {
                        final ByteArrayDataInput reader = new ByteArrayDataInput(output.bytes, output.offset, output.length);
                        final long codeOrig = reader.readVLong();
                        final long code = (f.fp << BlockTreeTermsReader.OUTPUT_FLAGS_NUM_BITS) | (f.hasTerms ? BlockTreeTermsReader.OUTPUT_FLAG_HAS_TERMS : 0) | (f.isFloor ? BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR : 0);
                        if (codeOrig != code) {
                            out.println("      broken seek state: output code=" + codeOrig + " doesn't match frame code=" + code);
                            throw new RuntimeException("seek state is broken");
                        }
                    }
                }
                if (f == currentFrame) {
                    break;
                }
                if (f.prefix == validIndexPrefix) {
                    isSeekFrame = false;
                }
                ord++;
            }
        }
    }

    /* Decodes only the term bytes of the next term.  If caller then asks for
       metadata, ie docFreq, totalTermFreq or pulls a D/&PEnum, we then (lazily)
       decode all metadata up to the current term. */
    // 遍历获取下一个term
    @Override
    public BytesRef next() throws IOException {
        // 延迟初始化
        if (in == null) {
            // Fresh TermsEnum; seek to first term:
            final FST.Arc<BytesRef> arc;
            if (fr.index != null) {
                // 初始化虚拟节点
                arc = fr.index.getFirstArc(arcs[0]);
                // Empty string prefix must have an output in the index!
                assert arc.isFinal();
            } else {
                arc = null;
            }
            // pushFrame 为当前frame填充相关信息
            currentFrame = pushFrame(arc, fr.rootCode, 0);
            // 这里就是以 block为单位 将 后缀数据，统计数据，元数据加载到内存中并存储起来
            currentFrame.loadBlock();
        }

        targetBeforeCurrentLength = currentFrame.ord;

        assert !eof;
        // if (DEBUG) {
        //   System.out.println("\nBTTR.next seg=" + fr.parent.segment + " term=" + brToString(term) + " termExists?=" + termExists + " field=" + fr.fieldInfo.name + " termBlockOrd=" + currentFrame.state.termBlockOrd + " validIndexPrefix=" + validIndexPrefix);
        //   printSeekState(System.out);
        // }

        // 先忽略该情况 因为在上面延迟初始化in的时候 currentFrame.ord为0 staticFrame.ord为 -1
        if (currentFrame == staticFrame) {
            // If seek was previously called and the term was
            // cached, or seek(TermState) was called, usually
            // caller is just going to pull a D/&PEnum or get
            // docFreq, etc.  But, if they then call next(),
            // this method catches up all internal state so next()
            // works properly:
            //if (DEBUG) System.out.println("  re-seek to pending term=" + term.utf8ToString() + " " + term);
            final boolean result = seekExact(term.get());
            assert result;
        }

        // Pop finished blocks
        // 首次加载完成时  nextEnt为0   entCount 为block处理的node数 TODO 这里先忽略
        while (currentFrame.nextEnt == currentFrame.entCount) {
            if (!currentFrame.isLastInFloor) {
                // Advance to next floor block
                currentFrame.loadNextFloorBlock();
                break;
            } else {
                //if (DEBUG) System.out.println("  pop frame");
                if (currentFrame.ord == 0) {
                    //if (DEBUG) System.out.println("  return null");
                    assert setEOF();
                    term.clear();
                    validIndexPrefix = 0;
                    currentFrame.rewind();
                    termExists = false;
                    return null;
                }
                final long lastFP = currentFrame.fpOrig;
                currentFrame = stack[currentFrame.ord - 1];

                if (currentFrame.nextEnt == -1 || currentFrame.lastSubFP != lastFP) {
                    // We popped into a frame that's not loaded
                    // yet or not scan'd to the right entry
                    currentFrame.scanToFloorFrame(term.get());
                    currentFrame.loadBlock();
                    currentFrame.scanToSubBlock(lastFP);
                }

                // Note that the seek state (last seek) has been
                // invalidated beyond this depth
                validIndexPrefix = Math.min(validIndexPrefix, currentFrame.prefix);
                //if (DEBUG) {
                //System.out.println("  reset validIndexPrefix=" + validIndexPrefix);
                //}
            }
        }

        // 先看这里
        while (true) {
            // 返回true 代表此时无法返回一个完整的term 因为遇到了一个blockNode 需要先将该blockNode 的前缀补充上 然后读取生成该blockNode 的所有termNode (途中遇到blockNode 则继续递归)
            if (currentFrame.next()) {
                // Push to new block:
                //if (DEBUG) System.out.println("  push frame");
                // 以构成该blockNode 对应的一组node 的文件偏移量为起点 生成新的frame对象
                currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length());
                // This is a "next" frame -- even if it's
                // floor'd we must pretend it isn't so we don't
                // try to scan to the right floor frame:
                // 读取该blockNode 的相关数据
                currentFrame.loadBlock();
            } else {
                // 返回false 此时 数据已经通过frame回填到term了
                //if (DEBUG) System.out.println("  return term=" + brToString(term) + " currentFrame.ord=" + currentFrame.ord);
                return term.get();
            }
        }
    }

    @Override
    public BytesRef term() {
        assert !eof;
        return term.get();
    }

    @Override
    public int docFreq() throws IOException {
        assert !eof;
        //if (DEBUG) System.out.println("BTR.docFreq");
        currentFrame.decodeMetaData();
        //if (DEBUG) System.out.println("  return " + currentFrame.state.docFreq);
        return currentFrame.state.docFreq;
    }

    @Override
    public long totalTermFreq() throws IOException {
        assert !eof;
        currentFrame.decodeMetaData();
        return currentFrame.state.totalTermFreq;
    }

    /**
     * 返回存储 term位置信息的迭代器   获取到的对象会迭代该term每次出现的位置 比如 出现在哪些doc
     * @param reuse pass a prior PostingsEnum for possible reuse
     * @param flags specifies which optional per-document values   在遍历过程中还需要获取哪些额外信息
     * @return
     * @throws IOException
     */
    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        assert !eof;
        //if (DEBUG) {
        //System.out.println("BTTR.docs seg=" + segment);
        //}
        // 解析此时term的元数据  获取对应 pos pay doc文件的起始偏移量
        currentFrame.decodeMetaData();
        //if (DEBUG) {
        //System.out.println("  state=" + currentFrame.state);
        //}
        // 生成获取该term position信息的 迭代器
        return fr.parent.postingsReader.postings(fr.fieldInfo, currentFrame.state, reuse, flags);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
        assert !eof;
        //if (DEBUG) {
        //System.out.println("BTTR.docs seg=" + segment);
        //}
        currentFrame.decodeMetaData();
        //if (DEBUG) {
        //System.out.println("  state=" + currentFrame.state);
        //}
        return fr.parent.postingsReader.impacts(fr.fieldInfo, currentFrame.state, flags);
    }

    @Override
    public void seekExact(BytesRef target, TermState otherState) {
        // if (DEBUG) {
        //   System.out.println("BTTR.seekExact termState seg=" + segment + " target=" + target.utf8ToString() + " " + target + " state=" + otherState);
        // }
        assert clearEOF();
        if (target.compareTo(term.get()) != 0 || !termExists) {
            assert otherState != null && otherState instanceof BlockTermState;
            currentFrame = staticFrame;
            currentFrame.state.copyFrom(otherState);
            term.copyBytes(target);
            currentFrame.metaDataUpto = currentFrame.getTermBlockOrd();
            assert currentFrame.metaDataUpto > 0;
            validIndexPrefix = 0;
        } else {
            // if (DEBUG) {
            //   System.out.println("  skip seek: already on target state=" + currentFrame.state);
            // }
        }
    }

    @Override
    public TermState termState() throws IOException {
        assert !eof;
        currentFrame.decodeMetaData();
        TermState ts = currentFrame.state.clone();
        //if (DEBUG) System.out.println("BTTR.termState seg=" + segment + " state=" + ts);
        return ts;
    }

    @Override
    public void seekExact(long ord) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long ord() {
        throw new UnsupportedOperationException();
    }
}
