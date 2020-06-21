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
package org.apache.lucene.util.automaton;


//import java.io.IOException;
//import java.io.PrintWriter;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.Sorter;




// TODO
//   - could use packed int arrays instead
//   - could encode dest w/ delta from to?

/** Represents an automaton and all its states and transitions.  States
 *  are integers and must be created using {@link #createState}.  Mark a
 *  state as an accept state using {@link #setAccept}.  Add transitions
 *  using {@link #addTransition}.  Each state must have all of its
 *  transitions added at once; if this is too restrictive then use
 *  {@link Automaton.Builder} instead.  State 0 is always the
 *  initial state.  Once a state is finished, either
 *  because you've starting adding transitions to another state or you
 *  call {@link #finishState}, then that states transitions are sorted
 *  (first by min, then max, then dest) and reduced (transitions with
 *  adjacent labels going to the same dest are combined).
 *
 * @lucene.experimental */
// 该对象只是相当于一个存储状态机的容器  内部定义了一系列的映射规则
public class Automaton implements Accountable {

  /** Where we next write to the int[] states; this increments by 2 for
   *  each added state because we pack a pointer to the transitions
   *  array and a count of how many transitions leave the state.  */
  // 代表下一个状态
  private int nextState;

  /** Where we next write to in int[] transitions; this
   *  increments by 3 for each added transition because we
   *  pack min, max, dest in sequence. */
  private int nextTransition;

  /** Current state we are adding transitions to; the caller
   *  must add all transitions for this state before moving
   *  onto another state. */
  // 当前状态
  private int curState = -1;

  /** Index in the transitions array, where this states
   *  leaving transitions are stored, or -1 if this state
   *  has not added any transitions yet, followed by number
   *  of transitions. */
  // 一组候选的状态
  private int[] states;

  private final BitSet isAccept;
  
  /** Holds toState, min, max for each transition. */
  private int[] transitions;

  /** True if no state has two transitions leaving with the same label. */
  private boolean deterministic = true;

  /** Sole constructor; creates an automaton with no states. */
  public Automaton() {
     this(2, 2);
  }

  /**
   * Constructor which creates an automaton with enough space for the given
   * number of states and transitions.
   * 
   * @param numStates
   *           Number of states.
   * @param numTransitions
   *           Number of transitions.
   *           在初始化状态机的时候  会定义状态的数量 以及状态转移的数量
   */
  public Automaton(int numStates, int numTransitions) {
    // 这里使用2倍的大小是为什么???
     states = new int[numStates * 2];
     isAccept = new BitSet(numStates);
     // 3倍的状态转换
     transitions = new int[numTransitions * 3];
  }

  /** Create a new state. */
  /**
   * 创建
   * @return
   */
  public int createState() {
    // 当nextState + 2 超过数组(states)大小时  进行扩容
    growStates();
    // nextState 每次加2  意味着 state 每次加1
    int state = nextState/2;
    // 对应的槽初始值为-1  代表一个状态机的起点
    states[nextState] = -1;
    // 每次创建一个新的状态时 nextState + 2
    nextState += 2;
    // 这里返回的是下标  不代表状态
    return state;
  }

  /** Set or clear this state as an accept state. */
  // 在位图中找到 某状态匹配的下标的位置 并设置  accept标识
  public void setAccept(int state, boolean accept) {
    Objects.checkIndex(state, getNumStates());
    isAccept.set(state, accept);
  }

  /** Sugar to get all transitions for all states.  This is
   *  object-heavy; it's better to iterate state by state instead. */
  // 获取所有的转换对象
  public Transition[][] getSortedTransitions() {
    // 获取当前已经初始化的状态
    int numStates = getNumStates();
    // 为每个状态创建一个   转换数组 这些转换对象描述 状态在什么条件下 如何变化
    Transition[][] transitions = new Transition[numStates][];
    for(int s=0;s<numStates;s++) {
      // 判断对应的状态 有多少种转换方式
      int numTransitions = getNumTransitions(s);
      transitions[s] = new Transition[numTransitions];
      for(int t=0;t<numTransitions;t++) {
        Transition transition = new Transition();
        // 通过特殊的偏移量映射方式 找到转换对象需要的属性
        getTransition(s, t, transition);
        // 填充到目标数组中
        transitions[s][t] = transition;
      }
    }

    return transitions;
  }

  /** Returns accept states.  If the bit is set then that state is an accept state. */
  BitSet getAcceptStates() {
    return isAccept;
  }

  /** Returns true if this state is an accept state. */
  public boolean isAccept(int state) {
    return isAccept.get(state);
  }

  /** Add a new transition with min = max = label. */
  public void addTransition(int source, int dest, int label) {
    addTransition(source, dest, label, label);
  }

  /** Add a new transition with the specified source, dest, min, max. */
  // 填入 相关参数追加 有关转换的信息
  public void addTransition(int source, int dest, int min, int max) {
    assert nextTransition%3 == 0;

    // 确保 source/dest 小于 bounds
    int bounds = nextState/2;
    Objects.checkIndex(source, bounds);
    Objects.checkIndex(dest, bounds);

    // 注意扩容  该容器与 states 不同 每次递增3个值
    growTransitions();
    if (curState != source) {
      // 接收当前状态
      if (curState != -1) {
        finishCurrentState();
      }

      // Move to next source:
      // 冻结完毕后 切换当前的状态
      curState = source;
      if (states[2*curState] != -1) {
        throw new IllegalStateException("from state (" + source + ") already had transitions added");
      }
      assert states[2*curState+1] == 0;
      // 将 state目标位置的值 标记成 nextTransition
      states[2*curState] = nextTransition;
    }

    // 将目标值 连续插入到数组中
    transitions[nextTransition++] = dest;
    transitions[nextTransition++] = min;
    transitions[nextTransition++] = max;

    // Increment transition count for this state
    // 代表当前位置增加了一种转换状态
    states[2*curState+1]++;
  }

  /** Add a [virtual] epsilon transition between source and dest.
   *  Dest state must already have all transitions added because this
   *  method simply copies those same transitions over to source. */
  // 发起一次 从 source到 dest的转换
  public void addEpsilon(int source, int dest) {
    Transition t = new Transition();
    int count = initTransition(dest, t);
    for(int i=0;i<count;i++) {
      getNextTransition(t);
      addTransition(source, t.dest, t.min, t.max);
    }
    if (isAccept(dest)) {
      setAccept(source, true);
    }
  }

  /** Copies over all states/transitions from other.  The states numbers
   *  are sequentially assigned (appended). */
  public void copy(Automaton other) {

    // Bulk copy and then fixup the state pointers:
    int stateOffset = getNumStates();
    states = ArrayUtil.grow(states, nextState + other.nextState);
    System.arraycopy(other.states, 0, states, nextState, other.nextState);
    for(int i=0;i<other.nextState;i += 2) {
      if (states[nextState+i] != -1) {
        states[nextState+i] += nextTransition;
      }
    }
    nextState += other.nextState;
    int otherNumStates = other.getNumStates();
    BitSet otherAcceptStates = other.getAcceptStates();
    int state = 0;
    while (state < otherNumStates && (state = otherAcceptStates.nextSetBit(state)) != -1) {
      setAccept(stateOffset + state, true);
      state++;
    }

    // Bulk copy and then fixup dest for each transition:
    transitions = ArrayUtil.grow(transitions, nextTransition + other.nextTransition);
    System.arraycopy(other.transitions, 0, transitions, nextTransition, other.nextTransition);
    for(int i=0;i<other.nextTransition;i += 3) {
      transitions[nextTransition+i] += stateOffset;
    }
    nextTransition += other.nextTransition;

    if (other.deterministic == false) {
      deterministic = false;
    }
  }

  /** Freezes the last state, sorting and reducing the transitions. */
  // 冻结当前状态
  private void finishCurrentState() {
    // 这里是定位到当前状态对应的 转换对象
    int numTransitions = states[2*curState+1];
    assert numTransitions > 0;

    int offset = states[2*curState];
    int start = offset/3;
    destMinMaxSorter.sort(start, start+numTransitions);

    // Reduce any "adjacent" transitions:
    int upto = 0;
    int min = -1;
    int max = -1;
    int dest = -1;

    for(int i=0;i<numTransitions;i++) {
      int tDest = transitions[offset+3*i];
      int tMin = transitions[offset+3*i+1];
      int tMax = transitions[offset+3*i+2];

      if (dest == tDest) {
        if (tMin <= max+1) {
          if (tMax > max) {
            max = tMax;
          }
        } else {
          if (dest != -1) {
            transitions[offset+3*upto] = dest;
            transitions[offset+3*upto+1] = min;
            transitions[offset+3*upto+2] = max;
            upto++;
          }
          min = tMin;
          max = tMax;
        }
      } else {
        if (dest != -1) {
          transitions[offset+3*upto] = dest;
          transitions[offset+3*upto+1] = min;
          transitions[offset+3*upto+2] = max;
          upto++;
        }
        dest = tDest;
        min = tMin;
        max = tMax;
      }
    }

    if (dest != -1) {
      // Last transition
      transitions[offset+3*upto] = dest;
      transitions[offset+3*upto+1] = min;
      transitions[offset+3*upto+2] = max;
      upto++;
    }

    nextTransition -= (numTransitions-upto)*3;
    states[2*curState+1] = upto;

    // Sort transitions by min/max/dest:
    minMaxDestSorter.sort(start, start+upto);

    if (deterministic && upto > 1) {
      int lastMax = transitions[offset+2];
      for(int i=1;i<upto;i++) {
        min = transitions[offset + 3*i + 1];
        if (min <= lastMax) {
          deterministic = false;
          break;
        }
        lastMax = transitions[offset + 3*i + 2];
      }
    }
  }

  /** Returns true if this automaton is deterministic (for ever state
   *  there is only one transition for each label). */
  public boolean isDeterministic() {
    return deterministic;
  }

  /** Finishes the current state; call this once you are done adding
   *  transitions for a state.  This is automatically called if you
   *  start adding transitions to a new source state, but for the last
   *  state you add you need to this method yourself. */
  public void finishState() {
    if (curState != -1) {
      finishCurrentState();
      curState = -1;
    }
  }

  // TODO: add finish() to shrink wrap the arrays?

  /** How many states this automaton has. */
  // 当前被使用的槽
  public int getNumStates() {
    return nextState/2;
  }

  /** How many transitions this automaton has. */
  public int getNumTransitions() {
    return nextTransition / 3;   
  }
  
  /** How many transitions this state has. */
  // 获取某状态的转换方式
  public int getNumTransitions(int state) {
    assert state >= 0;
    // 2倍状态下标的位置存放的就是转换的数量
    int count = states[2*state+1];
    if (count == -1) {
      return 0;
    } else {
      return count;
    }
  }

  private void growStates() {
    // 当下一个状态 + 2 超过了存储状态的数组 那么就进行扩容
    if (nextState+2 > states.length) {
      states = ArrayUtil.grow(states, nextState+2);
    }
  }

  private void growTransitions() {
    if (nextTransition+3 > transitions.length) {
      transitions = ArrayUtil.grow(transitions, nextTransition+3);
    }
  }

  /** Sorts transitions by dest, ascending, then min label ascending, then max label ascending */
  private final Sorter destMinMaxSorter = new InPlaceMergeSorter() {

      private void swapOne(int i, int j) {
        int x = transitions[i];
        transitions[i] = transitions[j];
        transitions[j] = x;
      }

      @Override
      protected void swap(int i, int j) {
        int iStart = 3*i;
        int jStart = 3*j;
        swapOne(iStart, jStart);
        swapOne(iStart+1, jStart+1);
        swapOne(iStart+2, jStart+2);
      };

      @Override
      protected int compare(int i, int j) {
        int iStart = 3*i;
        int jStart = 3*j;

        // First dest:
        int iDest = transitions[iStart];
        int jDest = transitions[jStart];
        if (iDest < jDest) {
          return -1;
        } else if (iDest > jDest) {
          return 1;
        }

        // Then min:
        int iMin = transitions[iStart+1];
        int jMin = transitions[jStart+1];
        if (iMin < jMin) {
          return -1;
        } else if (iMin > jMin) {
          return 1;
        }

        // Then max:
        int iMax = transitions[iStart+2];
        int jMax = transitions[jStart+2];
        if (iMax < jMax) {
          return -1;
        } else if (iMax > jMax) {
          return 1;
        }

        return 0;
      }
    };

  /** Sorts transitions by min label, ascending, then max label ascending, then dest ascending */
  private final Sorter minMaxDestSorter = new InPlaceMergeSorter() {

      private void swapOne(int i, int j) {
        int x = transitions[i];
        transitions[i] = transitions[j];
        transitions[j] = x;
      }

      @Override
      protected void swap(int i, int j) {
        int iStart = 3*i;
        int jStart = 3*j;
        swapOne(iStart, jStart);
        swapOne(iStart+1, jStart+1);
        swapOne(iStart+2, jStart+2);
      };

      @Override
      protected int compare(int i, int j) {
        int iStart = 3*i;
        int jStart = 3*j;

        // First min:
        int iMin = transitions[iStart+1];
        int jMin = transitions[jStart+1];
        if (iMin < jMin) {
          return -1;
        } else if (iMin > jMin) {
          return 1;
        }

        // Then max:
        int iMax = transitions[iStart+2];
        int jMax = transitions[jStart+2];
        if (iMax < jMax) {
          return -1;
        } else if (iMax > jMax) {
          return 1;
        }

        // Then dest:
        int iDest = transitions[iStart];
        int jDest = transitions[jStart];
        if (iDest < jDest) {
          return -1;
        } else if (iDest > jDest) {
          return 1;
        }

        return 0;
      }
    };

  /** Initialize the provided Transition to iterate through all transitions
   *  leaving the specified state.  You must call {@link #getNextTransition} to
   *  get each transition.  Returns the number of transitions
   *  leaving this state. */
  // 初始化转换对象
  public int initTransition(int state, Transition t) {
    assert state < nextState/2: "state=" + state + " nextState=" + nextState;
    t.source = state;
    t.transitionUpto = states[2*state];
    return getNumTransitions(state);
  }

  /** Iterate to the next transition after the provided one */
  public void getNextTransition(Transition t) {
    // Make sure there is still a transition left:
    assert (t.transitionUpto+3 - states[2*t.source]) <= 3*states[2*t.source+1];

    // Make sure transitions are in fact sorted:
    assert transitionSorted(t);

    t.dest = transitions[t.transitionUpto++];
    t.min = transitions[t.transitionUpto++];
    t.max = transitions[t.transitionUpto++];
  }

  private boolean transitionSorted(Transition t) {

    int upto = t.transitionUpto;
    if (upto == states[2*t.source]) {
      // Transition isn't initialized yet (this is the first transition); don't check:
      return true;
    }

    int nextDest = transitions[upto];
    int nextMin = transitions[upto+1];
    int nextMax = transitions[upto+2];
    if (nextMin > t.min) {
      return true;
    } else if (nextMin < t.min) {
      return false;
    }

    // Min is equal, now test max:
    if (nextMax > t.max) {
      return true;
    } else if (nextMax < t.max) {
      return false;
    }

    // Max is also equal, now test dest:
    if (nextDest > t.dest) {
      return true;
    } else if (nextDest < t.dest) {
      return false;
    }

    // We should never see fully equal transitions here:
    return false;
  }

  /** Fill the provided {@link Transition} with the index'th
   *  transition leaving the specified state. */
  public void getTransition(int state, int index, Transition t) {
    //  又是压缩的套路  state能够通过某种发则找到当前支持的转换数量  然后再通过下面的公式得到新的下标
    int i = states[2*state] + 3*index;
    t.source = state;
    // 从该下标开始连续读取3个值 分别是 Transition 需要的属性
    t.dest = transitions[i++];
    t.min = transitions[i++];
    t.max = transitions[i++];
  }

  static void appendCharString(int c, StringBuilder b) {
    if (c >= 0x21 && c <= 0x7e && c != '\\' && c != '"') b.appendCodePoint(c);
    else {
      b.append("\\\\U");
      String s = Integer.toHexString(c);
      if (c < 0x10) b.append("0000000").append(s);
      else if (c < 0x100) b.append("000000").append(s);
      else if (c < 0x1000) b.append("00000").append(s);
      else if (c < 0x10000) b.append("0000").append(s);
      else if (c < 0x100000) b.append("000").append(s);
      else if (c < 0x1000000) b.append("00").append(s);
      else if (c < 0x10000000) b.append("0").append(s);
      else b.append(s);
    }
  }

  /*
  public void writeDot(String fileName) {
    if (fileName.indexOf('/') == -1) {
      fileName = "/l/la/lucene/core/" + fileName + ".dot";
    }
    try {
      PrintWriter pw = new PrintWriter(fileName);
      pw.println(toDot());
      pw.close();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  */

  /** Returns the dot (graphviz) representation of this automaton.
   *  This is extremely useful for visualizing the automaton. */
  public String toDot() {
    // TODO: breadth first search so we can get layered output...

    StringBuilder b = new StringBuilder();
    b.append("digraph Automaton {\n");
    b.append("  rankdir = LR\n");
    b.append("  node [width=0.2, height=0.2, fontsize=8]\n");
    final int numStates = getNumStates();
    if (numStates > 0) {
      b.append("  initial [shape=plaintext,label=\"\"]\n");
      b.append("  initial -> 0\n");
    }

    Transition t = new Transition();

    for(int state=0;state<numStates;state++) {
      b.append("  ");
      b.append(state);
      if (isAccept(state)) {
        b.append(" [shape=doublecircle,label=\"").append(state).append("\"]\n");
      } else {
        b.append(" [shape=circle,label=\"").append(state).append("\"]\n");
      }
      int numTransitions = initTransition(state, t);
      //System.out.println("toDot: state " + state + " has " + numTransitions + " transitions; t.nextTrans=" + t.transitionUpto);
      for(int i=0;i<numTransitions;i++) {
        getNextTransition(t);
        //System.out.println("  t.nextTrans=" + t.transitionUpto + " t=" + t);
        assert t.max >= t.min;
        b.append("  ");
        b.append(state);
        b.append(" -> ");
        b.append(t.dest);
        b.append(" [label=\"");
        appendCharString(t.min, b);
        if (t.max != t.min) {
          b.append('-');
          appendCharString(t.max, b);
        }
        b.append("\"]\n");
        //System.out.println("  t=" + t);
      }
    }
    b.append('}');
    return b.toString();
  }

  /**
   * Returns sorted array of all interval start points.
   */
  int[] getStartPoints() {
    Set<Integer> pointset = new HashSet<>();
    pointset.add(Character.MIN_CODE_POINT);
    //System.out.println("getStartPoints");
    for (int s=0;s<nextState;s+=2) {
      int trans = states[s];
      int limit = trans+3*states[s+1];
      //System.out.println("  state=" + (s/2) + " trans=" + trans + " limit=" + limit);
      while (trans < limit) {
        int min = transitions[trans+1];
        int max = transitions[trans+2];
        //System.out.println("    min=" + min);
        pointset.add(min);
        if (max < Character.MAX_CODE_POINT) {
          pointset.add(max + 1);
        }
        trans += 3;
      }
    }
    int[] points = new int[pointset.size()];
    int n = 0;
    for (Integer m : pointset) {
      points[n++] = m;
    }
    Arrays.sort(points);
    return points;
  }

  /**
   * Performs lookup in transitions, assuming determinism.
   * 
   * @param state starting state
   * @param label codepoint to look up
   * @return destination state, -1 if no matching outgoing transition
   */
  public int step(int state, int label) {
    return next(state, 0, label, null);
  }

  /**
   * Looks for the next transition that matches the provided label, assuming determinism.
   * <p>
   * This method is similar to {@link #step(int, int)} but is used more efficiently
   * when iterating over multiple transitions from the same source state. It keeps
   * the latest reached transition index in {@code transition.transitionUpto} so
   * the next call to this method can continue from there instead of restarting
   * from the first transition.
   *
   * @param transition The transition to start the lookup from (inclusive, using its
   *                   {@link Transition#source} and {@link Transition#transitionUpto}).
   *                   It is updated with the matched transition;
   *                   or with {@link Transition#dest} = -1 if no match.
   * @param label      The codepoint to look up.
   * @return The destination state; or -1 if no matching outgoing transition.
   */
  public int next(Transition transition, int label) {
    return next(transition.source, transition.transitionUpto, label, transition);
  }

  /**
   * Looks for the next transition that matches the provided label, assuming determinism.
   *
   * @param state               The source state.
   * @param fromTransitionIndex The transition index to start the lookup from (inclusive); negative interpreted as 0.
   * @param label               The codepoint to look up.
   * @param transition          The output transition to update with the matching transition; or null for no update.
   * @return The destination state; or -1 if no matching outgoing transition.
   */
  private int next(int state, int fromTransitionIndex, int label, Transition transition) {
    assert state >= 0;
    assert label >= 0;
    int stateIndex = 2 * state;
    int firstTransitionIndex = states[stateIndex];
    int numTransitions = states[stateIndex + 1];

    // Since transitions are sorted,
    // binary search the transition for which label is within [minLabel, maxLabel].
    int low = Math.max(fromTransitionIndex, 0);
    int high = numTransitions - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int transitionIndex = firstTransitionIndex + 3 * mid;
      int minLabel = transitions[transitionIndex + 1];
      if (minLabel > label) {
        high = mid - 1;
      } else {
        int maxLabel = transitions[transitionIndex + 2];
        if (maxLabel < label){
          low = mid + 1;
        } else {
          int destState = transitions[transitionIndex];
          if (transition != null) {
            transition.dest = destState;
            transition.min = minLabel;
            transition.max = maxLabel;
            transition.transitionUpto = mid;
          }
          return destState;
        }
      }
    }
    int destState = -1;
    if (transition != null) {
      transition.dest = destState;
      transition.transitionUpto = low;
    }
    return destState;
  }

  /** Records new states and transitions and then {@link
   *  #finish} creates the {@link Automaton}.  Use this
   *  when you cannot create the Automaton directly because
   *  it's too restrictive to have to add all transitions
   *  leaving each state at once. */
  public static class Builder {
    private int nextState = 0;
    private final BitSet isAccept;
    private int[] transitions;
    private int nextTransition = 0;

    /** Default constructor, pre-allocating for 16 states and transitions. */
    public Builder() {
       this(16, 16);
    }

    /**
     * Constructor which creates a builder with enough space for the given
     * number of states and transitions.
     * 
     * @param numStates
     *           Number of states.
     * @param numTransitions
     *           Number of transitions.
     */
    public Builder(int numStates, int numTransitions) {
       isAccept = new BitSet(numStates);
       transitions = new int[numTransitions * 4];
    }

    /** Add a new transition with min = max = label. */
    public void addTransition(int source, int dest, int label) {
      addTransition(source, dest, label, label);
    }

    /** Add a new transition with the specified source, dest, min, max. */
    public void addTransition(int source, int dest, int min, int max) {
      if (transitions.length < nextTransition+4) {
        transitions = ArrayUtil.grow(transitions, nextTransition+4);
      }
      transitions[nextTransition++] = source;
      transitions[nextTransition++] = dest;
      transitions[nextTransition++] = min;
      transitions[nextTransition++] = max;
    }

    /** Add a [virtual] epsilon transition between source and dest.
     *  Dest state must already have all transitions added because this
     *  method simply copies those same transitions over to source. */
    public void addEpsilon(int source, int dest) {
      for (int upto = 0; upto < nextTransition; upto += 4) {
         if (transitions[upto] == dest) {
            addTransition(source, transitions[upto + 1], transitions[upto + 2], transitions[upto + 3]);
         }
      }
      if (isAccept(dest)) {
        setAccept(source, true);
      }
    }

    /** Sorts transitions first then min label ascending, then
     *  max label ascending, then dest ascending */
    private final Sorter sorter = new InPlaceMergeSorter() {

        private void swapOne(int i, int j) {
          int x = transitions[i];
          transitions[i] = transitions[j];
          transitions[j] = x;
        }

        @Override
        protected void swap(int i, int j) {
          int iStart = 4*i;
          int jStart = 4*j;
          swapOne(iStart, jStart);
          swapOne(iStart+1, jStart+1);
          swapOne(iStart+2, jStart+2);
          swapOne(iStart+3, jStart+3);
        };

        @Override
        protected int compare(int i, int j) {
          int iStart = 4*i;
          int jStart = 4*j;

          // First src:
          int iSrc = transitions[iStart];
          int jSrc = transitions[jStart];
          if (iSrc < jSrc) {
            return -1;
          } else if (iSrc > jSrc) {
            return 1;
          }

          // Then min:
          int iMin = transitions[iStart+2];
          int jMin = transitions[jStart+2];
          if (iMin < jMin) {
            return -1;
          } else if (iMin > jMin) {
            return 1;
          }

          // Then max:
          int iMax = transitions[iStart+3];
          int jMax = transitions[jStart+3];
          if (iMax < jMax) {
            return -1;
          } else if (iMax > jMax) {
            return 1;
          }

          // First dest:
          int iDest = transitions[iStart+1];
          int jDest = transitions[jStart+1];
          if (iDest < jDest) {
            return -1;
          } else if (iDest > jDest) {
            return 1;
          }

          return 0;
        }
      };

    /** Compiles all added states and transitions into a new {@code Automaton}
     *  and returns it. */
    public Automaton finish() {
      // Create automaton with the correct size.
      int numStates = nextState;
      int numTransitions = nextTransition / 4;
      Automaton a = new Automaton(numStates, numTransitions);
      
      // Create all states.
      for (int state = 0; state < numStates; state++) {
         a.createState();
         a.setAccept(state, isAccept(state));
      }
      
      // Create all transitions
      sorter.sort(0, numTransitions);
      for (int upto = 0; upto < nextTransition; upto += 4) {
        a.addTransition(transitions[upto],
                        transitions[upto+1],
                        transitions[upto+2],
                        transitions[upto+3]);
      }

      a.finishState();
      
      return a;
    }

    /** Create a new state. */
    public int createState() {
      return nextState++;
    }

    /** Set or clear this state as an accept state. */
    public void setAccept(int state, boolean accept) {
      Objects.checkIndex(state, getNumStates());      
      this.isAccept.set(state, accept);
    }

    /** Returns true if this state is an accept state. */
    public boolean isAccept(int state) {
      return this.isAccept.get(state);
    }

    /** How many states this automaton has. */
    public int getNumStates() {
      return nextState;
    }

    /** Copies over all states/transitions from other. */
    public void copy(Automaton other) {
      int offset = getNumStates();
      int otherNumStates = other.getNumStates();

      // Copy all states
      copyStates(other);
      
      // Copy all transitions
      Transition t = new Transition();
      for(int s=0;s<otherNumStates;s++) {
        int count = other.initTransition(s, t);
        for(int i=0;i<count;i++) {
          other.getNextTransition(t);
          addTransition(offset + s, offset + t.dest, t.min, t.max);
        }
      }
    }

    /** Copies over all states from other. */
    public void copyStates(Automaton other) {
      int otherNumStates = other.getNumStates();
      for (int s = 0; s < otherNumStates; s++) {
        int newState = createState();
        setAccept(newState, other.isAccept(s));
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    // TODO: BitSet RAM usage (isAccept.size()/8) isn't fully accurate...
    return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.sizeOf(states) + RamUsageEstimator.sizeOf(transitions) +
      RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + (isAccept.size() / 8) + RamUsageEstimator.NUM_BYTES_OBJECT_REF +
      2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF +
      3 * Integer.BYTES +
      1;
  }
}
