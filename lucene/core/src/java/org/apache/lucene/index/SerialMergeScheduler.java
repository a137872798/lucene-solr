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
package org.apache.lucene.index;


import java.io.IOException;

/** A {@link MergeScheduler} that simply does each merge
 *  sequentially, using the current thread. */
// 在单线程中 排队执行任务
public class SerialMergeScheduler extends MergeScheduler {

  /** Sole constructor. */
  public SerialMergeScheduler() {
  }

  /** Just do the merges in sequence. We do this
   * "synchronized" so that even if the application is using
   * multiple threads, only one merge may run at a time.
   *
   */
  @Override
  synchronized public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
    while(true) {
      // 实际上都是委托  注意这里在单线程中循环调用 nextMerge 获取下一个准备好的OneMerge 对象
      MergePolicy.OneMerge merge = mergeSource.getNextMerge();
      if (merge == null) {
        break;
      }
      mergeSource.merge(merge);
    }
  }

  @Override
  public void close() {}
}
