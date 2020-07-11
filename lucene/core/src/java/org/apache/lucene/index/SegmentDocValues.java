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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RefCount;

/**
 * Manages the {@link DocValuesProducer} held by {@link SegmentReader} and
 * keeps track of their reference counting.
 * 该对象以 segment为单位 划分
 */
final class SegmentDocValues {

  /**
   * key 是 gen
   * value 是 该gen对应的索引文件存储的所有 docValue 数据
   */
  private final Map<Long,RefCount<DocValuesProducer>> genDVProducers = new HashMap<>();

  /**
   * 初始化一个新的 生产源
   * @param si
   * @param dir
   * @param gen
   * @param infos
   * @return
   * @throws IOException
   */
  private RefCount<DocValuesProducer> newDocValuesProducer(SegmentCommitInfo si, Directory dir, final Long gen, FieldInfos infos) throws IOException {
    Directory dvDir = dir;
    String segmentSuffix = "";
    if (gen.longValue() != -1) {
      dvDir = si.info.dir; // gen'd files are written outside CFS, so use SegInfo directory
      segmentSuffix = Long.toString(gen.longValue(), Character.MAX_RADIX);
    }

    // set SegmentReadState to list only the fields that are relevant to that gen
    // 生成上下文对象
    SegmentReadState srs = new SegmentReadState(dvDir, si.info, infos, IOContext.READ, segmentSuffix);
    DocValuesFormat dvFormat = si.info.getCodec().docValuesFormat();
    // 返回一个 以field为单位 读取 docValue 的 reader对象
    return new RefCount<DocValuesProducer>(dvFormat.fieldsProducer(srs)) {
      @SuppressWarnings("synthetic-access")
      @Override
      protected void release() throws IOException {
        object.close();
        synchronized (SegmentDocValues.this) {
          genDVProducers.remove(gen);
        }
      }
    };
  }

  /** Returns the {@link DocValuesProducer} for the given generation. */
  synchronized DocValuesProducer getDocValuesProducer(long gen, SegmentCommitInfo si, Directory dir, FieldInfos infos) throws IOException {
    RefCount<DocValuesProducer> dvp = genDVProducers.get(gen);
    if (dvp == null) {
      // 生成连接到索引文件的reader 对象
      dvp = newDocValuesProducer(si, dir, gen, infos);
      assert dvp != null;
      genDVProducers.put(gen, dvp);
    } else {
      dvp.incRef();
    }
    return dvp.get();
  }
  
  /**
   * Decrement the reference count of the given {@link DocValuesProducer}
   * generations. 
   */
  synchronized void decRef(List<Long> dvProducersGens) throws IOException {
    IOUtils.applyToAll(dvProducersGens, gen -> {
      RefCount<DocValuesProducer> dvp = genDVProducers.get(gen);
      assert dvp != null : "gen=" + gen;
      dvp.decRef();
    });
  }
}
