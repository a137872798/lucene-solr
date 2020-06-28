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
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.CRC32;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.util.BitUtil;

/**
 * A {@link ByteBuffer}-based {@link Directory} implementation that
 * can be used to store index files on the heap.
 *
 * <p>Important: Note that {@link MMapDirectory} is nearly always a better choice as
 * it uses OS caches more effectively (through memory-mapped buffers).
 * A heap-based directory like this one can have the advantage in case of ephemeral, small,
 * short-lived indexes when disk syncs provide an additional overhead.</p>
 *
 * @lucene.experimental
 * 这是一个内存目录  也就是数据实际上还是写入到 内存中
 */
public final class ByteBuffersDirectory extends BaseDirectory {

  /**
   * 将多个 ByteBuffersDataOutput 模拟成一个 输出流
   */
  public static final BiFunction<String, ByteBuffersDataOutput, IndexInput> OUTPUT_AS_MANY_BUFFERS = 
      (fileName, output) -> {
        ByteBuffersDataInput dataInput = output.toDataInput();
        String inputName = String.format(Locale.ROOT, "%s (file=%s, buffers=%s)",
            ByteBuffersIndexInput.class.getSimpleName(),
            fileName,
            dataInput.toString());
        return new ByteBuffersIndexInput(dataInput, inputName);
      };

  public static final BiFunction<String, ByteBuffersDataOutput, IndexInput> OUTPUT_AS_ONE_BUFFER = 
      (fileName, output) -> {
        // 该对象内部只有一个  ByteBuffer
        ByteBuffersDataInput dataInput = new ByteBuffersDataInput(Arrays.asList(ByteBuffer.wrap(output.toArrayCopy())));
        String inputName = String.format(Locale.ROOT, "%s (file=%s, buffers=%s)",
            ByteBuffersIndexInput.class.getSimpleName(),
            fileName,
            dataInput.toString());
        return new ByteBuffersIndexInput(dataInput, inputName);
      };

  public static final BiFunction<String, ByteBuffersDataOutput, IndexInput> OUTPUT_AS_BYTE_ARRAY = OUTPUT_AS_ONE_BUFFER;

  public static final BiFunction<String, ByteBuffersDataOutput, IndexInput> OUTPUT_AS_MANY_BUFFERS_LUCENE = 
      (fileName, output) -> {
        List<ByteBuffer> bufferList = output.toBufferList();
        // 这里添加了一个空的 byteBuffer
        bufferList.add(ByteBuffer.allocate(0));

        int chunkSizePower;
        int blockSize = ByteBuffersDataInput.determineBlockPage(bufferList);
        // block的大小是 2的多少次
        if (blockSize == 0) {
          chunkSizePower = 30;
        } else {
          chunkSizePower = Integer.numberOfTrailingZeros(BitUtil.nextHighestPowerOfTwo(blockSize));
        }

        String inputName = String.format(Locale.ROOT, "%s (file=%s)",
            ByteBuffersDirectory.class.getSimpleName(),
            fileName);

        // 这里生成了一个守卫对象  该对象在buffer使用完毕后会选择释放内存(针对DirectByteBuffer  但是像ByteBuffer这种就不需要释放内存了  )
        ByteBufferGuard guard = new ByteBufferGuard("none", (String resourceDescription, ByteBuffer b) -> {});
        return ByteBufferIndexInput.newInstance(inputName,
            bufferList.toArray(new ByteBuffer [bufferList.size()]),
            output.size(), chunkSizePower, guard);
      };

  /**
   * 该函数用于决定临时文件的文件名
   */
  private final Function<String, String> tempFileName = new Function<String, String>() {
    private final AtomicLong counter = new AtomicLong();

    @Override
    public String apply(String suffix) {
      return suffix + "_" + Long.toString(counter.getAndIncrement(), Character.MAX_RADIX);
    }
  };

  /**
   * 模拟内部存储的一组文件
   */
  private final ConcurrentHashMap<String, FileEntry> files = new ConcurrentHashMap<>();

  /**
   * Conversion between a buffered index output and the corresponding index input
   * for a given file.
   * 该函数根据传入的 output 转换成 input
   */
  private final BiFunction<String, ByteBuffersDataOutput, IndexInput> outputToInput;

  /**
   * A supplier of {@link ByteBuffersDataOutput} instances used to buffer up 
   * the content of written files.
   * 该对象负责产生 Output对象
   */
  private final Supplier<ByteBuffersDataOutput> bbOutputSupplier;

  /**
   * 因为是针对内存级别的锁 所以只要保证线程间隔就好
   */
  public ByteBuffersDirectory() {
    this(new SingleInstanceLockFactory());
  }
  
  public ByteBuffersDirectory(LockFactory lockFactory) {
    this(lockFactory, ByteBuffersDataOutput::new, OUTPUT_AS_MANY_BUFFERS);
  }

  public ByteBuffersDirectory(LockFactory factory, 
      Supplier<ByteBuffersDataOutput> bbOutputSupplier,
      BiFunction<String, ByteBuffersDataOutput, IndexInput> outputToInput) {
    super(factory);
    this.outputToInput = Objects.requireNonNull(outputToInput);
    this.bbOutputSupplier = Objects.requireNonNull(bbOutputSupplier);
  }

  /**
   * 将基于内存模拟的文件名返回
   * @return
   * @throws IOException
   */
  @Override
  public String[] listAll() throws IOException {
    ensureOpen();
    return files.keySet().stream().sorted().toArray(String[]::new);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    FileEntry removed = files.remove(name);
    if (removed == null) {
      throw new NoSuchFileException(name);
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    FileEntry file = files.get(name);
    if (file == null) {
      throw new NoSuchFileException(name);
    }
    return file.length();
  }

  public boolean fileExists(String name) {
    ensureOpen();
    FileEntry file = files.get(name);
    return file != null;
  }

  /**
   * IndexOutput 模拟了一种索引文件   DataOutput 仅仅模拟数据输出流
   * @param name the name of the file to create.
   * @param context
   * @return
   * @throws IOException
   */
  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    FileEntry e = new FileEntry(name); 
    if (files.putIfAbsent(name, e) != null) {
      throw new FileAlreadyExistsException("File already exists: " + name);
    }
    return e.createOutput(outputToInput);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    ensureOpen();
    while (true) {
      // 创建的文件会携带 .tmp 的后缀名   代表一个临时文件
      String name = IndexFileNames.segmentFileName(prefix, tempFileName.apply(suffix), "tmp");
      FileEntry e = new FileEntry(name); 
      if (files.putIfAbsent(name, e) == null) {
        return e.createOutput(outputToInput);
      }
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    ensureOpen();

    FileEntry file = files.get(source);
    if (file == null) {
      throw new NoSuchFileException(source);
    }
    if (files.putIfAbsent(dest, file) != null) {
      throw new FileAlreadyExistsException(dest);
    }
    if (!files.remove(source, file)) {
      throw new IllegalStateException("File was unexpectedly replaced: " + source);
    }
    files.remove(source);
  }

  // 因为是基于内存的所有没有 刷盘或者写入元数据的动作
  @Override
  public void sync(Collection<String> names) throws IOException {
    ensureOpen();
  }

  @Override
  public void syncMetaData() throws IOException {
    ensureOpen();
  }

  /**
   * 获取对应文件(内存) 的输入流   必须要等待写入完毕 调用close 后才能获取input
   * @param name the name of an existing file.   定位资源位置
   * @param context
   * @return
   * @throws IOException
   */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    FileEntry e = files.get(name);
    if (e == null) {
      throw new NoSuchFileException(name);
    } else {
      return e.openInput();
    }
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
    files.clear();
  }

  @Override
  public Set<String> getPendingDeletions() {
    return Collections.emptySet();
  }

  /**
   * 这里使用内存模拟了一个文件
   */
  private final class FileEntry {
    private final String fileName;

    /**
     * 用于读取内部数据的输入流
     */
    private volatile IndexInput content;
    private volatile long cachedLength;

    public FileEntry(String name) {
      this.fileName = name;
    }

    public long length() {
      // We return 0 length until the IndexOutput is closed and flushed.
      return cachedLength;
    }

    public IndexInput openInput() throws IOException {
      IndexInput local = this.content;
      if (local == null) {
        throw new AccessDeniedException("Can't open a file still open for writing: " + fileName);
      }

      return local.clone();
    }

    final IndexOutput createOutput(BiFunction<String, ByteBuffersDataOutput, IndexInput> outputToInput) throws IOException {
      if (content != null) {
        throw new IOException("Can only write to a file once: " + fileName);
      }

      String clazzName = ByteBuffersDirectory.class.getSimpleName();
      String outputName = String.format(Locale.ROOT, "%s output (file=%s)", clazzName, fileName);

      return new ByteBuffersIndexOutput(
          bbOutputSupplier.get(), outputName, fileName,
          new CRC32(),
          // 该函数对应  close()动作  代表在输出流被关闭时  填充内部的文本  便于读取数据
          (output) -> {
            content = outputToInput.apply(fileName, output);
            cachedLength = output.size();
          });
    }
  }
}
