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


import static java.lang.invoke.MethodHandles.*;
import static java.lang.invoke.MethodType.methodType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException; // javadoc @link
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Future;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;

import org.apache.lucene.store.ByteBufferGuard.BufferCleaner;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SuppressForbidden;

/**
 * File-based {@link Directory} implementation that uses
 * mmap for reading, and {@link
 * FSDirectory.FSIndexOutput} for writing.
 *
 * <p><b>NOTE</b>: memory mapping uses up a portion of the
 * virtual memory address space in your process equal to the
 * size of the file being mapped.  Before using this class,
 * be sure your have plenty of virtual address space, e.g. by
 * using a 64 bit JRE, or a 32 bit JRE with indexes that are
 * guaranteed to fit within the address space.
 * On 32 bit platforms also consult {@link #MMapDirectory(Path, LockFactory, int)}
 * if you have problems with mmap failing because of fragmented
 * address space. If you get an OutOfMemoryException, it is recommended
 * to reduce the chunk size, until it works.
 *
 * <p>Due to <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038">
 * this bug</a> in Sun's JRE, MMapDirectory's {@link IndexInput#close}
 * is unable to close the underlying OS file handle.  Only when GC
 * finally collects the underlying objects, which could be quite
 * some time later, will the file handle be closed.
 *
 * <p>This will consume additional transient disk usage: on Windows,
 * attempts to delete or overwrite the files will result in an
 * exception; on other platforms, which typically have a &quot;delete on
 * last close&quot; semantics, while such operations will succeed, the bytes
 * are still consuming space on disk.  For many applications this
 * limitation is not a problem (e.g. if you have plenty of disk space,
 * and you don't rely on overwriting files on Windows) but it's still
 * an important limitation to be aware of.
 *
 * <p>This class supplies the workaround mentioned in the bug report
 * (see {@link #setUseUnmap}), which may fail on
 * non-Oracle/OpenJDK JVMs. It forcefully unmaps the buffer on close by using
 * an undocumented internal cleanup functionality. If
 * {@link #UNMAP_SUPPORTED} is <code>true</code>, the workaround
 * will be automatically enabled (with no guarantees; if you discover
 * any problems, you can disable it).
 * <p>
 * <b>NOTE:</b> Accessing this class either directly or
 * indirectly from a thread while it's interrupted can close the
 * underlying channel immediately if at the same time the thread is
 * blocked on IO. The channel will remain closed and subsequent access
 * to {@link MMapDirectory} will throw a {@link ClosedChannelException}. If
 * your application uses either {@link Thread#interrupt()} or
 * {@link Future#cancel(boolean)} you should use the legacy {@code RAFDirectory}
 * from the Lucene {@code misc} module in favor of {@link MMapDirectory}.
 * </p>
 *
 * @see <a href="http://blog.thetaphi.de/2012/07/use-lucenes-mmapdirectory-on-64bit.html">Blog post about MMapDirectory</a>
 * 基于 mmap 的目录对象 那么就是通过内存映射的方式来减少一次用户态到内核态的数据拷贝
 */
public class MMapDirectory extends FSDirectory {
    private boolean useUnmapHack = UNMAP_SUPPORTED;
    /**
     * 代表 mmap 创建后需要预加载
     */
    private boolean preload;

    /**
     * Default max chunk size.
     *
     * @see #MMapDirectory(Path, LockFactory, int)
     */
    public static final int DEFAULT_MAX_CHUNK_SIZE = Constants.JRE_IS_64BIT ? (1 << 30) : (1 << 28);

    /**
     * chunkSize 是2的多少次幂
     */
    final int chunkSizePower;

    /**
     * Create a new MMapDirectory for the named location.
     * The directory is created at the named location if it does not yet exist.
     *
     * @param path        the path of the directory
     * @param lockFactory the lock factory to use
     * @throws IOException if there is a low-level I/O error
     */
    public MMapDirectory(Path path, LockFactory lockFactory) throws IOException {
        this(path, lockFactory, DEFAULT_MAX_CHUNK_SIZE);
    }

    /**
     * Create a new MMapDirectory for the named location and {@link FSLockFactory#getDefault()}.
     * The directory is created at the named location if it does not yet exist.
     *
     * @param path the path of the directory
     * @throws IOException if there is a low-level I/O error
     */
    public MMapDirectory(Path path) throws IOException {
        this(path, FSLockFactory.getDefault());
    }

    /**
     * Create a new MMapDirectory for the named location and {@link FSLockFactory#getDefault()}.
     * The directory is created at the named location if it does not yet exist.
     *
     * @param path         the path of the directory
     * @param maxChunkSize maximum chunk size (default is 1 GiBytes for
     *                     64 bit JVMs and 256 MiBytes for 32 bit JVMs) used for memory mapping.
     * @throws IOException if there is a low-level I/O error
     */
    public MMapDirectory(Path path, int maxChunkSize) throws IOException {
        this(path, FSLockFactory.getDefault(), maxChunkSize);
    }

    /**
     * Create a new MMapDirectory for the named location, specifying the
     * maximum chunk size used for memory mapping.
     * The directory is created at the named location if it does not yet exist.
     * <p>
     * Especially on 32 bit platform, the address space can be very fragmented,
     * so large index files cannot be mapped. Using a lower chunk size makes
     * the directory implementation a little bit slower (as the correct chunk
     * may be resolved on lots of seeks) but the chance is higher that mmap
     * does not fail. On 64 bit Java platforms, this parameter should always
     * be {@code 1 << 30}, as the address space is big enough.
     * <p>
     * <b>Please note:</b> The chunk size is always rounded down to a power of 2.
     *
     * @param path         the path of the directory
     * @param lockFactory  the lock factory to use, or null for the default
     *                     ({@link NativeFSLockFactory});
     * @param maxChunkSize maximum chunk size (default is 1 GiBytes for
     *                     64 bit JVMs and 256 MiBytes for 32 bit JVMs) used for memory mapping.
     * @throws IOException if there is a low-level I/O error
     */
    public MMapDirectory(Path path, LockFactory lockFactory, int maxChunkSize) throws IOException {
        super(path, lockFactory);
        if (maxChunkSize <= 0) {
            throw new IllegalArgumentException("Maximum chunk size for mmap must be >0");
        }
        // size 是 2的多少次幂
        this.chunkSizePower = 31 - Integer.numberOfLeadingZeros(maxChunkSize);
        assert this.chunkSizePower >= 0 && this.chunkSizePower <= 30;
    }

    /**
     * This method enables the workaround for unmapping the buffers
     * from address space after closing {@link IndexInput}, that is
     * mentioned in the bug report. This hack may fail on non-Oracle/OpenJDK JVMs.
     * It forcefully unmaps the buffer on close by using
     * an undocumented internal cleanup functionality.
     * <p><b>NOTE:</b> Enabling this is completely unsupported
     * by Java and may lead to JVM crashes if <code>IndexInput</code>
     * is closed while another thread is still accessing it (SIGSEGV).
     * <p>To enable the hack, the following requirements need to be
     * fulfilled: The used JVM must be Oracle Java / OpenJDK 8
     * <em>(preliminary support for Java 9 EA build 150+ was added with Lucene 6.4)</em>.
     * In addition, the following permissions need to be granted
     * to {@code lucene-core.jar} in your
     * <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/security/PolicyFiles.html">policy file</a>:
     * <ul>
     * <li>{@code permission java.lang.reflect.ReflectPermission "suppressAccessChecks";}</li>
     * <li>{@code permission java.lang.RuntimePermission "accessClassInPackage.sun.misc";}</li>
     * </ul>
     *
     * @throws IllegalArgumentException if {@link #UNMAP_SUPPORTED}
     *                                  is <code>false</code> and the workaround cannot be enabled.
     *                                  The exception message also contains an explanation why the hack
     *                                  cannot be enabled (e.g., missing permissions).
     */
    public void setUseUnmap(final boolean useUnmapHack) {
        if (useUnmapHack && !UNMAP_SUPPORTED) {
            throw new IllegalArgumentException(UNMAP_NOT_SUPPORTED_REASON);
        }
        this.useUnmapHack = useUnmapHack;
    }

    /**
     * Returns <code>true</code>, if the unmap workaround is enabled.
     *
     * @see #setUseUnmap
     */
    public boolean getUseUnmap() {
        return useUnmapHack;
    }

    /**
     * Set to {@code true} to ask mapped pages to be loaded
     * into physical memory on init. The behavior is best-effort
     * and operating system dependent.
     *
     * @see MappedByteBuffer#load
     */
    public void setPreload(boolean preload) {
        this.preload = preload;
    }

    /**
     * Returns {@code true} if mapped pages should be loaded.
     *
     * @see #setPreload
     */
    public boolean getPreload() {
        return preload;
    }

    /**
     * Returns the current mmap chunk size.
     *
     * @see #MMapDirectory(Path, LockFactory, int)
     */
    public final int getMaxChunkSize() {
        return 1 << chunkSizePower;
    }

    /**
     * Creates an IndexInput for the file with the given name.
     */
    // 使用给定的name 从目录下找到某个文件 并生成输入流
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ensureCanRead(name);
        Path path = directory.resolve(name);
        try (FileChannel c = FileChannel.open(path, StandardOpenOption.READ)) {
            final String resourceDescription = "MMapIndexInput(path=\"" + path.toString() + "\")";
            final boolean useUnmap = getUseUnmap();
            // 创建一个 mmap 对象 同时创建一个 guard对象 管理该byteBuffer的回收工作
            return ByteBufferIndexInput.newInstance(resourceDescription,
                    map(resourceDescription, c, 0, c.size()),
                    c.size(), chunkSizePower, new ByteBufferGuard(resourceDescription, useUnmap ? CLEANER : null));
        }
    }

    /** Maps a file into a set of buffers */
    /**
     * 根据文件通道创建一组 MMapByteBuffer
     *
     * @param resourceDescription
     * @param fc
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    final ByteBuffer[] map(String resourceDescription, FileChannel fc, long offset, long length) throws IOException {
        // 将size换算成 chunk 后 发现chunk数量过多
        if ((length >>> chunkSizePower) >= Integer.MAX_VALUE)
            throw new IllegalArgumentException("RandomAccessFile too big for chunk size: " + resourceDescription);

        final long chunkSize = 1L << chunkSizePower;

        // we always allocate one more buffer, the last one may be a 0 byte one
        // 总是多分配一个 Buffer
        final int nrBuffers = (int) (length >>> chunkSizePower) + 1;

        ByteBuffer buffers[] = new ByteBuffer[nrBuffers];

        long bufferStart = 0L;
        for (int bufNr = 0; bufNr < nrBuffers; bufNr++) {
            // 计算当前buffer的长度     最后一个buffer的长度为0
            int bufSize = (int) ((length > (bufferStart + chunkSize)) ? chunkSize : (length - bufferStart));
            MappedByteBuffer buffer;
            try {
                // 为该段文件创建内存映射
                buffer = fc.map(MapMode.READ_ONLY, offset + bufferStart, bufSize);
            } catch (IOException ioe) {
                throw convertMapFailedIOException(ioe, resourceDescription, bufSize);
            }
            if (preload) {
                // 进行预加载  当调用map 时 只是建立了 内核态到用户态的内存映射关系  实际上此时文件的数据还没有加载到内存中
                // 而调用该方法会强制将内存页替换成文件的数据页  这样有助于之后连续的数据写入/读取  否则还是在使用时因为缺页触发的被动加载比较好
                // 如果这个文件此时还没有任何数据的话 应该就是提前创建一系列空的值来占位
                buffer.load();
            }
            buffers[bufNr] = buffer;
            bufferStart += bufSize;
        }

        return buffers;
    }

    private IOException convertMapFailedIOException(IOException ioe, String resourceDescription, int bufSize) {
        final String originalMessage;
        final Throwable originalCause;
        if (ioe.getCause() instanceof OutOfMemoryError) {
            // nested OOM confuses users, because it's "incorrect", just print a plain message:
            originalMessage = "Map failed";
            originalCause = null;
        } else {
            originalMessage = ioe.getMessage();
            originalCause = ioe.getCause();
        }
        final String moreInfo;
        if (!Constants.JRE_IS_64BIT) {
            moreInfo = "MMapDirectory should only be used on 64bit platforms, because the address space on 32bit operating systems is too small. ";
        } else if (Constants.WINDOWS) {
            moreInfo = "Windows is unfortunately very limited on virtual address space. If your index size is several hundred Gigabytes, consider changing to Linux. ";
        } else if (Constants.LINUX) {
            moreInfo = "Please review 'ulimit -v', 'ulimit -m' (both should return 'unlimited'), and 'sysctl vm.max_map_count'. ";
        } else {
            moreInfo = "Please review 'ulimit -v', 'ulimit -m' (both should return 'unlimited'). ";
        }
        final IOException newIoe = new IOException(String.format(Locale.ENGLISH,
                "%s: %s [this may be caused by lack of enough unfragmented virtual address space " +
                        "or too restrictive virtual memory limits enforced by the operating system, " +
                        "preventing us to map a chunk of %d bytes. %sMore information: " +
                        "http://blog.thetaphi.de/2012/07/use-lucenes-mmapdirectory-on-64bit.html]",
                originalMessage, resourceDescription, bufSize, moreInfo), originalCause);
        newIoe.setStackTrace(ioe.getStackTrace());
        return newIoe;
    }

    /**
     * <code>true</code>, if this platform supports unmapping mmapped files.
     */
    public static final boolean UNMAP_SUPPORTED;

    /**
     * if {@link #UNMAP_SUPPORTED} is {@code false}, this contains the reason why unmapping is not supported.
     */
    public static final String UNMAP_NOT_SUPPORTED_REASON;

    /**
     * Reference to a BufferCleaner that does unmapping; {@code null} if not supported.
     */
    private static final BufferCleaner CLEANER;

    /**
     * 该对象在创建的时候会检测当前系统是否支持使用 mmap
     */
    static {
        final Object hack = AccessController.doPrivileged((PrivilegedAction<Object>) MMapDirectory::unmapHackImpl);
        if (hack instanceof BufferCleaner) {
            CLEANER = (BufferCleaner) hack;
            // 代表支持 mmap
            UNMAP_SUPPORTED = true;
            UNMAP_NOT_SUPPORTED_REASON = null;
        } else {
            CLEANER = null;
            UNMAP_SUPPORTED = false;
            UNMAP_NOT_SUPPORTED_REASON = hack.toString();
        }
    }

    /**
     * 通过Unsafe类 检测是否能使用 Cleaner
     *
     * @return
     */
    @SuppressForbidden(reason = "Needs access to private APIs in DirectBuffer, sun.misc.Cleaner, and sun.misc.Unsafe to enable hack")
    private static Object unmapHackImpl() {
        final Lookup lookup = lookup();
        try {
            // *** sun.misc.Unsafe unmapping (Java 9+) ***
            final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            // first check if Unsafe has the right method, otherwise we can give up
            // without doing any security critical stuff:
            // 创建句柄对象 找到 Unsafe的 invokeCleaner 方法
            final MethodHandle unmapper = lookup.findVirtual(unsafeClass, "invokeCleaner",
                    methodType(void.class, ByteBuffer.class));
            // fetch the unsafe instance and bind it to the virtual MH:
            final Field f = unsafeClass.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            final Object theUnsafe = f.get(null);
            return newBufferCleaner(ByteBuffer.class, unmapper.bindTo(theUnsafe));
        } catch (SecurityException se) {
            return "Unmapping is not supported, because not all required permissions are given to the Lucene JAR file: " + se +
                    " [Please grant at least the following permissions: RuntimePermission(\"accessClassInPackage.sun.misc\") " +
                    " and ReflectPermission(\"suppressAccessChecks\")]";
        } catch (ReflectiveOperationException | RuntimeException e) {
            return "Unmapping is not supported on this platform, because internal Java APIs are not compatible with this Lucene version: " + e;
        }
    }

    /**
     * 通过调用句柄生成 cleaner对象  这里跟kafka使用mmap一毛一样 也是使用句柄来创建的
     *
     * @param unmappableBufferClass 应该是入参的类型吧
     * @param unmapper
     * @return
     */
    private static BufferCleaner newBufferCleaner(final Class<?> unmappableBufferClass, final MethodHandle unmapper) {
        assert Objects.equals(methodType(void.class, ByteBuffer.class), unmapper.type());
        return (String resourceDescription, ByteBuffer buffer) -> {
            // 无法映射 非DirectBuffer
            if (!buffer.isDirect()) {
                throw new IllegalArgumentException("unmapping only works with direct buffers");
            }
            if (!unmappableBufferClass.isInstance(buffer)) {
                throw new IllegalArgumentException("buffer is not an instance of " + unmappableBufferClass.getName());
            }
            final Throwable error = AccessController.doPrivileged((PrivilegedAction<Throwable>) () -> {
                try {
                    // invokeExact 在执行时方法必须严格匹配参数类型  如果找不到适配的方法  抛出异常
                    unmapper.invokeExact(buffer);
                    return null;
                } catch (Throwable t) {
                    return t;
                }
            });
            if (error != null) {
                throw new IOException("Unable to unmap the mapped buffer: " + resourceDescription, error);
            }
        };
    }

}
