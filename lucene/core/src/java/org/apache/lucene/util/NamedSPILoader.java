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
package org.apache.lucene.util;


import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Helper class for loading named SPIs from classpath (e.g. Codec, PostingsFormat).
 * @lucene.internal
 * 可以通过名称查找SPI 实现类   原本的话是通过 Interface 来定位SPI实现类的
 */
public final class NamedSPILoader<S extends NamedSPILoader.NamedSPI> implements Iterable<S> {

  /**
   * 存储了 name 与 SPIImpl的映射关系
   */
  private volatile Map<String,S> services = Collections.emptyMap();
  /**
   * 对应原生的SPI 基于该接口查找实现类
   * 首先要求该接口是 NamedSPI 子接口
   */
  private final Class<S> clazz;

  public NamedSPILoader(Class<S> clazz) {
    this(clazz, null);
  }
  
  public NamedSPILoader(Class<S> clazz, ClassLoader classloader) {
    this.clazz = clazz;
    // if clazz' classloader is not a parent of the given one, we scan clazz's classloader, too:
    final ClassLoader clazzClassloader = clazz.getClassLoader();
    if (classloader == null) {
      classloader = clazzClassloader;
    }
    // 如果不归属双亲委派模型 那么就各自加载类
    if (clazzClassloader != null && !ClassLoaderUtils.isParentClassLoader(clazzClassloader, classloader)) {
      reload(clazzClassloader);
    }
    reload(classloader);
  }
  
  /** 
   * Reloads the internal SPI list from the given {@link ClassLoader}.
   * Changes to the service list are visible after the method ends, all
   * iterators ({@link #iterator()},...) stay consistent. 
   * 
   * <p><b>NOTE:</b> Only new service providers are added, existing ones are
   * never removed or replaced.
   * 
   * <p><em>This method is expensive and should only be called for discovery
   * of new service providers on the given classpath/classloader!</em>
   */
  public void reload(ClassLoader classloader) {
    Objects.requireNonNull(classloader, "classloader");
    final LinkedHashMap<String,S> services = new LinkedHashMap<>(this.services);
    // 基于原生的 JDK SPI 机制 加载类  并将类名与实例的映射关系存储到容器中
    for (final S service : ServiceLoader.load(clazz, classloader)) {
      final String name = service.getName();
      // only add the first one for each name, later services will be ignored
      // this allows to place services before others in classpath to make 
      // them used instead of others
      if (!services.containsKey(name)) {
        checkServiceName(name);
        services.put(name, service);
      }
    }
    this.services = Collections.unmodifiableMap(services);
  }
  
  /**
   * Validates that a service name meets the requirements of {@link NamedSPI}
   */
  public static void checkServiceName(String name) {
    // based on harmony charset.java
    if (name.length() >= 128) {
      throw new IllegalArgumentException("Illegal service name: '" + name + "' is too long (must be < 128 chars).");
    }
    for (int i = 0, len = name.length(); i < len; i++) {
      char c = name.charAt(i);
      if (!isLetterOrDigit(c)) {
        throw new IllegalArgumentException("Illegal service name: '" + name + "' must be simple ascii alphanumeric.");
      }
    }
  }
  
  /**
   * Checks whether a character is a letter or digit (ascii) which are defined in the spec.
   */
  private static boolean isLetterOrDigit(char c) {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9');
  }
  
  public S lookup(String name) {
    final S service = services.get(name);
    if (service != null) return service;
    throw new IllegalArgumentException("An SPI class of type "+clazz.getName()+" with name '"+name+"' does not exist."+
     "  You need to add the corresponding JAR file supporting this SPI to your classpath."+
     "  The current classpath supports the following names: "+availableServices());
  }

  public Set<String> availableServices() {
    return services.keySet();
  }
  
  @Override
  public Iterator<S> iterator() {
    return services.values().iterator();
  }
  
  /**
   * Interface to support {@link NamedSPILoader#lookup(String)} by name.
   * <p>
   * Names must be all ascii alphanumeric, and less than 128 characters in length.
   */
  public static interface NamedSPI {
    String getName();
  }
  
}
