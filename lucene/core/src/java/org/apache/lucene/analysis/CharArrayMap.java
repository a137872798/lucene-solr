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
package org.apache.lucene.analysis;


import java.util.Arrays;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A simple class that stores key Strings as char[]'s in a
 * hash table. Note that this is not a general purpose
 * class.  For example, it cannot remove items from the
 * map, nor does it resize its hash table to be smaller,
 * etc.  It is designed to be quick to retrieve items
 * by char[] keys without the necessity of converting
 * to a String first.
 * key 是一个char[]
 */
public class CharArrayMap<V> extends AbstractMap<Object,V> {
  // private only because missing generics
  private static final CharArrayMap<?> EMPTY_MAP = new EmptyCharArrayMap<>();

  /**
   * 初始大小
   */
  private final static int INIT_SIZE = 8;
  /**
   * 是否忽略大小写
   */
  private boolean ignoreCase;  
  private int count;
  // key 实际上是一个string  也就是应该是   string[] keys
  // 不过这里将string 也拆分成了数组  就变成了 char[][]
  // 第一维 用于匹配slot 第二维才真正存储了数据
  char[][] keys; // package private because used in CharArraySet's non Set-conform CharArraySetIterator
  /**
   * 用于存储 value
   */
  V[] values; // package private because used in CharArraySet's non Set-conform CharArraySetIterator

  /**
   * Create map with enough capacity to hold startSize terms
   *
   * @param startSize
   *          the initial capacity
   * @param ignoreCase
   *          <code>false</code> if and only if the set should be case sensitive
   *          otherwise <code>true</code>.
   *          代表通过该容器查找时 是否要忽略大小写
   */
  @SuppressWarnings("unchecked")
  public CharArrayMap(int startSize, boolean ignoreCase) {
    this.ignoreCase = ignoreCase;
    int size = INIT_SIZE;
    // 初始大小 为传入值的 1.5倍
    while(startSize + (startSize>>2) > size)
      size <<= 1;
    keys = new char[size][];
    values = (V[]) new Object[size];
  }

  /**
   * Creates a map from the mappings in another map. 
   *
   * @param c
   *          a map whose mappings to be copied
   * @param ignoreCase
   *          <code>false</code> if and only if the set should be case sensitive
   *          otherwise <code>true</code>.
   */
  public CharArrayMap(Map<?,? extends V> c, boolean ignoreCase) {
    this(c.size(), ignoreCase);
    putAll(c);
  }
  
  /** Create set from the supplied map (used internally for readonly maps...) */
  private CharArrayMap(CharArrayMap<V> toCopy){
    this.keys = toCopy.keys;
    this.values = toCopy.values;
    this.ignoreCase = toCopy.ignoreCase;
    this.count = toCopy.count;
  }
  
  /** Clears all entries in this map. This method is supported for reusing, but not {@link Map#remove}. */
  @Override
  public void clear() {
    count = 0;
    Arrays.fill(keys, null);
    Arrays.fill(values, null);
  }

  /** true if the <code>len</code> chars of <code>text</code> starting at <code>off</code>
   * are in the {@link #keySet()} */
  public boolean containsKey(char[] text, int off, int len) {
    return keys[getSlot(text, off, len)] != null;
  }

  /** true if the <code>CharSequence</code> is in the {@link #keySet()} */
  public boolean containsKey(CharSequence cs) {
    return keys[getSlot(cs)] != null;
  }

  @Override
  public boolean containsKey(Object o) {
    if (o instanceof char[]) {
      final char[] text = (char[])o;
      return containsKey(text, 0, text.length);
    } 
    return containsKey(o.toString());
  }

  /** returns the value of the mapping of <code>len</code> chars of <code>text</code>
   * starting at <code>off</code> */
  // 通过getSlot 能够定位到keys 的下标 而该下标在values查询出来的值 也就是 key 对应的value 值
  public V get(char[] text, int off, int len) {
    return values[getSlot(text, off, len)];
  }

  /** returns the value of the mapping of the chars inside this {@code CharSequence} */
  public V get(CharSequence cs) {
    return values[getSlot(cs)];
  }

  @Override
  public V get(Object o) {
    if (o instanceof char[]) {
      final char[] text = (char[])o;
      return get(text, 0, text.length);
    } 
    return get(o.toString());
  }

  /**
   * 使用off 和 len 从text 中获取真正需要判断的部分 然后判断当前  keys:char[][] 中是否有匹配的key
   * @param text
   * @param off
   * @param len
   * @return
   */
  private int getSlot(char[] text, int off, int len) {
    // 基于部分 char 计算出hash值
    int code = getHashCode(text, off, len);
    // 定位到一维数组的某个下标
    int pos = code & (keys.length-1);
    // 通过hashCode 找到key所在的槽后
    char[] text2 = keys[pos];
    // 对比 key是否相同
    if (text2 != null && !equals(text, off, len, text2)) {
      // 不相同的情况下 计算一个偏移量 这个还要看put是怎么做的   这个实际上是一种线性探测法吧
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        pos = code & (keys.length-1);
        text2 = keys[pos];
      } while (text2 != null && !equals(text, off, len, text2));
    }
    return pos;
  }

  /** Returns true if the String is in the set */  
  private int getSlot(CharSequence text) {
    int code = getHashCode(text);
    int pos = code & (keys.length-1);
    char[] text2 = keys[pos];
    if (text2 != null && !equals(text, text2)) {
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        pos = code & (keys.length-1);
        text2 = keys[pos];
      } while (text2 != null && !equals(text, text2));
    }
    return pos;
  }

  /** Add the given mapping. */
  // 插入某个键值对
  public V put(CharSequence text, V value) {
    return put(text.toString(), value); // could be more efficient
  }

  @Override
  public V put(Object o, V value) {
    if (o instanceof char[]) {
      return put((char[])o, value);
    }
    return put(o.toString(), value);
  }
  
  /** Add the given mapping. */
  public V put(String text, V value) {
    return put(text.toCharArray(), value);
  }

  /** Add the given mapping.
   * If ignoreCase is true for this Set, the text array will be directly modified.
   * The user should never modify this text array after calling this method.
   * 插入某个键值对
   */
  public V put(char[] text, V value) {
    // 如果忽略大小写 一律先转换成小写
    if (ignoreCase) {
      CharacterUtils.toLowerCase(text, 0, text.length);
    }
    // 通过 散列函数 计算出hash值   然后通过线性探测法 解决冲突  只会出现2种情况
    // 1.找到相同key的下标
    // 2.找到一个空的slot
    int slot = getSlot(text, 0, text.length);
    if (keys[slot] != null) {
      // 找到相同下标的slot 并替换数据
      final V oldValue = values[slot];
      values[slot] = value;
      return oldValue;
    }
    // 填充数据
    keys[slot] = text;
    values[slot] = value;
    count++;

    // 必须确保始终有空的 slot可用
    if (count + (count>>2) > keys.length) {
      rehash();
    }

    return null;
  }

  /**
   * 扩容
   */
  @SuppressWarnings("unchecked")
  private void rehash() {
    assert keys.length == values.length;
    final int newSize = 2*keys.length;
    final char[][] oldkeys = keys;
    final V[] oldvalues = values;
    keys = new char[newSize][];
    values = (V[]) new Object[newSize];

    for(int i=0; i<oldkeys.length; i++) {
      char[] text = oldkeys[i];
      if (text != null) {
        // todo: could be faster... no need to compare strings on collision
        final int slot = getSlot(text,0,text.length);
        keys[slot] = text;
        values[slot] = oldvalues[i];
      }
    }
  }

  /**
   *
   * @param text1  需要查找的key
   * @param off
   * @param len
   * @param text2   当前map中存在的key
   * @return
   */
  private boolean equals(char[] text1, int off, int len, char[] text2) {
    // 长度不匹配必然不相同
    if (len != text2.length)
      return false;
    final int limit = off+len;
    if (ignoreCase) {
      for(int i=0;i<len;) {
        final int codePointAt = Character.codePointAt(text1, off+i, limit);
        if (Character.toLowerCase(codePointAt) != Character.codePointAt(text2, i, text2.length))
          return false;
        i += Character.charCount(codePointAt); 
      }
    } else {
      for(int i=0;i<len;i++) {
        if (text1[off+i] != text2[i])
          return false;
      }
    }
    return true;
  }

  private boolean equals(CharSequence text1, char[] text2) {
    int len = text1.length();
    if (len != text2.length)
      return false;
    if (ignoreCase) {
      for(int i=0;i<len;) {
        final int codePointAt = Character.codePointAt(text1, i);
        if (Character.toLowerCase(codePointAt) != Character.codePointAt(text2, i, text2.length))
          return false;
        i += Character.charCount(codePointAt);
      }
    } else {
      for(int i=0;i<len;i++) {
        if (text1.charAt(i) != text2[i])
          return false;
      }
    }
    return true;
  }

  /**
   * 基于部分文本生成 hashCode
   * @param text
   * @param offset
   * @param len
   * @return
   */
  private int getHashCode(char[] text, int offset, int len) {
    if (text == null)
      throw new NullPointerException();
    int code = 0;
    // 找到终点的位置
    final int stop = offset + len;
    // 如果是忽略大小写 那么都要统一转换成 小写
    if (ignoreCase) {
      for (int i=offset; i<stop;) {
        final int codePointAt = Character.codePointAt(text, i, stop);
        code = code*31 + Character.toLowerCase(codePointAt);
        i += Character.charCount(codePointAt);
      }
    } else {
      // 简单来看就是 取指定返回的数据 并计算hashCode
      for (int i=offset; i<stop; i++) {
        code = code*31 + text[i];
      }
    }
    return code;
  }

  /**
   * 使用整个 char流生成hashCode
   * @param text
   * @return
   */
  private int getHashCode(CharSequence text) {
    if (text == null)
      throw new NullPointerException();
    int code = 0;
    int len = text.length();
    if (ignoreCase) {
      for (int i=0; i<len;) {
        int codePointAt = Character.codePointAt(text, i);
        code = code*31 + Character.toLowerCase(codePointAt);
        i += Character.charCount(codePointAt);
      }
    } else {
      for (int i=0; i<len; i++) {
        code = code*31 + text.charAt(i);
      }
    }
    return code;
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return count;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("{");
    for (Map.Entry<Object,V> entry : entrySet()) {
      if (sb.length()>1) sb.append(", ");
      sb.append(entry);
    }
    return sb.append('}').toString();
  }

  private EntrySet entrySet = null;
  /**
   * keySet() 对应的值
   */
  private CharArraySet keySet = null;

  /**
   * 创建 键值对  并且默认允许通过 该对象修改 map的数据
   * @return
   */
  EntrySet createEntrySet() {
    return new EntrySet(true);
  }
  
  @Override
  public final EntrySet entrySet() {
    if (entrySet == null) {
      entrySet = createEntrySet();
    }
    return entrySet;
  }
  
  // helper for CharArraySet to not produce endless recursion
  final Set<Object> originalKeySet() {
    return super.keySet();
  }

  /** Returns an {@link CharArraySet} view on the map's keys.
   * The set will use the same {@code matchVersion} as this map. */
  @Override @SuppressWarnings({"unchecked","rawtypes"})
  public final CharArraySet keySet() {
    if (keySet == null) {
      // prevent adding of entries
      keySet = new CharArraySet((CharArrayMap) this) {
        @Override
        public boolean add(Object o) {
          throw new UnsupportedOperationException();
        }
        @Override
        public boolean add(CharSequence text) {
          throw new UnsupportedOperationException();
        }
        @Override
        public boolean add(String text) {
          throw new UnsupportedOperationException();
        }
        @Override
        public boolean add(char[] text) {
          throw new UnsupportedOperationException();
        }
      };
    }
    return keySet;
  }

  /** public iterator class so efficient methods are exposed to users */
  // 生成迭代器对象
  public class EntryIterator implements Iterator<Map.Entry<Object,V>> {
    private int pos=-1;
    private int lastPos;
    private final boolean allowModify;
    
    private EntryIterator(boolean allowModify) {
      this.allowModify = allowModify;
      goNext();
    }

    private void goNext() {
      lastPos = pos;
      pos++;
      // 因为线性探测法 导致有些 slot是空的 这里要跳过这些空的slot
      while (pos < keys.length && keys[pos] == null) pos++;
    }

    @Override
    public boolean hasNext() {
      return pos < keys.length;
    }

    /** gets the next key... do not modify the returned char[] */
    public char[] nextKey() {
      goNext();
      return keys[lastPos];
    }

    /** gets the next key as a newly created String object */
    public String nextKeyString() {
      return new String(nextKey());
    }

    /** returns the value associated with the last key returned */
    public V currentValue() {
      return values[lastPos];
    }

    /** sets the value associated with the last key returned */    
    public V setValue(V value) {
      if (!allowModify)
        throw new UnsupportedOperationException();
      V old = values[lastPos];
      values[lastPos] = value;
      return old;      
    }

    /** use nextCharArray() + currentValue() for better efficiency. */
    @Override
    public Map.Entry<Object,V> next() {
      goNext();
      return new MapEntry(lastPos, allowModify);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private final class MapEntry implements Map.Entry<Object,V> {
    private final int pos;
    private final boolean allowModify;

    private MapEntry(int pos, boolean allowModify) {
      this.pos = pos;
      this.allowModify = allowModify;
    }

    @Override
    public Object getKey() {
      // we must clone here, as putAll to another CharArrayMap
      // with other case sensitivity flag would corrupt the keys
      return keys[pos].clone();
    }

    @Override
    public V getValue() {
      return values[pos];
    }

    @Override
    public V setValue(V value) {
      if (!allowModify)
        throw new UnsupportedOperationException();
      final V old = values[pos];
      values[pos] = value;
      return old;
    }

    @Override
    public String toString() {
      return new StringBuilder().append(keys[pos]).append('=')
        .append((values[pos] == CharArrayMap.this) ? "(this Map)" : values[pos])
        .toString();
    }
  }

  /** public EntrySet class so efficient methods are exposed to users */
  // 该map 作为entry的职能
  public final class EntrySet extends AbstractSet<Map.Entry<Object,V>> {
    private final boolean allowModify;
    
    private EntrySet(boolean allowModify) {
      this.allowModify = allowModify;
    }
  
    @Override
    public EntryIterator iterator() {
      return new EntryIterator(allowModify);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object o) {
      if (!(o instanceof Map.Entry))
        return false;
      final Map.Entry<Object,V> e = (Map.Entry<Object,V>)o;
      final Object key = e.getKey();
      final Object val = e.getValue();
      final Object v = get(key);
      return v == null ? val == null : v.equals(val);
    }
    
    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public int size() {
      return count;
    }
    
    @Override
    public void clear() {
      if (!allowModify)
        throw new UnsupportedOperationException();
      CharArrayMap.this.clear();
    }
  }
  
  /**
   * Returns an unmodifiable {@link CharArrayMap}. This allows to provide
   * unmodifiable views of internal map for "read-only" use.
   * 
   * @param map
   *          a map for which the unmodifiable map is returned.
   * @return an new unmodifiable {@link CharArrayMap}.
   * @throws NullPointerException
   *           if the given map is <code>null</code>.
   */
  public static <V> CharArrayMap<V> unmodifiableMap(CharArrayMap<V> map) {
    if (map == null)
      throw new NullPointerException("Given map is null");
    if (map == emptyMap() || map.isEmpty())
      return emptyMap();
    if (map instanceof UnmodifiableCharArrayMap)
      return map;
    return new UnmodifiableCharArrayMap<>(map);
  }

  /**
   * Returns a copy of the given map as a {@link CharArrayMap}. If the given map
   * is a {@link CharArrayMap} the ignoreCase property will be preserved.
   * 
   * @param map
   *          a map to copy
   * @return a copy of the given map as a {@link CharArrayMap}. If the given map
   *         is a {@link CharArrayMap} the ignoreCase property as well as the
   *         matchVersion will be of the given map will be preserved.
   */
  @SuppressWarnings("unchecked")
  public static <V> CharArrayMap<V> copy(final Map<?,? extends V> map) {
    if(map == EMPTY_MAP)
      return emptyMap();
    if(map instanceof CharArrayMap) {
      CharArrayMap<V> m = (CharArrayMap<V>) map;
      // use fast path instead of iterating all values
      // this is even on very small sets ~10 times faster than iterating
      final char[][] keys = new char[m.keys.length][];
      System.arraycopy(m.keys, 0, keys, 0, keys.length);
      final V[] values = (V[]) new Object[m.values.length];
      System.arraycopy(m.values, 0, values, 0, values.length);
      m = new CharArrayMap<>(m);
      m.keys = keys;
      m.values = values;
      return m;
    }
    return new CharArrayMap<>(map, false);
  }
  
  /** Returns an empty, unmodifiable map. */
  @SuppressWarnings("unchecked")
  public static <V> CharArrayMap<V> emptyMap() {
    return (CharArrayMap<V>) EMPTY_MAP;
  }
  
  // package private CharArraySet instanceof check in CharArraySet
  static class UnmodifiableCharArrayMap<V> extends CharArrayMap<V> {

    UnmodifiableCharArrayMap(CharArrayMap<V> map) {
      super(map);
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public V put(Object o, V val){
      throw new UnsupportedOperationException();
    }
    
    @Override
    public V put(char[] text, V val) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V put(CharSequence text, V val) {
      throw new UnsupportedOperationException();
    }

    @Override
    public V put(String text, V val) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public V remove(Object key) {
      throw new UnsupportedOperationException();
    }
  
    @Override
    EntrySet createEntrySet() {
      return new EntrySet(false);
    }
  }
  
  /**
   * Empty {@link org.apache.lucene.analysis.CharArrayMap.UnmodifiableCharArrayMap} optimized for speed.
   * Contains checks will always return <code>false</code> or throw
   * NPE if necessary.
   */
  private static final class EmptyCharArrayMap<V> extends UnmodifiableCharArrayMap<V> {
    EmptyCharArrayMap() {
      super(new CharArrayMap<V>(0, false));
    }
    
    @Override
    public boolean containsKey(char[] text, int off, int len) {
      if(text == null)
        throw new NullPointerException();
      return false;
    }

    @Override
    public boolean containsKey(CharSequence cs) {
      if(cs == null)
        throw new NullPointerException();
      return false;
    }

    @Override
    public boolean containsKey(Object o) {
      if(o == null)
        throw new NullPointerException();
      return false;
    }
    
    @Override
    public V get(char[] text, int off, int len) {
      if(text == null)
        throw new NullPointerException();
      return null;
    }

    @Override
    public V get(CharSequence cs) {
      if(cs == null)
        throw new NullPointerException();
      return null;
    }

    @Override
    public V get(Object o) {
      if(o == null)
        throw new NullPointerException();
      return null;
    }
  }
}
