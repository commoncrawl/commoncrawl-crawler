package org.commoncrawl.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
* An LRU cache, based on <code>LinkedHashMap</code>.<br>
* This cache has a fixed maximum number of elements (<code>cacheSize</code>).
* If the cache is full and another entry is added, the LRU (least recently used) entry is dropped.
* <p>
* This class is thread-safe. All methods of this class are synchronized.<br>
* Author: Christian d'Heureuse (<a href="http://www.source-code.biz">www.source-code.biz</a>)<br>
* License: <a href="http://www.gnu.org/licenses/lgpl.html">LGPL</a>.
*/

public class LRUCache<K,V> {

  private static final float   hashTableLoadFactor = 0.75f;

  private LinkedHashMap<K,V>   map;
  private int                  cacheSize;

  /**
  * Creates a new LRU cache.
  * @param cacheSize the maximum number of entries that will be kept in this cache.
  */
  public LRUCache (int cacheSize) {
     this.cacheSize = cacheSize;
     int hashTableCapacity = (int)Math.ceil(cacheSize / hashTableLoadFactor) + 1;
     map = new LinkedHashMap<K,V>(hashTableCapacity, hashTableLoadFactor, true) {
        // (an anonymous inner class)
        private static final long serialVersionUID = 1;
        @Override protected boolean removeEldestEntry (Map.Entry<K,V> eldest) {
           return size() > LRUCache.this.cacheSize; }}; }

  /**
  * Retrieves an entry from the cache.<br>
  * The retrieved entry becomes the MRU (most recently used) entry.
  * @param key the key whose associated value is to be returned.
  * @return    the value associated to this key, or null if no value with this key exists in the cache.
  */
  public synchronized V get (K key) {
     return map.get(key); }

  /**
  * Adds an entry to this cache.
  * If the cache is full, the LRU (least recently used) entry is dropped.
  * @param key    the key with which the specified value is to be associated.
  * @param value  a value to be associated with the specified key.
  */
  public synchronized void put (K key, V value) {
     map.put (key,value); }

  /**
  * Clears the cache.
  */
  public synchronized void clear() {
     map.clear(); }

  /**
  * Returns the number of used entries in the cache.
  * @return the number of entries currently in the cache.
  */
  public synchronized int usedEntries() {
     return map.size(); }

  /**
  * Returns a <code>Collection</code> that contains a copy of all cache entries.
  * @return a <code>Collection</code> with a copy of the cache content.
  */
  public synchronized Collection<Map.Entry<K,V>> getAll() {
     return new ArrayList<Map.Entry<K,V>>(map.entrySet()); }
  
}
