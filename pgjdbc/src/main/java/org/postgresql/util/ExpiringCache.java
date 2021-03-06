/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

import org.checkerframework.checker.nullness.qual.KeyFor;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Expiring Cache class. This cache uses a LinkedHashMap to store values with a specific time they
 * were stored. Items that exceed the expired time will be removed when attempting to retrieve them.
 *
 * @param <K> The type of the key to store
 * @param <V> The type of the value to store
 */
public class ExpiringCache<K, V> implements Map<K, V> {

  private int expireTimeMs;

  /**
   * The HashMap which stores the key-value pair
   */
  private final LinkedHashMap<K, Hit<V>> linkedHashMap = new LinkedHashMap<K, Hit<V>>(1, 0.75F, true) {

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, Hit<V>> eldest) {
      if (eldest.getValue().isExpire(expireTimeMs)) {
        Iterator<Hit<V>> i = values().iterator();

        i.next();
        do {
          i.remove();
        } while (i.hasNext() && i.next().isExpire(expireTimeMs));
      }
      return false;
    }
  };

  /**
   * Expiring cache constructor
   *
   * @param expireTimeMs The expired time in Ms
   */
  public ExpiringCache(int expireTimeMs) {
    this.expireTimeMs = expireTimeMs;
  }

  /**
   * Mutator method for expireTimeMs. Sets the expired time for the cache
   *
   * @param expireTimeMs The expired time in Ms
   */
  public void setExpireTime(int expireTimeMs) {
    this.expireTimeMs = expireTimeMs;
  }

  /**
   * Accessor method for expireTimeMs
   *
   * @return Returns the time it takes for the cache to be "expired"
   */
  public int getExpireTime() {
    return this.expireTimeMs;
  }

  /**
   * Retrieves the number of items stored
   *
   * @return The size of items stored
   */
  @Override
  public int size() {
    return (int)this.linkedHashMap.values().stream()
        .filter(x -> !x.isExpire(expireTimeMs))
        .count();
  }

  /**
   * Checks whether or not it is empty
   *
   * @return True if it's empty
   */
  @Override
  public boolean isEmpty() {
    return this.linkedHashMap.values().stream().allMatch(x -> x.isExpire(expireTimeMs));
  }

  /**
   * Checks if the cache contains a specific key
   *
   * @param key The key to search for
   * @return True if the cache contains the key
   */
  @Override
  @SuppressWarnings("keyfor")
  // Disabling keyfor warnings as Checker Framework can't seem to understand our logic. A null check
  // on the payload returned by our underlying map should verify that "key" is indeed a key in this map
  public boolean containsKey(Object key) {
    V payload = this.get(key);
    return payload != null;
  }

  /**
   * Checks if the cache contains a specific value
   *
   * @param value The value to search for
   * @return True if the cache contains that value
   */
  @Override
  public boolean containsValue(Object value) {
    return this.linkedHashMap.values().stream().anyMatch(x -> !x.isExpire(expireTimeMs) && x.payload == value);
  }

  /**
   * Retrieves the value from a key
   *
   * @param key The key in the key-value pair
   * @return The value from the key
   */
  @Override
  public @Nullable V get(Object key) {
    Hit<V> hit = this.linkedHashMap.get(key);

    if (hit == null) {
      return null;
    }

    if (hit.isExpire(expireTimeMs)) {
      this.linkedHashMap.remove(key);
      return null;
    }

    return hit.payload;
  }

  /**
   * Associates a specificed value with a specified key in this map
   *
   * @param key The key in the key-value pair
   * @param value The value in the key-value pair
   * @return The previously associated value of the key. If there isn't any then it would return null
   */
  @Override
  @SuppressWarnings("keyfor")
  // Disabling keyfor warnings as Checker Framework can't seem to understand our logic. Adding the
  // entry to the underlying map is enough to guarantee that "key" is now indeed a key in this map
  public @Nullable V put(K key, V value) {
    Hit<V> prevValue = this.linkedHashMap.put(key, new Hit<>(value));
    return prevValue == null ? null : prevValue.payload;
  }

  /**
   * Removes the mapping for a key if it's present.
   *
   * @param key The key in the map
   * @return The value associated with the key, or null if there were no mappings for the key.
   */
  @Override
  public @Nullable V remove(Object key) {
    Hit<V> prevValue = this.linkedHashMap.remove(key);
    return prevValue == null ? null : prevValue.payload;
  }

  /**
   * Copies the content from one map to the current map
   *
   * @param m The map to copy from
   */
  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
      this.linkedHashMap.put(entry.getKey(), new Hit<>(entry.getValue()));
    }
  }

  /**
   * Clears all mapping from the cache
   */
  @Override
  public void clear() {
    this.linkedHashMap.clear();
  }

  /**
   * Returns a {@link Set} view of the keys
   *
   * @return A set view of the keys in the cache
   */
  @Override
  public Set<@KeyFor("this") K> keySet() {
    return this.linkedHashMap.entrySet().stream()
        .filter(x -> !x.getValue().isExpire(expireTimeMs))
        .map(Entry::getKey)
        .collect(Collectors.toSet());
  }

  /**
   * Returns a {@link Collection} view of the keys
   *
   * @return A collection view of the keys in the cache
   */
  @Override
  public Collection<V> values() {
    return this.linkedHashMap.values().stream()
        .filter(x -> !x.isExpire(expireTimeMs))
        .map(x -> x.payload)
        .collect(Collectors.toList());
  }

  /**
   * Returns a {@link Set} view of the keys
   *
   * @return A set view of the mappings in the cache
   */
  @Override
  public Set<Entry<@KeyFor("this") K, V>> entrySet() {
    return this.linkedHashMap.entrySet().stream()
        .filter(x -> !x.getValue().isExpire(expireTimeMs))
        .map(x -> new AbstractMap.SimpleEntry<>(x.getKey(), x.getValue().payload))
        .collect(Collectors.toSet());
  }

  /**
   * Class to contain the time of when a value was stored
   *
   * @param <V> Type of value
   */
  private static class Hit<V> {
    private final long time;
    private final V payload;

    /**
     * Constructor for Hit. Will record the current time the object will be stored.
     *
     * @param payload The value to store
     */
    Hit(V payload) {
      this.time = System.currentTimeMillis();
      this.payload = payload;
    }

    /**
     * Checks if the item is Expired
     *
     * @param expireTimeMs The expired time
     * @return True if the object is expired
     */
    boolean isExpire(int expireTimeMs) {
      return (System.currentTimeMillis() - this.time) >= expireTimeMs;
    }
  }
}
