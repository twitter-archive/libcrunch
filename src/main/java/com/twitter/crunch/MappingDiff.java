/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.crunch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class that computes the diffs between two mappings. The diff is based on the
 * <code>equals</code> contract. It also indicates whether the particular change is addition or
 * removal.
 */
public class MappingDiff {
  /**
   * Returns the difference between the two mappings. It only contains the keys with which there are
   * any differences. The value lists are neither null nor empty. There is no particular ordering in
   * the value returned, so one should not rely on the ordering of values.
   */
  public static <K,V> Map<K,List<Value<V>>> calculateDiff(Map<K,List<V>> before,
      Map<K,List<V>> after) {
    Map<K,List<Value<V>>> result = new HashMap<K,List<Value<V>>>();
    // iterate over m1 and compute the diff first
    for (K key: before.keySet()) {
      List<V> l1 = before.get(key);
      List<V> l2 = after.get(key);
      List<Value<V>> diff = calculateDiff(l1, l2);
      if (!diff.isEmpty()) {
        result.put(key, diff);
      }
    }
    // see if there is any key that is mapped in m2 but not in m1
    Set<K> m2Keys = new HashSet<K>(after.keySet());
    m2Keys.removeAll(before.keySet());
    for (K key: m2Keys) {
      // this is purely difference
      List<V> list = after.get(key);
      if (!list.isEmpty()) {
        result.put(key, wrapList(list, Difference.ADDED));
      }
    }
    return result;
  }

  /**
   * Returns the list that contains that have changed between before and after. If either is null,
   * the other list is returned. If both are null, an empty list is returned.
   */
  private static <V> List<Value<V>> calculateDiff(List<V> before, List<V> after) {
    if (before == null && after == null) {
      return Collections.emptyList();
    }
    if (before == null) {
      return wrapList(after, Difference.ADDED);
    }
    if (after == null) {
      return wrapList(before, Difference.REMOVED);
    }
    // neither list is null
    List<Value<V>> result = new ArrayList<Value<V>>();
    for (V v: before) {
      if (!after.contains(v)) {
        result.add(new Value<V>(v, Difference.REMOVED));
      }
    }
    for (V v: after) {
      if (!before.contains(v)) {
        result.add(new Value<V>(v, Difference.ADDED));
      }
    }
    return result;
  }

  private static <V> List<Value<V>> wrapList(List<V> list, Difference diff) {
    List<Value<V>> result = new ArrayList<Value<V>>();
    for (V v: list) {
      result.add(new Value<V>(v, diff));
    }
    return result;
  }

  public static class Value<V> {
    private final V value;
    private final Difference diff;

    public Value(V value, Difference diff) {
      this.value = value;
      this.diff = diff;
    }

    public V get() {
      return value;
    }

    public Difference getDifferenceType() {
      return diff;
    }
  }

  public enum Difference { ADDED, REMOVED }
}
