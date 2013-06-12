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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.twitter.crunch.MappingDiff.Difference;
import com.twitter.crunch.MappingDiff.Value;

public class MappingDiffTest {

  @Test
  public void testSimpleDifferences() {
    Map<Integer,List<Integer>> m1 = new HashMap<Integer,List<Integer>>();
    // 1 => [1, 2, 3]
    // 2 => [4, 5, 6]
    // 3 => [7, 8, 9]
    m1.put(1, Arrays.asList(1, 2, 3));
    m1.put(2, Arrays.asList(4, 5, 6));
    m1.put(3, Arrays.asList(7, 8, 9));
    Map<Integer,List<Integer>> m2 = new HashMap<Integer,List<Integer>>();
    // 1 => [1, 2, 4]
    // 2 => [3, 5, 6]
    // 3 => [7, 8, 9]
    m2.put(1, Arrays.asList(1, 2, 4));
    m2.put(2, Arrays.asList(3, 5, 6));
    m2.put(3, Arrays.asList(9, 7, 8));
    // expect
    // 1 => [3, 4]
    // 2 => [3, 4]
    Map<Integer,List<Value<Integer>>> diff = MappingDiff.calculateDiff(m1, m2);
    assertEquals(2, diff.size());
    Set<Integer> keys = diff.keySet();
    Set<Integer> expectedKeys = new HashSet<Integer>();
    Collections.addAll(expectedKeys, 1, 2);
    assertEquals(expectedKeys, keys);
    List<Value<Integer>> d1 = diff.get(1);
    assertNotNull(d1);
    for (Value<Integer> v: d1) {
      Difference type = v.getDifferenceType();
      switch (v.get()) {
      case 3:
        assertSame(Difference.REMOVED, type);
        break;
      case 4:
        assertSame(Difference.ADDED, type);
        break;
      default:
        fail("we shouldn't be here!");
      }
    }
    List<Value<Integer>> d2 = diff.get(2);
    assertNotNull(d2);
    for (Value<Integer> v: d2) {
      Difference type = v.getDifferenceType();
      switch (v.get()) {
      case 3:
        assertSame(Difference.ADDED, type);
        break;
      case 4:
        assertSame(Difference.REMOVED, type);
        break;
      default:
        fail("we shouldn't be here!");
      }
    }
  }

  @Test
  public void testMoreDiff() {
    Map<Integer,List<Integer>> m1 = new HashMap<Integer,List<Integer>>();
    // 1 => [1, 2, 3]
    // 2 => [4, 5, 6]
    m1.put(1, Arrays.asList(1, 2, 3));
    m1.put(2, Arrays.asList(4, 5, 6));
    Map<Integer,List<Integer>> m2 = new HashMap<Integer,List<Integer>>();
    // 1 => [1, 2, 4]
    // 2 => [3, 5, 6]
    // 3 => [7, 8, 9]
    m2.put(1, Arrays.asList(1, 2, 3));
    m2.put(2, Arrays.asList(4, 5, 6));
    m2.put(3, Arrays.asList(9, 7, 8));
    // expect
    // 3 => [7, 8, 9]
    Map<Integer,List<Value<Integer>>> diff = MappingDiff.calculateDiff(m1, m2);
    assertEquals(1, diff.size());
    Set<Integer> keys = diff.keySet();
    Set<Integer> expectedKeys = new HashSet<Integer>();
    expectedKeys.add(3);
    assertEquals(expectedKeys, keys);
    List<Value<Integer>> values = diff.get(3);
    assertNotNull(values);
    for (Value<Integer> v: values) {
      Difference type = v.getDifferenceType();
      switch (v.get()) {
      case 7:
      case 8:
      case 9:
        assertSame(Difference.ADDED, type);
        break;
      default:
        fail("we shouldn't be here!");
      }
    }
  }
}
