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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.twitter.crunch.MappingDiff.Value;
import com.twitter.crunch.Node.Selection;

public class BaseSelectionTest {
  protected void doTestBasic(Class<? extends Selector> type) {
    Node rack = createTree();
    Selector selector = createSelector(type, rack);
    long input = getHashFromString("some random key");
    Node selected = selector.select(input, 1);
    System.out.println(selected.getName());
    selected = selector.select(input, 2);
    System.out.println(selected.getName());
  }

  protected void doTestBalance(Class<? extends Selector> type, final int tries) {
    Node rack = createTree();
    Selector selector = createSelector(type, rack);
    Map<String,Integer> nodeCounts = new HashMap<String,Integer>();
    for (int i = 0; i < tries; i++) {
      long input = getHashFromString("key" + i);
      Node selected = selector.select(input, 1);
      String node = selected.getName();
      Integer old = nodeCounts.get(node);
      int value = (old == null) ? 1 : old.intValue()+1;
      nodeCounts.put(node, value);
    }

    for (Map.Entry<String,Integer> e: nodeCounts.entrySet()) {
      System.out.println(e.getKey() + ": " + e.getValue());
    }

    printDeviation(nodeCounts, tries);
  }

  private Node createTree() {
    Node rack = createNode("rack", StorageSystemTypes.RACK, 0, 0, null);
    List<Node> children = new ArrayList<Node>();
    children.add(createNode("node1", StorageSystemTypes.DISK, 1, 100, null));
    children.add(createNode("node2", StorageSystemTypes.DISK, 2, 50, null));
    children.add(createNode("node3", StorageSystemTypes.DISK, 3, 100, null));
    rack.setChildren(children);
    return rack;
  }

  private Node createNode(String name, int type, long id, long weight, Selection selection) {
    Node node = new Node();
    node.setName(name);
    node.setType(type);
    node.setId(id);
    node.setWeight(weight);
    node.setSelection(selection);
    return node;
  }

  private Node createLargeTree() {
    Node rack = createNode("rack", StorageSystemTypes.RACK, 0, 0, null);
    List<Node> children = new ArrayList<Node>();
    final int size = 1024;
    for (int i = 1; i <= size; i++) {
      children.add(createNode("node" + i, StorageSystemTypes.DISK, i, 100, null));
    }
    rack.setChildren(children);
    return rack;
  }

  private long getHashFromString(String string) {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException ignore) {}
    byte[] hash = md.digest(string.getBytes());
    return Utils.bstrTo32bit(hash);
  }

  private Map<String,Integer> getExpectedBalance(int tries) {
    Map<String,Integer> map = new HashMap<String,Integer>();
    map.put("node1", (int)(tries*0.4));
    map.put("node2", (int)(tries*0.2));
    map.put("node3", (int)(tries*0.4));
    return map;
  }

  private void printDeviation(Map<String,Integer> actual, int tries) {
    // compute the deviation
    Map<String,Integer> expected = getExpectedBalance(tries);
    double varianceSum = 0.0;
    int count = actual.size();
    for (Map.Entry<String,Integer> e: actual.entrySet()) {
      int expectedCount = expected.get(e.getKey());
      int actualCount = e.getValue();
      double diff = expectedCount - actualCount;
      varianceSum += diff*diff/expectedCount/expectedCount;
    }
    double deviation = Math.sqrt(varianceSum/count);
    System.out.println("relative deviation (%): " + deviation*100);
  }

  protected void doTestLargeTree(Class<? extends Selector> type) {
    Node rack = createLargeTree();
    Selector selector = createSelector(type, rack);
    Map<String,Integer> nodeCounts = new HashMap<String,Integer>();
    final int size = rack.getChildren().size();
    final int tries = 1024*128;
    for (int i = 1; i <= tries; i++) {
      Node selected = selector.select((long)i, 1);
      String node = selected.getName();
      Integer old = nodeCounts.get(node);
      int value = (old == null) ? 1 : old.intValue()+1;
      nodeCounts.put(node, value);
    }

    for (Map.Entry<String,Integer> e: nodeCounts.entrySet()) {
      System.out.println(e.getKey() + ": " + e.getValue());
    }

    // compute the deviation
    final int expectedCount = tries/size;
    double varianceSum = 0.0;
    int count = nodeCounts.size();
    for (Integer actualCount: nodeCounts.values()) {
      double diff = expectedCount - actualCount;
      varianceSum += diff*diff/expectedCount/expectedCount;
    }
    double deviation = Math.sqrt(varianceSum/count);
    System.out.println("relative deviation (%): " + deviation*100);
  }

  private void analyzeDiff(Map<Integer, List<String>> mapping1,
      Map<Integer, List<String>> mapping2) {
    // compute the diff
    Map<Integer,List<Value<String>>> diff = MappingDiff.calculateDiff(mapping1, mapping2);
    System.out.println("number of data objects that moved: " + diff.size());
    System.out.println("relative movement (%): " + ((float)diff.size())*100/mapping1.size());
  }

  protected void doTestStability(Class<? extends Selector> type, boolean removal) {
    // first try: create the full tree
    Node rack = createLargeTree();
    Selector selector = createSelector(type, rack);
    Map<Integer,List<String>> mapping1 = new HashMap<Integer,List<String>>();
    final int tries = 1024*128;
    for (int i = 1; i <= tries; i++) {
      Node selected = selector.select((long)i, 1);
      String node = selected.getName();
      mapping1.put(i, Collections.singletonList(node));
    }

    // second try: remove a node
    List<Node> children = rack.getChildren();
    if (removal) {
      children.remove(0); // remove the first node
    } else { // addition
      Node extra = createNode("node1000000", StorageSystemTypes.DISK, 1000000, 100, null);
      children.add(extra); // remove the first node
    }

    Selector selector2 = createSelector(type, rack);
    Map<Integer,List<String>> mapping2 = new HashMap<Integer,List<String>>();
    for (int i = 1; i <= tries; i++) {
      Node selected = selector2.select((long)i, 1);
      String node = selected.getName();
      mapping2.put(i, Collections.singletonList(node));
    }

    analyzeDiff(mapping1, mapping2);
  }

  /**
   * We require a constructor with a single argument that takes the Node.
   */
  private Selector createSelector(Class<? extends Selector> type, Node rack) {
    try {
      Constructor<? extends Selector> ctr = type.getConstructor(Node.class);
      return ctr.newInstance(rack);
    } catch (NoSuchMethodException e) { // should not occur
      throw new IllegalArgumentException(e);
    } catch (InstantiationException e) { // should not occur
      throw new IllegalArgumentException(e);
    } catch (InvocationTargetException e) { // should not occur
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) { // should not occur
        throw new IllegalArgumentException(e);
    }
  }
}
