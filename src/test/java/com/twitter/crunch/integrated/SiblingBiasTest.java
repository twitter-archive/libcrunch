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

package com.twitter.crunch.integrated;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.twitter.crunch.MappingDiff;
import com.twitter.crunch.MappingDiff.Difference;
import com.twitter.crunch.MappingDiff.Value;
import com.twitter.crunch.MappingFunction;
import com.twitter.crunch.Node;
import com.twitter.crunch.Node.Selection;
import com.twitter.crunch.RackIsolationPlacementRules;
import com.twitter.crunch.SimpleCRUSHMapping;
import com.twitter.crunch.StorageSystemTypes;
import com.twitter.crunch.Types;

public class SiblingBiasTest {

  @Test
  public void testMultiLevel() {
    Node topo = createSmallTree();
    doTest(topo, 400);
  }

  @Test
  public void testOneLevel() {
    Node topo = createFlatTree();
    doTest(topo, 400);
  }

  private void doTest(Node topo, int dataSize) {
    List<Long> data = createData(dataSize);

    MappingFunction mappingFunction = new SimpleCRUSHMapping(1, new RackIsolationPlacementRules()); // RF = 1
    Map<Long,List<Node>> before = mappingFunction.computeMapping(data, topo);
    analyzeDistribution(before);

    Node removed = removeOneNode(topo);
    Map<Long,List<Node>> after = mappingFunction.computeMapping(data, topo);
    analyzeDistribution(after);
    // let's figure out where the data that belong in old node went
    analyzeMovement(before, after, removed);
  }

  private <K,V> void printMap(Map<K,V> map) {
    for (Map.Entry<K,V> e: map.entrySet()) {
      System.out.println(e.getKey() + " => " + e.getValue());
    }
  }

  private void analyzeDistribution(Map<Long,List<Node>> map) {
    Map<Node,Integer> dist = new HashMap<Node,Integer>();
    for (Map.Entry<Long,List<Node>> e: map.entrySet()) {
      for (Node node: e.getValue()) {
        Integer count = dist.get(node);
        dist.put(node,
            count == null ? 1 : ++count);
      }
    }
    printMap(dist);
  }

  private Node createSmallTree() {
    final int rackCount = 2;
    final int hdCount = 2;

    int id = 0;
    // root
    Node root = new Node();
    root.setName("root");
    root.setId(id++);
    root.setType(Types.ROOT);
    root.setSelection(Selection.STRAW);
    // 2 racks
    List<Node> racks = new ArrayList<Node>();
    for (int j = 1; j <= rackCount; j++) {
      Node rack = new Node();
      racks.add(rack);
      rack.setName("rack" + j);
      rack.setId(id++);
      rack.setType(StorageSystemTypes.RACK);
      rack.setSelection(Selection.STRAW);
      rack.setParent(root);
      // 2 hds
      List<Node> hds = new ArrayList<Node>();
      for (int k = 1; k <= hdCount; k++) {
        Node hd = new Node();
        hds.add(hd);
        hd.setName(rack.getName() + "hd" + k);
        hd.setId(id++);
        hd.setType(StorageSystemTypes.DISK);
        hd.setWeight(100);
        hd.setParent(rack);
      }
      rack.setChildren(hds);
    }
    root.setChildren(racks);
    return root;
  }

  private Node createFlatTree() {
    int id = 0;
    // root
    Node root = new Node();
    root.setName("root");
    root.setId(id++);
    root.setType(Types.ROOT);
    root.setSelection(Selection.STRAW);
    // 4 hds
    List<Node> hds = new ArrayList<Node>();
    Node hd = createNode("rack1hd1", StorageSystemTypes.DISK, id++, 100, null);
    hd.setParent(root);
    hds.add(hd);
    hd = createNode("rack1hd2", StorageSystemTypes.DISK, id++, 100, null);
    hd.setParent(root);
    hds.add(hd);
    hd = createNode("rack2hd1", StorageSystemTypes.DISK, id++, 100, null);
    hd.setParent(root);
    hds.add(hd);
    hd = createNode("rack2hd2", StorageSystemTypes.DISK, id++, 100, null);
    hd.setParent(root);
    hds.add(hd);
    root.setChildren(hds);
    return root;
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

  private List<Long> createData(final int size) {
    List<Long> data = new ArrayList<Long>();
    for (int i = 1; i <= size; i++) {
      data.add((long)i);
    }
    return data;
  }

  private Node removeOneNode(Node topo) {
    Node node = topo;
    while (!node.isLeaf()) {
      List<Node> children = node.getChildren();
      node = children.get(children.size()-1);
      if (node.isLeaf()) {
        children.remove(node);
        System.out.println("marked " + node + " as failed");
      }
    }
    return node;
  }

  private void analyzeMovement(Map<Long,List<Node>> before, Map<Long,List<Node>> after, Node removed) {
    Map<Long,List<Value<Node>>> diff = MappingDiff.calculateDiff(before, after);
    Map<Node,Integer> distributed = new HashMap<Node,Integer>();
    for (Map.Entry<Long,List<Value<Node>>> e: diff.entrySet()) {
      Long data = e.getKey();
      List<Value<Node>> moves = e.getValue();
      boolean hit = false;
      for (Value<Node> v: moves) {
        Node node = v.get();
        if (node.equals(removed)) {
          hit = true;
          break;
        }
      }
      if (hit) {
        for (Value<Node> v: moves) {
          if (v.getDifferenceType() == Difference.ADDED) {
            Node destination = v.get();
            Integer count = distributed.get(destination);
            distributed.put(destination,
                count == null ? 1 : ++count);
            System.out.println(data + " moved from " + removed.getName() + " to " + destination.getName());
          }
        }
      }
    }
    printMap(distributed);
  }
}
