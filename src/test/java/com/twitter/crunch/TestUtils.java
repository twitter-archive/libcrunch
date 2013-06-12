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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.twitter.crunch.MappingDiff.Difference;
import com.twitter.crunch.MappingDiff.Value;
import com.twitter.crunch.Node.Selection;

public class TestUtils {
  public static Node createSimpleTree() {
    final int dcCount = 2;
    final int rackCount = 2;
    final int hdCount = 2;

    int id = 0;
    // root
    Node root = new Node();
    root.setName("root");
    root.setId(id++);
    root.setType(Types.ROOT);
    root.setSelection(Selection.STRAW);
    // 2 DCs
    List<Node> dcs = new ArrayList<Node>();
    for (int i = 1; i <= dcCount; i++) {
      Node dc = new Node();
      dcs.add(dc);
      dc.setName("dc" + i);
      dc.setId(id++);
      dc.setType(Types.DATA_CENTER);
      dc.setSelection(Selection.STRAW);
      dc.setParent(root);
      // 2 racks
      List<Node> racks = new ArrayList<Node>();
      for (int j = 1; j <= rackCount; j++) {
        Node rack = new Node();
        racks.add(rack);
        rack.setName(dc.getName() + "rack" + j);
        rack.setId(id++);
        rack.setType(StorageSystemTypes.RACK);
        rack.setSelection(Selection.STRAW);
        rack.setParent(dc);
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
      dc.setChildren(racks);
    }
    root.setChildren(dcs);
    return root;
  }

  /**
   * 2 datacenters x 6 racks x 6 storage nodes x 12 hard disks = 864 hard disks
   */
  public static Node createLargeTree() {
    final int dcCount = 2;
    final int rackCount = 6;
    final int snCount = 6;
    final int hdCount = 12;

    int id = 0;
    // root
    Node root = new Node();
    root.setName("root");
    root.setId(id++);
    root.setType(Types.ROOT);
    root.setSelection(Selection.STRAW);
    // DC
    List<Node> dcs = new ArrayList<Node>();
    for (int i = 1; i <= dcCount; i++) {
      Node dc = new Node();
      dcs.add(dc);
      dc.setName("dc" + i);
      dc.setId(id++);
      dc.setType(Types.DATA_CENTER);
      dc.setSelection(Selection.STRAW);
      dc.setParent(root);
      // racks
      List<Node> racks = new ArrayList<Node>();
      for (int j = 1; j <= rackCount; j++) {
        Node rack = new Node();
        racks.add(rack);
        rack.setName(dc.getName() + "rack" + j);
        rack.setId(id++);
        rack.setType(StorageSystemTypes.RACK);
        rack.setSelection(Selection.STRAW);
        rack.setParent(dc);
        // storage nodes
        List<Node> sns = new ArrayList<Node>();
        for (int k = 1; k <= snCount; k++) {
          Node sn = new Node();
          sns.add(sn);
          sn.setName(rack.getName() + "sn" + k);
          sn.setId(id++);
          sn.setType(StorageSystemTypes.STORAGE_NODE);
          sn.setSelection(Selection.STRAW);
          sn.setParent(rack);
          // hds
          List<Node> hds = new ArrayList<Node>();
          for (int l = 1; l <= hdCount; l++) {
            Node hd = new Node();
            hds.add(hd);
            hd.setName(sn.getName() + "hd" + l);
            hd.setId(id++);
            hd.setType(StorageSystemTypes.DISK);
            hd.setWeight(100);
            hd.setParent(sn);
          }
          sn.setChildren(hds);
        }
        rack.setChildren(sns);
      }
      dc.setChildren(racks);
    }
    root.setChildren(dcs);
    return root;
  }

  public static List<Long> createData() {
    final int tries = 128*1024;
    List<Long> data = new ArrayList<Long>();
    for (int i = 1; i <= tries; i++) {
      data.add((long)i);
    }
    return data;
  }


  public static void analyzeMapping(final int rf, final int dcCount, final int dataSize, final int nodeCount,
      Map<Long,List<Node>> mapping) {
    Map<Node,Integer> assignments = new HashMap<Node,Integer>();
    for (List<Node> nodes: mapping.values()) {
      // ensure there are no duplicates
      Set<Node> set = new HashSet<Node>(nodes);
      assertEquals(rf*dcCount, set.size());
      for (Node node: nodes) {
        addNodeCount(node, assignments);
      }
    }
    assertEquals(nodeCount, assignments.size());

    // print the values
    int expected = dataSize*rf*dcCount/nodeCount;
    int biggest = 0;
    double varianceSum = 0;
    List<Node> keys = new ArrayList<Node>(assignments.keySet());
    Collections.sort(keys);
    for (Node node: keys) {
      int actual = assignments.get(node);
      if (actual > biggest) {
        biggest = actual;
      }
      double diff = actual - expected;
      varianceSum += diff*diff;
    }
    double deviation = Math.sqrt(varianceSum/keys.size());
    System.out.println("mean assignment = " + expected);
    System.out.println("biggest assignment = " + biggest);
    System.out.println("deviation (%) = " + deviation/expected*100);
  }

  private static void addNodeCount(Node node, Map<Node,Integer> nodeCount) {
    Integer old = nodeCount.get(node);
    int value = (old == null) ? 1 : old + 1;
    nodeCount.put(node, value);
  }

  public static Map<Node,List<Node>> createDummyRdfMap(Node crunch) {
    Map<Node,List<Node>> rdfMap = new HashMap<Node,List<Node>>();
    List<Node> endNodes = crunch.getAllLeafNodes();
    for (Node n: endNodes) {
      List<Node> rdf = new ArrayList<Node>(endNodes);
      rdf.remove(n);
      rdfMap.put(n, rdf);
    }
    return rdfMap;
  }

  public static void analyzeRDFMapping(Map<Node,List<Node>> map) {
    Map<Node,Integer> nodeCount = new HashMap<Node,Integer>();
    for (Map.Entry<Node,List<Node>> e: map.entrySet()) {
      Node primary = e.getKey();
      TestUtils.addNodeCount(primary, nodeCount);
      for (Node node: e.getValue()) {
        TestUtils.addNodeCount(node, nodeCount);
      }
    }
  }

  public static Node removeOneNode(Node topo) {
    Node node = topo;
    while (!node.isLeaf()) {
      List<Node> children = node.getChildren();
      node = children.get(children.size()-1);
      if (node.isLeaf()) {
        node.setFailed(true);
        System.out.println("marked " + node + " as failed");
      }
    }
    return node;
  }

  // node -> vb movement on the node
  public static Map<Node, List<Value<Long>>> calculateReverseDiff(Map<Long,List<Node>> before,
      Map<Long,List<Value<Node>>> diff, Node removed) {
    Map<Node,List<Value<Long>>> map = new HashMap<Node,List<Value<Long>>>();
    for (Map.Entry<Long,List<Value<Node>>> e: diff.entrySet()) {
      Long l = e.getKey();
      for (Value<Node> v: e.getValue()) {
        Difference type = v.getDifferenceType();
        switch (type) {
        case REMOVED:
          Node n = v.get();
          // if it matches the removed node, we need to pick a different one to copy from
          // right now I'll just pick "randomly" (although this is not random for RF = 2)
          if (n.equals(removed)) {
            List<Node> candidates = before.get(l);
            for (Node c: candidates) {
              if (!c.equals(removed)) {
                n = c;
                break;
              }
            }
          }
          addNodeToDataDiff(n, l, Difference.REMOVED, map);
          break;
        case ADDED:
          addNodeToDataDiff(v.get(), l, Difference.ADDED, map);
          break;
        default:
          break;
        }
      }
    }
    return map;
  }

  private static void addNodeToDataDiff(Node node, long data, Difference type,
      Map<Node,List<Value<Long>>> map) {
    Value<Long> v = new Value<Long>(data, type);
    List<Value<Long>> list = map.get(node);
    if (list == null) {
      list = new ArrayList<Value<Long>>();
      map.put(node, list);
    }
    list.add(v);
  }
}
