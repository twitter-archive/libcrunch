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

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.twitter.crunch.Crunch;
import com.twitter.crunch.MappingFunction;
import com.twitter.crunch.Node;
import com.twitter.crunch.RDFMapping;
import com.twitter.crunch.RackIsolationPlacementRules;
import com.twitter.crunch.SimpleCRUSHMapping;
import com.twitter.crunch.TestUtils;
import com.twitter.crunch.Types;

public class RDFBalanceTest {
  @Test
  public void testRDFMapping() {
    Node topology = TestUtils.createLargeTree();
    // reduce weight on one node to see if it gets less data
    Node smallNode = pickOneNode(topology);
    smallNode.setWeight(10);
    System.out.println("reduced weight on " + smallNode + " from 100 to 10.");
    Node crunch = new Crunch().makeCrunch(topology);
    final int rf = 2;
    final int rdf = 32;
    RDFMapping mappingFunction = new RDFMapping(rdf, rf, new RackIsolationPlacementRules());
    Map<Node,List<Node>> rdfMapping = mappingFunction.createRDFMapping(crunch);
    List<Node> leafNodes = crunch.getAllLeafNodes();
    assertEquals(leafNodes.size(), rdfMapping.size());
    for (Map.Entry<Node,List<Node>> e: rdfMapping.entrySet()) {
      // ensure there are no duplicates
      List<Node> nodes = e.getValue();
      Set<Node> set = new HashSet<Node>(nodes);
      assertEquals(nodes.size(), set.size());
    }
    TestUtils.analyzeRDFMapping(rdfMapping);
  }

  private Node pickOneNode(Node topology) {
    Node node = topology;
    while (!node.isLeaf()) {
      List<Node> children = node.getChildren();
      node = children.get(children.size()-1);
    }
    return node;
  }

  @Test
  public void testFullMapping() {
    doTestFullMapping(8);
    doTestFullMapping(32);
    doTestFullMapping(128);
  }

  @Test
  public void testFullMappingWithTargetBalance() {
    final double targetBalance = 0.3d;
    doTestFullMapping(8, targetBalance);
    doTestFullMapping(32, targetBalance);
    doTestFullMapping(128, targetBalance);
  }

  private void doTestFullMapping(final int rdf) {
    doTestFullMapping(rdf, 0.0f);
  }

  private void doTestFullMapping(final int rdf, final double targetBalance) {
    Node topology = TestUtils.createLargeTree();
    final int rf = 2;
    System.out.print("RDF = " + rdf);
    if (targetBalance > 0.0d) {
      System.out.println(", target balance = " + targetBalance);
    } else {
      System.out.println("");
    }
    MappingFunction mappingFunction =
        new RDFMapping(rdf, rf, new RackIsolationPlacementRules(), targetBalance);
    List<Long> data = TestUtils.createData();

    long begin = System.nanoTime();
    Map<Long,List<Node>> mapping = mappingFunction.computeMapping(data, topology);
    long end = System.nanoTime();
    System.out.println("mapping time: " + (end - begin)/1000000 + " ms");
    TestUtils.analyzeMapping(rf, topology.getChildrenCount(Types.DATA_CENTER), data.size(),
        topology.getAllLeafNodes().size(), mapping);
  }

  @Test
  public void testPlainCrush() {
    System.out.println("testing distribution using plain CRUSH");
    Node topo = TestUtils.createLargeTree();
    final int rf = 2;
    MappingFunction mappingFunction = new SimpleCRUSHMapping(rf, new RackIsolationPlacementRules());
    List<Long> data = TestUtils.createData();
    long begin = System.nanoTime();
    Map<Long,List<Node>> map = mappingFunction.computeMapping(data, topo);
    long end = System.nanoTime();
    System.out.println("mapping time: " + (end - begin)/1000000 + " ms");
    TestUtils.analyzeMapping(rf, topo.getChildrenCount(Types.DATA_CENTER), data.size(),
        topo.getAllLeafNodes().size(), map);
  }

  @Test
  public void testPlainCrushWithTargetBalance() {
    final double targetBalance = 0.3d;
    System.out.println("testing distribution using plain CRUSH with target balance of " + targetBalance);
    Node topo = TestUtils.createLargeTree();
    final int rf = 2;
    MappingFunction mappingFunction =
        new SimpleCRUSHMapping(rf, new RackIsolationPlacementRules(), targetBalance);
    List<Long> data = TestUtils.createData();
    long begin = System.nanoTime();
    Map<Long,List<Node>> map = mappingFunction.computeMapping(data, topo);
    long end = System.nanoTime();
    System.out.println("mapping time: " + (end - begin)/1000000 + " ms");
    TestUtils.analyzeMapping(rf, topo.getChildrenCount(Types.DATA_CENTER), data.size(),
        topo.getAllLeafNodes().size(), map);
  }
}
