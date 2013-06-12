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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.twitter.crunch.MappingDiff;
import com.twitter.crunch.MappingDiff.Value;
import com.twitter.crunch.MappingFunction;
import com.twitter.crunch.Node;
import com.twitter.crunch.RDFMapping;
import com.twitter.crunch.RackIsolationPlacementRules;
import com.twitter.crunch.SimpleCRUSHMapping;
import com.twitter.crunch.TestUtils;
import com.twitter.crunch.Types;

public class RDFStabilityTest {
  @Test
  public void testStability() {
    doTestStability(8);
    doTestStability(32);
    doTestStability(128);
  }

  private void doTestStability(final int rdf) {
    Node topo = TestUtils.createLargeTree();
    final int nodeSize = topo.getAllLeafNodes().size();
    final int dcCount = topo.getChildrenCount(Types.DATA_CENTER);
    final int rf = 2;
    System.out.println("RDF = " + rdf);
    MappingFunction mappingFunction = new RDFMapping(rdf, rf, new RackIsolationPlacementRules(), 0.3d);
    List<Long> data = TestUtils.createData();

    Map<Long,List<Node>> before = mappingFunction.computeMapping(data, topo);
    verifyMapping(before, dcCount, rf, data.size());

    // make changes to the topology and compute the mapping again
    // reduce weight on one node to see if it gets less data
    Node removed = TestUtils.removeOneNode(topo);
    Map<Long,List<Node>> after = mappingFunction.computeMapping(data, topo);
    verifyMapping(after, dcCount, rf, data.size());

    // calculate the diff: vb -> its node movement
    Map<Long,List<Value<Node>>> diff = MappingDiff.calculateDiff(before, after);
    analyzeDiff(before, diff, rf, dcCount, nodeSize, removed);

    // calculate per-data replica-replacement counts: high values might indicate that data
    // completely swapped RDF groups
    int[] movementHistogram = new int[rf + 1];
    // for data not affected by the diff, increment 0
    for (Long input: before.keySet()) {
      if (!diff.containsKey(input))
        movementHistogram[0]++;
    }
    // for data affected by the diff, increment the appropriate movement count
    for (List<Value<Node>> value: diff.values()) {
      movementHistogram[value.size() / 2]++;
    }
    System.out.println("per-data movement histogram: " + Arrays.toString(movementHistogram));
  }

  private void verifyMapping(Map<Long,List<Node>> mapping, int dcCount, int rf, int dataSize) {
    assertEquals(dataSize, mapping.size());
    for (List<Node> nodes: mapping.values()) {
      // ensure there are no duplicates
      Set<Node> set = new HashSet<Node>(nodes);
      assertEquals(rf*dcCount, set.size());
    }
  }

  private void analyzeDiff(Map<Long,List<Node>> before, Map<Long,List<Value<Node>>> diff,
      final int rf, final int dcCount, final int nodeSize, Node removed) {
    final int beforeSize = before.size();
    // the same data may have moved in mulitple nodes (replicas)
    // need to count the actual movement
    int moves = 0;
    for (List<Value<Node>> l: diff.values()) {
      moves += l.size();
    }
    // it's a pretty good assumption that the moves are always pairs
    moves /= 2;
    System.out.println("number of data objects that moved: " + moves);
    float relativeMovement = ((float)moves)/(beforeSize*rf*dcCount);
    float multiplier = relativeMovement*nodeSize;
    System.out.println("relative movement (%): " + relativeMovement*100);
    System.out.println("movement multiplier: " + multiplier);
    System.out.println("data objects moved per node (mean): " + ((float)moves)/nodeSize);

    // reverse the map and process it again
    reverseDiff(before, diff, removed);
  }

  private void reverseDiff(Map<Long,List<Node>> before, Map<Long,List<Value<Node>>> diff, Node removed) {
    Map<Node,List<Value<Long>>> map = TestUtils.calculateReverseDiff(before, diff, removed);

    // identify the worst case number
    int maxMoves = 0;
    for (Map.Entry<Node,List<Value<Long>>> e: map.entrySet()) {
      List<Value<Long>> list = e.getValue();
      int send = 0;
      int receive = 0;
      for (Value<Long> v: list) {
        switch (v.getDifferenceType()) {
        case REMOVED:
          send++;
          break;
        case ADDED:
          receive++;
          break;
        }
      }
      if (send >= 50 || receive >= 50) {
        System.err.println("node " + e.getKey() + " has movement of 50 or greater!");
      }
      maxMoves = Math.max(Math.max(send, receive), maxMoves);
    }
    System.out.println("data objects moved per node (max): " + maxMoves);
  }

  @Test
  public void testStabilityPlainCrush() {
    System.out.println("testing stability using plain CRUSH");
    Node topo = TestUtils.createLargeTree();
    final int nodeSize = topo.getAllLeafNodes().size();
    final int dcCount = topo.getChildrenCount(Types.DATA_CENTER);
    final int rf = 2;
    MappingFunction mappingFunction = new SimpleCRUSHMapping(rf, new RackIsolationPlacementRules(), 0.3d);
    List<Long> data = TestUtils.createData();

    Map<Long,List<Node>> before = mappingFunction.computeMapping(data, topo);
    verifyMapping(before, dcCount, rf, data.size());

    // make changes to the topology and compute the mapping again
    // reduce weight on one node to see if it gets less data
    Node removed = TestUtils.removeOneNode(topo);

    Map<Long,List<Node>> after = mappingFunction.computeMapping(data, topo);
    verifyMapping(after, dcCount, rf, data.size());

    // calculate the diff
    Map<Long,List<Value<Node>>> diff = MappingDiff.calculateDiff(before, after);
    analyzeDiff(before, diff, rf, dcCount, nodeSize, removed);
  }
}
