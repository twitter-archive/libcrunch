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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class CrunchTest {
  @Test
  public void testMakeCrunch() {
    Node root = TestUtils.createSimpleTree();
    Node crunched = new Crunch().makeCrunch(root);
    verifyNode(crunched);
    assertEquals(800, crunched.getWeight());
  }

  private void verifyNode(Node node) {
    System.out.println(node + " => " + node.getWeight());
    assertTrue(node.getWeight() > 0);
    if (!node.isLeaf()) {
      assertNotNull(node.getSelector());
      for (Node child: node.getChildren()) {
        verifyNode(child);
      }
    }
  }

  /**
   * Creates a situation where some parent nodes have all of their children picked. The mapping
   * should still converge fast in this case.
   */
  @Test
  public void testSmallTopology() {
    Node root = TestUtils.createSimpleTree();

    PlacementRules rules = new PlacementRules() {
      public List<Node> select(Node topCNode, long data, int count, PlacementAlgorithm pa) {
        return pa.select(topCNode, data, count, getEndNodeType());
      }

      public int getEndNodeType() { return StorageSystemTypes.DISK; }

      public boolean acceptReplica(Node primary, Node replica) {
        return true;
      }
    };

    new SimpleCRUSHMapping(3, rules).computeMapping(Arrays.<Long>asList(3L), root);
    assertTrue(true);
  }

  @Test
  public void testNonRootCrunch() {
    Node root = TestUtils.createSimpleTree();
    // override the type to cause the exception
    root.setType(StorageSystemTypes.STORAGE_NODE);
    try {
      new Crunch().makeCrunch(root);
      fail("we shouldn't reach this line");
    } catch (IllegalArgumentException e) {
      assertTrue(true);
    }
  }

  @Test
  public void testRecrunch() {
    Node root = TestUtils.createSimpleTree();
    Node crunched = new Crunch().makeCrunch(root);
    // now remove a node from the tree and recrunch
    Node rack = crunched.getChildren().get(1).getChildren().get(1);
    assertEquals(StorageSystemTypes.RACK, rack.getType());
    Node hd = rack.getChildren().get(1);
    assertEquals(StorageSystemTypes.DISK, hd.getType());
    List<Node> children = rack.getChildren();
    children.remove(hd);
    // recrunch
    new Crunch().recrunch(crunched);
    verifyNode(crunched);
    assertEquals(700, crunched.getWeight());
  }
}
