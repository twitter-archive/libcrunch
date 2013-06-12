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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.twitter.crunch.Node.Selection;

public class NodeTest {
  @Test
  public void testCopyConstructor() {
    Node node = new Node();
    // properties that should be equal
    final String foo = "foo";
    final int id = 1234;
    final int type = StorageSystemTypes.RACK;
    final long weight = 100;
    final Selection selection = Selection.STRAW;

    node.setName(foo);
    node.setId(id);
    node.setType(type);
    node.setWeight(weight);
    node.setSelection(selection);

    // properties that should not be copied
    node.setChildren(new ArrayList<Node>());
    node.setParent(new Node());

    Node copy = new Node(node);
    assertEquals(node.getName(), copy.getName());
    assertEquals(node.getId(), copy.getId());
    assertEquals(node.getType(), copy.getType());
    assertEquals(node.getWeight(), copy.getWeight());
    assertEquals(node.getSelection(), copy.getSelection());
    // ensure relationship is not copied
    assertNull(copy.getChildren());
    assertNull(copy.getParent());
  }

  @Test
  public void testIsLeaf() {
    Node node = new Node();
    List<Node> children = new ArrayList<Node>();
    children.add(new Node());
    children.remove(0);
    node.setChildren(children);

    assertTrue(node.isLeaf());
  }

  @Test
  public void testGetAllLeafNodes() {
    Node root = TestUtils.createSimpleTree();
    List<Node> leaves = root.getAllLeafNodes();
    assertEquals(8, leaves.size());
    // test a leaf node itself
    Node leaf = leaves.get(0);
    List<Node> self = leaf.getAllLeafNodes();
    assertEquals(1, self.size());
    assertSame(leaf, self.get(0));
  }

  @Test
  public void testFindChildren() {
    Node root = TestUtils.createSimpleTree();
    List<Node> racks = root.findChildren(StorageSystemTypes.RACK);
    assertEquals(4, racks.size());
    for (Node r: racks) {
      assertEquals(StorageSystemTypes.RACK, r.getType());
    }
    // test the node itself
    Node rack = racks.get(0);
    List<Node> self = rack.findChildren(StorageSystemTypes.RACK);
    assertEquals(1, self.size());
    assertSame(rack, self.get(0));
    // test the no match
    List<Node> empty = rack.findChildren(Types.DATA_CENTER);
    assertTrue(empty.isEmpty());
  }

  @Test
  public void testChildrenCount() {
    Node root = TestUtils.createSimpleTree();
    int count = root.getChildrenCount(StorageSystemTypes.RACK);
    assertEquals(4, count);
    // test the node itself
    List<Node> racks = root.findChildren(StorageSystemTypes.RACK);
    Node rack = racks.get(0);
    count = rack.getChildrenCount(StorageSystemTypes.RACK);
    assertEquals(1, count);
    // test the no match
    count = rack.getChildrenCount(Types.DATA_CENTER);
    assertEquals(0, count);
  }

  @Test
  public void testFindParent() {
    Node root = TestUtils.createSimpleTree();
    Node rack = root.findChildren(StorageSystemTypes.RACK).get(0);
    Node dc = rack.findParent(Types.DATA_CENTER);
    assertEquals(Types.DATA_CENTER, dc.getType());
    Node self = rack.findParent(StorageSystemTypes.RACK);
    assertSame(rack, self);
    Node none = rack.findParent(StorageSystemTypes.STORAGE_NODE);
    assertNull(none);
  }

  @Test
  public void testGetRoot() {
    Node root = TestUtils.createSimpleTree();
    Node hd = root.findChildren(StorageSystemTypes.DISK).get(0);
    Node ret = hd.getRoot();
    assertSame(root, ret);
  }
}
