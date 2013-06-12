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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;


public class Crunch {
  private final MessageDigest md;

  public Crunch() {
    try {
      md = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException ignore) {
      throw new IllegalArgumentException(ignore);
    }
  }

  /**
   * Creates a "crunched" tree from the topological tree input. It is assumed that the topological
   * tree begins with a root node with the right root type.
   * <br/>
   * As a result of this operation, a copy with the following properties is created:
   * <ul>
   *   <li>name, type, and selection properties are copied from the topological nodes</li>
   *   <li>id's are assigned as a SHA-1 hash of the node name</li>
   *   <li>both children and parent properties are set</li>
   *   <li>weights are assigned as sums of child weights</li>
   *   <li>the selector objects are created</li>
   * </ul>
   * No modifications are done on the original topological nodes.
   */
  public Node makeCrunch(Node topology) {
    if (topology.getType() != Types.ROOT) {
      throw new IllegalArgumentException("the root node is not the ROOT type!");
    }

    return makeCrunchNode(topology);
  }

  private Node makeCrunchNode(Node topologicalNode) {
    // copy the intrinsic properties: id, weights, relationship, and selectors will be set
    Node node = new Node(topologicalNode);
    // assign the id from the name hash
    node.setId(computeId(node));
    if (!topologicalNode.isLeaf()) {
      List<Node> newChildren = new ArrayList<Node>();
      List<Node> children = topologicalNode.getChildren();
      for (Node child: children) {
        // depth-first traversal
        Node newChild = makeCrunchNode(child);
        // set the child-parent relationship
        newChildren.add(newChild);
        newChild.setParent(node);
      }
      node.setChildren(newChildren);

      // weights and selector should be set after all lower nodes are crunched
      computeWeightAndSelector(node);
    }
    return node;
  }

  private long computeId(Node node) {
    byte[] h = md.digest(node.getName().getBytes());
    // TODO see if this is adequate as a unique id: I suspect it is...
    return Utils.bstrTo32bit(h);
  }

  /**
   * Performs modifications, and reassigns certain properties on the tree in place. The input is
   * assumed to be a properly "crunched" tree. This is mainly to aid creating the "mini-tree" for
   * the data selection in the RDF mapping.
   * <br/>
   * The only properties that are recalculated are the weights and the selectors.
   */
  public void recrunch(Node node) {
    if (!node.isLeaf()) {
      for (Node child: node.getChildren()) {
        recrunch(child);
      }

      computeWeightAndSelector(node);
    }
  }

  private void computeWeightAndSelector(Node node) {
    // set the weight after all its children are already "crunched"
    int weight = 0;
    for (Node child: node.getChildren()) {
      weight += child.getWeight();
    }
    node.setWeight(weight);
    // set the selector
    node.setSelector(pickSelector(node));
  }

  private Selector pickSelector(Node node) {
    switch (node.getSelection()) {
    case CONSISTENT_HASHING:
      return new ConsistentHashingSelector(node);
    case STRAW:
      return new StrawSelector(node);
    default:
      throw new IllegalArgumentException("unrecognized type!");
    }
  }
}
