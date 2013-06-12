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
import java.util.List;

public class Node implements Comparable<Node> {
  public enum Selection { STRAW, CONSISTENT_HASHING }

  private String name;
  private int type;
  private long id;
  private long weight;
  private Selection selection;

  private List<Node> children;
  private Node parent;

  private Selector selector;

  private boolean failed;

  public Node() {}

  public Node(Node node) {
    this.name = node.name;
    this.type = node.type;
    this.id = node.id;
    this.weight = node.weight;
    this.selection = node.selection;
    this.failed = node.failed;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getWeight() {
    return weight;
  }

  public void setWeight(long weight) {
    this.weight = weight;
  }

  public Selection getSelection() {
    return selection;
  }

  public void setSelection(Selection selection) {
    this.selection = selection;
  }


  public boolean isFailed() {
    return failed;
  }

  public void setFailed(boolean failed) {
    if (!isLeaf()) {
      throw new UnsupportedOperationException("you cannot set failed on a non-leaf!");
    }
    this.failed = failed;
  }

  public List<Node> getChildren() {
    return children;
  }

  public void setChildren(List<Node> children) {
    this.children = children;
  }

  public boolean isLeaf() {
    return children == null || children.isEmpty();
  }

  public Node getParent() {
    return parent;
  }

  public void setParent(Node parent) {
    this.parent = parent;
  }

  public Selector getSelector() {
    return selector;
  }

  public void setSelector(Selector selector) {
    this.selector = selector;
  }

  /**
   * Uses the selection algorithm that is assigned to the node and return the selected node.
   */
  public Node select(long input, long round) {
    return selector.select(input, round);
  }

  /**
   * Returns all leaf nodes that belong in the tree. Returns itself if this node is a leaf. As with
   * most other methods in this class, the nodes are added via depth-first traversal.
   */
  public List<Node> getAllLeafNodes() {
    // TODO optimize for performance (cache)
    List<Node> nodes = new ArrayList<Node>();
    if (isLeaf()) {
      nodes.add(this);
    } else {
      for (Node child: children) {
        nodes.addAll(child.getAllLeafNodes());
      }
    }
    return nodes;
  }

  /**
   * Returns all child nodes that match the type. Returns itself if this node matches it. If no
   * child matches the type, an empty list is returned.
   */
  public List<Node> findChildren(int type) {
    List<Node> nodes = new ArrayList<Node>();
    if (this.type == type) {
      nodes.add(this);
    } else if (!isLeaf()) {
      for (Node child: children) {
        nodes.addAll(child.findChildren(type));
      }
    }
    return nodes;
  }

  /**
   * Returns the number of all child nodes that match the type. Returns 1 if this node matches it.
   * Returns 0 if no child matches the type.
   */
  public int getChildrenCount(int type) {
    int count = 0;
    if (this.type == type) {
      count++;
    } else if (!isLeaf()) {
      for (Node child: children) {
        count += child.getChildrenCount(type);
      }
    }
    return count;
  }

  /**
   * Finds a parent that matches the given type. If the node itself matches it, it is returned. If
   * there is no matching parent in the hierarchy, null is returned.
   */
  public Node findParent(int type) {
    Node node = this;
    while (node != null) {
      if (node.type == type) {
        return node;
      }
      node = node.parent; // keep walking up the tree
    }
    return null; // no match was found
  }

  /**
   * Returns the top-most ("root") node from this node. If this node itself does not have a parent,
   * returns itself.
   */
  public Node getRoot() {
    Node node = this;
    while (node.parent != null) {
      node = node.parent;
    }
    return node;
  }

  @Override
  public String toString() {
    return name + ":" + id;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Node)) {
      return false;
    }
    Node that = (Node)obj;
    return name.equals(that.name);
  }

  public int compareTo(Node o) {
    return name.compareTo(o.name);
  }
}
