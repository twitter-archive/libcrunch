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

import java.util.List;

import com.google.common.base.Predicate;

/**
 * Encapsulation of the algorithm that selects a number of child nodes in the topology to place the
 * data based on the data input as well as the node properties such as the selection algorithm,
 * the weight, and the type. It is orthogonal to the placement rules, and is used as a building
 * block operations to create placement rules.
 */
public interface PlacementAlgorithm {
  /**
   * Returns a list of nodes of the desired type. If the count is more than the number of available
   * nodes, an exception is thrown.
   *
   * @return a list of nodes
   */
  List<Node> select(Node parent, long input, int count, int type);

  /**
   * Returns a list of nodes that have the matching type and pass the predicate. If the count is
   * more than the number of available nodes, an exception is thrown.
   *
   * @return a list of nodes
   */
  List<Node> select(Node parent, long input, int count, int type, Predicate<Node> pred);
}
