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

/**
 * A way to express the placement rules for the crunch/CRUSH mapping. Placement rules are often
 * combined with a more specific topology (i.e. type definitions). In general, it should only
 * express the prescription of how a number of end nodes should be selected, and should not rely on
 * specific data, the top node from which the selection begins, or the placement algorithm.
 */
public interface PlacementRules {
  /**
   * Describes how a number of end nodes should be selected from the top node.
   */
  List<Node> select(Node topNode, long data, int n, PlacementAlgorithm pa);
  /**
   * Returns the types values that the placement rules use.
   */
  int getEndNodeType();
  /**
   * Given a node, returns whether the replica end node is acceptable. It must be consistent with
   * the selection prescribed in the select methods.
   */
  boolean acceptReplica(Node primary, Node replica);
}
