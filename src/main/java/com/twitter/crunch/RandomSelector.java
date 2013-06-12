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
import java.util.Random;

/**
 * Implementation of deterministic-but-random selection: identical series of calls will result
 * in identical selections. This is strictly for test purposes, and should not be used for any
 * real selection.
 */
class RandomSelector implements Selector {
  private final Random rng = new Random(42);
  private final Node node;

  public RandomSelector(Node node) {
    if (node.isLeaf()) {
      throw new IllegalArgumentException("count is larger than the number of nodes!");
    }
    this.node = node;
  }

  public Node select(long input, long round) {
    List<Node> children = node.getChildren();
    final int length = children.size();
    if (length == 1) {
      return children.get(0);
    }

    // compute the sum of weights
    int totalWeight = 0;
    for (Node n: children) {
      totalWeight += n.getWeight();
    }
    // random number
    int draw = rng.nextInt(totalWeight);
    // pick a node based on the random number
    int sum = 0;
    for (Node n: children) {
      sum += n.getWeight();
      if (draw < sum) {
        // have a match: make a copy
        return n;
      }
    }
    return null;
  }
}
