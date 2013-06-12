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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * Based on a topology based on racks, prescribes rack isolation placement rules. Specific
 * implementations should mix in their specific types based on the {@link RackBasedTypes} and define
 * the end type.
 */
public abstract class BaseRackIsolationPlacementRules implements PlacementRules, RackBasedTypes {
  /**
   * In case we get less than full return values from the placement algorithm, we retry by changing
   * the input to the placement algorithm. This should converge pretty rapidly under normal
   * circumstances. However, if it fails to converge after a certain number of tries, we throw a
   * MappingException to indicate the failure.
   */
  private static final int CONVERGENCE_LIMIT = 20;

  private final MultiInputHash hashFunction = new JenkinsHash();

  /**
   * Enforce rack isolation. The caller will either get the expected number of selected nodes as a
   * result, or an exception will be thrown.
   *
   * @return the number of selected nodes with the rack isolation placement rules enforced. The size
   * will match the input count
   * @throws MappingException if it is unable to find the mapping that satisfies all constraints
   */
  public List<Node> select(Node topNode, long data, int n, PlacementAlgorithm pa)
      throws MappingException {
    List<Node> nodes = new ArrayList<Node>(n);
    Set<Node> selectedRacks = new HashSet<Node>();
    long input = data;
    int count = n;
    int tries = 0;
    while (nodes.size() < n) {
      doSelect(topNode, input, count, pa, nodes, selectedRacks);
      count = n - nodes.size();
      if (count > 0) { // still not all picked
        input = hash(input); // hash the input to create a different data value
        tries++;
        if (tries >= CONVERGENCE_LIMIT) {
          throw new MappingException(String.format("could not fulfill all selection after %d tries",
              tries));
        }
      }
    }
    return nodes;
  }

  private void doSelect(Node topNode, long input, int count, PlacementAlgorithm pa,
      List<Node> selectedNodes, Set<Node> selectedRacks) {
    // pick (count) racks avoiding the racks already picked
    List<Node> racks = pa.select(topNode, input, count, RACK, getRackPredicate(selectedRacks));
    // add the racks to the selected racks
    selectedRacks.addAll(racks);
    // pick one end node
    for (Node rack: racks) {
      List<Node> endNode = pa.select(rack, input, 1, getEndNodeType());
      selectedNodes.addAll(endNode);
    }
  }

  /**
   * Use the predicate to reject already selected racks.
   */
  private Predicate<Node> getRackPredicate(Set<Node> selectedRacks) {
    return Predicates.not(Predicates.in(selectedRacks));
  }

  /**
   * Do a simple hashing of the original data.
   */
  private long hash(long data) {
    return hashFunction.hash(data);
  }

  /**
   * Rejects the replica if they share the rack.
   */
  public boolean acceptReplica(Node primary, Node replica) {
    Node primaryRack = primary.findParent(RACK);
    Node replicaRack = replica.findParent(RACK);
    return primaryRack.getId() != replicaRack.getId();
  }
}
