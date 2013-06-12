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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mapping function that computes a simple CRUSH mapping. By default, it accepts RF as the only
 * parameter to control the replication factor.
 */
public class SimpleCRUSHMapping implements MappingFunction {
  private final int rf;
  private final PlacementRules rules;
  private final double targetBalance;

  public SimpleCRUSHMapping(int rf, PlacementRules rules) {
    this(rf, rules, 0.0d);
  }

  public SimpleCRUSHMapping(int rf, PlacementRules rules, double targetBalance) {
    this.rf = rf;
    this.rules = rules;
    this.targetBalance = targetBalance;
  }

  public Map<Long,List<Node>> computeMapping(List<Long> data, Node topology) {
    // sort the data to ensure data is used in the same order
    List<Long> sorted = new ArrayList<Long>(data);
    Collections.sort(sorted);

    Node crunch = new Crunch().makeCrunch(topology);
    Map<Long,List<Node>> map = new HashMap<Long,List<Node>>(sorted.size());
    // iterate over datacenters
    List<Node> datacenters = crunch.findChildren(Types.DATA_CENTER);
    for (Node datacenter: datacenters) {
      AssignmentTracker tracker =
          AssignmentTrackerFactory.create(datacenter, rf*sorted.size(), targetBalance);
      PlacementAlgorithm pa = new CRUSHPlacementAlgorithm(tracker);

      for (Long l: sorted) {
        // apply the placement rules
        List<Node> selected = rules.select(datacenter, l, rf, pa);
        List<Node> nodes = map.get(l);
        if (nodes == null) {
          nodes = new ArrayList<Node>(rf*datacenters.size());
          map.put(l, nodes);
        }
        nodes.addAll(selected);
      }
    }
    return map;
  }
}
