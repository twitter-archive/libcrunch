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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The mapping function based on implementing RDF (replica distribution factor), RF (replication
 * factor), and a multi-level topology and placement rules.
 * <br/>
 * Both parameters are interpreted as per-datacenter; i.e. if RF = 2, you will have two replicas per
 * datacenter.
 */

public class RDFMapping implements MappingFunction {
  private static final Logger logger = LoggerFactory.getLogger(RDFMapping.class);

  private final int rdf;
  private final int rf;
  private final PlacementRules rules;

  private final boolean bidirectional;
  private final boolean handleOverload;
  private final double targetBalance;

  private final Crunch cruncher = new Crunch();

  private Map<Node,List<Node>> rdfMap;

  public RDFMapping(int rdf, int rf, PlacementRules rules) {
    this(rdf, rf, rules, false, false, 0.0d); // bi-di and overload handling are off by default
  }

  public RDFMapping(int rdf, int rf, PlacementRules rules, double targetBalance) {
    this(rdf, rf, rules, false, false, targetBalance);
  }

  public RDFMapping(int rdf, int rf, PlacementRules rules, boolean bidirectional) {
    this(rdf, rf, rules, bidirectional, false, 0.0d); // overload handling is off by default
  }

  private RDFMapping(int rdf, int rf, PlacementRules rules, boolean bidirectional,
      boolean handleOverload, double targetBalance) {
    if (rf < 1) {
      throw new IllegalArgumentException("RF must be positive");
    }
    if (rdf < rf) {
      throw new IllegalArgumentException("RDF must be equal to or greater than RF");
    }
    this.rdf = rdf;
    this.rf = rf;
    this.rules = rules;
    this.bidirectional = bidirectional;
    this.handleOverload = handleOverload;
    this.targetBalance = targetBalance;
  }

  /**
   * Given the topology and the list of data as represented by long values, and the placement rules
   * specified by the RDF mapping object, produces the mapping from data to list of nodes.
   */
  public Map<Long,List<Node>> computeMapping(List<Long> data, Node topology) {
    Node crunched = cruncher.makeCrunch(topology);
    long begin = System.nanoTime();
    rdfMap = createRDFMapping(crunched);
    long end = System.nanoTime();
    logger.info("time taken to create the RDF mapping: {} ms", (end-begin)/1000000L);
    begin = System.nanoTime();
    RDFCRUSHMapping crushMapping = new RDFCRUSHMapping(rf, rules, targetBalance);
    Map<Long,List<Node>> map = crushMapping.createMapping(data, crunched, rdfMap);
    end = System.nanoTime();
    logger.info("time taken to create mapping: {} ms", (end-begin)/1000000L);
    return map;
  }

  /**
   * Given the processed topology, returns the mapping from end nodes to lists of secondary end
   * nodes that are allowed to store the replicas. This mapping uses the same CRUSH algorithm as the
   * basic placement algorithm.
   */
  public Map<Node,List<Node>> createRDFMapping(Node crunchedRoot) {
    // iterate on all datacenters
    List<Node> datacenters = crunchedRoot.findChildren(Types.DATA_CENTER);
    Map<Node,List<Node>> map = new HashMap<Node,List<Node>>(crunchedRoot.getAllLeafNodes().size());
    for (Node datacenter: datacenters) {
      createRDFMappingPerDC(datacenter, map);
    }
    return map;
  }

  public Map<String, List<String>> getNewRdfMap() {
    Map<String, List<String>> map = new HashMap<String, List<String>>();

    for (Map.Entry<Node, List<Node>> entry: rdfMap.entrySet()) {
      List<String> nodeList = new ArrayList<String>();
      for (Node node: entry.getValue()) {
        nodeList.add(node.getName());
      }
      map.put(entry.getKey().getName(), nodeList);
    }

    return map;
  }

  private Map<Node,List<Node>> createRDFMappingPerDC(Node datacenter, Map<Node,List<Node>> map) {
    final List<Node> allLeaves = datacenter.getAllLeafNodes();
    final int endNodeSize = allLeaves.size();
    final int totalMapping = endNodeSize*(rdf-1);

    // use a placement algorithm object for this run and keep track of successive rounds
    PlacementAlgorithm pa = new CRUSHPlacementAlgorithm(true);

    // create the quota so we avoid overloading nodes
    Map<Node,Integer> quota = handleOverload ? createQuota(allLeaves) : null;
    int mapped = 0;

    while (mapped < totalMapping) {
      for (Node primary: allLeaves) { // <~ n
        List<Node> secondaries = map.get(primary);
        if (secondaries == null) {
          secondaries = new ArrayList<Node>(rdf-1);
          map.put(primary, secondaries);
        }
        // if it is already filled, we don't need to look at it
        if (secondaries.size() < rdf-1) {
          // CRUSH selection of nodes using the primary's id
          Node selected = pa.select(datacenter, primary.getId(), 1, rules.getEndNodeType()).get(0);

          if (handleOverload) {
            // pass through a number of filters to reject the selection
            int currentQuota = quota.get(selected);
            if (currentQuota == 0) { // we have used all the quota for this node
              logger.trace("rejecting {} because it is fully committed.", selected);
              continue;
            }
          }
          // first run it through placement rules' acceptance
          if (!rules.acceptReplica(primary, selected)) {
            // reject and move onto the next primary
            logger.trace("rejecting {} for {} from placement rules: we're at {} %",
                new Object[] {selected, primary, ((float)mapped)*100/totalMapping});
            continue;
          }

          // for bi-di, we need to reject the mapping if the secondary is full too
          if (bidirectional) {
            List<Node> other = map.get(selected);
            if (other == null) {
              other = new ArrayList<Node>(rdf-1);
              map.put(selected, other);
            } else if (other.size() >= rdf-1) {
              // reject and move onto the next primary
              logger.trace("rejecting {} for {} because secondary is fully mapped already.",
                  selected, primary);
              continue;
            }
            // make sure it's not selected already
            if (secondaries.contains(selected) || other.contains(primary)) { // these are really one and the same
              logger.trace("secondary {} is already mapped for {}", selected, primary);
              continue;
            }
            logger.trace("accepting {} for {}", selected, primary);
            // accept the node pair
            secondaries.add(selected);
            other.add(primary);
            mapped += 2;
            if (handleOverload) {
              // make sure to decrement the quota
              decrementQuota(selected, quota);
              decrementQuota(primary, quota);
            }
          } else { // uni-directional
            // make sure it's not selected already
            if (secondaries.contains(selected)) {
              logger.trace("secondary {} is already mapped for {}", selected, primary);
              continue;
            }
            logger.trace("accepting {} for {}", selected, primary);
            secondaries.add(selected);
            mapped++;
            if (handleOverload) {
              // make sure to decrement the quota
              decrementQuota(selected, quota);
            }
          }
        }
      }
    }
    return map;
  }

  private Map<Node,Integer> createQuota(List<Node> nodes) {
    Map<Node,Integer> quota = new HashMap<Node,Integer>();
    final int headroom = 1;
    final int size = nodes.size();
    long totalWeight = 0;
    for (Node node: nodes) {
      totalWeight += node.getWeight();
    }
    for (Node node: nodes) {
      int value = (int)(node.getWeight()*(rdf-1)*size/totalWeight) + headroom;
//      logger.debug("quota for {}: {}", node, value);
      quota.put(node, value);
    }
    return quota;
  }

  private void decrementQuota(Node node, Map<Node,Integer> quota) {
    int value = quota.get(node);
    quota.put(node, --value);
  }
}
