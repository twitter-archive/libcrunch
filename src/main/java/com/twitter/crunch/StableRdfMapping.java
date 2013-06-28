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

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class StableRdfMapping implements MappingFunction {
  private static final Logger logger = LoggerFactory.getLogger(StableRdfMapping.class);

  private final int rdf;
  private final int rf;
  private final PlacementRules rules;
  private final Map<String, List<String>> oldRdfMap;
  private final int rdfMin;
  private final int rdfMax;
  private final double targetBalance;
  private final int rackDiversity;
  private final Map<String, List<String>> migrationMap;

  private Map<Node, List<Node>> newRdfMap;

  private final boolean trackCapacity;
  private Map<String, Double> replicaCapacity = new HashMap<String, Double>();
  private Map<String, Map<String, Double>> replicaUsage = new HashMap<String, Map<String, Double>>();

  private final Crunch cruncher = new Crunch();

  public StableRdfMapping(int rdf, int rf, PlacementRules rules, Map<String, List<String>> oldRdfMap,
                          int rdfMin, int rdfMax, double targetBalance, int rackDiversity, boolean trackCapacity, Map<String, List<String>> migrationMap) {
    this.rdf = rdf;
    this.rf = rf;
    this.rules = rules;
    this.oldRdfMap = oldRdfMap;
    this.newRdfMap = null;
    this.rdfMin = rdfMin;
    this.rdfMax = rdfMax;
    this.targetBalance = targetBalance;
    this.rackDiversity = rackDiversity;
    this.trackCapacity = trackCapacity;
    this.migrationMap = migrationMap;
  }
  public StableRdfMapping(int rdf, int rf, PlacementRules rules, Map<String, List<String>> oldRdfMap,
                          int rdfMin, int rdfMax, double targetBalance, int rackDiversity, boolean trackCapacity) {
    this(rdf, rf, rules, oldRdfMap, rdfMin, rdfMax, targetBalance, rackDiversity, trackCapacity, null);
  }

  public Map<String, List<String>> getNewRdfMap() {
    Map<String, List<String>> rdfMap = new HashMap<String, List<String>>();

    for (Map.Entry<Node, List<Node>> entry: newRdfMap.entrySet()) {
      List<String> nodeList = new ArrayList<String>();
      for (Node node: entry.getValue()) {
        nodeList.add(node.getName());
      }
      rdfMap.put(entry.getKey().getName(), nodeList);
    }

    return rdfMap;
  }

  private void initializeCapcity(List<Node> allNodes, Map<Node, List<Node>> mapping) {
    if (!this.trackCapacity) return;

    // Calculate total weight
    double totalWeight = 0;
    for (Node node: allNodes) {
      totalWeight += node.getWeight();
    }

    // Calculate replica capacity and initialize replica usage
    for (Node node: allNodes) {
      this.replicaCapacity.put(node.getName(), node.getWeight() / totalWeight);
      this.replicaUsage.put(node.getName(), new HashMap<String, Double>());
    }

    for (Node node: mapping.keySet()) {
      updateReplicaUsage(mapping, node);
    }
  }

  private void updateReplicaUsage(Map<Node, List<Node>> mapping, Node primary) {
    if (!this.trackCapacity) return;

    final List<Node> replicas = mapping.get(primary);
    String primaryName = primary.getName();

    // Nodes < rdfMin will be removed at the end
    if (replicas.size() < this.rdfMin) return;

    double totalReplicasWeight = 0;
    for (Node replica: replicas) {
      totalReplicasWeight += replica.getWeight();
    }

    for (Node replica: replicas) {
      String replicaName = replica.getName();
      Map<String, Double> usage = this.replicaUsage.get(replicaName);
      usage.put(primaryName, this.replicaCapacity.get(primaryName) * replica.getWeight() / totalReplicasWeight);
    }
  }

  private double getReplicaUsage(Node replica) {
    if (!this.trackCapacity) return 0;

    Map<String, Double> usage = replicaUsage.get(replica.getName());

    double totalUsage = 0;
    for (String node: usage.keySet()) {
      totalUsage += usage.get(node);
    }

    return totalUsage;
  }

  private boolean hasCapacity(Node node) {
    if (!this.trackCapacity) return true;

    return this.replicaCapacity.get(node.getName()) > getReplicaUsage(node);
  }

  private boolean hasConflict(Node node, List<Node> existingNodes) {
    int conflicts = 0;
    for (Node replica: existingNodes) {
      if (!rules.acceptReplica(replica, node)) {
        conflicts ++;
      }
    }
    return (conflicts >= this.rackDiversity);
  }

  private Node findCandidate(Node ownerNode, Map<Node, List<Node>> candidateMap, Multimap<Integer, Node> nodeReplicaSizeMap,
                             Map<Node, List<Node>> rdfMap) {
    Node candidate = null;
    List<Node> candidates = candidateMap.get(ownerNode);

    if (candidates == null) return null;

    // Find candidate
    for (Map.Entry<Integer, Node> entry: nodeReplicaSizeMap.entries()) {
      Node node = entry.getValue();
      int replicaSize = entry.getKey();

      if(!candidates.contains(node)) continue;

      if (replicaSize >= this.rdfMax) {
        // Remove nodes with RDF >= rdfMax
        candidates.remove(node);
      } else if (!hasCapacity(node)) {
        continue;
      } else if (hasConflict(node, rdfMap.get(ownerNode)) || hasConflict(ownerNode, rdfMap.get(node))) {
        candidates.remove(node);
        candidateMap.get(node).remove(ownerNode);
        continue;
      } else {
        if (migrationMap == null || !migrationMap.containsKey(ownerNode.getName()) ||
          migrationMap.get(ownerNode.getName()).contains(node.getName())) {
          candidate = node;
          break;
        } else {
          if (candidate == null) {
            candidate = node;
            continue;
          }
        }
      }
    }

    return candidate;
  }

  private void buildRDFMapping(Node datacenter, Map<Node, List<Node>> mapping) throws MappingException {
    final List<Node> allNodes = datacenter.getAllLeafNodes();

    initializeCapcity(allNodes, mapping);

    // Generate candidates
    Map<Node, List<Node>> candidateMap = new TreeMap<Node, List<Node>>();
    for (Node node: mapping.keySet()) {
      for (Node candidate: mapping.keySet()) {
        if (mapping.get(candidate).size() >= this.rdfMax) continue;

        if (candidate != node && rules.acceptReplica(node, candidate) && !mapping.get(node).contains(candidate)) {
          List<Node> nodeList = candidateMap.get(node);
          if (nodeList == null) {
            nodeList = new ArrayList<Node>();
            candidateMap.put(node, nodeList);
          }
          nodeList.add(candidate);
        }
      }
    }

    TreeMultimap<Integer, Node> nodeReplicaSizeMap = TreeMultimap.create();
    for (Node node: mapping.keySet()) {
      nodeReplicaSizeMap.put(mapping.get(node).size(), node);
    }

    while(true) {
      // Pick node with least number of replicas
      Node candidate = null;
      Node minNode = null;
      int min = this.rdfMax;

      for (Map.Entry<Integer, Node> entry: nodeReplicaSizeMap.entries()) {
        int replicaSize = entry.getKey();
        Node node = entry.getValue();

        // break at this point since it sorted
        if (replicaSize >= this.rdfMax) break;

        Node findResult = findCandidate(node, candidateMap, nodeReplicaSizeMap, mapping);
        if (findResult != null) {
          candidate = findResult;
          minNode = node;
          min = replicaSize;
          break;
        }
      }

      if (minNode == null || mapping.get(minNode).size() >= this.rdfMin) break;

      // Pair the candidate up
      mapping.get(candidate).add(minNode);
      mapping.get(minNode).add(candidate);
      candidateMap.get(minNode).remove(candidate);
      candidateMap.get(candidate).remove(minNode);

      nodeReplicaSizeMap.remove(min, minNode);
      nodeReplicaSizeMap.put(min + 1, minNode);
      int candidateSize = mapping.get(candidate).size();
      nodeReplicaSizeMap.remove(candidateSize - 1, candidate);
      nodeReplicaSizeMap.put(candidateSize, candidate);

      updateReplicaUsage(mapping, minNode);

      logger.info("Added {} for {}", candidate.getName(), minNode.getName());
    }
  }

  private Map<Node,List<Node>> optimizeRDFMapping(Node topology) throws MappingException {
    Map<Node, List<Node>> mapping = new TreeMap<Node, List<Node>>();

    for(Node datacenter : topology.findChildren(Types.DATA_CENTER)) {
      final List<Node> allNodes = datacenter.getAllLeafNodes();

      Map<Node, List<Node>> dcMapping = new TreeMap<Node, List<Node>>();

      // Remove dead nodes
      for(String nodeName: this.oldRdfMap.keySet()) {
        Node node = new Node();
        node.setName(nodeName);
        if (allNodes.contains(node)) {
          node = allNodes.get(allNodes.indexOf(node));
          if (node.isFailed() || node.getWeight() <= 0) continue;
          List<Node> replicas = new ArrayList<Node>();
          for (String replicaName: this.oldRdfMap.get(nodeName)) {
            Node replica = new Node();
            replica.setName(replicaName);
            if (allNodes.contains(replica)) {
              replica = allNodes.get(allNodes.indexOf(replica));
              if (replica.isFailed() || replica.getWeight() <= 0) continue;
              replicas.add(replica);
            }
          }
          dcMapping.put(node, replicas);
        }
      }

      // Add new nodes
      for(Node node: allNodes) {
        if (node.isFailed() || node.getWeight() <= 0) continue;
        if (!dcMapping.containsKey(node)) {
          dcMapping.put(node, new ArrayList<Node>());
        }
      }

      // Add removed nodes (<rdfMin) back
      for (Node node: dcMapping.keySet()) {
        for (Node replica : dcMapping.get(node)) {
          if (!this.oldRdfMap.containsKey(replica.getName())) {
            dcMapping.get(replica).add(node);
          }
        }
      }

      buildRDFMapping(datacenter, dcMapping);
      mapping.putAll(dcMapping);
    }

    return mapping;
  }

  private Map<Long,List<Node>> optimizeTargetBalance(List<Long> data, Node crunched) {
    Map<Long,List<Node>> map = null;
    RDFCRUSHMapping rdfMapping = new RDFCRUSHMapping(this.rf, this.rules, this.targetBalance);
    map = rdfMapping.createMapping(data, crunched, this.newRdfMap);

    logger.info(String.format("created mapping with target balance %.2f", this.targetBalance));
    return map;
  }

  private void removeNodes(Node crunched) {
    // Remove nodes with RDF < minRDF
    List<Node> toBeRemoved = new ArrayList<Node>();
    for (Node node : this.newRdfMap.keySet()) {
      if (this.newRdfMap.get(node).size() < this.rdfMin) toBeRemoved.add(node);
    }

    for (Node node: toBeRemoved) {
      this.newRdfMap.remove(node);

      Node parent = node.getParent();
      Node child = node;
      while (parent != null && parent.getChildren().size() == 1) {
        child = parent;
        parent = parent.getParent();
      }
      if (parent != null) parent.getChildren().remove(child);
    }
  }

  public Map<Long,List<Node>> computeMapping(List<Long> data, Node topology) {
    Node crunched = cruncher.makeCrunch(topology);

    long begin = System.nanoTime();
    this.newRdfMap = optimizeRDFMapping(crunched);
    long end = System.nanoTime();
    logger.info("time taken to create the RDF mapping: {} ms", (end - begin)/1000000L);

    removeNodes(crunched);
    crunched = cruncher.makeCrunch(crunched);

    begin = System.nanoTime();
    Map<Long,List<Node>> map = optimizeTargetBalance(data, crunched);
    end = System.nanoTime();
    logger.info("time taken to create mapping: {} ms", (end - begin)/1000000L);

    return map;
  }
}
