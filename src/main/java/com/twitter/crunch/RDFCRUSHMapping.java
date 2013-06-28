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

import java.util.*;

public class RDFCRUSHMapping {
    private final int rf;
    private final PlacementRules rules;
    private final double targetBalance;

    private final Crunch cruncher = new Crunch();

    public RDFCRUSHMapping(int rf, PlacementRules rules, double targetBalance) {
        if (rf < 1) {
            throw new IllegalArgumentException("RF must be positive");
        }
        this.rf = rf;
        this.rules = rules;
        this.targetBalance = targetBalance;
    }

    /**
     * Given the list of data objects (as expressed as long values) and the processed topology,
     * returns the mapping from data objects to lists of end nodes onto which the data may be stored.
     */
    public Map<Long,List<Node>> createMapping(List<Long> data, Node crunchedRoot, Map<Node,List<Node>> rdfMap) {
        // sort the data to ensure data is used in the same order
        List<Long> sorted = new ArrayList<Long>(data);
        Collections.sort(sorted);

        Map<Long,List<Node>> map = new HashMap<Long,List<Node>>(sorted.size());
        // performance optimization
        // we create mini-trees to select the replicas; instead of creating the mini-trees every time,
        // we cache the result
        Map<Node,Node> miniTreeCache = new HashMap<Node,Node>();
        List<Node> datacenters = crunchedRoot.findChildren(Types.DATA_CENTER);
        // iterate on all datacenters
        for (Node datacenter: datacenters) {
            AssignmentTracker tracker = AssignmentTrackerFactory.create(datacenter, rf*sorted.size(), targetBalance);
            PlacementAlgorithm pa = new CRUSHPlacementAlgorithm(tracker);

            for (Long l: sorted) { // ~ N
                List<Node> selected = pickNodes(l, datacenter, pa, rdfMap, miniTreeCache);
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

    private List<Node> pickNodes(long data, Node datacenter, PlacementAlgorithm pa,
                                 Map<Node,List<Node>> rdfMap, Map<Node,Node> miniTreeCache) {
        List<Node> nodes = new ArrayList<Node>(rf);
        // get the primary node
        Node primary = pa.select(datacenter, data, 1, rules.getEndNodeType()).get(0);
        nodes.add(primary);

        // obtain the "mini-tree"
        Node miniTree = miniTreeCache.get(primary);
        if (miniTree == null) {
            // we haven't seen this primary yet
            // get the (RF-1) secondary nodes
            List<Node> secondaries = rdfMap.get(primary);
            // construct the "mini-tree"
            miniTree = makeMiniTree(secondaries);
            miniTreeCache.put(primary, miniTree);
        }
        // select (RF-1) nodes from the mini-tree using the placement rules
        List<Node> selected = rules.select(miniTree, data, rf-1, pa);
        nodes.addAll(selected);
        return nodes;
    }

    private Node makeMiniTree(List<Node> nodes) {
        // this is used to look up parents nodes that are already created
        Map<Long,Node> lookup = new HashMap<Long,Node>();
        Node root = null;
        for (Node node: nodes) { // ~ RDF
            // create a copy for this purpose
            Node newNode = new Node(node);
            root = handleParent(node, newNode, lookup);
        }

        // crunch
        crunchNode(root);
        return root;
    }

    /**
     * Recursively handles all the parents. Returns the root node as a result.
     */
    private Node handleParent(Node node, Node newNode, Map<Long,Node> lookup) {
        Node parent = node.getParent();
        if (parent == null) {
            // root node: return it
            return newNode;
        }

        // process the parent
        Node newParent = lookup.get(parent.getId());
        if (newParent != null) { // it is already mapped
            // set the relationship
            setRelationship(newNode, newParent);
            // we do not need to walk further because it is already processed
            // simply return the root
            return newParent.getRoot();
        } else {
            // this is the first time we are seeing this node: we need to walk up the tree
            // create a copy
            newParent = new Node(parent);
            // add it to the lookup map
            lookup.put(newParent.getId(), newParent);
            // set the relationship
            setRelationship(newNode, newParent);
            // recurse for its parent
            return handleParent(parent, newParent, lookup);
        }
    }

    private void setRelationship(Node newNode, Node newParent) {
        newNode.setParent(newParent);
        List<Node> childList = newParent.getChildren();
        if (childList == null) {
            childList = new ArrayList<Node>();
            newParent.setChildren(childList);
        }
        childList.add(newNode);
    }

    private void crunchNode(Node root) {
        cruncher.recrunch(root);
    }
}
