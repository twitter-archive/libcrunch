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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ProbingRDFMapping implements MappingFunction {
    private static final Logger logger = LoggerFactory.getLogger(StableRdfMapping.class);

    private final int rdf;
    private final int rf;
    private final PlacementRules rules;;
    private final int weightBalanceTries;
    private final double weightBalanceFactor;
    private final int historyCount;
    private final double sdThreshold;
    private final double targetBalanceMax;

    private final Crunch cruncher = new Crunch();

    private class DistributionResult {
        public Map<Node,List<Node>> mapping;
        public Map<String, Long> keyDistribution;
        public double mean;
        public double stdDev;
    }

    public ProbingRDFMapping(int rdf, int rf, PlacementRules rules, int weightBalanceTries, double weightBalanceFactor,
                             int historyCount, double sdThreshold, double targetBalanceMax) {
        this.rdf = rdf;
        this.rf = rf;
        this.rules = rules;
        this.weightBalanceTries = weightBalanceTries;
        this.weightBalanceFactor = weightBalanceFactor;
        this.historyCount = historyCount;
        this.sdThreshold = sdThreshold;
        this.targetBalanceMax = targetBalanceMax;
    }

    private DistributionResult getBestMapping(List<DistributionResult> results) {
        DistributionResult best = results.get(0);

        for (DistributionResult result : results) {
            if (result.stdDev < best.stdDev) best = result;
        }

        return best;
    }

    private void adjustWeight(Node topology, Map<String, Long> distribution, Map<String, Long> weight, double weightBalanceFactor) {
        final List<Node> allLeaves = topology.getAllLeafNodes();
        long totalWeight = 0;
        long totalItem = 0;

        for (String node : weight.keySet()) {
            totalWeight += weight.get(node);
        }

        for (String node : distribution.keySet()) {
            totalItem += distribution.get(node);
        }

        for (Node node : allLeaves) {
            final long count = distribution.get(node.getName());
            final long idealCount = weight.get(node.getName()) * totalItem/totalWeight;
            final long newWeight = (long)(node.getWeight()*(1 + (1 - count/idealCount)*weightBalanceFactor));
            node.setWeight(newWeight);
        }
    }

    private Node copyTopology(Node topology) {
        Node node = new Node(topology);

        if (!topology.isLeaf()) {
            List<Node> newChildren = new ArrayList<Node>();
            List<Node> children = topology.getChildren();
            for (Node child: children) {
                // depth-first traversal
                Node newChild = copyTopology(child);
                newChildren.add(newChild);
                newChild.setParent(node);
            }
            node.setChildren(newChildren);
        }

        return node;
    }

    private boolean evaluateResult(List<DistributionResult> results, double sdThreshold) {
        List<Double> distribution = new ArrayList<Double>();

        for (DistributionResult result : results) {
            distribution.add(result.stdDev);
        }

        return MappingEvaluator.getStandardDeviation(distribution) <= sdThreshold;
    }

    private DistributionResult calcDistribution(Map<Node,List<Node>> mapping, Map<String, Long> weight) {
        DistributionResult result = new DistributionResult();

        result.keyDistribution = new HashMap<String, Long>();
        for (Node key: mapping.keySet()) {
            for (Node node: mapping.get(key)) {
                final String nodeName = node.getName();
                if (result.keyDistribution.containsKey(nodeName)) {
                    final long count = result.keyDistribution.get(nodeName) + 1;
                    result.keyDistribution.put(nodeName, count);
                } else {
                    result.keyDistribution.put(nodeName, (long)1);
                }
            }
        }

        for (String node : weight.keySet()) {
            if (!result.keyDistribution.containsKey(node)) result.keyDistribution.put(node, (long)0);
        }

        result.mapping = mapping;
        result.mean = MappingEvaluator.getWeightedMean(result.keyDistribution, weight);
        result.stdDev = MappingEvaluator.getWeightedStandardDeviation(result.keyDistribution, weight);

        return result;
    }

    private Map<Node,List<Node>> optimizeRDFMapping(Node topology) throws MappingException {
        final List<Node> allLeaves = topology.getAllLeafNodes();
        Map<String, Long> definedWeight = new HashMap<String, Long>();
        int bestTries = 0;

        for (Node node : allLeaves) {
            definedWeight.put(node.getName(), node.getWeight());
        }

        final Node root = copyTopology(topology);

        ArrayList<DistributionResult> results = new ArrayList<DistributionResult>(historyCount);
        DistributionResult bestResult = new DistributionResult();
        bestResult.stdDev = 100;
        final RDFMapping rdfMapping = new RDFMapping(rdf, rf, rules);

        for (int tries = 0; tries < weightBalanceTries; tries++) {
            final Node crunched = cruncher.makeCrunch(root);
            final Map<Node,List<Node>> rdfMap = rdfMapping.createRDFMapping(crunched);

            final DistributionResult result = calcDistribution(rdfMap, definedWeight);
            if (tries < historyCount) {
                results.add(result);
            } else {
                results.set(tries % historyCount, result);
            }

            adjustWeight(root, result.keyDistribution, definedWeight, weightBalanceFactor);

            if (result.stdDev < bestResult.stdDev) {
                bestResult = result;
                bestTries = tries;
            }
        }

        logger.info("created RDF mapping at {} iteration with sd {}", bestTries, bestResult.stdDev);
        return bestResult.mapping;
    }

    private Map<Node,List<Node>> optimizeRDFMappingWithThreshold(Node topology) throws MappingException {
        final List<Node> allLeaves = topology.getAllLeafNodes();
        Map<String, Long> definedWeight = new HashMap<String, Long>();

        for (Node node : allLeaves) {
            definedWeight.put(node.getName(), node.getWeight());
        }

        final Node root = copyTopology(topology);

        ArrayList<DistributionResult> results = new ArrayList<DistributionResult>(historyCount);
        final RDFMapping rdfMapping = new RDFMapping(rdf, rf, rules);

        for (int tries = 0; tries < weightBalanceTries; tries++) {
            final Node crunched = cruncher.makeCrunch(root);
            final Map<Node,List<Node>> rdfMap = rdfMapping.createRDFMapping(crunched);

            final DistributionResult result = calcDistribution(rdfMap, definedWeight);
            if (tries < historyCount) {
                results.add(result);
            } else {
                results.set(tries % historyCount, result);
            }

            if (tries >= (historyCount - 1) && evaluateResult(results, sdThreshold)) {
                logger.info("created RDF mapping at {} iteration", tries);
                return getBestMapping(results).mapping;
            } else {
                adjustWeight(root, result.keyDistribution, definedWeight, weightBalanceFactor);
            }
        }

        throw new MappingException("Cannot find desired mapping");
    }

    private Map<Long,List<Node>> optimizeTargetBalance(List<Long> data, Node crunched, Map<Node,List<Node>> rdfMap) {
        Map<Long,List<Node>> lastMap = null;

        for (double balance = targetBalanceMax; balance >= 0; balance -= 0.01 ) {
            if (targetBalanceMax != 0 && balance == 0) break;

            Map<Long,List<Node>> map = null;
            try {
                RDFCRUSHMapping rdfMapping = new RDFCRUSHMapping(rf, rules, balance);
                map = rdfMapping.createMapping(data, crunched, rdfMap);
            } catch (Exception e) {
                break;
            }

            logger.info(String.format("created mapping with target balance %.2f", balance));
            lastMap = map;
        }

        return lastMap;
    }

    public Map<Long,List<Node>> computeMapping(List<Long> data, Node topology) {
        final Node crunched = cruncher.makeCrunch(topology);

        long begin = System.nanoTime();
        Map<Node, List<Node>> rdfMap = optimizeRDFMapping(crunched);
        long end = System.nanoTime();
        logger.info("time taken to create the RDF mapping: {} ms", (end - begin)/1000000L);

        begin = System.nanoTime();
        Map<Long,List<Node>> map = optimizeTargetBalance(data, crunched, rdfMap);
        end = System.nanoTime();
        logger.info("time taken to create mapping: {} ms", (end - begin)/1000000L);

        return map;
    }
}
