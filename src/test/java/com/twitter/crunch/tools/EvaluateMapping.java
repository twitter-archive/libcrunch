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

package com.twitter.crunch.tools;

import com.twitter.crunch.Node;
import com.twitter.crunch.MappingEvaluator;
import com.twitter.crunch.tools.jsontopology.JsonTopologyDeserializer;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

public class EvaluateMapping {
  private static void evaluateMap(Map<Long,List<String>> mapping, Map<String, Long> weight, Map<String, List<String>> rdfMap) {
    Map<String, Long> keyDistribution;
    double mean;
    double stdDev;
    Set<String> primaryNodes = null;

    if (rdfMap != null) primaryNodes = rdfMap.keySet();

    keyDistribution = new HashMap<String, Long>();
    for (Long key: mapping.keySet()) {
      for (String node: mapping.get(key)) {
        if (primaryNodes != null && !primaryNodes.contains(node)) {
          //System.out.println("RDF under min - " + node);
          continue;
        }

        if (keyDistribution.containsKey(node)) {
          final long count = keyDistribution.get(node) + 1;
          keyDistribution.put(node, count);
        } else {
          keyDistribution.put(node, (long)1);
        }
      }
    }

    for (String node : weight.keySet()) {
      if (primaryNodes != null && !primaryNodes.contains(node)) continue;

      if (!keyDistribution.containsKey(node)) keyDistribution.put(node, (long)0);
    }

    mean = MappingEvaluator.getWeightedMean(keyDistribution, weight);
    stdDev = MappingEvaluator.getWeightedStandardDeviation(keyDistribution, weight);
    Long min = Collections.min(keyDistribution.values());
    Long max = Collections.max(keyDistribution.values());
    final int replicaOnlyNodes = primaryNodes == null ? 0 : (weight.keySet().size() - primaryNodes.size());

    System.out.print(String.format("%d,%d,%.4f,%.4f,%d", min, max, mean, stdDev, replicaOnlyNodes));
  }

  public static void  main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Usage: yaml|json topology_file map_filename rdfmap_filename");
      System.out.println("  min,max,mean,sd,replicaOnlyNodes");
      return;
    }

    final String topologyConfig = args[1];
    final String mapFileName = args[2];
    Map<String, List<String>> rdfMap = null;

    Node root = null;
    if (args[0].equalsIgnoreCase("yaml")) {
      String yamlContents = new String(Utils.slurp((new FileInputStream(topologyConfig))));
      final Yaml yaml = new Yaml(new Constructor(YamlTopologyFactory.class));
      final YamlTopologyFactory topologyFactory = (YamlTopologyFactory)yaml.load(yamlContents);
      root = topologyFactory.loadTopology();
    } else {
      try {
        final String rdfMapFileName = args[3];
        rdfMap = Utils.importRDFMap(rdfMapFileName);
      } catch (FileNotFoundException ex) {
        // ignore this
      }
      JsonTopologyDeserializer deserializer = new JsonTopologyDeserializer();
      com.twitter.crunch.tools.jsontopology.Topology topology = deserializer.readTopology(new File(topologyConfig));
      root = topology.getRootNode();
    }

    Map<Long, List<String>> map = Utils.importMap(mapFileName);

    final List<Node> allLeaves = root.getAllLeafNodes();

    Map<String, Long> definedWeight = new HashMap<String, Long>();
    for (Node node : allLeaves) {
      if (!node.isFailed() && node.getWeight() != 0) definedWeight.put(node.getName(), node.getWeight());
      //if (node.getWeight() == 0) System.out.println("Weight 0 - " + node.getName());
    }

    evaluateMap(map, definedWeight, rdfMap);
  }
}
