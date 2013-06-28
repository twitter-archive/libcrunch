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

import java.io.*;
import java.util.*;

import com.twitter.crunch.*;
import com.twitter.crunch.tools.jsontopology.JsonTopologyDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class CreateDataMapping {
    private static final Logger logger = LoggerFactory.getLogger(CreateDataMapping.class);

    private static List<Long> initializeVirtualBuckets(final int count) {
        List<Long> data = new ArrayList<Long>(count);
        for (long l = 1; l <= count; l++) {
            data.add(l);
        }

        return Collections.unmodifiableList(data);
    }

    public static Map<Long, List<Node>> createNodeMapv1(YamlTopologyFactory factory, Node root) throws InvalidTopologyException {
        final MappingFunction mappingFunction = new RDFMapping(
                factory.replica_distribution_factor,
                factory.replication_factor,
                new RackIsolationPlacementRules(),
                factory.target_balance_max);

        final List<Long> buckets = initializeVirtualBuckets(factory.number_of_buckets);
        final Map<Long, List<Node>> mapping = mappingFunction.computeMapping(buckets, root);
        return mapping;
    }

    public static Map<Long, List<Node>> createNodeMapv2(YamlTopologyFactory factory, Node root) throws InvalidTopologyException {
        final ProbingRDFMapping mappingFunction = new ProbingRDFMapping(
                factory.replica_distribution_factor,
                factory.replication_factor,
                new RackIsolationPlacementRules(),
                factory.weight_balance_tries,
                factory.weight_balance_factor,
                factory.history_count,
                factory.sd_threshold,
                factory.target_balance_max);

        final List<Long> buckets = initializeVirtualBuckets(factory.number_of_buckets);
        final Map<Long, List<Node>> mapping = mappingFunction.computeMapping(buckets, root);
        return mapping;
    }

    public static Map<Long, List<Node>> createNodeMapv3(YamlTopologyFactory factory, Node root, String oldFileName, String newFileName) throws InvalidTopologyException {
        final List<Long> buckets = initializeVirtualBuckets(factory.number_of_buckets);

        Map<String, List<String>> currentMap = null;
        try {
            currentMap = Utils.importRDFMap(oldFileName);
        } catch (Exception e) {
            currentMap = new HashMap<String, List<String>>();
        }
        final StableRdfMapping mappingFunction = new StableRdfMapping(
                factory.replica_distribution_factor,
                factory.replication_factor,
                new RackIsolationPlacementRules(),
                currentMap,
                factory.replica_distribution_factor_min,
                factory.replica_distribution_factor_max,
                factory.target_balance_max,
                8,
                false);

        final Map<Long, List<Node>> mapping = mappingFunction.computeMapping(buckets, root);

        final Map<String, List<String>> newRdfMap = mappingFunction.getNewRdfMap();
        try {
            Utils.exportRDFMap(newFileName, newRdfMap);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return mapping;
    }

    public static void  main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: yaml version topology_yaml bucket_map_filename [old_rdf_filename] [new_rdf_filename]");
            System.out.println("       json version topology_yaml topology_json bucket_map_filename [old_rdf_filename] [new_rdf_filename]");
            System.out.println("  version 1: Generate RDF Map using libcrunch");
            System.out.println("  version 2: Generate RDF Map using libcrunch with probing");
            System.out.println("  version 3: Generate RDF Map using stateful distribution");
            return;
        }

        final int version = Integer.parseInt(args[1]);
        final String topologyConfig = args[2];

        String yamlContents = new String(Utils.slurp((new FileInputStream(topologyConfig))));
        final Yaml yaml = new Yaml(new Constructor(YamlTopologyFactory.class));
        final YamlTopologyFactory topologyFactory = (YamlTopologyFactory)yaml.load(yamlContents);

        int offset = 0;
        Node root = null;
        if (args[0].equalsIgnoreCase("yaml")) {
            offset = 0;
            root = topologyFactory.loadTopology();
        } else {
            offset = 1;
            String topologyJson = args[3];
            JsonTopologyDeserializer deserializer = new JsonTopologyDeserializer();
            com.twitter.crunch.tools.jsontopology.Topology topology = deserializer.readTopology(new File(topologyJson));
            root = topology.getRootNode();
        }
        final String fileName = args[3 + offset];

        Map<Long, List<Node>> map = null;
        switch(version) {
            case 1:
                map = createNodeMapv1(topologyFactory, root);
                break;
            case 2:
                map = createNodeMapv2(topologyFactory, root);
                break;
            case 3:
                final String oldFileName = args[4 + offset];
                final String newFileName = args[5 + offset];
                map = createNodeMapv3(topologyFactory, root, oldFileName, newFileName);
                break;
            default:
                System.out.println("Wrong version");
                break;
        }

        // Dump map
        Writer out = new OutputStreamWriter(new FileOutputStream(fileName), "UTF-8");
        try {
            for (Long bucket: new TreeSet<Long>(map.keySet())) {
                out.append(bucket.toString());
                List<Node> replicas = map.get(bucket);
                for (Node replica: replicas) {
                    out.append(',');
                    out.append(replica.getName());
                }
                out.append('\n');
            }
        } finally {
            out.flush();
            out.close();
        }
    }
}
