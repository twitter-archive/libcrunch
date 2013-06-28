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
import com.twitter.crunch.tools.jsontopology.MappingParameters;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateBlobstoreMapping {
  private static final Logger logger = LoggerFactory.getLogger(CreateBlobstoreMapping.class);

  private static List<Long> initializeVirtualBuckets(final int count) {
    List<Long> data = new ArrayList<Long>(count);
    for (long l = 1; l <= count; l++) {
      data.add(l);
    }

    return Collections.unmodifiableList(data);
  }

  public static Map<Long, List<Node>> createNodeMapv1(MappingParameters params, Node root) throws InvalidTopologyException, IOException {
    final RDFMapping mappingFunction = new RDFMapping(
      params.getRdf(),
      params.getRf(),
      new RackIsolationPlacementRules(),
      params.getTargetBalance());

    final List<Long> buckets = initializeVirtualBuckets(params.getVirtualBucketCount());
    final Map<Long, List<Node>> mapping = mappingFunction.computeMapping(buckets, root);

    Map<String,List<String>> rdfMap = mappingFunction.getNewRdfMap();
    Utils.exportRDFMap("rdfmap_v1", rdfMap);

    return mapping;
  }

  public static Map<Long, List<Node>> createNodeMapv3(MappingParameters params, Node root, String oldFileName, String newFileName,
                                                      int rackDiversity, boolean trackCapacity, String migrationMapFileName) throws InvalidTopologyException {
    final List<Long> buckets = initializeVirtualBuckets(params.getVirtualBucketCount());

    Map<String, List<String>> currentMap;
    Map<String, List<String>> migrationMap = null;
    try {
      currentMap = Utils.importRDFMap(oldFileName);
    } catch (Exception e) {
      currentMap = new HashMap<String, List<String>>();
    }

    try {
      if (migrationMapFileName != null)
        migrationMap = Utils.importRDFMap(migrationMapFileName);
    } catch (Exception e) {
      migrationMap = null;
    }

    int rdfRange = (int)(params.getRdf() * 0.2);

    final StableRdfMapping mappingFunction = new StableRdfMapping(
      params.getRdf(),
      params.getRf(),
      new RackIsolationPlacementRules(),
      currentMap,
      params.getRdf() - rdfRange,
      params.getRdf() + rdfRange,
      params.getTargetBalance(),
      rackDiversity,
      trackCapacity,
      migrationMap);

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
      System.out.println("Usage: version topology_json topology_params_json mapping_filename rdf target_balance rack_diversity track_capacity [new_rdf_filename] [old_rdf_filename] [migration_map]");
      System.out.println("  version 1: Generate RDF Map using libcrunch");
      System.out.println("  version 3: Generate RDF Map using stateful distribution");
      return;
    }

    final int version = Integer.parseInt(args[0]);
    final String topologyJson = args[1];
    final String topologyParamsJson = args[2];
    final String fileName = args[3];

    final int rdf = Integer.parseInt(args[4]);
    final double targetBalance = Double.parseDouble(args[5]);

    final int rackDiversity = Integer.parseInt(args[6]);
    final boolean trackCapacity = Boolean.parseBoolean(args[7]);
    final String newFileName = args[8];
    final String oldFileName = args[9];
    String migrationMapFileName = null;
    if (args.length == 11) {
      migrationMapFileName = args[10];
    }

    JsonTopologyDeserializer deserializer = new JsonTopologyDeserializer();
    com.twitter.crunch.tools.jsontopology.Topology topology = deserializer.readTopology(new File(topologyJson));
    Node root = topology.getRootNode();

    ObjectMapper mapper = new ObjectMapper();
    MappingParameters params = mapper.readValue(new File(topologyParamsJson), MappingParameters.class);

    Map<Long, List<Node>> map = null;
    switch(version) {
      case 1:
        map = createNodeMapv1(params, root);
        break;
      case 3:
        params.setRdf(rdf);
        params.setTargetBalance(targetBalance);
        map = createNodeMapv3(params, root, oldFileName, newFileName, rackDiversity, trackCapacity, migrationMapFileName);
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
