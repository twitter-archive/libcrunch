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

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;

public class TopologyGenerator {
    public static void  main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Usage: topology.template.yaml node_count node_weight output_filename");
            return;
        }

        final String topologyTemplate = args[0];
        final int nodeCount = Integer.parseInt(args[1]);
        final long nodeWeight = Long.parseLong(args[2]);
        final String fileName = args[3];

        String yamlContents = new String(Utils.slurp((new FileInputStream(topologyTemplate))));
        final Yaml yaml = new Yaml(new Constructor(YamlTopologyFactory.class));
        final YamlTopologyFactory topologyFactory = (YamlTopologyFactory)yaml.load(yamlContents);

        topologyFactory.machine_list = new ArrayList<YamlTopologyFactory.TopologyMachine>();
        for (int i = 1; i <= nodeCount; i++) {
            YamlTopologyFactory.TopologyMachine machine = new YamlTopologyFactory.TopologyMachine();
            machine.name = String.format("smf1-%03d-01-sr1.prod.twitter.com", i);
            machine.weight = nodeWeight;
            topologyFactory.machine_list.add(machine);
        }

        PrintWriter out = new PrintWriter(fileName);
        out.print(yaml.dump(topologyFactory));
        out.close();
    }
}
