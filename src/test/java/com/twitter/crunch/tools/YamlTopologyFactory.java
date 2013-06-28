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

import com.twitter.crunch.*;

import java.util.*;

public class YamlTopologyFactory {
    public int number_of_buckets = 50000;
    public int replica_distribution_factor = 6;
    public int replication_factor = 3;
    public int replica_distribution_factor_min = 5;
    public int replica_distribution_factor_max = 7;

    public double target_balance_max = 0;
    public boolean dump_detail_map = false;
    public int weight_balance_tries = 1;
    public double weight_balance_factor = 0.1;
    public int history_count = 10;
    public double sd_threshold = 0.05;

    public List<TopologyMachine> machine_list = null;
    public static class TopologyMachine {
        public String name;
        public Long weight;
        public String datacenter;
        public String rack;
    }

    private static void parseMachineName(TopologyMachine machine) throws InvalidTopologyException {
        String[] nameParts = machine.name.split("\\.");
        if (nameParts.length == 0) {
            throw new InvalidTopologyException("Machine name " + machine.name + " is not fully qualified domain name");
        }
        String machineName = nameParts[0];
        String[] parts = machineName.split("-");
        if (parts.length != 4) {
            throw new InvalidTopologyException("Machine name " + machineName + " is not in dc-rack-subrack-# format");
        }
        machine.datacenter = parts[0];
        machine.rack = parts[1];
    }

    private Node buildLibcrunchTree(Map<String, Map<String, Set<TopologyMachine>>> datacenters) {
        int id = 0;

        // Build the root
        Node libcrunchRoot = new Node();
        libcrunchRoot.setName("root");
        libcrunchRoot.setId(id++);
        libcrunchRoot.setType(Types.ROOT);
        libcrunchRoot.setSelection(Node.Selection.STRAW);

        List<Node> libcrunchDcs = new ArrayList<Node>();
        for (String datacenter : datacenters.keySet()) {
            Node libcrunchDc = new Node();
            libcrunchDc.setName(datacenter);
            libcrunchDc.setId(id++);
            libcrunchDc.setType(Types.DATA_CENTER);
            libcrunchDc.setSelection(Node.Selection.STRAW);
            libcrunchDc.setParent(libcrunchRoot);

            List<Node> libcrunchRacks = new ArrayList<Node>();
            for (String rack : datacenters.get(datacenter).keySet()) {
                Node libcrunchRack = new Node();
                libcrunchRack.setName(rack);
                libcrunchRack.setId(id++);
                libcrunchRack.setType(StorageSystemTypes.RACK);
                libcrunchRack.setSelection(Node.Selection.STRAW);
                libcrunchRack.setParent(libcrunchDc);

                List<Node> libcrunchNodes = new ArrayList<Node>();
                for (TopologyMachine machine : datacenters.get(datacenter).get(rack)) {
                    Node libcrunchNode = new Node();
                    libcrunchNode.setName(machine.name);
                    libcrunchNode.setWeight(machine.weight);
                    libcrunchNode.setId(id++);
                    libcrunchNode.setType(StorageSystemTypes.DISK);
                    libcrunchNode.setSelection(Node.Selection.STRAW);
                    libcrunchNode.setParent(libcrunchRack);
                    libcrunchNodes.add(libcrunchNode);
                }
                libcrunchRack.setChildren(libcrunchNodes);
                libcrunchRacks.add(libcrunchRack);
            }
            libcrunchDc.setChildren(libcrunchRacks);
            libcrunchDcs.add(libcrunchDc);
        }
        libcrunchRoot.setChildren(libcrunchDcs);

        return libcrunchRoot;
    }

    public Node loadTopology() throws InvalidTopologyException {
        // Parse machine name to get datacenter and rack information
        Map<String, Map<String, Set<TopologyMachine>>> datecenters = new HashMap<String, Map<String, Set<TopologyMachine>>>();
        for(TopologyMachine machine: machine_list) {
            parseMachineName(machine);
            if (datecenters.containsKey(machine.datacenter)) {
                Map<String, Set<TopologyMachine>> racks = datecenters.get(machine.datacenter);
                if (racks.containsKey(machine.rack)) {
                    Set<TopologyMachine> machines = racks.get(machine.rack);
                    machines.add(machine);
                } else {
                    Set<TopologyMachine> machines = new HashSet<TopologyMachine>();
                    machines.add(machine);
                    racks.put(machine.rack, machines);
                }
            } else {
                Map<String, Set<TopologyMachine>> rack = new HashMap<String, Set<TopologyMachine>>();
                Set<TopologyMachine> machines = new HashSet<TopologyMachine>();
                machines.add(machine);
                rack.put(machine.rack, machines);
                datecenters.put(machine.datacenter, rack);
            }
        }

        return buildLibcrunchTree(datecenters);
    }
}
