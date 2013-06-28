"""
Copyright 2013 Twitter, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
import glob
import os
import logging
import subprocess
import shutil


def runProcess(exe):
    logging.info(exe)
    return subprocess.check_output(exe)


def evaluateMapping(versions, topology_files, output_dir):
    output = ""
    for version in versions:
        for i, topology_file in enumerate(topology_files):
            topology_file_name = os.path.basename(topology_file)
            map_file = output_dir + "/map-" + str(version) + "-" + topology_file_name
            
            rdf_file_new = output_dir + "/rdfmap-" + topology_file_name

            eval_command_line = "EvaluateMapping json " + topology_file + " " + map_file + " " + rdf_file_new
            print eval_command_line
            map_output = runProcess(['./runtask.sh', eval_command_line])

            if i != 0:
                old_map_file = output_dir + "/map-" + str(version) + "-" + os.path.basename(topology_files[i-1])
                calc_command_line = "CalculateMovement " + " " + old_map_file + " " + map_file
                map_output = map_output + "," + runProcess(['./runtask.sh', calc_command_line])

            map_output += '\n'
            print map_output
            output += map_output

    return output


def generateMapping(versions, topology_files, output_dir, rdf, target_balance, rack_diversity, track_capacity):
    for version in versions:
        for i, topology_file in enumerate(topology_files):
            topology_file_name = os.path.basename(topology_file)
            params_file = os.path.dirname(topology_file) + "/" + "params_" + topology_file_name
            map_file = output_dir + "/map-" + str(version) + "-" + topology_file_name
            rdf_file_new = output_dir + "/rdfmap-" + topology_file_name
            command_line = "CreateBlobstoreMapping " + str(version) + " " + topology_file + " " + params_file
            command_line = command_line + " " + map_file + " " + str(rdf) + " " + str(target_balance)

            if i != 0:
                rdf_file_old = output_dir + "/rdfmap-" + os.path.basename(topology_files[i-1])
            else:
                rdf_file_old = "null"

            command_line = command_line + " " + str(rack_diversity) + " " + track_capacity
            command_line = command_line + " " + rdf_file_new + " " + rdf_file_old

            logging.info(command_line)
            print command_line
            subprocess.call(['./runtask.sh', command_line])

            if not os.path.isfile(map_file):
                return False
    return True


def compareMappings(topology_files, evaluate, output_dir):
    rdf_min = 8
    rdf_max = 88
    tb_min = 0.05
    tb_max = 0.15
    rd_min = 3
    rd_max = 8
    rdf = rdf_min
    while rdf <= rdf_max:
        rd = rd_min
        rdf_rd = int(rdf/rd) + 1
        while rd <= rd_max:
            if ((int(rdf/rd) + 1)  == rdf_rd  and rd != rd_min):
              rd += 1
              continue
            else:
              rdf_rd = int(rdf/rd) + 1
            tb = tb_min
            while tb <= tb_max:
                scenario_name = output_dir + "/" + "rdf-" + str(rdf) + "-rd-" + str(rd) + "-tb-" + str(tb)
                if (evaluate):
                    print "Evaluating mappings " + scenario_name
                    output = evaluateMapping("3", topology_files, scenario_name)
                    f = open(scenario_name + ".csv", 'w')
                    f.write(output)
                    f.close()
                else:
                    if not os.path.exists(scenario_name):
                        os.makedirs(scenario_name)
                    print "Generating mappings " + scenario_name
                    result = generateMapping("3", topology_files, scenario_name, rdf, tb, rdf_rd, "false")
                    if not result:
                        print "Failed to converge on scenario: " + scenario_name
                        shutil.rmtree(scenario_name)
                tb += 0.02
            rd += 1
        rdf += 8


def main():
    # parse the commandline arguments
    parser = argparse.ArgumentParser(description='Generate mapping files for topologies from Blobstore')
    parser.add_argument("-t", dest='topology_path', type=str, required=True, help='path for the topology files')
    parser.add_argument("-o", dest='output_dir', type=str, default="./", required=False, help='output location')

    parser.add_argument("-s", dest='single_mapping', action="store_true", default=False, required=False, help='calculate single map')
    parser.add_argument("-v", dest='algo_version', type=int, default=3, required=False, help='version of algorithm')
    parser.add_argument("-r", dest='rack_diversity', type=str, default="8", required=False, help='rack diversity')
    parser.add_argument("-c", dest='track_capacity', type=str, default="false", required=False, help='track replica capacity')
    parser.add_argument("-b", dest='target_balance', type=str, default="0.25", required=False, help='target balance')
    parser.add_argument("-d", dest='rdf', type=str, default="10", required=False, help='rdf')

    parser.add_argument("-g", dest='skip_generate', action="store_true", default=False, required=False, help='skip generating maps')
    parser.add_argument("-e", dest='skip_evaluate', action="store_true", default=False, required=False, help='skip evaluating maps')
    args = parser.parse_args()

    logging.basicConfig(filename="testblob.log", level=logging.INFO)

    # read topology files
    topology_files = glob.glob(args.topology_path + "/topology_*")

    if not args.skip_generate:
        print "Generating mappings..."
        if args.single_mapping:
            versions = [args.algo_version]
            generateMapping(versions, sorted(topology_files), args.output_dir, args.rdf, args.target_balance, args.rack_diversity, args.track_capacity)
        else:
            compareMappings(sorted(topology_files), False, args.output_dir)
    if not args.skip_evaluate:
        print "Evaluate mappings..."
        if args.single_mapping:
            versions = [args.algo_version]
            output = evaluateMapping(versions, sorted(topology_files), args.output_dir)
            f = open(args.output_dir + "/result.csv", 'w')
            f.write(output)
            f.close()
        else:
            compareMappings(sorted(topology_files), True)


if __name__ == '__main__':
    main()
