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
import csv
import re


def evaluateMappings(result_files, start_point, print_count, output_filename):
    output = ""
    for result_file in result_files:
        m = re.match(r'.*/rdf-(.*)-rd-(.*)-tb-(.*)\.csv', result_file)
        if not m:
            print "Cannot parse " + result_file

        with open(result_file, 'r') as result:
            result_reader = csv.reader(result, delimiter=',')
            skip = 0
            while skip < start_point:
                result_reader.next()
                skip += 1
            moves = 0
            std = 0
            c = 0
            for row in result_reader:
                moves += int(row[5])
                std += float(row[3])
                c += 1
            if print_count:
                print c

            output += m.group(1) + "," + m.group(2) + "," + m.group(3) + "," + str(moves) + "," + str(std) + "\n"

    f = open(output_filename, 'w')
    f.write(output)
    f.close()


def main():
    # parse the commandline arguments
    parser = argparse.ArgumentParser(description='Evaluate mapping files for topologies from Blobstore')
    parser.add_argument("-t", dest='result_path', type=str, required=True, help='path for the result files')
    parser.add_argument("-o", dest='output_filename', type=str, required=True, help='output file name')
    parser.add_argument("-s", dest='start_point', type=int, required=False, default=1, help='starting point for the calculation')
    parser.add_argument("-c", dest='print_count', action="store_true", required=False, default=False, help='print count')

    args = parser.parse_args()

    # read topology files
    result_files = glob.glob(args.result_path + "/*.csv")

    evaluateMappings(sorted(result_files), args.start_point, args.print_count, args.output_filename)


if __name__ == '__main__':
    main()
