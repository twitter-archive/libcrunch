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

import java.util.*;

public class CalculateMovement {


    private static void calTopologyChange(Map<Long, List<String>> before, Map<Long, List<String>> after) {
        int moved = 0;

        for (Long bucket: before.keySet()) {
            List<String> beforeMap = before.get(bucket);
            List<String> afterMap = after.get(bucket);
            for (String node: beforeMap) {
                if (!afterMap.contains(node))  moved++;
            }
        }

        System.out.print(String.format("%d", moved));
    }

    public static void  main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: old_map_filename new_map_filename");
            System.out.println("  moved");
            return;
        }

        String before = args[0];
        String after = args[1];

        calTopologyChange(Utils.importMap(before), Utils.importMap(after));
    }
}
