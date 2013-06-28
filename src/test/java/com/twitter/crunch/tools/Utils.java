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

public class Utils {
    public static byte[] slurp(InputStream in) throws IOException {
        byte[] buf = new byte[Math.max(in.available(), 4096)];

        int sofar = 0;
        while (true) {
            if (sofar == buf.length) {
                byte[] tmp = new byte[buf.length + 2];
                System.arraycopy(buf, 0, tmp, 0, buf.length);
                buf = tmp;
            }
            int read = in.read(buf, sofar, buf.length - sofar);
            if (read == -1) {
                byte[] ret = new byte[sofar];
                System.arraycopy(buf, 0, ret, 0, sofar);
                return ret;
            }
            sofar += read;
        }
    }

    public static Map<Long, List<String>> importMap(String fileName) throws IOException {
        Scanner scanner = new Scanner(new FileInputStream(fileName), "UTF-8");
        Map<Long, List<String>> map = new HashMap<Long, List<String>>();
        try {
            while (scanner.hasNextLine()){
                String line = scanner.nextLine();
                if (line.contains(",")) {
                    String[] parts = line.split(",");
                    Long name = Long.parseLong(parts[0]);
                    String[] replicas = Arrays.copyOfRange(parts, 1, parts.length);
                    map.put(name, Arrays.asList(replicas));
                }
            }
        }finally {
            scanner.close();
        }

        return map;
    }

    public static void exportRDFMap(String fileName, Map<String, List<String>> map) throws IOException {

        Writer out = new OutputStreamWriter(new FileOutputStream(fileName), "UTF-8");
        try {
            for (String node: map.keySet()) {
                out.append(node);
                List<String> replicas = map.get(node);
                for (String replica: replicas) {
                    out.append(',');
                    out.append(replica);
                }
                out.append('\n');
            }
        } finally {
            out.flush();
            out.close();
        }
    }

    public static Map<String, List<String>> importRDFMap(String fileName) throws IOException {

        Scanner scanner = new Scanner(new FileInputStream(fileName), "UTF-8");
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        try {
            while (scanner.hasNextLine()){
                String line = scanner.nextLine();
                if (line.contains(",")) {
                    String[] parts = line.split(",");
                    String name = parts[0];
                    String[] replicas = Arrays.copyOfRange(parts, 1, parts.length);
                    map.put(name, Arrays.asList(replicas));
                }
            }
        }finally {
            scanner.close();
        }

        return map;
    }
}
