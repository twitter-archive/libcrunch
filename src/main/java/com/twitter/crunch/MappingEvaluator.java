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

import java.util.List;
import java.util.Map;

public class MappingEvaluator {

    public static double getMean(List<Double> distribution) {
        double sum = 0;
        for(double a: distribution)
            sum += a;

        return sum/distribution.size();
    }

    public static double getStandardDeviation(List<Double> distribution)
    {
        double mean = getMean(distribution);

        double temp = 0;
        for(double a: distribution)
            temp += (mean-a)*(mean-a);

        return Math.sqrt(temp/distribution.size());
    }

    public static double getWeightedMean(Map<String, Long> distribution, Map<String, Long> weight) {
        assert(distribution.size() == weight.size());
        long sum1 = 0;
        long sum2 = 0;

        for (String node : distribution.keySet()) {
            sum1 += distribution.get(node) * weight.get(node);
            sum2 += weight.get(node);
        }

        return sum1/sum2;
    }

    public static double getWeightedStandardDeviation(Map<String, Long> distribution, Map<String, Long> weight){
        assert(distribution.size() == weight.size());
        double mean = getWeightedMean(distribution, weight);
        double sum1 = 0;
        double sum2 = 0;
        int m = 0;

        for (String node : distribution.keySet()) {
            sum1 += weight.get(node) * Math.pow(distribution.get(node) - mean, 2);
        }

        for (String node : weight.keySet()) {
            sum2 += weight.get(node);
            if (weight.get(node) != 0) m++;
        }

        return Math.sqrt(sum1/((m - 1)*sum2/m));
    }
}
