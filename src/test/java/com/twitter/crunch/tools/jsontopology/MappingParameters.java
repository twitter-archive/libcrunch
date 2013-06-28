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

package com.twitter.crunch.tools.jsontopology;

public class MappingParameters {
  // keys for mapping parameters
  public static final String RF = "rf";
  public static final String RDF = "rdf";
  public static final String TARGET_BALANCE = "target_balance";
  public static final String VIRTUAL_BUCKET_COUNT = "virtual_bucket_count";
  public static final String USE_CRUSH_MAPPING = "use_crush_mapping";

  private volatile int rf;
  private volatile int rdf;
  private volatile double targetBalance;
  private volatile int virtualBucketCount;
  private volatile boolean useCrushMapping;

  public MappingParameters() {}

  public MappingParameters(MappingParameters params) {
    this.rf = params.rf;
    this.rdf = params.rdf;
    this.targetBalance = params.targetBalance;
    this.virtualBucketCount = params.virtualBucketCount;
    this.useCrushMapping = params.useCrushMapping;
  }

  public int getRf() {
    return rf;
  }

  public void setRf(int rf) {
    this.rf = rf;
  }

  public int getRdf() {
    return rdf;
  }

  public void setRdf(int rdf) {
    this.rdf = rdf;
  }

  public double getTargetBalance() {
    return targetBalance;
  }

  public void setTargetBalance(double targetBalance) {
    this.targetBalance = targetBalance;
  }

  public int getVirtualBucketCount() {
    return virtualBucketCount;
  }

  public void setVirtualBucketCount(int virtualBucketCount) {
    this.virtualBucketCount = virtualBucketCount;
  }

  public boolean isUseCrushMapping() {
    return useCrushMapping;
  }

  public void setUseCrushMapping(boolean useCrushMapping) {
    this.useCrushMapping = useCrushMapping;
  }

  @Override
  public String toString() {
    return "(" + RF + "=" + rf + ", " + RDF + "=" + rdf + ", " + TARGET_BALANCE + "=" +
      targetBalance + ", " + VIRTUAL_BUCKET_COUNT + "=" + virtualBucketCount + ", " +
      USE_CRUSH_MAPPING + "=" + useCrushMapping + ")";
  }
}
