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

/**
 * Object that encapsulates the algorithm (or "bucket type" in CRUSH terms) that, given a node,
 * selects from its immediate children. On instantiation, it may calculate certain properties and
 * attributes specific to the selection algorithm but independent of the data input, and maintain
 * that state. Those properties will be used as part of the selection input.
 */
public interface Selector {
  /**
   * Selects one node based on the input value and an additional round value.
   */
  Node select(long input, long round);
}
