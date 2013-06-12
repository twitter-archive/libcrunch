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
 * Type system that is based on racks. The first three types are defined as ROOT, DATA_CENTER, and
 * RACK. It can be extended to describe more layers of topologies. It is used by
 * {@link BaseRackIsolationPlacementRules}.
 *
 * @see BaseRackIsolationPlacementRules
 *
 */
public interface RackBasedTypes extends Types {
  int RACK = 2;
}
