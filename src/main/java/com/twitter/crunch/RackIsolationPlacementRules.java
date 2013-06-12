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
 * Rack isolation placement rules based on storage system types with disks as end nodes.
 * <br/>
 * This is provided as a typical concrete implementation for certain types of topologies. One should
 * define their own types and extend the {@link BaskRackIsolationPlacementRules} to suit their
 * needs.
 *
 * @see StorageSystemTypes
 */
public class RackIsolationPlacementRules extends BaseRackIsolationPlacementRules
    implements StorageSystemTypes {
  public int getEndNodeType() {
    return DISK;
  }
}
