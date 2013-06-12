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
 * Tracker that keeps track of data assignment during the course of mapping generation, and rejects
 * assignments based on the target balance parameter.
 * <br/>
 * It is important to note that this keeps track of the assignment status, and therefore is
 * stateful. One object needs to be created and retained for the duration of the mapping generation.
 */
interface AssignmentTracker {
  /**
   * Tracks assignment of this particular node. Assignment tracking happens essentially with the
   * leaf nodes. When a leaf node is positively selected, the assignment of the leaf node is
   * recorded, and any parent node whose type assignment is being tracked for is also tracked at
   * that point.
   *
   * @return whether the particular node is tracked directly.
   */
  boolean trackAssignment(Node node);

  /**
   * Returns whether the node should be rejected due to high assignment against the target balance.
   * The determination of whether to reject it is a function of the current data assignment level of
   * the node. The exact nature of how the selection is rejected is an implementation detail. The
   * only guaranteed behavior is the node will be rejected 100% of the time if it reaches the
   * assignment level specified by the target balance.
   *
   */
  boolean rejectAssignment(Node node);
}
