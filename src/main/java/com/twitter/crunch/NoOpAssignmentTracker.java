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
 * Implementation of the assignment tracker that simply does not track. It is used when assignment
 * tracking is disabled (i.e. target balance is not used).
 */
class NoOpAssignmentTracker implements AssignmentTracker {
  /**
   * No tracking.
   */
  public boolean trackAssignment(Node node) {
    return false;
  }

  /**
   * No rejection.
   */
  public boolean rejectAssignment(Node node) {
    return false;
  }
}
