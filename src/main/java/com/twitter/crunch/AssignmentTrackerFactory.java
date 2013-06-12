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
 * Factory class that provides a single static factory method to create an assignment tracker
 * instance.
 */
class AssignmentTrackerFactory {
  /**
   * Factory method that creates an assignment tracker instance. If target balance is not a positive
   * number, a no-op instance will be returned.
   *
   * @param rootNode the root node under which nodes will have assignments tracked
   * @param dataSize the size of the data objects; this is used to come up with the mean and max
   * assignments
   * @param targetBalance the expected target balance in relative percentages; e.g. 0.3 (30%). It
   * means that this target will be used to control and curb over-assignment to nodes. Note that
   * this is a target, and some small over-assignment may still occur if it becomes difficult to
   * meet this target. Must be positive.
   * @return newly created assignment tracker instance
   */
  public static AssignmentTracker create(Node rootNode, int dataSize, double targetBalance) {
    if (rootNode != null && dataSize > 0 && targetBalance > 0.0d) {
      return new AssignmentTrackerImpl(rootNode, dataSize, targetBalance);
    }
    return new NoOpAssignmentTracker(); // do not track
  }
}
