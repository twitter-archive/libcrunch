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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracker that keeps track of data assignment during the course of mapping generation, and rejects
 * assignments based on the target balance parameter.
 * <br/>
 * It is important to note that this keeps track of the assignment status, and therefore is
 * stateful. One object needs to be created and retained for the duration of the mapping generation.
 */
class AssignmentTrackerImpl implements AssignmentTracker {
  /**
   * If the expected mean assignment is too small (and consequently so is the target max assignment)
   * we do not track or enforce assignments. This constant is that threshold.
   */
  public static final long LOW_WATERMARK = 5L;
  /**
   * If the difference between the mean and max is too small, it will lead to a number of mapping
   * problems. If the specified max is closer to the mean than this threshold, assignment tracking
   * is disabled.
   */
  public static final long DIFFERENCE_THRESHOLD = 3L;
  private static final Logger logger = LoggerFactory.getLogger(AssignmentTrackerImpl.class);

  private final Node rootNode;
  private final int dataSize;
  private final double targetBalance;
  private final Map<Integer,Assignment> assignments;

  AssignmentTrackerImpl(Node rootNode, int dataSize, double targetBalance) {
    if (rootNode == null) {
      throw new IllegalArgumentException("null root node was passed");
    }
    if (dataSize <= 0) {
      throw new IllegalArgumentException("non-positive data size");
    }
    if (targetBalance <= 0.0d) {
      throw new IllegalArgumentException("non-positive target balance ratio");
    }
    this.rootNode = rootNode;
    this.dataSize = dataSize;
    this.targetBalance = targetBalance;
    assignments = new HashMap<Integer,Assignment>();
  }

  /**
   * Tracks assignment of this particular node. Assignment tracking happens essentially with the
   * leaf nodes. When a leaf node is positively selected, the assignment of the leaf node is
   * recorded, and any parent node whose type assignment is being tracked for is also tracked at
   * that point.
   *
   * @return whether the particular node is tracked directly.
   */
  public boolean trackAssignment(Node node) {
    int type = node.getType();
    Assignment assignment = getAssignment(type);
    boolean tracked = false;
    if (node.isLeaf()) {
      tracked = assignment.addCount(node);

      // parents' assignment is tracked along with the leaf node itself: this helps tracking
      // different types of nodes in an encapsulated manner
      // recursively check parents' assignment
      for (Map.Entry<Integer,Assignment> e: assignments.entrySet()) {
        int parentType = e.getKey();
        if (parentType != type) {
          Node parent = node.findParent(parentType);
          if (parent != null) {
            Assignment parentAssignment = e.getValue();
            parentAssignment.addCount(parent);
          }
        }
      }
    }
    return tracked;
  }

  /**
   * Returns whether the node should be rejected due to high assignment against the target balance.
   * The determination of whether to reject it is a function of the current data assignment level of
   * the node. The exact nature of how the selection is rejected is an implementation detail. The
   * only guaranteed behavior is the node will be rejected 100% of the time if it reaches the
   * assignment level specified by the target balance.
   *
   */
  public boolean rejectAssignment(Node node) {
    int type = node.getType();
    Assignment assignment = getAssignment(type);
    return assignment.reject(node);
  }

  /**
   * Lazily creates or gets the assignment object for the given type.
   */
  private Assignment getAssignment(int type) {
    Assignment assignment = assignments.get(type);
    if (assignment == null) {
      // get all the right nodes directly from the root node
      List<Node> nodes = rootNode.findChildren(type);
      assignment = new Assignment(nodes);
      assignments.put(type, assignment);
    }
    return assignment;
  }

  private class Assignment {
    private final Map<Node,NodeStats> assignments;

    public Assignment(List<Node> nodes) {
      // initialize assignment data for all nodes of given type
      assignments = new HashMap<Node,NodeStats>((int)1.5f*nodes.size());
      // process the set of nodes and populate the right mean-max values
      // get the sum of weights
      long sum = 0;
      for (Node node: nodes) {
        // failed leaf nodes should be excluded from the sum
        if (!node.isLeaf() || !node.isFailed()) {
          sum += node.getWeight();
        }
      }

      for (Node node: nodes) {
        long mean;
        if (sum == 0L) {
          // unlikely situations where sum is zero
          mean = 0L;
        } else if (node.isLeaf() && node.isFailed()) {
          // if the node has failed, no need to track its assignment
          mean = 0L;
        } else { // ordinary node
          mean = node.getWeight()*dataSize/sum;
        }

        NodeStats data;
        if (mean < LOW_WATERMARK) {
          // disable assignment tracking and enforcement if the mean is below the low watermark
          logger.debug("the mean ({}) for node {} is below the low watermark; assignment will " +
              "not be tracked", mean, node.getName());
          data = DisabledStats.INSTANCE;
        } else {
          logger.trace("mean assignments for node {}: {}", node.getName(), mean);
          // rounded up to the nearest long
          long max = (long)Math.ceil((1.0d + targetBalance)*mean);
          logger.trace("maximum allowed assignments for node {}: {}", node.getName(), max);
          if (max - mean < DIFFERENCE_THRESHOLD) {
            logger.debug("the difference between max and mean for node {} is below the " +
                "threshold; assignment will not be tracked", node.getName());
            data = DisabledStats.INSTANCE;
          } else {
            data = new NormalNodeStats(mean, max);
          }
        }
        // add it to the map
        assignments.put(node, data);
      }
    }

    /**
     * A simple rejection scheme that rejects assignment when the node reaches the targeted maximum
     * assignment.
     */
    public boolean reject(Node node) {
      NodeStats data = assignments.get(node);
      if (data == null || data.isDisabled()) {
        return false;
      }

      // if the assignment already reached max, it's rejected
      long max = data.getMax();
      long current = data.getCount();
      return current >= max;
    }

    public boolean addCount(Node node) {
      NodeStats data = assignments.get(node);
      if (data != null && !data.isDisabled()) {
        return data.addCount();
      }
      return false;
    }
  }

  private static interface NodeStats {
    boolean addCount();
    long getCount();
    long getMax();
    boolean isDisabled();
  }

  private static class DisabledStats implements NodeStats {
    public static final NodeStats INSTANCE = new DisabledStats();

    private DisabledStats() {}

    public boolean isDisabled() {
      return true;
    }

    public boolean addCount() {
      throw new UnsupportedOperationException("stats are disabled");
    }

    public long getCount() {
      throw new UnsupportedOperationException("stats are disabled");
    }

    public long getMax() {
      throw new UnsupportedOperationException("stats are disabled");
    }

    @Override
    public String toString() {
      return "(stats disabled)";
    }
  }

  private static class NormalNodeStats implements NodeStats {
    private final long mean;
    private final long max;
    private final AtomicLong count;

    public NormalNodeStats(long mean, long max) {
      this.mean = mean;
      this.max = max;
      this.count = new AtomicLong(0L);
    }

    public boolean isDisabled() {
      return false;
    }

    public long getMax() {
      return max;
    }

    public boolean addCount() {
      count.incrementAndGet();
      return true;
    }

    public long getCount() {
      return count.get();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("(count=").append(count.get()).append(", mean=").append(mean).
          append(", max=").append(max).append(")");
      return sb.toString();
    }
  }
}
