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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class AssignmentTrackerImplTest {
  @Test
  public void testLowWatermark() {
    final int childType = 3;
    Node child = mockChildNode(childType);
    Node root = mockRootNode(childType, child);
    AssignmentTracker tracker =
        new AssignmentTrackerImpl(root, (int)(AssignmentTrackerImpl.LOW_WATERMARK-1), 0.25d);
    assertFalse(tracker.trackAssignment(child));
  }

  private Node mockRootNode(final int childType, Node child) {
    Node root = mock(Node.class);
    List<Node> children = new ArrayList<Node>();
    children.add(child);
    when(root.findChildren(childType)).thenReturn(children);
    return root;
  }

  private Node mockChildNode(final int childType) {
    Node child = mock(Node.class);
    when(child.getType()).thenReturn(childType);
    when(child.getWeight()).thenReturn(65536L);
    when(child.isLeaf()).thenReturn(true);
    return child;
  }

  @Test
  public void testDifferenceThreshold() {
    final int childType = 3;
    Node child = mockChildNode(childType);
    Node root = mockRootNode(childType, child);
    final int dataCount = 100;
    final double maxAllowed = ((double)(AssignmentTrackerImpl.DIFFERENCE_THRESHOLD - 1))/dataCount;
    assertTrue(dataCount*maxAllowed < AssignmentTrackerImpl.DIFFERENCE_THRESHOLD);
    AssignmentTracker tracker = new AssignmentTrackerImpl(root, dataCount, maxAllowed);
    assertFalse(tracker.trackAssignment(child));
  }

  @Test
  public void testRejectAssignment() {
    final int childType = 3;
    Node child = mockChildNode(childType);
    Node root = mockRootNode(childType, child);
    final int dataCount = 50;
    final double maxAllowed = 0.25d;
    final int max = (int)Math.ceil(dataCount*(1.0d + maxAllowed));
    AssignmentTracker tracker = new AssignmentTrackerImpl(root, dataCount, maxAllowed);
    // no assignment: should not be rejected
    assertFalse(tracker.rejectAssignment(child));

    // fill it up to max
    for (int i = 0; i < max; i++) {
      assertTrue(tracker.trackAssignment(child));
    }
    assertTrue(tracker.rejectAssignment(child));
  }
}
