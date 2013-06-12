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

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple implementation of selection based on consistent hashing.
 */
class ConsistentHashingSelector implements Selector {
  public static final long DEFAULT_MAX_TOKENS_PER_NODE = 500;

  private final MessageDigest md;
  private final List<Long> tokenList;
  private final Map<Long,Node> tokenMap;

  public ConsistentHashingSelector(Node node) {
    if (node.isLeaf()) {
      throw new IllegalArgumentException();
    }
    try {
      md = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException ignore) {
      throw new IllegalArgumentException(ignore);
    }

    final long maxTokensPerNode = DEFAULT_MAX_TOKENS_PER_NODE;
//    long totalWeight = 0;
    long maxWeight = 0;
    List<Node> nodes = node.getChildren();
    for (Node n: nodes) {
//      totalWeight += n.getWeight();
      maxWeight = Math.max(n.getWeight(), maxWeight);
    }

    MessageDigest ringMd;
    try {
      ringMd = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException ignore) {
      throw new IllegalArgumentException(ignore);
    }
    tokenMap = new HashMap<Long,Node>();
    for (Node n: nodes) {
      long tokenCount = maxTokensPerNode*n.getWeight()/maxWeight;
      byte[] h = null;
      for (int i = 0; i < tokenCount; i++) {
        byte[] input = (h == null) ? n.getName().getBytes() : h;
        h = ringMd.digest(input);
        long token = Utils.bstrTo32bit(h);
        if (!tokenMap.containsKey(token)) {
          tokenMap.put(token, n);
        }
      }
    }

    tokenList = new ArrayList<Long>(tokenMap.keySet());
    Collections.sort(tokenList);
  }

  public Node select(long input, long round) {
    byte[] b = longToBytes(input, round);
    byte[] h = md.digest(b);
    long token = Utils.bstrTo32bit(h);
    return tokenMap.get(findSuccessorToken(token));
  }

  private byte[] longToBytes(long a, long b) {
    ByteBuffer buf = ByteBuffer.allocate(8*2).putLong(a).putLong(b);
    return buf.array();
  }

  private long findSuccessorToken(long token) {
    int i = Collections.binarySearch(tokenList, token);
    if (i < 0) {
      i = -1 - i;
    }
    // [sjlee] why?
    if (i == tokenList.size()) {
      i = 0;
    }
    return tokenList.get(i);
  }
}
