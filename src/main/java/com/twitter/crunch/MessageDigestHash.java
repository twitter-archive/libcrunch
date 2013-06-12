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

public class MessageDigestHash implements MultiInputHash {
  private final String algorithm;
  private final MessageDigest md;

  public MessageDigestHash(String algorithm) {
    this.algorithm = algorithm;
    try {
      md = MessageDigest.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException("invalid algorithm passed in", e);
    }
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public long hash(long a) {
    ByteBuffer buf = ByteBuffer.allocate(8).putLong(a);
    return hashFromBuffer(buf);
  }

  public long hash(long a, long b) {
    ByteBuffer buf = ByteBuffer.allocate(8*2).
        putLong(a).putLong(b);
    return hashFromBuffer(buf);
  }

  public long hash(long a, long b, long c) {
    ByteBuffer buf = ByteBuffer.allocate(8*3).
        putLong(a).putLong(b).putLong(c);
    return hashFromBuffer(buf);
  }

  private long hashFromBuffer(ByteBuffer buf) {
    byte[] result = md.digest(buf.array());
    return Utils.bstrTo32bit(result);
  }
}
