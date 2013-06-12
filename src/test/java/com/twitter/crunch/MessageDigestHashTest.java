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

import org.junit.Test;

public class MessageDigestHashTest {
  @Test
  public void testHashLongLongLong() {
    long a = 1;
    long b = 2;
    long c = 3;
    MultiInputHash hf = new MessageDigestHash("SHA-1");
    long val = hf.hash(a, b, c);
    System.out.println(val);
    a = 2; b = 2; c = 3;
    val = hf.hash(a, b, c);
    System.out.println(val);
  }

}
