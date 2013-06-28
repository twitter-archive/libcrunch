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

package com.twitter.crunch.tools.jsontopology;

import com.twitter.crunch.Node;

public class Topology {
  protected volatile Node root;
  protected volatile long version;

  public Topology() {}

  public Topology(Node root, long version) {
    this.root = root;
    this.version = version;
  }

  public long getVersion() {
    return version;
  }

  public Node getRootNode() {
    return root;
  }
}
