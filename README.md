# libcrunch
Libcrunch is a lightweight mapping framework that maps data objects to a number of nodes, subject to user-specified constraints.

The libcrunch implementation was heavily inspired by the paper on the [CRUSH algorithm](http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf).

## Features
* flexible cluster topology definition
* define your placement rules
* supports replication factor (RF) and replica distribution factor (RDF)
* balanced distribution of data that reflects weights
* stability against topology changes
* supports target balancing

## Getting Started
The latest libcrunch artifacts are published to maven central. You can include libcrunch in your project by adding the following to your maven pom.xml file:

```xml
  <dependencies>
    <dependency>
      <groupId>com.twitter</groupId>
      <artifactId>libcrunch</artifactId>
      <version>1.0.0</version>
    </dependency>
  </dependencies>
```

### Quickstart
Creating and using the libcrunch mapping function is pretty straightforward. Once you define your data and the inputs to the mapping function, you get the mapping result via the computeMapping method. For example, to use the RDF mapping,

```java
// set up the input to the mapping function
PlacementRules rules = createPlacementRules();

// instantiate the mapping function
MappingFunction mappingFunction = new RDFMapping(rdf, rf, rules, targetBalance);

// prepare your data
List<Long> data = prepareYourDataIds();
// set up the topology
Node root = createTopology();

// compute the mapping
Map<Long,List<Node>> mapping = mappingFunction.computeMapping(data, root);
```

## Problems?

If you find any issues please [report them](https://github.com/twitter/libcrunch/issues) or better,
send a [pull request](https://github.com/twitter/libcrunch/pulls).

## Authors:
* Jerry Xu
* Peter Schuller
* Sangjin Lee

## License
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
