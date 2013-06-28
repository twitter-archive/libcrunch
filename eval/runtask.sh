#!/bin/bash

java -Dfile.encoding=UTF-8 -Xms64m -Xmx512m -classpath \
../target/test-classes:\
../target/classes:\
$HOME/.m2/repository/com/google/guava/guava/12.0.1/guava-12.0.1.jar:\
$HOME/.m2/repository/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar:\
$HOME/.m2/repository/junit/junit/4.10/junit-4.10.jar:\
$HOME/.m2/repository/org/hamcrest/hamcrest-core/1.1/hamcrest-core-1.1.jar:\
$HOME/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.9.4/jackson-mapper-asl-1.9.4.jar:\
$HOME/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.9.4/jackson-core-asl-1.9.4.jar:\
$HOME/.m2/repository/log4j/log4j/1.2.16/log4j-1.2.16.jar:\
$HOME/.m2/repository/org/slf4j/slf4j-api/1.6.4/slf4j-api-1.6.4.jar:\
$HOME/.m2/repository/org/mockito/mockito-core/1.9.0/mockito-core-1.9.0.jar:\
$HOME/.m2/repository/org/objenesis/objenesis/1.0/objenesis-1.0.jar:\
$HOME/.m2/repository/ch/qos/logback/logback-core/1.0.1/logback-core-1.0.1.jar:\
$HOME/.m2/repository/org/yaml/snakeyaml/1.10/snakeyaml-1.10.jar:\
$HOME/.m2/repository/ch/qos/logback/logback-classic/1.0.1/logback-classic-1.0.1.jar \
com.twitter.crunch.tools.$@
