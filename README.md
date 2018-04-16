<!--
{% comment %}
License ...
{% endcomment %}
-->

[![buildstatus](https://travis-ci.org/IBM/lagraph.svg?branch=master)](https://travis-ci.org/IBM/lagraph)
[![codecov.io](http://codecov.io/github/IBM/lagraph/coverage.svg?branch=master)](http://codecov.io/github/IBM/lagraph?branch=master)

# LAGraph

LAGraph is a Scala API that supports the implementation of scalable
graph algorithms using algebraic techniques.  Its distinguishing
characteristics are:

  1. **Support for both conventional and custom semirings**
  1. **Support for both primitive and custom types**
  1. **Multiple execution modes**, including Spark and pure Scala.
  1. **Automatic optimization** based on data and cluster characteristics to ensure both efficiency and scalability.


LAGraph is evolving, while several classical algorithms have been
implemented on top of LAGraph, much work still needs to be done.

## Coordinates

For Sbt:
```
resolvers ++= Seq(

   "sonatype-snaphots" at "https://oss.sonatype.org/content/repositories/snapshots"
   )

libraryDependencies += "com.github.ibm" %% "lagraph-core" % "0.1.0-SNAPSHOT"
```

For Maven:
```
<dependencies>
    <dependency>
        <groupId>com.ibm.github</groupId>
        <artifactId>lagraph-core</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </dependency>
</dependencies>

<repositories>
    <repository>
        <id>sonatype-snapshots</id>
        <name>Sonatype Public</name>
        <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

## Getting Started

To get start visit [**LAGraph Documentation**](https://ibm.github.io/lagraph/).


## Contributing

Interested in contributing. Please visit [Contributing](CONTRIBUTING.md)
