---
layout: global
title: LAGraph
displayTitle: LAGraph
description: LAGraph
---
<!--
{% comment %}
License ...
{% endcomment %}
-->

[//]: LAGraph is now a **SparkPackages** project.
[//]: Hopefully, LAGraph will soon be a **SparkPackages** project.

LAGraph is (actually, will soon be) a [**SparkPackages**
project](https://spark-packages.org/?q=tags%3A%22Graph%22) that is
being used to investigate algebraicly-expressed graph algorithms
implemented using an interface that exploits functional programming
techniques and uses a cluster to achieve scale and performance.
The LAGraph interface is written in Scala and has implementations for
both pure-Scala and Spark environments. Scala permits a elegant
expression of the semantics of semirings and Spark provides a
distributed compute platform to achieve scale and performance.
The proposed interface provides a concise, high-level abstraction that
spares the user from the details of the underlying implementation of
the linear algebra operations.  This decoupling permits the
implementation to focus on performance and scalability.
The decoupling also allows issues to be partitioned into those
involving interface and those involving implementation.
Based on user feedback from our [Graph Algorithms Building Blocks
(GABB’2017)](http://graphanalysis.org/workshop2017.html) workshop we
are publishing an early release of LAGraph to facilitate a review of
goals and issues pertaining to the interface.

## Interface Goals / Issues

The primary goal of the interface are the standards of "minimal yet complete".
To evaluate the interface we've [implemented a number of standard graph algorithms](algorithms-reference.md). But to establish minimality and completeness more algorithms need to be implemented.

## Execution Goals / Issues

The implementation is a novel adaptation of the Scalable Universal
Matrix Multiplication Algorithm (SUMMA) to Spark RDD semantics. The
implementation is currently performant when compared to GraphX.
However, to improve performance, it needs to be updated from RDDs to a
Spark Datasets. Also, we are actively looking to exploit intra-task
concurrency afforded by specific computer architectures. Neither of
these execution issues should affect the interface.


[//]: LAGraph is still evolving and several challenges remain before it is ready for prime time.

[//]: 1. **Minimal yet complete**: while several
[//]: classical algorithms have been implemented on top of LAGraph, the
[//]: completeness of interface has not been established and the list
[//]: of supported algorithms needs to expand.
[//]: 1. **Instance-specific interface** E.g., `hc.mTv(semiRing, lagMatrix, lagVector)` will become `lagMatrix.Tv(semiRing, lagVector)`
[//]: 1. **Unit test**: coverage needs improvement


[//]: The [**LAGraph GitHub README**](https://github.ibm.com/hornwp/lagraph) describes
[//]: building, testing, and running LAGraph. Please read [**Contributing to LAGraph**](contributing-to-lagraph)
[//]: to find out how to help improve LAGraph.

[//]: To download LAGraph,
[//]: Eventually, to download LAGraph,
[//]: visit the [LAGraph SparkPackages homepage](https://spark-packages.org/?q=tags%3A%22Graph%22).
[//]: But, for now, download LAGraph from [pokgsa](http://pokgsa.ibm.com/projects/s/spp/public/lagraph/lagraph_2.11-0.1.0-SNAPSHOT.jar).


## Quick Start

This tutorial provides a quick introdution to how LAGraph can be used
to implement a linear algebra-based version of the
[Bellman Ford algorithm](https://en.wikipedia.org/wiki/Bellman%E2%80%93Ford_algorithm):

1. If you are not familiar with linear algebra-based formulations
   (e.g., semiring-based formulations) of graph algrothims read
   [Graphs and Big Data](graphs-overview).

1. To understand how to express semirings using LAGraph read
   [Programming with Semirings](programming-with-semirings).

1. Read the [algebraic formulation of the Bellman-Ford algorithm](algorithms-bellmanford).

1. [Install Zeppelin](http://zeppelin.apache.org/docs/0.7.1/install/install.html#quick-start),
   download the LAGraph jar, and start the zeppelin daemon.

   E.g., on linux (assuming you have java with a version >=1.7):

   ```bash
   # e.g., work from your home directory
   cd ~
   # download zeppelin and install
   wget http://mirror.cc.columbia.edu/pub/software/apache/zeppelin/zeppelin-0.7.1/zeppelin-0.7.1-bin-all.tgz
   tar -zf zeppelin-0.7.1-bin-all.tgz
   cd zeppelin-0.7.1-bin-all
   # get the LAGraph jar
   wget http://pokgsa.ibm.com/projects/s/spp/public/lagraph/lagraph_2.11-0.1.0-SNAPSHOT.jar
   # get the Zepplin 'lagraph-tutorial' notebook
   wget http://pokgsa.ibm.com/projects/s/spp/public/lagraph/lagraph-tutorial.json
   # start the Zeppelin daemon
   bin/zeppelin-daemon.sh start
   # when ur done playing w/ the tutorial don't forget to stop the daemon
   # bin/zeppelin-daemon.sh stop
   ```

   E.g., on mac OS

   ```bash
   # e.g., work from your home directory
   cd ~
   # if you don't already have brew (http://brew.sh)
   /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
   # if you don't already have wget
   brew install wget
   # if you don't already have java
   brew cask install java
   # if you don't already have Zeppelin
   brew install apache-zeppelin
   # get the LAGraph jar
   wget http://pokgsa.ibm.com/projects/s/spp/public/lagraph/lagraph_2.11-0.1.0-SNAPSHOT.jar
   # get the Zepplin 'lagraph-tutorial' notebook
   wget http://pokgsa.ibm.com/projects/s/spp/public/lagraph/lagraph-tutorial.json
   # start the Zeppelin daemon
   zeppelin-daemon.sh start
   # when ur done playing w/ the tutorial don't forget to stop the daemon
   # zeppelin-daemon.sh stop
   ```

1. Open a browser, go to URL `http://localhost:8080`, select the
   "Zeppelin" icon in the upper left hand corner, select "Import
   Note", select "Choose a JSON here", select the
   'lagraph-tutorial.json' file that you downloaded in the previous
   step, and select "open".

1. From Zeppelin, use the "Notebook" pull down on the menu bar to select the
   'lagraph-tutorial' notebook.

1. Follow the instructions in the notebook.

## Site Contents

### Graphs

* [**Graphs and Big Data**](graphs-overview) - Overview of LAGraph

* [**Programming with Semirings**](programming-with-semirings) - Semirings in Scala

* [**LagContext Programming Guide**](spark-lagcontext-programming-guide) - Running LAGraph

* [**Algorithms**](spark-lagcontext-programming-guide) - LAGraph Algorithms Reference

### Other

* [**Troubleshooting Guide**](troubleshooting-guide) - Troubleshooting Guide

* [**Developer Guide**](lagraph-dev-guide) - Developer Guide

* [**Contributing to LAGraph**](contributing-to-lagraph) - Contributing to LAGraph

* [**Release Process**](release-process) - Release Process

