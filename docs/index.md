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

__Please note that LAGraph is still evolving and several challenges
remain before it is ready for production applications.__

LAGraph is a Scala API for implementing graph algorithms expressed in
the language of linear algebra.  The interface is written in Scala
using functional programming techniques and has implementations for
both pure-Scala and Spark environments.  Scala permits an elegant
expression of the semantics of semirings and Spark provides a
distributed compute platform to achieve scale and performance.  The
interface provides a concise, high-level abstraction that hides the
user from the details of the underlying implementation of the linear
algebra operations.  This decoupling permits the user to focus on
their algorithm and the implementation to focus on performance and
scalability.

Based on user feedback from our [Graph
Algorithms Building Blocks
(GABB’2017)](http://graphanalysis.org/workshop2017.html) workshop we
are publishing an early release of LAGraph to facilitate a review of
goals and issues pertaining to both the interface and its
implementation.

## Issues

1. **Minimal yet complete interface** The primary goal of the interface are the standards of "minimal yet
complete".  To evaluate the interface we've [implemented a number of
standard graph algorithms](algorithms-reference.md). But to establish
minimality and completeness more algorithms need to be implemented.
1. **Instance-specific interface** E.g., `hc.mTv(semiRing, lagMatrix, lagVector)` will become `lagMatrix.Tv(semiRing, lagVector)`
1. **Datasets**: The implementation is based on a novel adaptation of
the Scalable Universal Matrix Multiplication Algorithm (SUMMA) to
Spark RDD semantics. The implementation is currently performant when
compared to GraphX.  However, there may be opportunities to reduce
serialization costs (e.g., using Spark Datasets?).
1. **Intra-task concurrency**: exploit intra-task concurrency afforded by specific computer architectures.
1. **Unit test**: coverage needs improvement


## Quick Start

This tutorial provides a quick introduction to how LAGraph can be used
to implement a linear algebra-based version of the
[Bellman Ford algorithm](https://en.wikipedia.org/wiki/Bellman%E2%80%93Ford_algorithm):

1. If you are not familiar with linear algebra-based formulations
   (e.g., semiring-based formulations) of graph algorithms read
   [Graphs and Big Data](graphs-overview).

1. To understand how to express semirings using LAGraph read
   [Programming with Semirings](programming-with-semirings).

1. Read the [algebraic formulation of the Bellman-Ford algorithm](algorithms-bellmanford).

1. [Install Zeppelin](http://zeppelin.apache.org/docs/0.7.3/install/install.html#quick-start),
   download the LAGraph jar, and start the zeppelin daemon.

   E.g., on linux (assuming you have java with a version >=1.7):

   ```bash
   # e.g., work from your home directory
   cd ~
   # download zeppelin and install
   wget http://mirror.cc.columbia.edu/pub/software/apache/zeppelin/zeppelin-0.7.3/zeppelin-0.7.3-bin-all.tgz
   tar -zf zeppelin-0.7.3-bin-all.tgz
   cd zeppelin-0.7.3-bin-all

   # start the Zeppelin daemon
   bin/zeppelin-daemon.sh start
   # when ur done playing w/ the tutorial don't forget to stop the daemon
   bin/zeppelin-daemon.sh stop
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

   # start the Zeppelin daemon
   zeppelin-daemon.sh start
   # when ur done playing w/ the tutorial don't forget to stop the daemon
   zeppelin-daemon.sh stop
   ```

1. Open a browser, go to URL `http://localhost:8080`, select the
   "Zeppelin" icon in the upper left hand corner, select "Import
   Note", select "Add from URL", enter the url [https://ibm.github.io/lagraph/files/lagraph-tutorial.json](https://ibm.github.io/lagraph/files/lagraph-tutorial.json) and select "Import Note".

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

