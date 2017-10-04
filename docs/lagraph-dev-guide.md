---
layout: global
displayTitle: LAGraph Developer Guide
title: LAGraph Developer Guide
description: LAGraph Developer Guide
---
<!--
{% comment %}
License ...
{% endcomment %}
-->
* This will become a table of contents (this text will be scraped).
{:toc}

## Building LAGraph

LAGraph is built using [sbt](http://www.scala-sbt.org/).
LAGraph requires sbt 0.13 (or higher).
To build LAGraph, run:

    sbt package

* * *

## Testing LAGraph

LAGraph features a comprehensive set of integration tests. To perform these tests, run:

    sbt test

* * *

## Development Environment

LAGraph itself is written in Scala and is managed using sbt. As a result, LAGraph can readily be
imported into a standard development environment such as the
[ScalaIDE for Eclipse](http://scala-ide.org/).

To generate project metadata for eclipse, run:

    sbt eclipse

For eclipse, if necessary, select the appropriate Scala compiler, e.g.,

    right_click_on_project -> properties -> Scala Compiler -> Scala Installation

to

    Latest 2.11 bundle (dynamic)

or

    Fixed Scala Installation 2.11.x (built-in)
