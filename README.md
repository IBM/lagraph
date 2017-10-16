-<!--
{% comment %}
License ...
{% endcomment %}
-->

[![buildstatus](https://travis-ci.org/ibm/lagraph.svg?branch=master)](https://ibm/lagraph/spark-testing-base)
-<!--
[![codecov.io](http://codecov.io/github/ibm/lagraph/coverage.svg?branch=master)](http://codecov.io/github/ibm/lagraph?branch=master)
-->

# LAGraph

**Documentation:** [LAGraph Documentation](https://ibm.github.io/lagraph/)<br/>
**Mailing List:** [Dev Mailing List](TBD)<br/>
**Build Status:** [![Build Status](https://sparktc.ibmcloud.com/jenkins/job/LAGraph-DailyTest/badge/icon)](https://sparktc.ibmcloud.com/jenkins/job/LAGraph-DailyTest)<br/>
**Issue Tracker:** [JIRA](TBD)<br/>
**Download:** [Download LAGraph](http://pokgsa.ibm.com/projects/s/spp/public/lagraph/lagraph_2.11-0.1.0-SNAPSHOT.jar)<br/>

Hopefully, **LAGraph** will soon be a **SparkPackages** project! When that happens, please see the [LAGraph SparkPackages homepage](https://spark-packages.org/?q=tags%3A%22Graph%22) website for more information. The latest project documentation can be found at the
[**LAGraph Documentation**](https://pages.github.ibm.com/hornwp/lagraph/) website on GitHub.

LAGraph is a Scala package that supports the implementation of scalable graph algorithms using algebraic techniques.
Its distinguishing characteristics are:

  1. **Support for both conventional and custom semirings**
  1. **Support for both primative and custom types**
  1. **Multiple execution modes**, including Spark and pure Scala.
  1. **Automatic optimization** based on data and cluster characteristics to ensure both efficiency and scalability.


LAGraph is evolving, while several
classical algorithms have been implemented on top of LAGraph, the
completeness of interface has not been established. Current efforts
are focused on expanding the list of supported algorithms.

## Download & Setup

For a first pass, instead of performing a full Spark install, you
might consider working through the
[LAGraph Quick Start (using Zeppelin)](https://ibm.github.io/lagraph/#quick-start-using-zeppelin).

Before you get started on LAGraph, make sure that your environment is set up and ready to go.

  1. **If you're on OS X, we recommend installing
     [Homebrew](http://brew.sh) if you haven't already.  For Linux
     users, the [Linuxbrew project](http://linuxbrew.sh/) is
     equivalent.**

  OS X:
  ```
  /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
  ```
  Linux:
  ```
  ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install)"
  ```

  2. **Install Java (need Java 8).**
  ```
  brew tap caskroom/cask
  brew install Caskroom/cask/java
  ```

  3. **Install Spark 2.1.**
  ```
  brew tap homebrew/versions
  brew install apache-spark21
  ```

  4. **Download LAGraph.**

  Download [lagraph_2.11-0.1.0.jar](http://pokgsa.ibm.com/projects/s/spp/public/lagraph/lagraph_2.11-0.1.0-SNAPSHOT.jar)
  to a location of your choice.

  5. **[OPTIONAL] Install Zeppelin.**

  ```
  brew install apache-zeppelin
  ```

**Congrats! You can now use LAGraph!**

## Next Steps!

To get started, please consult the
[LAGraph Documentation](https://ibm.github.io/lagraph/) website on GitHub.
