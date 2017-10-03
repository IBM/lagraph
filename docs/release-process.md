---
layout: global
title: LAGraph Release Process
description: Description of the LAGraph release process and validation.
displayTitle: LAGraph Release Process
---
<!--
{% comment %}
License ...
{% endcomment %}
-->

* This will become a table of contents (this text will be scraped).
{:toc}

# TBD

__This section was lifted from SystemML to serve as a template for a TBD
LAGraph Release Process__

# Release Candidate Build and Deployment

To be written. (Describe how the release candidate is built, including checksums. Describe how
the release candidate is deployed to servers for review.)


## Release Documentation

The `LAGRAPH_VERSION` value in docs/_config.yml should be updated to the correct release version. The documentation
site should be built.

__TBD__

The Javadocs should be generated for the project and should be deployed to ... __TBD__, need to figure out how to best handle multiple versions of docs ...

The version number resides is specified in the project build.sbt file.

Additionally, the Javadocs should be deployed to the "latest" ... __TBD__ 


# Release Candidate Checklist

## All Artifacts and Checksums Present

<a href="#release-candidate-checklist">Up to Checklist</a>

Verify that each expected artifact is present at __TBD__ and that each artifact has accompanying checksums (such as .asc and .md5).


## Release Candidate Build

<a href="#release-candidate-checklist">Up to Checklist</a>

The release candidate should build on OS X and Linux. To do this cleanly,
the following procedure can be performed.

Clone the LAGraph GitHub repository
to an empty location. Next, check out the release tag. Following
this, build the distributions using sbt. This should be performed
with an empty local ivy repository.

Here is an example:

	$ git clone https://github.com/ibm/incubator-systemml.git
	$ cd incubator-lagraph
	$ git tag -l
	$ git checkout tags/0.10.0-incubating-rc1 -b 0.10.0-incubating-rc1
	$ java -Dsbt.ivy.home=/tmp/.ivy2/ -Divy.home=/tmp/.ivy2/ -jar `dirname $0`/sbt-launch.jar "$@" __TBD__ see [https://stackoverflow.com/questions/3142856/how-to-configure-ivy-cache-directory-per-user-or-system-wide](How to configure Ivy cache directory per-user or system-wide?)
	$ ... clean package -P distribution


## Test Suite Passes

<a href="#release-candidate-checklist">Up to Checklist</a>

The entire test suite should pass on OS X and Linux.
The test suite can be run using:

	$ sbt ???


## All Binaries Execute

<a href="#release-candidate-checklist">Up to Checklist</a>

Validate that all of the binary artifacts can execute, including those artifacts packaged
in other artifacts (in the tar.gz and zip artifacts).

The build artifacts should be downloaded from __TBD__ and these artifacts should be tested, as in this OS X example.

	# download artifacts
	wget -r -nH -nd -np -R index.html* https://.../0.10.0-incubating-rc1/

	# verify standalone tar.gz works
	TBD

	# verify main jar works
	TBD

	# verify standalone jar works
	TBD

	# verify src works
	TBD

	# verify distrib tar.gz works
	TBD

	# verify spark mode
	TBD


Here is an example of doing a basic
sanity check on OS X after building the artifacts manually.

	# build distribution artifacts
	TBD

	cd target

	# verify main jar works
	TBD

	# verify LAGraph.jar works
	TBD

	# verify standalone jar works
	TBD

	# verify src works
	TBD

        # verify standalone tar.gz works
	TBD

	# verify distrib tar.gz works
	TBD

        # verify spark mode
	TBD

## Check LICENSE and NOTICE Files

<a href="#release-candidate-checklist">Up to Checklist</a>

Each artifact *must* contain LICENSE and NOTICE files. These files must reflect the
contents of the artifacts. If the project dependencies (ie, libraries) have changed
since the last release, the LICENSE and NOTICE files must be updated to reflect these
changes.

Each artifact *should* contain a DISCLAIMER file.

For more information, see:

1. <http://incubator.apache.org/guides/releasemanagement.html>
2. <http://www.apache.org/dev/licensing-howto.html>


## Src Artifact Builds and Tests Pass

<a href="#release-candidate-checklist">Up to Checklist</a>

The project should be built using the `src` (tar.gz and zip) artifacts.
In addition, the test suite should be run using an `src` artifact and
the tests should pass.

	tar -xvzf lagraph-0.10.0-incubating-src.tar.gz
	cd lagraph-0.10.0-incubating-src
	sbt TBD
	sbt TBD


## Single-Node Standalone

<a href="#release-candidate-checklist">Up to Checklist</a>

TBD

## Single-Node Spark

<a href="#release-candidate-checklist">Up to Checklist</a>

Verify that LAGraph runs algorithms on Spark locally.

TBD


## Notebooks

<a href="#release-candidate-checklist">Up to Checklist</a>

Verify that LAGraph can be executed from Zeppelin notebooks.

TBD


## Performance Suite

<a href="#release-candidate-checklist">Up to Checklist</a>

TBD

# Voting

TBD

Following a successful release candidate vote by LAGraph PMC members on the LAGraph mailing list, the release candidate
is voted on by Incubator PMC members on the general incubator mailing list. If this vote succeeds, the release candidate
has been approved.


# Release


## Release Deployment

To be written. (What steps need to be done? How is the release deployed to the central maven repo? What updates need to
happen to the main website, such as updating the Downloads page? Where do the release notes for the release go?)


