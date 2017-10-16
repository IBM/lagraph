organization := "com.github.ibm"

name := "lagraph-core"

publishMavenStyle := true

version := "0.1.0-SNAPSHOT"

sparkVersion := "2.2.0"

scalaVersion := {
  if (sparkVersion.value >= "2.0.0") {
    "2.11.11"
  } else {
    "2.10.6"
  }
}

// See https://github.com/scala/scala/pull/3799
coverageHighlighting := {
  if (sparkVersion.value >= "2.0.0") {
    true
  } else {
    false
  }
}

crossScalaVersions := {
  if (sparkVersion.value >= "2.3.0") {
    Seq("2.11.11")
  } else {
    Seq("2.10.6", "2.11.11")
  }
}

javacOptions ++= {
    if (sparkVersion.value >= "2.1.1") {
      Seq("-source", "1.8", "-target", "1.8")
    } else {
      Seq("-source", "1.7", "-target", "1.7")
    }
}

//tag::spName[]
spName := "ibm/lagraph"
//end::spName[]

sparkComponents := Seq("core")

parallelExecution in Test := false
fork := true

javaOptions ++= Seq("-Xms2G", "-Xmx2G", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")


libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.11" % "2.1.0_0.6.0" % "test"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.0" % "test"

// additional libraries
// none

scalacOptions ++= Seq("-deprecation", "-unchecked")

pomIncludeRepository := { x => false }

// publish settings
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

homepage := Some(url("https://github.com/ibm/lagraph"))

pomExtra := (
  <scm>
    <url>git@github.com:ibm/lagraph.git</url>
    <connection>scm:git@github.com:ibm/lagraph.git</connection>
  </scm>
  <developers>
    <developer>
      <id>hornwp</id>
      <name>Bill Horn</name>
      <url>https://github.com/hornwp</url>
      <email>hornwp@us.ibm.com</email>
    </developer>
  </developers>
)

credentials ++= Seq(Credentials(Path.userHome / ".ivy2" / ".sbtcredentials"), Credentials(Path.userHome / ".ivy2" / ".sparkcredentials"))

spIncludeMaven := true

useGpg := true


// ******
// lagraph specific

// scala doc
scalacOptions in (Compile, doc) := List("-skip-packages",  "com.ibm.lagraph.impl") 
// Display full-length stacktraces from ScalaTest:
testOptions in Test += Tests.Argument("-oF")
