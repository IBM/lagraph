organization := "com.ibm"
name := "lagraph"
publishMavenStyle := true
version := "0.1.0-SNAPSHOT"
sparkVersion := "2.1.0"
scalaVersion := "2.11.8"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// for spark package
spName := "ibm/lagraph"

sparkComponents := Seq("core")

parallelExecution in Test := false
fork := true

coverageHighlighting := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

// scala doc
scalacOptions in (Compile, doc) := List("-skip-packages",  "com.ibm.lagraph.impl") 

// libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
// libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.1.0" % "provided"
// libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
// libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.0" % "provided"
// libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided"
// libraryDependencies += "org.apache.systemml" % "systemml" % "0.9.0-incubating"

//libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.11" % "2.1.0_0.6.0" % "test" withSources() withJavadoc()
//libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.0" % "test" withSources() withJavadoc()
libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.11" % "2.1.0_0.6.0" % "test"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.0" % "test"

// libraryDependencies += "com.twitter" %% "algebird-core" % "0.10.0" withSources() withJavadoc()
// libraryDependencies += "org.scalanlp" %% "breeze" % "0.10" withSources() withJavadoc()
// libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.10" withSources() withJavadoc()

scalacOptions ++= Seq("-deprecation", "-unchecked")

//x organization := "com.holdenkarau"
//x 
//x name := "spark-testing-base"
//x 
//x publishMavenStyle := true
//x 
//x version := "0.4.6"
//x 
//x sparkVersion := "2.0.0"
//x 
//x scalaVersion := {
//x   if (sparkVersion.value >= "2.0.0") {
//x     "2.11.8"
//x   } else {
//x     "2.10.6"
//x   }
//x }
//x 
//x crossScalaVersions := {
//x   if (sparkVersion.value > "2.0.0") {
//x     Seq("2.11.8")
//x   } else {
//x     Seq("2.10.6", "2.11.8")
//x   }
//x }
//x 
//x javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
//x 
//x //tag::spName[]
//x spName := "holdenk/spark-testing-base"
//x //end::spName[]
//x 
//x sparkComponents := {
//x   if (sparkVersion.value >= "2.0.0") Seq("core", "streaming", "sql", "catalyst", "hive", "yarn", "mllib")
//x   else Seq("core", "streaming", "sql", "catalyst", "hive", "streaming-kafka", "yarn", "mllib")
//x }
//x 
//x parallelExecution in Test := false
//x fork := true
//x 
//x 
//x coverageHighlighting := {
//x   if (scalaBinaryVersion.value == "2.10") false
//x   else true
//x }
//x 
//x // Allow kafka (and other) utils to have version specific files
//x unmanagedSourceDirectories in Compile  := {
//x   if (sparkVersion.value >= "2.0.0") Seq(
//x     (sourceDirectory in Compile)(_ / "2.0/scala"),
//x     (sourceDirectory in Compile)(_ / "1.6/scala"),
//x     (sourceDirectory in Compile)(_ / "1.5/scala"),
//x     (sourceDirectory in Compile)(_ / "1.4/scala"),
//x     (sourceDirectory in Compile)(_ / "1.3/scala"), (sourceDirectory in Compile)(_ / "1.3/java")
//x   ).join.value
//x   else if (sparkVersion.value >= "1.6") Seq(
//x     (sourceDirectory in Compile)(_ / "pre-2.0/scala"),
//x     (sourceDirectory in Compile)(_ / "1.6/scala"),
//x     (sourceDirectory in Compile)(_ / "1.5/scala"),
//x     (sourceDirectory in Compile)(_ / "1.4/scala"),
//x     (sourceDirectory in Compile)(_ / "kafka/scala"),
//x     (sourceDirectory in Compile)(_ / "1.3/scala"), (sourceDirectory in Compile)(_ / "1.3/java")
//x   ).join.value
//x   else if (sparkVersion.value >= "1.5") Seq(
//x     (sourceDirectory in Compile)(_ / "pre-2.0/scala"),
//x     (sourceDirectory in Compile)(_ / "1.5/scala"),
//x     (sourceDirectory in Compile)(_ / "1.4/scala"),
//x     (sourceDirectory in Compile)(_ / "kafka/scala"),
//x     (sourceDirectory in Compile)(_ / "1.3/scala"), (sourceDirectory in Compile)(_ / "1.3/java")
//x   ).join.value
//x   else if (sparkVersion.value >= "1.4") Seq(
//x     (sourceDirectory in Compile)(_ / "pre-2.0/scala"),
//x     (sourceDirectory in Compile)(_ / "pre-1.5/scala"),
//x     (sourceDirectory in Compile)(_ / "1.4/scala"),
//x     (sourceDirectory in Compile)(_ / "kafka/scala"),
//x     (sourceDirectory in Compile)(_ / "1.3/scala"), (sourceDirectory in Compile)(_ / "1.3/java")
//x   ).join.value
//x   else Seq(
//x     (sourceDirectory in Compile)(_ / "pre-2.0/scala"),
//x     (sourceDirectory in Compile)(_ / "pre-1.5/scala"),
//x     (sourceDirectory in Compile)(_ / "1.3/scala"), (sourceDirectory in Compile)(_ / "1.3/java"),
//x     (sourceDirectory in Compile)(_ / "1.3-only/scala")
//x   ).join.value
//x }
//x 
//x unmanagedSourceDirectories in Test  := {
//x   if (sparkVersion.value >= "2.0.0") Seq(
//x     (sourceDirectory in Test)(_ / "1.6/scala"), (sourceDirectory in Test)(_ / "1.6/java"),
//x     (sourceDirectory in Test)(_ / "1.4/scala"),
//x     (sourceDirectory in Test)(_ / "1.3/scala"), (sourceDirectory in Test)(_ / "1.3/java")
//x   ).join.value
//x   else if (sparkVersion.value >= "1.6") Seq(
//x     (sourceDirectory in Test)(_ / "pre-2.0/scala"), (sourceDirectory in Test)(_ / "pre-2.0/java"),
//x     (sourceDirectory in Test)(_ / "1.6/scala"), (sourceDirectory in Test)(_ / "1.6/java"),
//x     (sourceDirectory in Test)(_ / "1.4/scala"),
//x     (sourceDirectory in Test)(_ / "kafka/scala"),
//x     (sourceDirectory in Test)(_ / "1.3/scala"), (sourceDirectory in Test)(_ / "1.3/java")
//x   ).join.value
//x   else if (sparkVersion.value >= "1.4") Seq(
//x     (sourceDirectory in Test)(_ / "pre-2.0/scala"), (sourceDirectory in Test)(_ / "pre-2.0/java"),
//x     (sourceDirectory in Test)(_ / "1.4/scala"),
//x     (sourceDirectory in Test)(_ / "kafka/scala"),
//x     (sourceDirectory in Test)(_ / "1.3/scala"), (sourceDirectory in Test)(_ / "1.3/java")
//x   ).join.value
//x   else Seq(
//x     (sourceDirectory in Test)(_ / "pre-2.0/scala"), (sourceDirectory in Test)(_ / "pre-2.0/java"),
//x     (sourceDirectory in Test)(_ / "1.3/scala"), (sourceDirectory in Test)(_ / "1.3/java")
//x   ).join.value
//x }
//x 
//x 
//x javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
//x 
//x // additional libraries
//x libraryDependencies ++= Seq(
//x   "org.scalatest" %% "scalatest" % "2.2.6",
//x   "io.github.nicolasstucki" %% "multisets" % "0.3",
//x   "org.scalacheck" %% "scalacheck" % "1.12.5",
//x   "junit" % "junit" % "4.11",
//x   "org.eclipse.jetty" % "jetty-util" % "9.3.11.v20160721",
//x   "com.novocode" % "junit-interface" % "0.11" % "test->default")
//x 
//x // Based on Hadoop Mini Cluster tests from Alpine's PluginSDK (Apache licensed)
//x // javax.servlet signing issues can be tricky, we can just exclude the dep
//x def excludeFromAll(items: Seq[ModuleID], group: String, artifact: String) =
//x   items.map(_.exclude(group, artifact))
//x 
//x def excludeJavaxServlet(items: Seq[ModuleID]) =
//x   excludeFromAll(items, "javax.servlet", "servlet-api")
//x 
//x lazy val miniClusterDependencies = excludeJavaxServlet(Seq(
//x   "org.apache.hadoop" % "hadoop-hdfs" % "2.6.4" % "compile,test" classifier "" classifier "tests",
//x   "org.apache.hadoop" % "hadoop-common" % "2.6.4" % "compile,test" classifier "" classifier "tests" ,
//x   "org.apache.hadoop" % "hadoop-client" % "2.6.4" % "compile,test" classifier "" classifier "tests" ,
//x   "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.6.4" % "compile,test" classifier "" classifier "tests",
//x   "org.apache.hadoop" % "hadoop-yarn-server-tests" % "2.6.4" % "compile,test" classifier "" classifier "tests",
//x   "org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % "2.6.4" % "compile,test" classifier "" classifier "tests",
//x   "org.apache.hadoop" % "hadoop-minicluster" % "2.6.4"))
//x 
//x libraryDependencies ++= miniClusterDependencies
//x 
//x scalacOptions ++= Seq("-deprecation", "-unchecked")
//x 
//x pomIncludeRepository := { x => false }
//x 
//x resolvers ++= Seq(
//x   "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
//x   "Spray Repository" at "http://repo.spray.cc/",
//x   "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
//x   "Akka Repository" at "http://repo.akka.io/releases/",
//x   "Twitter4J Repository" at "http://twitter4j.org/maven2/",
//x   "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
//x   "Twitter Maven Repo" at "http://maven.twttr.com/",
//x   "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
//x   "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
//x   "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
//x   "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
//x   "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
//x   Resolver.sonatypeRepo("public")
//x )
//x 
//x // publish settings
//x publishTo := {
//x   val nexus = "https://oss.sonatype.org/"
//x   if (isSnapshot.value)
//x     Some("snapshots" at nexus + "content/repositories/snapshots")
//x   else
//x     Some("releases"  at nexus + "service/local/staging/deploy/maven2")
//x }
//x 
//x licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
//x 
//x homepage := Some(url("https://github.com/holdenk/spark-testing-base"))
//x 
//x pomExtra := (
//x   <scm>
//x     <url>git@github.com:holdenk/spark-testing-base.git</url>
//x     <connection>scm:git@github.com:holdenk/spark-testing-base.git</connection>
//x   </scm>
//x   <developers>
//x     <developer>
//x       <id>holdenk</id>
//x       <name>Holden Karau</name>
//x       <url>http://www.holdenkarau.com</url>
//x       <email>holden@pigscanfly.ca</email>
//x     </developer>
//x   </developers>
//x )
//x 
//x //credentials += Credentials(Path.userHome / ".ivy2" / ".spcredentials")
//x credentials ++= Seq(Credentials(Path.userHome / ".ivy2" / ".sbtcredentials"), Credentials(Path.userHome / ".ivy2" / ".sparkcredentials"))
//x 
//x spIncludeMaven := true
//x 
//x useGpg := true
//x 
