/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.lagraph

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.charset.Charset

import scala.collection.immutable.ListMap
import scala.collection.mutable.{ HashSet => MSet }
import scala.collection.mutable.{ Map => MMap }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.Dependency
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/** Lag Utility object */
object LagUtils {

  /** Given a Natural number compute the bounding power of two (scale) */
  def pow2Bound(nv: Long): Int = {
    require(nv > 0L)
    def recur(curScale: Int, curBound: Long): (Boolean, Int, Long) = {
      if (curBound >= nv) {
        (true, curScale, curBound)
      } else {
        recur(curScale + 1, curBound * 2L)
      }
    }
    recur(0, 1L)._2
  }

  /**
   * Rebase an RDD of rcv edges such that lowest index is 0L
   *
   *  @tparam T the type of the edge values
   *
   *  @param sc the SparkContext
   *  @param eRdd pair RDD of [(row, column), value] edges
   *
   *  @return Tuple3(rebased EdgeRdd, old-to-new map,new-to-old map)
   */
  def rebaseEdgeRdd[T](sc: SparkContext,
                       eRdd: RDD[((Long, Long), T)]): (RDD[((Long, Long), T)], RDD[(Long, Long)], RDD[(Long, Long)]) = {
    // get RDD of all vertices
    val vRdd = eRdd.flatMap { case (k, v) => List(k._1, k._2) }.distinct
    // create index and create RDD map for old2new and new2old
    val pvids = vRdd.zipWithIndex().map { case (pt1, pt2) => ((pt1, pt2), (pt2, pt1)) }
    val o2n = pvids.keys.cache()
    val n2o = pvids.values
    val rebasedErdd = eRdd.
      map { case (k, v) => (k._1, (k._2, v)) }.
      join(o2n).
      map { case (k, v) => (v._1._1, (v._2, v._1._2)) }.
      join(o2n).
      map { case (k, v) => ((v._1._1, v._2), v._1._2) }
    (rebasedErdd, o2n, n2o)
  }

  /** Utility for computing time delta in seconds */
  def tt(t0: Long, t1: Long) = { (t1 - t0) * 1.0e-9 }
  // http://stackoverflow.com/questions/9160001/how-to-profile-methods-in-scala
  /** print clock-time-of-execution for a block of code and return the result */
  def timeblock[R](block: => R, msg: String = ""): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println(msg + "elapsed time: >%.3f< ms".format((t1 - t0) * 1.0e-6))
    result
  }

  /** Utility to produce a debug string for an RDD */
  def rddToDebugString(root_rdd: RDD[_]): String = {

    val sb = new StringBuilder()
    def preorder_recur(
      depth: Int,
      bar: MSet[Int],
      dependency: Dependency[_],
      visit: (Int, MSet[Int], Dependency[_]) => Unit): Unit = {
      visit(depth, bar, dependency)
      val it = dependency.rdd.dependencies.toIterator
      while (it.hasNext) {
        val d = it.next()
        if (it.hasNext) bar += depth else bar -= depth
        preorder_recur(depth + 1, bar, d, visit)
      }
    }

    def auxInfo(rdd: RDD[_]): (String, String) = {
      /**
       * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
       */
      def bytesToString(size: Long): String = {
        val TB = 1L << 40
        val GB = 1L << 30
        val MB = 1L << 20
        val KB = 1L << 10

        val (value, unit) = {
          if (size >= 2 * TB) {
            (size.asInstanceOf[Double] / TB, "TB")
          } else if (size >= 2 * GB) {
            (size.asInstanceOf[Double] / GB, "GB")
          } else if (size >= 2 * MB) {
            (size.asInstanceOf[Double] / MB, "MB")
          } else if (size >= 2 * KB) {
            (size.asInstanceOf[Double] / KB, "KB")
          } else {
            (size.asInstanceOf[Double], "B")
          }
        }
        "%.1f %s".format(value, unit)
      }
      val storageLevel = rdd.getStorageLevel
      val storageInfo = rdd.context.getRDDStorageInfo.filter(_.id == rdd.id).map(info =>
        "numCachedPartitions: %d; memSize: %s; externalBlockStoreSize: %s; diskSize: %s".format(
          info.numCachedPartitions, bytesToString(info.memSize),
          bytesToString(info.externalBlockStoreSize), bytesToString(info.diskSize)))
      require(storageInfo.length < 2)
      val storageInfoStr = if (storageInfo.length == 1) storageInfo(0) else ""
      (storageLevel.description, storageInfoStr)

    }
    val (storageLevel, storageInfo) = auxInfo(root_rdd)
    sb.append("(%d) %s, storageLevel: >%s<\n".format(root_rdd.partitions.size, root_rdd, storageLevel))
    if (storageInfo != "") sb.append("    >>>>%s<<<<\n".format(storageInfo))
    def depadd(depth: Int, bar: MSet[Int], d: Dependency[_]) = {
      def addbar(prefix: String, bar: MSet[Int], i: Int, depth: Int): String = {
        if (i == depth) {
          prefix
        } else {
          if (bar(i - 1)) addbar(prefix + "| ", bar, i + 1, depth) else addbar(prefix + "  ", bar, i + 1, depth)
        }
      }
      val prefix = addbar("  ", bar, 0, depth)
      val partitionStr = if (d.isInstanceOf[ShuffleDependency[_, _, _]]) "(" + d.rdd.partitions.size + ") " else ""
      val (storageLevel, storageInfo) = auxInfo(d.rdd)
      sb.append(prefix + "%s%s, storageLevel: >%s<\n".format(partitionStr, d.rdd, storageLevel))
      val postprefix = if (bar(depth - 1)) "|   " else "    "
      if (storageInfo != "") sb.append(prefix + postprefix + ">>>>%s<<<<\n".format(storageInfo))
    }
    val it = root_rdd.dependencies.toIterator
    while (it.hasNext) {
      val depth = 0
      val d = it.next()
      val bar = MSet[Int]()
      if (it.hasNext) bar += depth else bar -= depth
      preorder_recur(depth, bar, d, depadd)
    }
    sb.toString()
  }

  /**
   * Read a text file from DFS, a local file system (available on all nodes),
   *  or any Hadoop-supported file system URI, and return it as an RDD of edges.
   *
   *  Each line in the file must contain a pair of positive (>=0) Longs representing a directed edge.
   *
   *  @param sc a spark context
   *  @param fspec the file specification
   *  @param minPartitions If specified contains a suggested value
   *         for the minimal number of splits. If 'None' it defaults
   *         to sc.defaultMinPartitions.
   *  @return an RDD of edges
   */
  def adjacencyFileToRcv(sc: SparkContext, fspec: String, minPartitions: Option[Int] = None): RDD[(Long, Long)] = {
    // Load and parse the data
    val data = if (minPartitions == None)
      sc.textFile(fspec)
    else
      sc.textFile(fspec, minPartitions = minPartitions.get)
    data.map {
      line =>
        val parts = line.split(' ')
        (parts(0).toLong, parts(1).toLong)
    }
  }

  private def inputStreamFromDfs(dfs: String, fspec: String): InputStream = {
    // graph
    val hdfs_pattern = "(hdfs:.*)".r
    val file_pattern = "(file:.*)".r
    dfs match {
      case hdfs_pattern(dfs) => {
        val hfs = HdfsAccessPoint("%s".format(dfs))
        hfs.getFile(fspec)
      }
      case file_pattern(dfs) => {
        val url = new java.net.URL(dfs)
        url.openStream()
      }
    }
  }
  /**
   * Read a text file of edges from DFS and walk it looking for the maximum node number.
   *
   *  Each line in the file must contain a pair of positive (>=0) Longs representing an edge.
   *
   *  Not optimized (i.e., not parallelized), meant for reading test files
   *
   *  @param dfs 'protocol + host + port' part of dfs url
   *  @param fspec 'file' part of dfs url
   *
   *  @return largest node number
   */
  def maxNodeFromDfs(dfs: String, fspec: String): Long = {
    // graph
    val fis = inputStreamFromDfs(dfs, fspec)
    val isr = new InputStreamReader(fis, Charset.forName("UTF-8"))
    val br = new BufferedReader(isr)

    var maxNode = -1L
    var ok = true
    while (ok) {
      val ln = br.readLine()
      ok = ln != null
      if (ok) {
        val parts = ln.split(' ')
        val r = parts(0).toLong
        val c = parts(1).toLong
        require(r < Long.MaxValue && c < Long.MaxValue, "indices out of range, cant process")
        maxNode = math.max(maxNode, r.toLong)
        maxNode = math.max(maxNode, c.toLong)
      }
    }
    br.close
    maxNode
  }

  /**
   * Convert a sequence of edges into file containing a histogram of count vs. out degree.
   *
   *  Output file will contain sorted list of (count, out degree) pairs sorted in ascending
   *  order by out degree.
   *
   *  @param rc sequence of edges
   *  @param fspec the file specification
   */

  def rcRddToBroom(rcRdd: RDD[(Long, Long)], outputLog2: Boolean = false, fspecOpt: Option[String] = None): Unit = {
    val rowCoundRdd = rcRdd.map { case (r, c) => (r, 1L) }.combineByKey((v: Long) => v, (v1: Long, v2: Long) => v1 + v2, (c1: Long, c2: Long) => c1 + c2)
    val countCountRdd = rowCoundRdd.map { case (r, c) => (c, 1L) }.combineByKey((v: Long) => v, (v1: Long, v2: Long) => v1 + v2, (c1: Long, c2: Long) => c1 + c2)
    val rcvlm = ListMap(countCountRdd.collect.sortBy(_._1): _*)
    def log2(x: Long) = scala.math.log(x) / scala.math.log(2)
    def xform(x: Long) = if (outputLog2) "%f".format(log2(x)) else "%d".format(x)
    if (fspecOpt.isDefined) {
      val file = new java.io.File(fspecOpt.get)
      val bw = new java.io.BufferedWriter(new java.io.FileWriter(file))
      for (item <- rcvlm) {
        bw.write("%s %s\n".format(xform(item._1), xform(item._2)))
      }
      bw.close()
    } else {
      for (item <- rcvlm) {
        println("%s %s".format(xform(item._1), xform(item._2)))
      }
    }
  }

  /**
   * Convert an DFS text file into a list of edges.
   *
   *  Each line in the file must contain a pair of positive (>=0) Longs representing an edge.
   *
   *  @param dfs 'protocol + host + port' part of dfs url
   *  @param fspec 'file' part of dfs url
   *
   *  @return list of edges
   */
  def adjListFromDfs(dfs: String, fspec: String): List[(Long, Long)] = {
    val fis = inputStreamFromDfs(dfs, fspec)
    val isr = new InputStreamReader(fis, Charset.forName("UTF-8"))
    val br = new BufferedReader(isr)

    var hbr = List[(Long, Long)]()
    var ok = true
    while (ok) {
      val ln = br.readLine()
      ok = ln != null
      if (ok) {
        val parts = ln.split(' ')
        val r = parts(0).toLong
        val c = parts(1).toLong
        require(r < Int.MaxValue && c < Int.MaxValue, "indices out of range, cant process")
        hbr = hbr ::: List((r, c))
      }
    }
    br.close
    hbr
  }
}

/**
 * An access point for HDFS.
 *
 *  @constructor create a base access point for HDFS
 *
 *  @param accessUrl a url from which HDFS is accessed
 *  @return a new HDFS access point
 */
private case class HdfsAccessPoint(val accessUrl: String = "hdfs://ngp-c2-virt-4.watson.ibm.com:8020") {
  private val conf = new Configuration()
  conf.set("fs.defaultFS", accessUrl)

  private val fileSystem = FileSystem.get(conf)

  /**
   * Creates an input stream from an HDFS file.
   *
   *  @param name their name
   *  @param age the age of the person to create
   */
  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }
}
/** Object containing map (Map[String, List[(Long, Long)]]) definitions of standard graphs */
object GraphLib {
  /**
   * Some small graphs borrowed from networkx.
   *
   * No distinction is made between undirected and directed graphs.
   * If a graph is meant to be undirected an arbitrary direction is chosen,
   * it is up to the user to treat (eg convert) directed graphs
   * that are meant to be undirected.
   *
   * Keys are: bull, chvatal, cubical, desargues, diamond, dodecahedral,
   * frucht, heawood, house, house_x, icosahedral, krackhardt_kite,
   * moebius_kantor, octahedral, pappus, petersen, sedgewick_maze, tetrahedral,
   * truncated_cube, truncated_tetrahedron, tutte, line
   *
   */
  lazy val smallGraphs = Map(
    // from networkX
    "bull" -> List((0L, 1L), (0L, 2L), (1L, 2L), (1L, 3L), (2L, 4L)),
    "chvatal" -> List((0L, 1L), (0L, 4L), (0L, 6L), (0L, 9L), (1L, 2L), (1L, 5L), (1L, 7L), (2L, 8L), (2L, 3L), (2L, 6L), (3L, 9L), (3L, 4L), (3L, 7L), (4L, 8L), (4L, 5L), (5L, 10L), (5L, 11L), (6L, 10L), (6L, 11L), (7L, 8L), (7L, 11L), (8L, 10L), (9L, 10L), (9L, 11L)),
    "cubical" -> List((0L, 1L), (0L, 3L), (0L, 4L), (1L, 2L), (1L, 7L), (2L, 3L), (2L, 6L), (3L, 5L), (4L, 5L), (4L, 7L), (5L, 6L), (6L, 7L)),
    "desargues" -> List((0L, 1L), (0L, 19L), (0L, 5L), (1L, 16L), (1L, 2L), (2L, 11L), (2L, 3L), (3L, 4L), (3L, 14L), (4L, 9L), (4L, 5L), (5L, 6L), (6L, 15L), (6L, 7L), (7L, 8L), (7L, 18L), (8L, 9L), (8L, 13L), (9L, 10L), (10L, 19L), (10L, 11L), (11L, 12L), (12L, 17L), (12L, 13L), (13L, 14L), (14L, 15L), (15L, 16L), (16L, 17L), (17L, 18L), (18L, 19L)),
    "diamond" -> List((0L, 1L), (0L, 2L), (1L, 2L), (1L, 3L), (2L, 3L)),
    "dodecahedral" -> List((0L, 1L), (0L, 10L), (0L, 19L), (1L, 8L), (1L, 2L), (2L, 3L), (2L, 6L), (3L, 19L), (3L, 4L), (4L, 17L), (4L, 5L), (5L, 6L), (5L, 15L), (6L, 7L), (7L, 8L), (7L, 14L), (8L, 9L), (9L, 10L), (9L, 13L), (10L, 11L), (11L, 18L), (11L, 12L), (12L, 16L), (12L, 13L), (13L, 14L), (14L, 15L), (15L, 16L), (16L, 17L), (17L, 18L), (18L, 19L)),
    "frucht" -> List((0L, 1L), (0L, 6L), (0L, 7L), (1L, 2L), (1L, 7L), (2L, 8L), (2L, 3L), (3L, 9L), (3L, 4L), (4L, 9L), (4L, 5L), (5L, 10L), (5L, 6L), (6L, 10L), (7L, 11L), (8L, 9L), (8L, 11L), (10L, 11L)),
    "heawood" -> List((0L, 1L), (0L, 5L), (0L, 13L), (1L, 2L), (1L, 10L), (2L, 3L), (2L, 7L), (3L, 12L), (3L, 4L), (4L, 9L), (4L, 5L), (5L, 6L), (6L, 11L), (6L, 7L), (7L, 8L), (8L, 9L), (8L, 13L), (9L, 10L), (10L, 11L), (11L, 12L), (12L, 13L)),
    "house" -> List((0L, 1L), (0L, 2L), (1L, 3L), (2L, 3L), (2L, 4L), (3L, 4L)),
    "house_x" -> List((0L, 1L), (0L, 2L), (0L, 3L), (1L, 2L), (1L, 3L), (2L, 3L), (2L, 4L), (3L, 4L)),
    "icosahedral" -> List((0L, 8L), (0L, 1L), (0L, 11L), (0L, 5L), (0L, 7L), (1L, 8L), (1L, 2L), (1L, 5L), (1L, 6L), (2L, 8L), (2L, 9L), (2L, 3L), (2L, 6L), (3L, 9L), (3L, 10L), (3L, 4L), (3L, 6L), (4L, 10L), (4L, 11L), (4L, 5L), (4L, 6L), (5L, 11L), (5L, 6L), (7L, 8L), (7L, 9L), (7L, 10L), (7L, 11L), (8L, 9L), (9L, 10L), (10L, 11L)),
    "krackhardt_kite" -> List((0L, 1L), (0L, 2L), (0L, 3L), (0L, 5L), (1L, 3L), (1L, 4L), (1L, 6L), (2L, 3L), (2L, 5L), (3L, 4L), (3L, 5L), (3L, 6L), (4L, 6L), (5L, 6L), (5L, 7L), (6L, 7L), (7L, 8L), (8L, 9L)),
    "moebius_kantor" -> List((0L, 1L), (0L, 5L), (0L, 15L), (1L, 2L), (1L, 12L), (2L, 3L), (2L, 7L), (3L, 4L), (3L, 14L), (4L, 9L), (4L, 5L), (5L, 6L), (6L, 11L), (6L, 7L), (7L, 8L), (8L, 9L), (8L, 13L), (9L, 10L), (10L, 11L), (10L, 15L), (11L, 12L), (12L, 13L), (13L, 14L), (14L, 15L)),
    "octahedral" -> List((0L, 1L), (0L, 2L), (0L, 3L), (0L, 4L), (1L, 2L), (1L, 3L), (1L, 5L), (2L, 4L), (2L, 5L), (3L, 4L), (3L, 5L), (4L, 5L)),
    "pappus" -> List((0L, 1L), (0L, 5L), (0L, 17L), (1L, 8L), (1L, 2L), (2L, 3L), (2L, 13L), (3L, 10L), (3L, 4L), (4L, 5L), (4L, 15L), (5L, 6L), (6L, 11L), (6L, 7L), (7L, 8L), (7L, 14L), (8L, 9L), (9L, 16L), (9L, 10L), (10L, 11L), (11L, 12L), (12L, 17L), (12L, 13L), (13L, 14L), (14L, 15L), (15L, 16L), (16L, 17L)),
    "petersen" -> List((0L, 1L), (0L, 4L), (0L, 5L), (1L, 2L), (1L, 6L), (2L, 3L), (2L, 7L), (3L, 8L), (3L, 4L), (4L, 9L), (5L, 8L), (5L, 7L), (6L, 8L), (6L, 9L), (7L, 9L)),
    "sedgewick_maze" -> List((0L, 2L), (0L, 5L), (0L, 7L), (1L, 7L), (2L, 6L), (3L, 4L), (3L, 5L), (4L, 5L), (4L, 6L), (4L, 7L)),
    "tetrahedral" -> List((0L, 1L), (0L, 2L), (0L, 3L), (1L, 2L), (1L, 3L), (2L, 3L)),
    "truncated_cube" -> List((0L, 1L), (0L, 2L), (0L, 4L), (1L, 11L), (1L, 14L), (2L, 3L), (2L, 4L), (3L, 8L), (3L, 6L), (4L, 5L), (5L, 16L), (5L, 18L), (6L, 8L), (6L, 7L), (7L, 10L), (7L, 12L), (8L, 9L), (9L, 17L), (9L, 20L), (10L, 11L), (10L, 12L), (11L, 14L), (12L, 13L), (13L, 21L), (13L, 22L), (14L, 15L), (15L, 19L), (15L, 23L), (16L, 17L), (16L, 18L), (17L, 20L), (18L, 19L), (19L, 23L), (20L, 21L), (21L, 22L), (22L, 23L)),
    "truncated_tetrahedron" -> List((0L, 1L), (0L, 2L), (0L, 9L), (1L, 2L), (1L, 6L), (2L, 3L), (3L, 11L), (3L, 4L), (4L, 11L), (4L, 5L), (5L, 6L), (5L, 7L), (6L, 7L), (7L, 8L), (8L, 9L), (8L, 10L), (9L, 10L), (10L, 11L)),
    "tutte" -> List((0L, 1L), (0L, 2L), (0L, 3L), (1L, 26L), (1L, 4L), (2L, 10L), (2L, 11L), (3L, 18L), (3L, 19L), (4L, 33L), (4L, 5L), (5L, 29L), (5L, 6L), (6L, 27L), (6L, 7L), (7L, 8L), (7L, 14L), (8L, 9L), (8L, 38L), (9L, 10L), (9L, 37L), (10L, 39L), (11L, 12L), (11L, 39L), (12L, 35L), (12L, 13L), (13L, 14L), (13L, 15L), (14L, 34L), (15L, 16L), (15L, 22L), (16L, 17L), (16L, 44L), (17L, 18L), (17L, 43L), (18L, 45L), (19L, 20L), (19L, 45L), (20L, 41L), (20L, 21L), (21L, 22L), (21L, 23L), (22L, 40L), (23L, 24L), (23L, 27L), (24L, 32L), (24L, 25L), (25L, 26L), (25L, 31L), (26L, 33L), (27L, 28L), (28L, 32L), (28L, 29L), (29L, 30L), (30L, 33L), (30L, 31L), (31L, 32L), (34L, 35L), (34L, 38L), (35L, 36L), (36L, 37L), (36L, 39L), (37L, 38L), (40L, 41L), (40L, 44L), (41L, 42L), (42L, 43L), (42L, 45L), (43L, 44L)),
    // other
    "line" -> List((0L, 1L), (1L, 2L), (2L, 3L), (3L, 4L), (4L, 5L), (5L, 6L), (6L, 7L), (7L, 8L), (8L, 9L), (9L, 10L), (10L, 11L), (11L, 12L), (12L, 13L), (13L, 14L), (14L, 15L)))

  /**
   * Convert a small graph (designated by key) into a rcvRdd representation.
   *
   * @param sc the Spark context
   * @param graph the key for the graph (see above)
   * @param numSlicesOpt optional number of slices
   */
  def graphToRcv(sc: SparkContext, graph: String, numSlicesOpt: Option[Int] = None): RDD[(Long, Long)] = {
    if (numSlicesOpt == None)
      sc.parallelize(smallGraphs(graph))
    else
      sc.parallelize(smallGraphs(graph), numSlicesOpt.get)
  }
}

