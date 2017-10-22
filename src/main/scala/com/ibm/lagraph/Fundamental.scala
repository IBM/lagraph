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
// scalastyle:off println

import scala.annotation.tailrec

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

/**
  * Several fundamental graph algorithms as algebraic operations from:
  *
  * Fineman, Jeremy T., and Eric Robinson.
  * "Fundamental graph algorithms."
  * Graph Algorithms in the Language of Linear Algebra 22 (2011): 45.
  *
  * TODO: move to tests
  */
object Fundamental {

  /**
    * The Floyd-Warshall algorithm, solves the all-pairs shortest paths problem.
    *
    * @param sc the Spark context
    */
  def FloydWarshall(sc: SparkContext): Unit = {
    // ********
    // Setup some utility functions: Common

    // some imports ...
    import com.ibm.lagraph.{LagContext, LagMatrix, LagSemigroup, LagSemiring}
    // for verbose printing
    import scala.reflect.classTag
    def float2Str(f: Float): String = {
      if (f == LagSemigroup.infinity(classTag[Float])) "   inf"
      else if (f == LagSemigroup.minfinity(classTag[Float])) "   minf"
      else "%6.3f".format(f)
    }
    def long2Str(l: Long): String = {
      if (l == LagSemigroup.infinity(classTag[Long])) " inf"
      else if (l == LagSemigroup.minfinity(classTag[Long])) " minf"
      else "%4d".format(l)
    }
    def pathType2Str(d: (Float, Long, Long)): String = {
      val d1 = float2Str(d._1)
      val d2 = long2Str(d._2)
      val d3 = long2Str(d._3)
      "(%s,%s,%s)".format(d1, d2, d3)
    }

    // ********
    // Create a Simple (directed) Graph from an RDD: Common
    // define graph
    val numv = 5L
    val houseEdges = List(((1L, 0L), 20.0F),
                          ((2L, 0L), 10.0F),
                          ((3L, 1L), 15.0F),
                          ((3L, 2L), 30.0F),
                          ((4L, 2L), 50.0F),
                          ((4L, 3L), 5.0F))
    val rcvGraph = sc.parallelize(houseEdges)

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, numv, nblock)

    println("Input graph: >\n%s<".format(hc.mFromRcvRdd(rcvGraph, 0.0F).toString(float2Str)))

    //    // ********
    //    // Floyd-Warshall: Distance-only Semirings: Initialize: 1st Pass
    //    // Initialize distance-only Floyd-Warshall
    //    type PathType = Float
    //    val PathNopPlusSr = LagSemiring.min_plus[PathType]
    //    val PathNopMinSr = LagSemiring.nop_min[PathType]
    //
    //    // for edge initialization (weight mapping)
    //    val eInit = (kv: ((Long, Long), Float)) => (kv._1, kv._2.toFloat) // edge (weight)
    //
    //    // for print
    //    val d2Str = float2Str(_)

    // ********
    // Floyd-Warshall: Path-augmented Semiring: Initialize: 2nd Pass
    // Define path-augmented type and semirings

    type PathType = Tuple3[Float, Long, Long]

    // some handy constants
    val FloatInf = LagSemigroup.infinity(classTag[Float])
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val FloatMinf = LagSemigroup.minfinity(classTag[Float])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])
    val NodeNil: Long = -1L

    // Ordering for PathType
    trait PathTypeOrdering extends Ordering[PathType] {
      def compare(ui: PathType, vi: PathType): Int = {
        val w1 = ui._1; val h1 = ui._2; val p1 = ui._3
        val w2 = vi._1; val h2 = vi._2; val p2 = vi._3
        if (w1 < w2) -1
        else if ((w1 == w2) && (h1 < h2)) -1
        else if ((w1 == w2) && (h1 == h2) && (p1 < p2)) -1
        else 1
      }
    }

    // Numeric for PathType
    trait PathTypeAsNumeric
        extends com.ibm.lagraph.LagSemiringAsNumeric[PathType]
        with PathTypeOrdering {
      def plus(ui: PathType, vi: PathType): PathType = {
        def f(x: Float, y: Float): Float =
          if (x == FloatInf || y == FloatInf) FloatInf
          else x + y
        def g(x: Long, y: Long): Long =
          if (x == LongInf || y == LongInf) LongInf
          else x + y

        val _zero = fromInt(0)
        val w1 = ui._1; val h1 = ui._2; val p1 = ui._3
        val w2 = vi._1; val h2 = vi._2; val p2 = vi._3
        if (p2 != _zero._3)
          if (p1 == _zero._3) (f(w1, w2), g(h1, h2), p2)
          else if (p2 != NodeNil) (f(w1, w2), g(h1, h2), p2)
          else (f(w1, w2), g(h1, h2), p1) // original
        else (f(w1, w2), g(h1, h2), p1)
      }
      def times(x: PathType, y: PathType): PathType = min(x, y)
      def fromInt(x: Int): PathType = x match {
        case 0 => ((0.0).toFloat, 0L, NodeNil)
        case 1 => (FloatInf, LongInf, LongInf)
        case other =>
          throw new RuntimeException("fromInt for: >%d< not implemented".format(other))
      }
    }

    implicit object PathTypeAsNumeric extends PathTypeAsNumeric
    val PathNopPlusSr =
      LagSemiring.nop_plus[PathType](Tuple3(FloatInf, LongInf, LongInf))
    val PathNopMinSr = LagSemiring
      .nop_min[PathType](Tuple3(FloatInf, LongInf, LongInf), Tuple3(FloatMinf, LongMinf, LongMinf))

    // for edge initialization (weight mapping)
    val eInit = (kv: ((Long, Long), Float)) => (kv._1, (kv._2.toFloat, 1L, kv._1._2))

    // for print
    val d2Str = pathType2Str(_)

    // ********
    // FloydWarshall: Initialize: Common
    // initialize adjacency matrix
    // strip diagonal and since diagonal is PathNopPlusSr.zero,
    // since diagonal is zero, no need to add back in.
    val rcvAdj = rcvGraph.flatMap { kv =>
      if (kv._1._1 == kv._1._2) None else Some(eInit(kv))
    }
    // use distributed context-specific utility to convert from RDD to LagMatrix
    val mAdj = hc.mFromRcvRdd(rcvAdj, PathNopPlusSr.zero)
    println("mAdj: >\n%s<".format(mAdj.toString(d2Str)))

    // ********
    // FloydWarshall: Iterate: Common
    // iterate
    @tailrec def iterate(k: Long, D: LagMatrix[PathType]): LagMatrix[PathType] =
      if (k == D.size._1) { D } else {
        iterate(k + 1, D.hM(PathNopMinSr, D.oP(PathNopPlusSr, k, D, k)))
      }
    val D = iterate(0L, mAdj)
    println("D: >\n%s<".format(D.toString(d2Str)))

    //        for (k <- 0L until D.size._1) {
    //      if (verbose)
    //        println("****************")
    //      val d = D.oP(PathNopPlusSr, k, D, k)
    //      if (verbose)
    //        println("k: >%d<, d: >\n%s<".format(k, d.toString(float2Str)))
    //      D = D.hM(PathNopMinSr, d)
    //      if (verbose)
    //        println("k: >%d<, D: >\n%s<".format(k, D.toString(float2Str)))
    //    }
    //    D
  }

  /**
    * Prim's algorithm, solves the minimum-spanning-tree problem
    *
    * @param sc the Spark context
    */
  def Prims(sc: SparkContext): Unit = {
    // ********
    // Setup some utility functions

    // some imports ...
    import com.ibm.lagraph.{LagContext, LagSemigroup, LagSemiring, LagVector}
    // for verbose printing
    import scala.reflect.classTag
    def float2Str(f: Float): String = {
      if (f == LagSemigroup.infinity(classTag[Float])) "   inf"
      else if (f == LagSemigroup.minfinity(classTag[Float])) "   minf"
      else "%6.3f".format(f)
    }
    def long2Str(l: Long): String = {
      if (l == LagSemigroup.infinity(classTag[Long])) " inf"
      else if (l == LagSemigroup.minfinity(classTag[Long])) " minf"
      else "%4d".format(l)
    }
    def primType2Str(d: (Float, Long)): String = {
      val d1 = float2Str(d._1)
      val d2 = long2Str(d._2)
      "(%s,%s)".format(d1, d2)
    }
    // ********
    // Create a Simple (directed) Graph from an RDD
    // define graph
    val numv = 5L
    val houseEdges = List(((1L, 0L), 20.0F),
                          ((2L, 0L), 10.0F),
                          ((3L, 1L), 15.0F),
                          ((3L, 2L), 30.0F),
                          ((4L, 2L), 50.0F),
                          ((4L, 3L), 5.0F))
    // For Prim's, make the graph undirected
    val rcvGraph = sc
      .parallelize(houseEdges)
      .flatMap { x =>
        List(((x._1._1, x._1._2), x._2), ((x._1._2, x._1._1), x._2))
      }
      .distinct()

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, numv.toInt, nblock)

    println("Input graph: >\n%s<".format(hc.mFromRcvRdd(rcvGraph, 0.0F).toString(float2Str)))

    // ********
    // Prim's: Path-augmented Semiring: Initialization
    // Define path-augmented type and semirings

    type PrimTreeType = Tuple2[Float, Long]

    // some handy constants
    val FloatInf = LagSemigroup.infinity(classTag[Float])
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val FloatMinf = LagSemigroup.minfinity(classTag[Float])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])
    val NodeNil: Long = -1L

    // Ordering for PrimTreeType
    trait PrimTreeTypeOrdering extends Ordering[PrimTreeType] {
      def compare(ui: PrimTreeType, vi: PrimTreeType): Int = {
        val w1 = ui._1; val p1 = ui._2
        val w2 = vi._1; val p2 = vi._2
        if (w1 < w2) -1
        else if ((w1 == w2) && (p1 < p2)) -1
        else if ((w1 == w2) && (p1 == p2)) 0
        else 1
      }
    }

    // Numeric for PrimTreeType
    trait PrimTreeTypeAsNumeric
        extends com.ibm.lagraph.LagSemiringAsNumeric[PrimTreeType]
        with PrimTreeTypeOrdering {
      def plus(ui: PrimTreeType, vi: PrimTreeType): PrimTreeType =
        throw new RuntimeException(
          "PrimSemiring has nop for addition: ui: >%s<, vi: >%s<".format(ui, vi))
      def times(x: PrimTreeType, y: PrimTreeType): PrimTreeType = min(x, y)
      def fromInt(x: Int): PrimTreeType = x match {
        case 0 => ((0.0).toFloat, NodeNil)
        case 1 => (FloatInf, LongInf)
        case other =>
          throw new RuntimeException(
            "PrimSemiring: fromInt for: >%d< not implemented".format(other))
      }
    }

    implicit object PrimTreeTypeAsNumeric extends PrimTreeTypeAsNumeric
    val PrimSemiring =
      LagSemiring.nop_min[PrimTreeType](Tuple2(FloatInf, LongInf), Tuple2(FloatMinf, LongMinf))

    // ****
    // Need a nop_min semiring Float so add proper behavior for infinity
    type FloatWithInfType = Float

    // Ordering for FloatWithInfType
    trait FloatWithInfTypeOrdering extends Ordering[FloatWithInfType] {
      def compare(ui: FloatWithInfType, vi: FloatWithInfType): Int = {
        compare(ui, vi)
      }
    }
    // Numeric for FloatWithInfType
    trait FloatWithInfTypeAsNumeric
        extends com.ibm.lagraph.LagSemiringAsNumeric[FloatWithInfType]
        with FloatWithInfTypeOrdering {
      def plus(ui: FloatWithInfType, vi: FloatWithInfType): FloatWithInfType = {
        if (ui == FloatInf || vi == FloatInf) FloatInf
        else ui + vi
      }
      def times(ui: FloatWithInfType, vi: FloatWithInfType): FloatWithInfType = {
        if (ui == FloatInf || vi == FloatInf) FloatInf
        else ui + vi
      }
    }

    // ********
    // Algebraic Prim's
    // initialize adjacency matrix
    // strip diagonal and add back in as PathSemiring.one, use weight to derive PathType for edge
    val diagStripped = rcvGraph.flatMap { kv =>
      if (kv._1._1 == kv._1._2) None else Some((kv._1, (kv._2, kv._1._1)))
    }
    val rcvAdj = diagStripped.union(sc.range(0L, numv, 1L, rcvGraph.getNumPartitions).map { i =>
      ((i, i), PrimSemiring.zero)
    })
    // use distributed context-specific utility to convert from RDD to LagMatrix
    val mAdj = hc.mFromRcvRdd(rcvAdj, Tuple2(FloatInf, NodeNil))
    //    // ****************
    //    val mAdjIn = hc.mFromRcvRdd(rcvGraph, 0.0F)
    //    // initialize adjacency matrix
    //    def mInit(v: Float, rc: (Long, Long)): PrimTreeType =
    //      if (rc._1 == rc._2) PrimSemiring.zero
    //      else if (v != 0.0F) Tuple2(v, rc._1)
    //      else Tuple2(FloatInf, NodeNil)
    //
    //    val mAdj = mInit.zipWithIndex(mAdjIn)
    //    // ****************
    println("mAdj: >\n%s<".format(mAdj.toString(primType2Str)))

    val weight_initial = 0

    // arbitrary vertex to start from
    val source = 0L

    // initial membership in spanning tree set
    val s_initial = hc.vReplicate(0.0F).set(source, FloatInf)
    println("s_initial: >\n%s<".format(s_initial.toString(float2Str)))

    // initial membership in spanning tree set
    val s_final_test = hc.vReplicate(FloatInf)
    println("s_final_test: >\n%s<".format(s_final_test.toString(float2Str)))
    //    s_final_test.asInstanceOf[LagDstrVector[PrimTreeType]].dstrBvec.vecRdd.collect.foreach
    //      { case (k, v) => println("s_final_test: (%s,%s): %s".format(k._1, k._2, v)) }

    val d_initial = mAdj.vFromRow(0)
    println("d_initial: >\n%s<".format(d_initial.toString(primType2Str)))

    val pi_initial = hc.vReplicate(NodeNil)
    println("pi_initial: >\n%s<".format(pi_initial.toString(long2Str)))

    @tailrec
    def iterate(
        weight: Float,
        d: LagVector[PrimTreeType],
        s: LagVector[Float],
        pi: LagVector[Long]): (Float, LagVector[PrimTreeType], LagVector[Float], LagVector[Long]) =
      if (s.equiv(s_final_test)) (weight, d, s, pi)
      else {
        //      if (s.equiv(s_final_test)) {
        //        println("DONE")
        //        println("s: >\n%s<".format(s.toString(float2Str)))
        //        println("s_final_test: >\n%s<".format(s_final_test.toString(float2Str)))
        //        (weight, d, s, pi)
        //      } else {
        //        println("  iterate ****************************************")
        //        val pt1 = d.map({ wp: PrimTreeType => wp._1 })
        //        println("    pt1: >\n%s<".format(pt1.toString(float2Str)))
        //        //  val pt2 = pt1.zip(LagSemiring.nop_plus[LongWithInfType].multiplication, s)
        //        val pt2 = pt1.zip(LagSemiring.nop_plus[FloatWithInfType].multiplication, s)
        //        println("    pt2: >\n%s<".format(pt2.toString(float2Str)))
        //        val u = pt2.argmin
        //        println("    u: >%d<".format(u._2))
        //        val s2 = s.set(u._2, FloatInf)
        //        println("    s_i: >\n%s<".format(s2.toString(float2Str)))
        //        s2.asInstanceOf[LagDstrVector[PrimTreeType]].dstrBvec.vecRdd.collect.foreach
        //          { case (k, v) => println("s_i: (%s,%s): %s".format(k._1, k._2, v)) }
        //        val wp = d.ele(u._2)
        //        val weight2 = weight + wp._1.get._1
        //        println("    w_i: >%f<".format(weight2))
        //        val pi2 = pi.set(u._2, wp._1.get._2)
        //        println("    pi_i: >\n%s<".format(pi2.toString(long2Str)))
        //        val aui = mAdj.vFromRow(u._2)
        //        println("    aui: >\n%s<".format(aui.toString(primType2Str)))
        //        val d2 = d.zip(PrimSemiring.multiplication, aui)
        //        println("    d_i: >\n%s<".format(d2.toString(primType2Str)))
        //        iterate(weight2, d2, s2, pi2)
        val u = hc.vArgmin(hc.vZip(LagSemiring.nop_plus[FloatWithInfType].multiplication, hc.vMap({
          wp: PrimTreeType =>
            wp._1
        }, d), s))
        val wp = hc.vEle(d, u._2)
        println("  iterate: wp: >%d<".format(u._2))
        iterate(weight + wp._1.get._1,
                d.zip(PrimSemiring.multiplication, mAdj.vFromRow(u._2)),
                s.set(u._2, FloatInf),
                pi.set(u._2, wp._1.get._2))
      }

    val (weight_final, d_final, s_final, pi_final) =
      iterate(weight_initial, d_initial, s_initial, pi_initial)

    //    println("weight_final: >%f<".format(weight_final))
    //    println("d_final: >\n%s<".format(d_final.toString(primType2Str)))
    //    println("s_final: >\n%s<".format(s_final.toString(float2Str)))
    //    println("pi_final: >\n%s<".format(pi_final.toString(long2Str)))

    println("weight_final: >%f<".format(weight_final))
    println("pi_final: >\n%s<".format(pi_final.toString(long2Str)))

  }

  /**
    * The Bellman-Ford algorithm, solves the single-source shortest paths problem.
    *
    * @param sc the Spark context
    */
  def BellmanFord(sc: SparkContext): Unit = {
    // ********
    // Setup some utility functions: Common

    // some imports ...
    import com.ibm.lagraph.{LagContext, LagSemigroup, LagSemiring, LagVector}
    // for verbose printing
    import scala.reflect.classTag
    def float2Str(f: Float): String = {
      if (f == LagSemigroup.infinity(classTag[Float])) "   inf"
      else if (f == LagSemigroup.minfinity(classTag[Float])) "   minf"
      else "%6.3f".format(f)
    }
    def long2Str(l: Long): String = {
      if (l == LagSemigroup.infinity(classTag[Long])) " inf"
      else if (l == LagSemigroup.minfinity(classTag[Long])) " minf"
      else "%4d".format(l)
    }
    def bfType2Str(d: (Float, Long, Long)): String = {
      val d1 = float2Str(d._1)
      val d2 = long2Str(d._2)
      val d3 = long2Str(d._3)
      "(%s,%s,%s)".format(d1, d2, d3)
    }
    // ********
    // Create a Simple (directed) Graph from an RDD: Common
    // define graph
    val numv = 5L
    val houseEdges = List(((1L, 0L), 20.0F),
                          ((2L, 0L), 10.0F),
                          ((3L, 1L), 15.0F),
                          ((3L, 2L), 30.0F),
                          ((4L, 2L), 50.0F),
                          ((4L, 3L), 5.0F))
    val rcvGraph = sc.parallelize(houseEdges)

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, numv, nblock)

    println("Input graph: >\n%s<".format(hc.mFromRcvRdd(rcvGraph, 0.0F).toString(float2Str)))

    // ********
    // Bellman-Ford: Distance-only Semiring: Initialization: 1st Pass
    // Define distance-only type and semirings
    type PathType = Float
    val PathMinPlusSr = LagSemiring.min_plus[PathType]

    // for edge initialization (weight mapping)
    val eInit = (kv: ((Long, Long), Float)) => (kv._1, kv._2.toFloat) // edge (weight)

    // for print
    val d2Str = float2Str(_)

    //    // ********
    //    // Bellman-Ford: Path-augmented Semiring: Initialization: 2nd Pass
    //    // Define path-augmented type and semirings
    //
    //    type PathType = Tuple3[Float, Long, Long]
    //
    //    // some handy constants
    //    val FloatInf = LagSemigroup.infinity(classTag[Float])
    //    val LongInf = LagSemigroup.infinity(classTag[Long])
    //    val nodeNil: Long = -1L
    //
    //    // Ordering for PathType
    //    trait PathTypeOrdering extends Ordering[PathType] {
    //      def compare(ui: PathType, vi: PathType): Int = {
    //        val w1 = ui._1; val h1 = ui._2; val p1 = ui._3
    //        val w2 = vi._1; val h2 = vi._2; val p2 = vi._3
    //        if (w1 < w2) -1
    //        else if ((w1 == w2) && (h1 < h2)) -1
    //        else if ((w1 == w2) && (h1 == h2) && (p1 < p2)) -1
    //        else 1
    //      }
    //    }
    //
    //    // Numeric for PathType
    //    trait PathTypeAsNumeric extends
    //        com.ibm.lagraph.LagSemiringAsNumeric[PathType] with PathTypeOrdering {
    //      def plus(ui: PathType, vi: PathType): PathType = {
    //        def f(x: Float, y: Float): Float =
    //          if (x == FloatInf || y == FloatInf) FloatInf
    //          else x + y
    //        def g(x: Long, y: Long): Long =
    //          if (x == LongInf || y == LongInf) LongInf
    //          else x + y
    //
    //        val _zero = fromInt(0)
    //        val w1 = ui._1; val h1 = ui._2; val p1 = ui._3
    //        val w2 = vi._1; val h2 = vi._2; val p2 = vi._3
    //        if (p2 != _zero._3)
    //          if (p1 == _zero._3) (f(w1, w2), g(h1, h2), p2)
    //          else if (p2 != nodeNil) (f(w1, w2), g(h1, h2), p2)
    //          else (f(w1, w2), g(h1, h2), p1) // original
    //        else (f(w1, w2), g(h1, h2), p1)
    //      }
    //      def times(x: PathType, y: PathType): PathType = min(x, y)
    //      def fromInt(x: Int): PathType = x match {
    //        case 0     => ((0.0).toFloat, 0L, nodeNil)
    //        case 1     => (FloatInf, LongInf, LongInf)
    //        case other => throw new RuntimeException(
    //          "fromInt for: >%d< not implemented".format(other))
    //      }
    //    }
    //
    //    implicit object PathTypeAsNumeric extends PathTypeAsNumeric
    //    val PathMinPlusSr = LagSemiring.min_plus[PathType](Tuple3(FloatInf, LongInf, LongInf))
    //
    //    // for edge initialization (weight mapping)
    //    val eInit = (kv: ((Long, Long), Float)) =>
    //      (kv._1, (kv._2.toFloat, 1L, kv._1._2)) // edge (weight)
    //
    //    // for print
    //    val d2Str = bfType2Str(_)

    // ********
    // Bellman-Ford: Initialize: Common
    // initialize adjacency matrix
    // strip diagonal and add back in as PathMinPlusSr.one, use weight to derive PathType for edge
    val diagStripped = rcvGraph.flatMap { kv =>
      if (kv._1._1 == kv._1._2) None else Some(eInit(kv))
    }
    val rcvAdj = diagStripped.union(sc.range(0L, numv, 1L, rcvGraph.getNumPartitions).map { i =>
      ((i, i), PathMinPlusSr.one)
    })

    // use distributed context-specific utility to convert from RDD to LagMatrix
    val mAdj = hc.mFromRcvRdd(rcvAdj, PathMinPlusSr.zero)

    println("mAdj: >\n%s<".format(mAdj.toString(d2Str)))

    // initialize vector d w/ source (input)
    val source = 0L
    def dInit(di: PathType, ui: Long): PathType =
      if (ui == source) PathMinPlusSr.one else di
    val d_prev = hc
      .vReplicate(PathMinPlusSr.zero)
      .zipWithIndex(dInit, Option(PathMinPlusSr.zero))
    println("d_initial: >\n%s<".format(d_prev.toString(d2Str)))

    // ********
    // Bellman-Ford: Iterate: Common
    // iterate, relaxing edges
    val maxiter = d_prev.size
    @tailrec def iterate(k: Long, d_prev: LagVector[PathType]): LagVector[PathType] =
      if (k == maxiter) d_prev
      else iterate(k + 1, mAdj.tV(PathMinPlusSr, d_prev))

    val d_final = iterate(0L, d_prev)

    // are we at a fixed point?
    if (d_final.equiv(mAdj.tV(PathMinPlusSr, d_final))) {
      println("d_final: >\n%s<".format(d_final.toString(d2Str)))
    } else {
      println("A negative-weight cycle exists.")
    }
    // ********
    // ********
    // ********
    // ********
    // ********
    // ********
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // settings from environment
    val spark_home = scala.util.Properties
      .envOrElse("SPARK_HOME", "/home/hduser/spark-1.4.1-bin-hadoop2.6")
    println("spark_home: >%s<".format(spark_home))
    val master = "local[1]"
    println("master: >%s<".format(master))
    val sc = new SparkContext(master, "TestMain")
    Fundamental.BellmanFord(sc)
    Fundamental.Prims(sc)
    Fundamental.FloydWarshall(sc)
  }
}
// scalastyle:on println
