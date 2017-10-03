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
package com.ibm.lagraph.impl

import scala.reflect.ClassTag

import scala.collection.mutable.{ Map => MMap, ArrayBuffer }

import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import com.ibm.lagraph._
object TestMain {
  def mult[A](a: Vector[Vector[A]], b: Vector[Vector[A]])(implicit n: Numeric[A]) = {
    import n._
    for (row <- a)
      yield for (col <- b.transpose)
      yield row zip col map Function.tupled(_ * _) reduceLeft (_ + _)
  }
  def toArray[A: ClassTag](a: Vector[Vector[A]]): Array[Array[A]] = {
    a.map(x => x.toArray).toArray
  }

  object TestStr extends Serializable {
    /** Numeric for BfSemiringType */
    type BfSemiringType = String
    /** Ordering for BfSemiringType */
    trait BfSemiringTypeOrdering extends Ordering[BfSemiringType] {
      def compare(ui: BfSemiringType, vi: BfSemiringType): Int = {
        if (ui.size < vi.size) -1
        else if (ui.size == vi.size) 0
        else 1
      }
    }
    trait BfSemiringTypeAsNumeric extends LagSemiringAsNumeric[BfSemiringType] with BfSemiringTypeOrdering {
      def plus(ui: BfSemiringType, vi: BfSemiringType): BfSemiringType = {
        ui + "+" + vi
      }
      def times(x: BfSemiringType, y: BfSemiringType): BfSemiringType = {
        x + "*" + y
      }
      def fromInt(x: Int): BfSemiringType = x match {
        case 0     => "ZERO"
        case 1     => "ONE"
        case other => throw new RuntimeException("fromInt for: >%d< not implemented".format(other))
      }
    }
    implicit object BfSemiringTypeAsNumeric extends BfSemiringTypeAsNumeric
    val BfSemiring = LagSemiring.plus_times[BfSemiringType]
  }

  //***************************************************************************
  //***************************************************************************
  //***************************************************************************
  //***************************************************************************
  //***************************************************************************
  import com.ibm.lagraph.{ LagContext, LagSemigroup, LagSemiring, LagVector }
  def fundamentalPrimsForDebug(sc: SparkContext) = {
    // ********
    // Setup some utility functions
    // some imports ...

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
    // Create a Simple Graph from an RDD
    // define graph
    val numv = 5L
    val houseEdges = List(
      ((1L, 0L), 20.0F),
      ((2L, 0L), 10.0F),
      ((3L, 1L), 15.0F),
      ((3L, 2L), 30.0F),
      ((4L, 2L), 50.0F),
      ((4L, 3L), 5.0F))

    // Undirected
    val rcvGraph = sc.parallelize(houseEdges).flatMap {
      x =>
        List(((x._1._1, x._1._2), x._2),
          ((x._1._2, x._1._1), x._2))
    }.distinct()

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, numv, nblock)

    // use distributed context-specific utility to convert from RDD to 
    // adjacency LagMatrix
    val mAdjIn = hc.mFromRcvRdd(rcvGraph, 0.0F)

    println("mAdjIn: >\n%s<".format(hc.mToString(mAdjIn, float2Str)))

    // ********
    // Prim's: Path-augmented Semiring: Initialization

    type PrimSemiringType = Tuple2[Float, Long]

    // some handy constants
    val FloatInf = LagSemigroup.infinity(classTag[Float])
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val FloatMinf = LagSemigroup.minfinity(classTag[Float])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])
    val NodeNil: Long = -1L

    // Ordering for PrimSemiringType
    trait PrimSemiringTypeOrdering extends Ordering[PrimSemiringType] {
      def compare(ui: PrimSemiringType, vi: PrimSemiringType): Int = {
        val w1 = ui._1; val p1 = ui._2
        val w2 = vi._1; val p2 = vi._2
        if (w1 < w2) -1
        else if ((w1 == w2) && (p1 < p2)) -1
        else if ((w1 == w2) && (p1 == p2)) 0
        else 1
      }
    }

    // Numeric for PrimSemiringType
    trait PrimSemiringTypeAsNumeric extends com.ibm.lagraph.LagSemiringAsNumeric[PrimSemiringType] with PrimSemiringTypeOrdering {
      def plus(ui: PrimSemiringType, vi: PrimSemiringType): PrimSemiringType = throw new RuntimeException("PrimSemiring has nop for addition: ui: >%s<, vi: >%s<".format(ui, vi))
      def times(x: PrimSemiringType, y: PrimSemiringType): PrimSemiringType = min(x, y)
      def fromInt(x: Int): PrimSemiringType = x match {
        case 0     => ((0.0).toFloat, NodeNil)
        case 1     => (FloatInf, LongInf)
        case other => throw new RuntimeException("PrimSemiring: fromInt for: >%d< not implemented".format(other))
      }
    }

    implicit object PrimSemiringTypeAsNumeric extends PrimSemiringTypeAsNumeric
    val PrimSemiring = LagSemiring.nop_min[PrimSemiringType](Tuple2(FloatInf, LongInf), Tuple2(FloatMinf, LongMinf))

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
    trait FloatWithInfTypeAsNumeric extends com.ibm.lagraph.LagSemiringAsNumeric[FloatWithInfType] with FloatWithInfTypeOrdering {
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
    def mInit(v: Float, rc: (Long, Long)): PrimSemiringType =
      if (rc._1 == rc._2) PrimSemiring.zero
      else if (v != 0.0F) Tuple2(v, rc._1)
      else Tuple2(FloatInf, NodeNil)

    val mAdj = hc.mZipWithIndex(mInit, mAdjIn)
    println("mAdj: >\n%s<".format(hc.mToString(mAdj, primType2Str)))

    val weight_initial = 0

    // arbitrary vertex to start from
    val source = 0L

    // initial membership in spanning tree set
    val s_initial = hc.vSet(hc.vReplicate(0.0F), source, FloatInf)
    println("s_initial: >\n%s<".format(hc.vToString(s_initial, float2Str)))

    // initial membership in spanning tree set
    val s_final_test = hc.vReplicate(FloatInf)
    println("s_final_test: >\n%s<".format(hc.vToString(s_final_test, float2Str)))
    s_final_test.asInstanceOf[LagDstrVector[PrimSemiringType]].dstrBvec.vecRdd.collect.foreach { case (k, v) => println("s_final_test: (%s,%s): %s".format(k._1, k._2, v)) }

    val d_initial = hc.vFromMrow(mAdj, 0)
    println("d_initial: >\n%s<".format(hc.vToString(d_initial, primType2Str)))

    val pi_initial = hc.vReplicate(NodeNil)
    println("pi_initial: >\n%s<".format(hc.vToString(pi_initial, long2Str)))

    def iterate(weight: Float,
                d: LagVector[PrimSemiringType],
                s: LagVector[Float],
                pi: LagVector[Long]): (Float, LagVector[PrimSemiringType], LagVector[Float], LagVector[Long]) =
      if (hc.vEquiv(s, s_final_test)) {
        println("DONE")
        println("s: >\n%s<".format(hc.vToString(s, float2Str)))
        println("s_final_test: >\n%s<".format(hc.vToString(s_final_test, float2Str)))
        (weight, d, s, pi)
      } else {
        println("  iterate ****************************************")
        val pt1 = hc.vMap({ wp: PrimSemiringType => wp._1 }, d)
        println("    pt1: >\n%s<".format(hc.vToString(pt1, float2Str)))
        //        val pt2 = hc.vZip(LagSemiring.nop_plus[LongWithInfType].multiplication, pt1, s)
        val pt2 = hc.vZip(LagSemiring.nop_plus[FloatWithInfType].multiplication, pt1, s)
        println("    pt2: >\n%s<".format(hc.vToString(pt2, float2Str)))
        val u = hc.vArgmin(pt2)
        println("    u: >%d<".format(u._2))
        val s2 = hc.vSet(s, u._2, FloatInf)
        println("    s_i: >\n%s<".format(hc.vToString(s2, float2Str)))
        s2.asInstanceOf[LagDstrVector[PrimSemiringType]].dstrBvec.vecRdd.collect.foreach { case (k, v) => println("s_i: (%s,%s): %s".format(k._1, k._2, v)) }
        val wp = hc.vEle(d, u._2)
        val weight2 = weight + wp._1.get._1
        println("    w_i: >%f<".format(weight2))
        val pi2 = hc.vSet(pi, u._2, wp._1.get._2)
        println("    pi_i: >\n%s<".format(hc.vToString(pi2, long2Str)))
        val aui = hc.vFromMrow(mAdj, u._2)
        println("    aui: >\n%s<".format(hc.vToString(aui, primType2Str)))
        val d2 = hc.vZip(PrimSemiring.multiplication, d, aui)
        println("    d_i: >\n%s<".format(hc.vToString(d2, primType2Str)))
        iterate(weight2, d2, s2, pi2)
      }

    val (weight_final, d_final, s_final, pi_final) = iterate(weight_initial, d_initial, s_initial, pi_initial)

    println("weight_final: >%f<".format(weight_final))
    println("d_final: >\n%s<".format(hc.vToString(d_final, primType2Str)))
    println("s_final: >\n%s<".format(hc.vToString(s_final, float2Str)))
    println("pi_final: >\n%s<".format(hc.vToString(pi_final, long2Str)))

  }

  //***************************************************************************
  import com.ibm.lagraph.{ LagContext, LagSemigroup, LagSemiring, LagVector }
  def fundamentalPrimsForPub(sc: SparkContext) = {
    // ********
    // Setup some utility functions
    // some imports ...

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
    // Create a Simple Graph from an RDD
    // define graph
    val numv = 5L
    val houseEdges = List(
      ((1L, 0L), 20.0F),
      ((2L, 0L), 10.0F),
      ((3L, 1L), 15.0F),
      ((3L, 2L), 30.0F),
      ((4L, 2L), 50.0F),
      ((4L, 3L), 5.0F))

    // Undirected
    val rcvGraph = sc.parallelize(houseEdges).flatMap {
      x =>
        List(((x._1._1, x._1._2), x._2),
          ((x._1._2, x._1._1), x._2))
    }.distinct()

    // obtain a distributed context for Spark environment
    val nblock = 1 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, numv, nblock)

    // use distributed context-specific utility to convert from RDD to 
    // adjacency LagMatrix
    val mAdjIn = hc.mFromRcvRdd(rcvGraph, 0.0F)

    println("mAdjIn: >\n%s<".format(hc.mToString(mAdjIn, float2Str)))

    // ********
    // Prim's: Path-augmented Semiring: Initialization

    type PrimSemiringType = Tuple2[Float, Long]

    // some handy constants
    val FloatInf = LagSemigroup.infinity(classTag[Float])
    val LongInf = LagSemigroup.infinity(classTag[Long])
    val FloatMinf = LagSemigroup.minfinity(classTag[Float])
    val LongMinf = LagSemigroup.minfinity(classTag[Long])
    val NodeNil: Long = -1L

    // Ordering for PrimSemiringType
    trait PrimSemiringTypeOrdering extends Ordering[PrimSemiringType] {
      def compare(ui: PrimSemiringType, vi: PrimSemiringType): Int = {
        val w1 = ui._1; val p1 = ui._2
        val w2 = vi._1; val p2 = vi._2
        if (w1 < w2) -1
        else if ((w1 == w2) && (p1 < p2)) -1
        else if ((w1 == w2) && (p1 == p2)) 0
        else 1
      }
    }

    // Numeric for PrimSemiringType
    trait PrimSemiringTypeAsNumeric extends com.ibm.lagraph.LagSemiringAsNumeric[PrimSemiringType] with PrimSemiringTypeOrdering {
      def plus(ui: PrimSemiringType, vi: PrimSemiringType): PrimSemiringType = throw new RuntimeException("PrimSemiring has nop for addition: ui: >%s<, vi: >%s<".format(ui, vi))
      def times(x: PrimSemiringType, y: PrimSemiringType): PrimSemiringType = min(x, y)
      def fromInt(x: Int): PrimSemiringType = x match {
        case 0     => ((0.0).toFloat, NodeNil)
        case 1     => (FloatInf, LongInf)
        case other => throw new RuntimeException("PrimSemiring: fromInt for: >%d< not implemented".format(other))
      }
    }

    implicit object PrimSemiringTypeAsNumeric extends PrimSemiringTypeAsNumeric
    val PrimSemiring = LagSemiring.nop_min[PrimSemiringType](Tuple2(FloatInf, LongInf), Tuple2(FloatMinf, LongMinf))

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
    trait FloatWithInfTypeAsNumeric extends com.ibm.lagraph.LagSemiringAsNumeric[FloatWithInfType] with FloatWithInfTypeOrdering {
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
    def mInit(v: Float, rc: (Long, Long)): PrimSemiringType =
      if (rc._1 == rc._2) PrimSemiring.zero
      else if (v != 0.0F) Tuple2(v, rc._1)
      else Tuple2(FloatInf, NodeNil)

    val mAdj = hc.mZipWithIndex(mInit, mAdjIn)
    println("mAdj: >\n%s<".format(hc.mToString(mAdj, primType2Str)))

    val weight_initial = 0

    // arbitrary vertex to start from
    val source = 0L

    // initial membership in spanning tree set
    val s_initial = hc.vSet(hc.vReplicate(0.0F), source, FloatInf)

    // initial membership in spanning tree set
    val s_final_test = hc.vReplicate(FloatInf)

    val d_initial = hc.vFromMrow(mAdj, 0)

    val pi_initial = hc.vReplicate(NodeNil)

    def iterate(weight: Float,
                d: LagVector[PrimSemiringType],
                s: LagVector[Float],
                pi: LagVector[Long]): (Float, LagVector[PrimSemiringType], LagVector[Float], LagVector[Long]) =
      if (hc.vEquiv(s, s_final_test)) (weight, d, s, pi) else {
        println("  iterate ****************************************")
        val u = hc.vArgmin(hc.vZip(LagSemiring.nop_plus[FloatWithInfType].multiplication, hc.vMap({ wp: PrimSemiringType => wp._1 }, d), s))
        val wp = hc.vEle(d, u._2)
        val aui = hc.vFromMrow(mAdj, u._2)
        iterate(weight + wp._1.get._1, hc.vZip(PrimSemiring.multiplication, d, aui), hc.vSet(s, u._2, FloatInf), hc.vSet(pi, u._2, wp._1.get._2))
      }

    val (weight_final, d_final, s_final, pi_final) = iterate(weight_initial, d_initial, s_initial, pi_initial)

    println("weight_final: >%f<".format(weight_final))
    println("d_final: >\n%s<".format(hc.vToString(d_final, primType2Str)))
    println("s_final: >\n%s<".format(hc.vToString(s_final, float2Str)))
    println("pi_final: >\n%s<".format(hc.vToString(pi_final, long2Str)))

  }

  //  test("LagDstrContext.vEquiv3") {
  def LagDstrContext_vEquiv3(sc: SparkContext) = {
    import scala.reflect.classTag
    def long2Str(l: Long): String = {
      if (l == LagSemigroup.infinity(classTag[Long])) " inf"
      else if (l == LagSemigroup.minfinity(classTag[Long])) " minf"
      else "%4d".format(l)
    }
    val denseGraphSizes = (2 until 16).toList
    val nblocks = (1 until 12).toList
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vEquiv 01", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val LongInf = LagSemigroup.infinity(classTag[Long])
        val source = 0L
        val s_1 = hc.vSet(hc.vReplicate(0L), source, LongInf)
        val s_2 = hc.vReplicate(LongInf)
        //        println("s_1: >\n%s<".format(hc.vToString(s_1, long2Str)))
        //        println("s_2: >\n%s<".format(hc.vToString(s_2, long2Str)))
        require(!hc.vEquiv(s_1, s_2))
      }
    }
  }
  // ********
  //  test("LagDstrContext.vZipWithIndex3") {
  def LagDstrContext_vZipWithIndex3(sc: SparkContext) = {
    val denseGraphSizes = (2 until 16).toList
    val nblocks = (1 until 24).toList
    val sparseValueInt = 0
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vZipWithIndex", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val u = hc.vIndices(0)
        val f = (a: Long, b: Long) => (a, b)
        val w = hc.vZipWithIndex(f, u)
        val wRes = hc.vToVector(w)
        val vScala = (0 until graphSize).map { a => Tuple2(a, a) }.toVector
        assert(wRes.size == graphSize)
        assert(vScala.corresponds(wRes)(_ == _))
      }
    }
  }
  // ********
  //  test("LagDstrContext.vZipWithIndexSparse3") {
  def LagDstrContext_vZipWithIndexSparse3(sc: SparkContext) = {
    val denseGraphSizes = (2 until 16).toList
    val nblocks = (1 until 24).toList
    val sparseValueInt = 0
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vZipWithIndexSparse", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val u = hc.vReplicate(0L)
        val f = (a: Long, b: Long) => (a, b)
        val w = hc.vZipWithIndex(f, u)
        val wRes = hc.vToVector(w)
        val vScala = (0 until graphSize).map { a => Tuple2(0L, a) }.toVector
        assert(wRes.size == graphSize)
        assert(vScala.corresponds(wRes)(_ == _))
      }
    }
  }
  // ********  
  //  test("LagDstrContext.mZipWithIndexSparse3") {
  def LagDstrContext_mZipWithIndexSparse3(sc: SparkContext) = {
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 7).toList

    val (sparseNr, sparseNc) = (1, 1)
    val sparseValueDouble: Double = -99.0
    for (graphSize <- denseGraphSizes) {
      val nr = graphSize
      val nc = graphSize
      for (nblock <- nblocks) {
        println("LagDstrContext.mZipWithIndex", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val mA = hc.mFromMap(LagContext.mapFromSeqOfSeq(Vector.tabulate(sparseNr, sparseNc)((r, c) => (r * nc + c).toDouble), sparseValueDouble), sparseValueDouble)

        val (mAres, mAresSparse) = hc.mToMap(mA)
        //        println("mAresSparse: >%s<".format(mAresSparse))
        //        println("mA: >%s<".format(LagContext.vectorOfVectorFromMap(mAres, mAresSparse, (nr, nc))))

        val f = (a: Double, b: (Long, Long)) => Tuple2(a, b)
        val mZipWithIndex = hc.mZipWithIndex(f, mA)
        //        println("mZipWithIndex: >\n%s<".format(hc.mToString(mZipWithIndex, {v:(Double,(Long, Long)) => "%s".format(v)})))
        //
        val (mZipWithIndexResMap, mZipWithIndexResSparse) = hc.mToMap(mZipWithIndex)

        val mZipWithIndexResVector = LagContext.vectorOfVectorFromMap(mZipWithIndexResMap, mZipWithIndexResSparse, (nr, nc))

        var mZipWithIndexActualMap = Map[(Long, Long), (Double, (Long, Long))]()
        (0L until nr).map { r =>
          (0L until nc).map { c =>
            {
              val v = r * nc + c
              mZipWithIndexActualMap = mZipWithIndexActualMap + (Tuple2(r, c) -> Tuple2(sparseValueDouble, (r, c)))
            }
          }
        }
        (0L until sparseNr).map { r =>
          (0L until sparseNc).map { c =>
            {
              val v = r * nc + c
              mZipWithIndexActualMap = mZipWithIndexActualMap + (Tuple2(r, c) -> Tuple2(v.toDouble, (r, c)))
            }
          }
        }
        val mZipWithIndexActualVector = LagContext.vectorOfVectorFromMap(mZipWithIndexActualMap, mZipWithIndexResSparse, (nr, nc))
        //        println("mZipWithIndexResVector: >%s<".format(toArray(mZipWithIndexActualVector).deep.mkString("\n")))
        assert(mZipWithIndexResVector.corresponds(mZipWithIndexActualVector)(_ == _))
      }
    }
  }
  def isolate(sc: SparkContext) = {
    val numv = 5
    val nblock = 2 // set parallelism (blocks on one axis)
    val hc = LagContext.getLagDstrContext(sc, numv.toInt, nblock)
    val s_final_test = hc.vReplicate(99)
    val s_i = hc.vSet(hc.vReplicate(99), 4, -99)
    val test = hc.vEquiv(s_final_test, s_i)
    println("test: >%s<".format(test))
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // settings from environment
    val spark_home = scala.util.Properties.envOrElse("SPARK_HOME", "/home/hduser/spark-1.4.1-bin-hadoop2.6")
    println("spark_home: >%s<".format(spark_home))
    val master = "local[1]"
    println("master: >%s<".format(master))
    val sc = new SparkContext(master, "TestMain")
    //    TestMain.testit(sc)
    //    TestMain.LagDstrContext_mTv(sc)
    //    TestMain.LagDstrContext_mTv3(sc)
    //    TestMain.LagDstrContext_mTm3(sc)
    //    TestMain.LagDstrContext_vFromMap_Dense(sc)
    //    TestMain.LagDstrContext_vIndices(sc)
    //    TestMain.LagDstrContext_vIndices2(sc)
    //    TestMain.LagDstrContext_vIndices3(sc)
    //    TestMain.LagDstrContext_vZip(sc)
    //        TestMain.LagDstrContext_vZip3(sc)
    //    TestMain.LagSmpContext_vReduceWithIndex(sc)
    //        TestMain.LagDstrContext_mZipWithIndex3(sc)
    TestMain.fundamentalPrimsForPub(sc)
    //        isolate(sc)
    //        TestMain.LagDstrContext_vEquiv3(sc)
    //    TestMain.LagDstrContext_vZipWithIndex3(sc)
    //    TestMain.LagDstrContext_vZipWithIndexSparse3(sc)
    //    TestMain.LagDstrContext_mZipWithIndexSparse3(sc)
  }
}
