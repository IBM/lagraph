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

import org.scalatest.FunSuite
import org.scalatest.Matchers
import scala.reflect.ClassTag
import scala.collection.mutable.{ Map => MMap }
import com.holdenkarau.spark.testing.SharedSparkContext
import com.ibm.lagraph._

class LagDstrV3Suite extends FunSuite with Matchers with SharedSparkContext {

  def mult[A](a: Vector[Vector[A]], b: Vector[Vector[A]])(implicit n: Numeric[A]) = {
    import n._
    for (row <- a)
      yield for (col <- b.transpose)
      yield row zip col map Function.tupled(_ * _) reduceLeft (_ + _)
  }
  def toArray[A: ClassTag](a: Vector[Vector[A]]): Array[Array[A]] = {
    a.map(x => x.toArray).toArray
  }

  // ********  
  test("LagDstrContext.mTm3") {
    //  def LagDstrContext_mTm3(sc: SparkContext) = {
    val add_mul = LagSemiring.plus_times[Double]
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList
    val sr = LagSemiring.plus_times[Double]
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.mTm", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)

        val nv = graphSize

        val nr = nv
        val nc = nv
        val nA = Vector.tabulate(nv, nv)((r, c) => r * nv + c + 1.0)
        val nB = Vector.tabulate(nv, nv)((r, c) => r * nv + c + 101.0)
        val sparseValue = 0.0
        val mA = hc.mFromMap(LagContext.mapFromSeqOfSeq(nA, sparseValue), sparseValue)
        val mB = hc.mFromMap(LagContext.mapFromSeqOfSeq(nB, sparseValue), sparseValue)

        val mTmRes = hc.mTm(sr, mA, mB)

        val resScala = mult(nA, nB)
        //        println(mA)
        //        println(mB)
        //        println(mTmRes)
        ////        println(toArray(resScala).deep.mkString("\n"))
        assert(toArray(LagContext.vectorOfVectorFromMap(hc.mToMap(mTmRes)._1, sparseValue, (nv.toLong, nv.toLong))).deep == toArray(resScala).deep)
      }
    }
  }

  // ********  
  test("LagDstrContext.mTv3") {
    //  def LagDstrContext_mTv3(sc: SparkContext) = {
    val add_mul = LagSemiring.plus_times[Double]
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.mTv", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        // vector
        val sparseValue = 0.0
        val vMap = Map((0 until graphSize).map { r => (r.toLong, r.toDouble) }: _*)
        val v = hc.vFromMap[Double](vMap, sparseValue)

        // matrix
        val mMap = Map((0 until graphSize).map { r => (0 until graphSize).map { c => ((r.toLong, c.toLong), (r * graphSize + c).toDouble) } }.flatten: _*)
        val m = hc.mFromMap[Double](mMap, sparseValue)

        // multiply
        val u = hc.mTv(add_mul, m, v)

        // back to map
        val (uvm, uvmSparseValue) = hc.vToMap(u)
        assert(uvmSparseValue == sparseValue)

        // compare
        val uvma = LagContext.vectorFromMap(uvm, sparseValue, graphSize)

        val mva = Vector.tabulate(graphSize, graphSize)((r, c) => (r * graphSize + c).toDouble)
        val vva = Vector.tabulate(graphSize, 1)((r, c) => r.toDouble)
        val mua = mult(mva, vva)
        val muat = mua.transpose
        val ua = muat(0)
        assert(ua.corresponds(uvma)(_ == _))
        //  }
      }
    }
  }
  test("LagDstrContext.vIndices3") {
    //  def LagDstrContext_vIndices3(sc: SparkContext) = {
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vIndices", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val start = 100
        val end = start + hc.graphSize
        val v = hc.vIndices(start)
        val vRes = hc.vToVector(v)
        assert(v.size == hc.graphSize)
        //        assert(vRes.isInstanceOf[Vector[Int]])
        assert(vRes.size == (end - start))
        (start until end.toInt).map { r => assert(vRes(r - start) == r) }
      }
    }
  }
  // ********
  test("LagDstrContext.vZip3") {
    //  def LagDstrContext_vZip3(sc: SparkContext) = {
    //    val denseGraphSizes = List(1 << 4, 1 << 5)
    //    //  val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 29, 1 << 30)
    //    val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 26, 1 << 27)
    //    //  val nblocks = List(1 << 0, 1 << 1, 1 << 2, 1 << 3)
    //    val nblocks = List(1 << 0, 1 << 1, 1 << 2)
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList
    val sparseValueDouble: Double = 0.0
    val sparseValueInt: Int = 0
    val offset = 100
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vZip", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        // vectors
        val u = hc.vIndices(0)
        val v = hc.vIndices(offset)
        val f = (a: Long, b: Long) => (a + b).toDouble
        val w = hc.vZip(f, u, v)
        val (wResMap, waSparse) = hc.vToMap(w)
        assert(waSparse == sparseValueDouble)
        val wRes = LagContext.vectorFromMap(wResMap, waSparse, graphSize)
        val vScala = (0 until graphSize).map { a => (a + a + offset).toDouble }.toVector
        assert(wRes.size == graphSize)
        assert(vScala.corresponds(wRes)(_ == _))
      }
    }
  }
  // ********  
  test("LagDstrContext.mZipWithIndex3") {
    //  def LagDstrContext_mZipWithIndex3(sc: SparkContext) = {
    val denseGraphSizes = (1 until 16).toList
    val nblocks = (1 until 12).toList

    val sparseValueDouble: Double = -99.0
    val f = (a: Double, b: (Long, Long)) => Tuple2(a, b)
    for (graphSize <- denseGraphSizes) {
      val nr = graphSize
      val nc = graphSize
      for (nblock <- nblocks) {
        println("LagDstrContext.mZipWithIndex", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val mA = hc.mFromMap(LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble), sparseValueDouble), sparseValueDouble)

        val (mAres, mAresSparse) = hc.mToMap(mA)
        //        println("mAresSparse: >%s<".format(mAresSparse))
        //        println("mA: >%s<".format(LagContext.vectorOfVectorFromMap(mAres, mAresSparse, (nr, nc))))

        val mZipWithIndex = hc.mZipWithIndex(f, mA)

        val (mZipWithIndexResMap, mZipWithIndexResSparse) = hc.mToMap(mZipWithIndex)

        val mZipWithIndexResVector = LagContext.vectorOfVectorFromMap(mZipWithIndexResMap, mZipWithIndexResSparse, (nr, nc))

        var mZipWithIndexActualMap = Map[(Long, Long), (Double, (Long, Long))]()
        (0L until nr).map { r =>
          (0L until nc).map { c =>
            {
              val v = r * nc + c
              mZipWithIndexActualMap = mZipWithIndexActualMap + (Tuple2(r, c) -> Tuple2(v.toDouble, (r, c)))
            }
          }
        }
        val mZipWithIndexActualVector = LagContext.vectorOfVectorFromMap(mZipWithIndexActualMap, mZipWithIndexResSparse, (nr, nc))
        assert(mZipWithIndexResVector.corresponds(mZipWithIndexActualVector)(_ == _))
      }
    }
  }
}

