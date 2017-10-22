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
// TODO get rid of printlns
// scalastyle:off println

import org.scalatest.FunSuite
import org.scalatest.Matchers
import scala.reflect.ClassTag
import scala.collection.mutable.{Map => MMap}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.ibm.lagraph._

import scala.collection.mutable.ArrayBuffer

class LagDstrMechanicSuite extends FunSuite with Matchers with SharedSparkContext {
  val denseGraphSizes = List(1 << 4, 1 << 5)
  //  val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 29, 1 << 30)
  val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 26, 1 << 27)
  //  val nblocks = List(1 << 0, 1 << 1, 1 << 2, 1 << 3)
  val nblocks = List(1 << 0, 1 << 1, 1 << 2)

  def toArray[A: ClassTag](a: Vector[ArrayBuffer[A]]): Array[Array[A]] = {
    a.map(x => x.toArray).toArray
  }

  test("LagDstrContext.mTranspose") {
    val sparseValueDouble: Double = 0.0
    val sparseValueInt: Int = 0
    for (graphSize <- denseGraphSizes) {
      val nr = graphSize
      var nc = graphSize
      for (nblock <- nblocks) {
        println("LagDstrContext.mTranspose", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val m = hc.mFromMap(
          LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble),
                                     sparseValueDouble),
          sparseValueDouble)
        val mT = m.transpose

        val (mTres, mTresSparse) = hc.mToMap(mT)

        val a = Vector.fill(nr)(ArrayBuffer.fill[Double](nc)(mTresSparse))
        mTres.map { case (k, v) => a(k._1.toInt)(k._2.toInt) = v }

        val b = Vector.fill(nr)(ArrayBuffer.fill[Double](nc)(sparseValueDouble))
        val y = (0 until nr).map { r =>
          (0 until nc).map { c =>
            b(r)(c) = (c * nr + r).toDouble
          }
        }

        assert(toArray(a).deep == toArray(b).deep)
      }
    }
  }

  test("LagDstrContext.vFromMrow") {
    val sparseValueDouble: Double = 0.0
    val sparseValueInt: Int = 0
    for (graphSize <- denseGraphSizes) {
      val nr = graphSize
      var nc = graphSize
      for (nblock <- nblocks) {
        println("LagDstrContext.vFromMrow", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val m = hc.mFromMap(
          LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble),
                                     sparseValueDouble),
          sparseValueDouble)

        val testRow = 1
        val v = hc.vFromMrow(m, testRow)

        val (vResMap, vResSparse) = hc.vToMap(v)

        val vRes = LagContext.vectorFromMap(vResMap, vResSparse, nc)

        assert(vRes.size == nr)
        vRes.zipWithIndex.map {
          case (v, r) => {
            assert(v == (testRow * nr + r).toDouble)
          }
        }
      }
    }
  }

  test("LagDstrContext.vFromMcol") {
    val sparseValueDouble: Double = 0.0
    val sparseValueInt: Int = 0
    for (graphSize <- denseGraphSizes) {
      val nr = graphSize
      var nc = graphSize
      for (nblock <- nblocks) {
        println("LagDstrContext.vFromMrow", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
//        val m = hc.mFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) =>
//          (r * nc + c).toDouble), sparseValueDouble)
        val m = hc.mFromMap(
          LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble),
                                     sparseValueDouble),
          sparseValueDouble)

        val testCol = 1
        val v = hc.vFromMcol(m, testCol)

        val (vResMap, vResSparse) = hc.vToMap(v)

        val vRes = LagContext.vectorFromMap(vResMap, vResSparse, nc)

        assert(vRes.size == nr)
        vRes.zipWithIndex.map {
          case (v, r) => {
            assert(v == (r * nr + testCol).toDouble)
          }
        }
      }
    }
  }

  //  // TODO
  //  test("LagDstrContext.mTranspose") {
  //    for (graphSize <- List(16, 17, 31, 32)) {
  //      for (nblockScale <- List(1, 2, 3, 4)) {
  //        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblockScale)
  //      }
  //    }
  //  }

  //  test("LagDstrContext.mSlice") { // TODO TEST NOT TESTED SINCE DSTR SLICE IS TBD
  //    for (graphSize <- List(16, 17, 31, 32)) {
  //      for (nblockScale <- List(1, 2, 3, 4)) {
  //        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblockScale)
  //        val nr = graphSize
  //        val nc = graphSize
  //        val sparseValue = 0.0
  //        val rRange = (2, 5)
  //        val cRange = (0, nc)
  //        val cSparseIndex = 0
  //        val mMap = Map((0 until graphSize).map { r => (0 until graphSize).map {
  //          c => ((r, c), (r * graphSize + c).toDouble) } }.flatten: _*)
  //        val m = hc.mFromMap[Double](mMap, sparseValue)
  //        //    val vMatrix = Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble)
  //        //    val m = hc.mFromSeqOfSeq(vMatrix, sparseValue)
  //
  //        val mS = hc.mSlice(m, rowRangeOpt = Option(rRange))
  //        val (mSresMap, sv1) = hc.mToMap(mS)
  //        assert(mSresMap.size == (rRange._2 - rRange._1) * graphSize) // keep
  //
  //        val mSres = LagContext.vectorOfVectorFromMap(
  //          mSresMap, sv1, (rRange._2 - rRange._1, graphSize))
  //        mSres.zipWithIndex.map {
  //          case (vr, r) => {
  //            assert(vr.size == graphSize)
  //            vr.zipWithIndex.map {
  //              case (vc, c) => assert(vc == ((r + rRange._1) * nc + c).toDouble)
  //            }
  //          }
  //        }
  //
  //        // repeat for colRangeOpt TODO
  //      }
  //    }
  //  }
}
// scalastyle:on println
