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

class LagDstrImportExportSuite extends FunSuite with Matchers with SharedSparkContext {

  val lar = 1L
  val lac = 2L
  // TODO activate when INT vs LONG problem is fixed
  val denseGraphSizes = List(1 << 4, 1 << 5)
  //  val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 29, 1 << 30)
  val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 26, 1 << 27)
  val nblocks = List(1 << 0, 1 << 1, 1 << 2, 1 << 3)
  // ********
  test("LagDstrContext.mFromMap Empty") {
    for (graphSize <- sparseGraphSizes) {
      for (nblock <- nblocks) {
//        println("LagDstrContext.mFromMap Empty graphSize: >%d<, nblock: >%d<".format(graphSize, nblock))
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val sparseValue = 0.0
        val mMap = Map[(Long, Long), Double]()
        val m = hc.mFromMap[Double](mMap, sparseValue)
        val (mMapRes1, sv1) = hc.mToMap(m)
        //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
        assert(mMapRes1.size == 0)
        assert(sv1.isInstanceOf[Double])
        assert(sv1 == sparseValue)

        // mToMapOfMap not tested TODO
        // mToVectorOfVector not tested TODO
      }
    }
  }
  test("LagDstrContext.mFromMap Sparse") {
    for (graphSize <- sparseGraphSizes) {
      for (nblock <- nblocks) {
        val lbr = (1L << graphSize) - 1L
        val lbc = (1L << graphSize) - 1L
//        println("LagDstrContext.mFromMap Sparse graphSize: >%d<, nblock: >%d<".format(graphSize, nblock))
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val sparseValue = 0.0
        val testValue = 99.0
        val mMap = Map[(Long, Long), Double]((lar, lac) -> testValue, (lbr, lbc) -> testValue)
        val m = hc.mFromMap[Double](mMap, sparseValue)
        val (mMapRes1, sv1) = hc.mToMap(m)
        assert(mMapRes1.size == 2)
        assert(mMapRes1((lar, lac)) == testValue)
        assert(mMapRes1((lbr, lbc)) == testValue)
        assert(sv1.isInstanceOf[Double])
        assert(sv1 == sparseValue)

        // mToMapOfMap not tested TODO
        // mToVectorOfVector not tested TODO
      }
    }
  }
  test("LagDstrContext.mFromMap Dense") {
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val sparseValue = 0.0
        val rSparseIndex = 0
        val cSparseIndex = 0
        val mMap = Map((0 until graphSize).map { r => (0 until graphSize).map { c => ((r.toLong, c.toLong), (r * graphSize + c).toDouble) } }.flatten: _*)
        val m = hc.mFromMap[Double](mMap, sparseValue)
        val (mMapRes1, sv1) = hc.mToMap(m)
        //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
        assert(mMapRes1.size == graphSize * graphSize - 1)
        (0 until graphSize).map { r => (0 until graphSize).map { c => if (r != rSparseIndex && c != cSparseIndex) assert(mMapRes1((r, c)) == (r * graphSize + c).toDouble) } }
        assert(sv1.isInstanceOf[Double])
        assert(sv1 == sparseValue)

        // mToMapOfMap not tested TODO
        // mToVectorOfVector not tested TODO
      }
    }
  }
  test("LagDstrContext.mFromMap Sparse Long") {
    for (graphSize <- sparseGraphSizes) {
      for (nblock <- nblocks) {
        val lbr = (1L << graphSize) - 1L
        val lbc = (1L << graphSize) - 1L
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val sparseValue = 0.0
        val testValue = 99.0
        val mMap = Map[(Long, Long), Double]((lar, lac) -> testValue, (lbr, lbc) -> testValue)
        val m = hc.mFromMap[Double](mMap, sparseValue)
        val (mMapRes1, sv1) = hc.mToMap(m)
        //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
        assert(mMapRes1.size == 2)
        assert(mMapRes1((lar, lac)) == testValue)
        assert(mMapRes1((lbr, lbc)) == testValue)
        assert(sv1.isInstanceOf[Double])
        assert(sv1 == sparseValue)

        // mToMapOfMap not tested TODO
        // mToVectorOfVector not tested TODO
      }
    }
  }
  //****
  test("LagDstrContext.vFromMap Empty") {
    for (graphSize <- sparseGraphSizes) {
      for (nblock <- nblocks) {
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val sparseValue = 0.0
        val vMap = Map[Long, Double]()
        val v = hc.vFromMap[Double](vMap, sparseValue)
        val (vMapRes1, sv1) = hc.vToMap(v)
        //    println("vMapRes1: >%s<, sv1: >%s<".format(vMapRes1, sv1))
        assert(vMapRes1.size == 0)
        assert(sv1.isInstanceOf[Double])
        assert(sv1 == sparseValue)

        // vToVector not tested TODO
      }
    }
  }
  test("LagDstrContext.vFromMap Sparse") {
    for (graphSize <- sparseGraphSizes) {
      for (nblock <- nblocks) {
        val lbr = (1L << graphSize) - 1L
        val lbc = (1L << graphSize) - 1L
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val sparseValue = 0.0
        val testValue = 99.0
        val vMap = Map[Long, Double](lar -> testValue, lbr -> testValue)
        val v = hc.vFromMap[Double](vMap, sparseValue)
        val (vMapRes1, sv1) = hc.vToMap(v)
        //    println("vMapRes1: >%s<, sv1: >%s<".format(vMapRes1, sv1))
        assert(vMapRes1.size == 2)
        assert(vMapRes1(lar) == testValue)
        assert(vMapRes1(lbr) == testValue)
        assert(sv1.isInstanceOf[Double])
        assert(sv1 == sparseValue)

        // vToVector not tested TODO
      }
    }
  }
  test("LagDstrContext.vFromMap Dense") {
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val sparseValue = 0.0
        val sparseIndex = 0
        val vMap = Map((0 until graphSize).map { r => (r.toLong, r.toDouble) }: _*)
        val v = hc.vFromMap[Double](vMap, sparseValue)
        val (vMapRes1, sv1) = hc.vToMap(v)
        //    println("vMapRes1: >%s<, sv1: >%s<".format(vMapRes1, sv1))
        assert(vMapRes1.size == graphSize - 1)
        (0 until graphSize).map { r => if (r != sparseIndex) assert(vMapRes1(r) == r.toDouble) }
        assert(sv1.isInstanceOf[Double])
        assert(sv1 == sparseValue)

        // vToVector not tested TODO
      }
    }
  }
  // ********
  test("LagDstrContext.mToRcvRdd Empty") {
    for (graphSize <- sparseGraphSizes) {
      for (nblock <- nblocks) {
        val hc: LagDstrContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val sparseValue = 0.0
        val mMap = Map[(Long, Long), Double]()
        val m = hc.mFromMap[Double](mMap, sparseValue)
        val (mMapRes1Rdd, sv1) = hc.mToRcvRdd(m)
        val mMapRes1 = mMapRes1Rdd.collect.toMap
        //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
        assert(mMapRes1.size == 0)
        assert(sv1.isInstanceOf[Double])
        assert(sv1 == sparseValue)
      }
    }
  }
  test("LagDstrContext.mToRcvRdd Sparse") {
    for (graphSize <- sparseGraphSizes) {
      for (nblock <- nblocks) {
        val lbr = (1L << graphSize) - 1L
        val lbc = (1L << graphSize) - 1L
        val hc: LagDstrContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val sparseValue = 0.0
        val testValue = 99.0
        val mMap = Map[(Long, Long), Double]((lar, lac) -> testValue, (lbr, lbc) -> testValue)
        val m = hc.mFromMap[Double](mMap, sparseValue)
        val (mMapRes1Rdd, sv1) = hc.mToRcvRdd(m)
        val mMapRes1 = mMapRes1Rdd.collect.toMap
        //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
        assert(mMapRes1.size == 2)
        assert(mMapRes1((lar, lac)) == testValue)
        assert(mMapRes1((lbr, lbc)) == testValue)
        assert(sv1.isInstanceOf[Double])
        assert(sv1 == sparseValue)
      }
    }
  }
  test("LagDstrContext.mToRcvRdd Dense") {
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        val hc: LagDstrContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val sparseValue = 0.0
        val rSparseIndex = 0
        val cSparseIndex = 0
        val mMap = Map((0 until graphSize).map { r => (0 until graphSize).map { c => ((r.toLong, c.toLong), (r * graphSize + c).toDouble) } }.flatten: _*)
        val m = hc.mFromMap[Double](mMap, sparseValue)
        val (mMapRes1Rdd, sv1) = hc.mToRcvRdd(m)
        val mMapRes1 = mMapRes1Rdd.collect.toMap
        //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
        assert(mMapRes1.size == graphSize * graphSize - 1)
        (0 until graphSize).map { r => (0 until graphSize).map { c => if (r != rSparseIndex && c != cSparseIndex) assert(mMapRes1((r, c)) == (r * graphSize + c).toDouble) } }
        assert(sv1.isInstanceOf[Double])
        assert(sv1 == sparseValue)
      }
    }
  }
  // TOOD add tests for vToRvRdd
}

