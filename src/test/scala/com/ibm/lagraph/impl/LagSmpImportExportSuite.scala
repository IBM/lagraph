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
import com.ibm.lagraph._

class LagSmpImportExportSuite extends FunSuite with Matchers {

  test("LagSmpContext.mFromMap Empty") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val sparseValue = 0.0
    val mMap = Map[(Long, Long), Double]()
    val m = hc.mFromMap[Double](mMap, sparseValue)
    val (mMapRes1, sv1) = hc.mToMap(m)
    //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
//    assert(mMapRes1.isInstanceOf[Map[(Int, Int), Double]])
    assert(mMapRes1.size == 0)
    assert(sv1.isInstanceOf[Double])
    assert(sv1 == sparseValue)
//    val (mMapRes2, sv2) = hc.mToMapOfMap(m)
//    //    println("mMapRes2: >%s<, sv: >%s<".format(mMapRes2, sv2))
//    assert(mMapRes2.isInstanceOf[Map[Int, Map[Int, Double]]])
//    assert(mMapRes2.size == 0)
//    assert(sv2.isInstanceOf[Double])
//    assert(sv2 == sparseValue)
    val (vvm,vvs) = hc.mToMap(m)
    val mMapRes3 = LagContext.vectorOfVectorFromMap(vvm, vvs, (nv,nv))
    //    println("mMapRes3: >%s<".format(mMapRes3))
    assert(mMapRes3.isInstanceOf[Vector[Vector[Double]]])
    assert(mMapRes3.size == nv)
    mMapRes3.map { vr => vr.map { v => assert(v == sparseValue) } }
  }
  test("LagSmpContext.mFromMap Sparse") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val sparseValue = 0.0
    val singleValue = 99.0
    val mMap = Map[(Long, Long), Double]((1L, 2L) -> singleValue)
    val m = hc.mFromMap[Double](mMap, sparseValue)
    val (mMapRes1, sv1) = hc.mToMap(m)
    //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
//    assert(mMapRes1.isInstanceOf[Map[(Int, Int), Double]])
    assert(mMapRes1.size == 1)
    assert(mMapRes1((1, 2)) == 99.0)
    assert(sv1.isInstanceOf[Double])
    assert(sv1 == sparseValue)
//    val (mMapRes2, sv2) = hc.mToMapOfMap(m)
//    //    println("mMapRes2: >%s<, sv: >%s<".format(mMapRes2, sv2))
//    assert(mMapRes2.isInstanceOf[Map[Int, Map[Int, Double]]])
//    assert(mMapRes2.size == 1)
//    assert(mMapRes2(1)(2) == 99.0)
//    assert(sv2.isInstanceOf[Double])
//    assert(sv2 == sparseValue)
    val (vvm,vvs) = hc.mToMap(m)
    val mMapRes3 = LagContext.vectorOfVectorFromMap(vvm, vvs, (nv,nv))
    //    println("mMapRes3: >%s<".format(mMapRes3))
    assert(mMapRes3.isInstanceOf[Vector[Vector[Double]]])
    assert(mMapRes3.size == nv)
    mMapRes3.zipWithIndex.map {
      case (vr, r) => vr.zipWithIndex.map {
        case (vc, c) => assert(if (((r == 1) && (c == 2)) && (vc == singleValue)) true else (vc == sparseValue))
      }
    }
  }
  test("LagSmpContext.mFromMap Dense") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val sparseValue = 0.0
    val rSparseIndex = 0
    val cSparseIndex = 0
    // could have used a mutable map ...
    //    val mm = MMap[(Int, Int), Double]()
    //    (0 until nv).map { r => (0 until nv).map { c => mm((r, c)) = (r * nv + c).toDouble } }
    //    val mMap = mm.toMap
    // but flatten is neater ...
    val mMap = Map((0 until nv).map { r => (0 until nv).map { c => ((r.toLong, c.toLong), (r * nv + c).toDouble) } }.flatten: _*)
    val m = hc.mFromMap[Double](mMap, sparseValue)
    val (mMapRes1, sv1) = hc.mToMap(m)
    //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
//    assert(mMapRes1.isInstanceOf[Map[(Int, Int), Double]])
    assert(mMapRes1.size == nv * nv - 1)
    (0 until nv).map { r => (0 until nv).map { c => if (r != rSparseIndex && c != cSparseIndex) assert(mMapRes1((r, c)) == (r * nv + c).toDouble) } }
    assert(sv1.isInstanceOf[Double])
    assert(sv1 == sparseValue)
//    val (mMapRes2, sv2) = hc.mToMapOfMap(m)
//    //    println("mMapRes2: >%s<, sv: >%s<".format(mMapRes2, sv2))
//    assert(mMapRes2.isInstanceOf[Map[Int, Map[Int, Double]]])
//    assert(mMapRes2.size == nv)
//    //    println("XXXXX: >%d<".format(mMapRes2.foldLeft(0)(_ + _._2.size)))
//    assert(mMapRes2.foldLeft(0)(_ + _._2.size) == nv * nv - 1)
//    (0 until nv).map { r => (0 until nv).map { c => if (r != rSparseIndex && c != cSparseIndex) assert(mMapRes2(r)(c) == (r * nv + c).toDouble) } }
//    assert(sv2.isInstanceOf[Double])
//    assert(sv2 == sparseValue)
    val (vvm,vvs) = hc.mToMap(m)
    val mMapRes3 = LagContext.vectorOfVectorFromMap(vvm, vvs, (nv,nv))
    //    println("mMapRes3: >%s<".format(mMapRes3))
    assert(mMapRes3.isInstanceOf[Vector[Vector[Double]]])
    assert(mMapRes3.size == nv)
    mMapRes3.zipWithIndex.map {
      case (vr, r) => vr.zipWithIndex.map {
        case (vc, c) => assert(vc == (r * nv + c).toDouble)
      }
    }
  }

//  test("LagSmpContext.mFromMapOfMap Empty") {
//    val nv = 10
//    val hc: LagContext = LagContext.getLagSmpContext(nv)
//    val sparseValue = 0.0
//    val mMap = Map[Long, Map[Long, Double]]()
//    val m = hc.mFromMapOfMap[Double](mMap, sparseValue)
//    val (mMapRes1, sv1) = hc.mToMap(m)
//    //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
//    assert(mMapRes1.isInstanceOf[Map[(Int, Int), Double]])
//    assert(mMapRes1.size == 0)
//    assert(sv1.isInstanceOf[Double])
//    assert(sv1 == sparseValue)
//    val (mMapRes2, sv2) = hc.mToMapOfMap(m)
//    //    println("mMapRes2: >%s<, sv: >%s<".format(mMapRes2, sv2))
//    assert(mMapRes2.isInstanceOf[Map[Int, Map[Int, Double]]])
//    assert(mMapRes2.size == 0)
//    assert(sv2.isInstanceOf[Double])
//    assert(sv2 == sparseValue)
//    val mMapRes3 = hc.mToVectorOfVector(m)
//    //    println("mMapRes3: >%s<".format(mMapRes3))
//    assert(mMapRes3.isInstanceOf[Vector[Vector[Double]]])
//    assert(mMapRes3.size == nv)
//    mMapRes3.map { vr => vr.map { v => assert(v == sparseValue) } }
//  }
//  test("LagSmpContext.mFromMapOfMap Sparse") {
//    val nv = 10
//    val hc: LagContext = LagContext.getLagSmpContext(nv)
//    val sparseValue = 0.0
//    val singleValue = 99.0
//    val mMap = Map(1L -> Map(2L -> singleValue))
//    val m = hc.mFromMapOfMap[Double](mMap, sparseValue)
//    val (mMapRes1, sv1) = hc.mToMap(m)
//    //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
//    assert(mMapRes1.isInstanceOf[Map[(Int, Int), Double]])
//    assert(mMapRes1.size == 1)
//    assert(mMapRes1((1, 2)) == 99.0)
//    assert(sv1.isInstanceOf[Double])
//    assert(sv1 == sparseValue)
//    val (mMapRes2, sv2) = hc.mToMapOfMap(m)
//    //    println("mMapRes2: >%s<, sv: >%s<".format(mMapRes2, sv2))
//    assert(mMapRes2.isInstanceOf[Map[Int, Map[Int, Double]]])
//    assert(mMapRes2.size == 1)
//    assert(mMapRes2(1)(2) == 99.0)
//    assert(sv2.isInstanceOf[Double])
//    assert(sv2 == sparseValue)
//    val mMapRes3 = hc.mToVectorOfVector(m)
//    //    println("mMapRes3: >%s<".format(mMapRes3))
//    assert(mMapRes3.isInstanceOf[Vector[Vector[Double]]])
//    assert(mMapRes3.size == nv)
//    mMapRes3.zipWithIndex.map {
//      case (vr, r) => vr.zipWithIndex.map {
//        case (vc, c) => assert(if (((r == 1) && (c == 2)) && (vc == singleValue)) true else (vc == sparseValue))
//      }
//    }
//  }
//  test("LagSmpContext.mFromMapOfMap Dense") {
//    val nv = 10
//    val hc: LagContext = LagContext.getLagSmpContext(nv)
//    val sparseValue = 0.0
//    val rSparseIndex = 0
//    val cSparseIndex = 0
//    val mMap = Map((0 until nv).map { r => (r.toLong, Map((0 until nv).map { c => (c.toLong, (r * nv + c).toDouble) }: _*)) }: _*)
//    //    (0 until nv).map { r => MMap() }.
//    //    val mm = Map[Int, Double]()
//    //    (0 until nv).map { r => (0 until nv).map { c => mm((r, c)) = (r * nv + c).toDouble } }
//    //    val mMap = mm.toMap
//    val m = hc.mFromMapOfMap[Double](mMap, sparseValue)
//    val (mMapRes1, sv1) = hc.mToMap(m)
//    //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
//    assert(mMapRes1.isInstanceOf[Map[(Int, Int), Double]])
//    assert(mMapRes1.size == nv * nv - 1)
//    (0 until nv).map { r => (0 until nv).map { c => if (r != rSparseIndex && c != rSparseIndex) assert(mMapRes1((r, c)) == (r * nv + c).toDouble) } }
//    assert(sv1.isInstanceOf[Double])
//    assert(sv1 == sparseValue)
//    val (mMapRes2, sv2) = hc.mToMapOfMap(m)
//    //    println("mMapRes2: >%s<, sv: >%s<".format(mMapRes2, sv2))
//    assert(mMapRes2.isInstanceOf[Map[Int, Map[Int, Double]]])
//    assert(mMapRes2.size == nv)
//    //    println("XXXXX: >%d<".format(mMapRes2.foldLeft(0)(_ + _._2.size)))
//    assert(mMapRes2.foldLeft(0)(_ + _._2.size) == nv * nv - 1)
//    (0 until nv).map { r => (0 until nv).map { c => if (r != rSparseIndex && c != cSparseIndex) assert(mMapRes2(r)(c) == (r * nv + c).toDouble) } }
//    assert(sv2.isInstanceOf[Double])
//    assert(sv2 == sparseValue)
//    val mMapRes3 = hc.mToVectorOfVector(m)
//    //    println("mMapRes3: >%s<".format(mMapRes3))
//    assert(mMapRes3.isInstanceOf[Vector[Vector[Double]]])
//    assert(mMapRes3.size == nv)
//    mMapRes3.zipWithIndex.map {
//      case (vr, r) => vr.zipWithIndex.map {
//        case (vc, c) => assert(vc == (r * nv + c).toDouble)
//      }
//    }
//  }
//
//  test("LagSmpContext.mFromSeqOfSeq Empty") {
//    val nv = 10
//    val hc: LagContext = LagContext.getLagSmpContext(nv)
//    val sparseValue = 0.0
//    val vMatrix = Vector.tabulate(nv, nv)((r, c) => sparseValue)
//    val m = hc.mFromSeqOfSeq(vMatrix, sparseValue)
//    val (mMapRes1, sv1) = hc.mToMap(m)
//    //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
//    assert(mMapRes1.isInstanceOf[Map[(Int, Int), Double]])
//    assert(mMapRes1.size == 0)
//    assert(sv1.isInstanceOf[Double])
//    assert(sv1 == sparseValue)
//    val (mMapRes2, sv2) = hc.mToMapOfMap(m)
//    //    println("mMapRes2: >%s<, sv: >%s<".format(mMapRes2, sv2))
//    assert(mMapRes2.isInstanceOf[Map[Int, Map[Int, Double]]])
//    assert(mMapRes2.size == 0)
//    assert(sv2.isInstanceOf[Double])
//    assert(sv2 == sparseValue)
//    val mMapRes3 = hc.mToVectorOfVector(m)
//    //    println("mMapRes3: >%s<".format(mMapRes3))
//    assert(mMapRes3.isInstanceOf[Vector[Vector[Double]]])
//    assert(mMapRes3.size == nv)
//    mMapRes3.map { vr => vr.map { v => assert(v == sparseValue) } }
//  }
//  test("LagSmpContext.mFromSeqOfSeq Sparse") {
//    val nv = 10
//    val hc: LagContext = LagContext.getLagSmpContext(nv)
//    val sparseValue = 0.0
//    val singleValue = 99.0
//    val vMatrix = Vector.tabulate(nv, nv)((r, c) => if ((r, c) == (1, 2)) singleValue else sparseValue)
//    val m = hc.mFromSeqOfSeq(vMatrix, sparseValue)
//    val (mMapRes1, sv1) = hc.mToMap(m)
//    //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
//    assert(mMapRes1.isInstanceOf[Map[(Int, Int), Double]])
//    assert(mMapRes1.size == 1)
//    assert(mMapRes1((1, 2)) == 99.0)
//    assert(sv1.isInstanceOf[Double])
//    assert(sv1 == sparseValue)
//    val (mMapRes2, sv2) = hc.mToMapOfMap(m)
//    //    println("mMapRes2: >%s<, sv: >%s<".format(mMapRes2, sv2))
//    assert(mMapRes2.isInstanceOf[Map[Int, Map[Int, Double]]])
//    assert(mMapRes2.size == 1)
//    assert(mMapRes2(1)(2) == 99.0)
//    assert(sv2.isInstanceOf[Double])
//    assert(sv2 == sparseValue)
//    val mMapRes3 = hc.mToVectorOfVector(m)
//    //    println("mMapRes3: >%s<".format(mMapRes3))
//    assert(mMapRes3.isInstanceOf[Vector[Vector[Double]]])
//    assert(mMapRes3.size == nv)
//    mMapRes3.zipWithIndex.map {
//      case (vr, r) => vr.zipWithIndex.map {
//        case (vc, c) => assert(if (((r == 1) && (c == 2)) && (vc == singleValue)) true else (vc == sparseValue))
//      }
//    }
//  }
//  test("LagSmpContext.mFromSeqOfSeq Dense") {
//    val nv = 10
//    val hc: LagContext = LagContext.getLagSmpContext(nv)
//    val sparseValue = 0.0
//    val rSparseIndex = 0
//    val cSparseIndex = 0
//    val vMatrix = Vector.tabulate(nv, nv)((r, c) => (r * nv + c).toDouble)
//    val m = hc.mFromSeqOfSeq(vMatrix, sparseValue)
//    val (mMapRes1, sv1) = hc.mToMap(m)
//    //    println("mMapRes1: >%s<, sv1: >%s<".format(mMapRes1, sv1))
//    assert(mMapRes1.isInstanceOf[Map[(Int, Int), Double]])
//    assert(mMapRes1.size == nv * nv - 1)
//    (0 until nv).map { r => (0 until nv).map { c => if (r != rSparseIndex && c != cSparseIndex) assert(mMapRes1((r, c)) == (r * nv + c).toDouble) } }
//    assert(sv1.isInstanceOf[Double])
//    assert(sv1 == sparseValue)
//    val (mMapRes2, sv2) = hc.mToMapOfMap(m)
//    //    println("mMapRes2: >%s<, sv: >%s<".format(mMapRes2, sv2))
//    assert(mMapRes2.isInstanceOf[Map[Int, Map[Int, Double]]])
//    assert(mMapRes2.size == nv)
//    //    println("XXXXX: >%d<".format(mMapRes2.foldLeft(0)(_ + _._2.size)))
//    assert(mMapRes2.foldLeft(0)(_ + _._2.size) == nv * nv - 1)
//    (0 until nv).map { r => (0 until nv).map { c => if (r != rSparseIndex && c != cSparseIndex) assert(mMapRes2(r)(c) == (r * nv + c).toDouble) } }
//    assert(sv2.isInstanceOf[Double])
//    assert(sv2 == sparseValue)
//    val mMapRes3 = hc.mToVectorOfVector(m)
//    //    println("mMapRes3: >%s<".format(mMapRes3))
//    assert(mMapRes3.isInstanceOf[Vector[Vector[Double]]])
//    assert(mMapRes3.size == nv)
//    mMapRes3.zipWithIndex.map {
//      case (vr, r) => vr.zipWithIndex.map {
//        case (vc, c) => assert(vc == (r * nv + c).toDouble)
//      }
//    }
//  }
  test("LagSmpContext.vFromMap Empty") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val sparseValue = 0.0
    val vMap = Map[Long, Double]()
    val v = hc.vFromMap[Double](vMap, sparseValue)
    val (vMapRes1, sv1) = hc.vToMap(v)
//    println("vMapRes1: >%s<, sv1: >%s<".format(vMapRes1, sv1))
//    assert(vMapRes1.isInstanceOf[Map[Int, Double]])
    assert(vMapRes1.size == 0)
    assert(sv1.isInstanceOf[Double])
    assert(sv1 == sparseValue)
    val vMapRes3 = hc.vToVector(v)
//    println("vMapRes3: >%s<".format(vMapRes3))
    assert(vMapRes3.isInstanceOf[Vector[Double]])
    assert(vMapRes3.size == nv)
    vMapRes3.map { v => assert(v == sparseValue) }
  }
  test("LagSmpContext.vFromMap Sparse") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val sparseValue = 0.0
    val singleValue = 99.0
    val vMap = Map[Long, Double](2L -> singleValue)
    val v = hc.vFromMap[Double](vMap, sparseValue)
    val (vMapRes1, sv1) = hc.vToMap(v)
    //    println("vMapRes1: >%s<, sv1: >%s<".format(vMapRes1, sv1))
//    assert(vMapRes1.isInstanceOf[Map[Int, Double]])
    assert(vMapRes1.size == 1)
    assert(vMapRes1(2) == 99.0)
    assert(sv1.isInstanceOf[Double])
    assert(sv1 == sparseValue)
    val vMapRes3 = hc.vToVector(v)
    //    println("mMapRes3: >%s<".format(mMapRes3))
    assert(vMapRes3.isInstanceOf[Vector[Double]])
    assert(vMapRes3.size == nv)
    vMapRes3.zipWithIndex.map {
      case (v, r) => assert(if (r == 2 && v == singleValue) true else (v == sparseValue))
    }
  }
  test("LagSmpContext.vFromMap Dense") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val sparseValue = 0.0
    val sparseIndex = 0
    val vMap = Map((0 until nv).map { r => (r.toLong, r.toDouble) }: _*)
    val v = hc.vFromMap[Double](vMap, sparseValue)
    val (vMapRes1, sv1) = hc.vToMap(v)
    //    println("vMapRes1: >%s<, sv1: >%s<".format(vMapRes1, sv1))
//    assert(vMapRes1.isInstanceOf[Map[Int, Double]])
    assert(vMapRes1.size == nv - 1)
    (0 until nv).map { r => if (r != sparseIndex) assert(vMapRes1(r) == r.toDouble) }
    assert(sv1.isInstanceOf[Double])
    assert(sv1 == sparseValue)
    val vMapRes3 = hc.vToVector(v)
    //    println("vMapRes3: >%s<".format(vMapRes3))
    assert(vMapRes3.isInstanceOf[Vector[Double]])
    assert(vMapRes3.size == nv)
    (0 until nv).map { r => if (r != sparseIndex) assert(vMapRes1(r) == r.toDouble) }
  }
}

