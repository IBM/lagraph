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

//object add_mul extends LagSemiring[Double] {
//  override val addition = (x: Double, y: Double) => x + y
//  val multiplication = (x: Double, y: Double) => x * y
//  val zero = 0.0
//  val one = 1.0
//}
class LagDstrGpiSuite extends FunSuite with Matchers with SharedSparkContext {
  val denseGraphSizes = List(1 << 4, 1 << 5)
  //  val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 29, 1 << 30)
  val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 26, 1 << 27)
  //  val nblocks = List(1 << 0, 1 << 1, 1 << 2, 1 << 3)
  val nblocks = List(1 << 0, 1 << 1, 1 << 2)

  val add_mul = LagSemiring.plus_times[Double]
  def hadamardProduct[A](a: Vector[Vector[A]], b: Vector[Vector[A]])(implicit n: Numeric[A]) = {
    import n._
    a.zip(b).map { case (ar, br) => ar.zip(br).map { case (av, bv) => av + bv } }
  }
  test("LagDstrContext.mHm") {
    val sparseValue = 0.0
    for (graphSize <- denseGraphSizes) {
      for (nblock <- List(1 << 0, 1 << 1)) {
        println("LagDstrContext.mHm", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val nv = graphSize
        val nr = nv
        val nc = nv
        val nA = Vector.tabulate(nv, nv)((r, c) => (r * nv + c).toDouble)
        val nB = Vector.tabulate(nv, nv)((r, c) => (r * nv + c).toDouble)
        val mA = hc.mFromMap(LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble),sparseValue), sparseValue)
        val mB = hc.mFromMap(LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble),sparseValue), sparseValue)
        val _mul = LagSemiring.min_plus[Double]
        val mHm = hc.mHm(_mul, mA, mB)
        val (mResMap, mResSparse) = hc.mToMap(mHm)
        assert(mResSparse == sparseValue)
        val mHmRes = LagContext.vectorOfVectorFromMap(mResMap, sparseValue, (nv.toLong, nv.toLong))
        val mHmAct = hadamardProduct(nA, nB)
        (0 until nv).map { r => (0 until nv).map { c => assert(mHmRes(r)(c) == mHmAct(r)(c)) } }

        (0 until nr).map { r => (0 until nc).map { c => assert(mHmRes(r)(c) == (2 * (r * nc + c)).toDouble) } }
      }
    }
  }
  def outerproduct[A](a: Vector[Vector[A]], acol: Int, b: Vector[Vector[A]], brow: Int)(implicit n: Numeric[A]) = {
    import n._
    val bw = b.zipWithIndex
    a.map { ar => bw.withFilter { case (br, bri) => bri == brow }.flatMap { case (br, bri) => br.map { _ * ar(acol) } } }
  }
  test("LagDstrContext.mOp") {
    val sparseValue = 0.0
    //    for (graphSize <- List(16, 17, 31, 32)) {
    //      for (nblock <- List(1, 2, 3, 4)) {
    //    for (graphSize <- List(16)) {
    for (graphSize <- denseGraphSizes) {
      for (nblock <- List(1 << 0, 1 << 1)) {
        println("LagDstrContext.mOp", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val nv = graphSize
        val nA = Vector.tabulate(nv, nv)((r, c) => (r * nv + c).toDouble)
        val nB = Vector.tabulate(nv, nv)((r, c) => (r * nv + c).toDouble)
        val mA = hc.mFromMap(LagContext.mapFromSeqOfSeq(nA,sparseValue), sparseValue)
        val mB = hc.mFromMap(LagContext.mapFromSeqOfSeq(nB,sparseValue), sparseValue)
        //    }
        val _mul = LagSemiring.plus_times[Double]
        (0 until nv).map { oc =>
          (0 until nv).map { or =>
            {
              //              println("XXXXX",nv,nblock, oc,or)
              val mOp = hc.mOp(_mul, mA, oc.toLong, mB, or.toLong)
              val (mResMap, mResSparse) = hc.mToMap(mOp)
              assert(mResSparse == sparseValue)
              val mHmRes = LagContext.vectorOfVectorFromMap(mResMap, sparseValue, (nv.toLong, nv.toLong))
              val mHmAct = outerproduct(nA, oc, nB, or)
              (0 until nv).map { r => (0 until nv).map { c => assert(mHmRes(r)(c) == mHmAct(r)(c)) } }
            }
          }
        }
      }
    }
  }
  //
  //  test("LagDstrContext.mMap") {
  //    val nv = 10
  //    val hc: LagContext = LagContext.getLagDstrContext(nv)
  //    val nr = nv
  //    val nc = nv
  //    val sparseValue = 0.0
  //    val mA = hc.mFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble), sparseValue)
  //
  //    val f = (a: Double) => 2.0 * a
  //    val mMap = hc.mMap(f, mA)
  //
  //    val mMapRes = hc.mToVectorOfVector(mMap)
  //
  //    (0 until nr).map { r => (0 until nc).map { c => assert(mMapRes(r)(c) == (2 * (r * nc + c)).toDouble) } }
  //  }
  //
  def mult[A](a: Vector[Vector[A]], b: Vector[Vector[A]])(implicit n: Numeric[A]) = {
    import n._
    for (row <- a)
      yield for (col <- b.transpose)
      yield row zip col map Function.tupled(_ * _) reduceLeft (_ + _)
  }
  def toArray[A: ClassTag](a: Vector[Vector[A]]): Array[Array[A]] = {
    a.map(x => x.toArray).toArray
  }

  test("LagDstrContext.mTm nr == nc") {
    println("LagDstrContext.mTm nr == nc")
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
//        println(toArray(resScala).deep.mkString("\n"))
        assert(toArray(LagContext.vectorOfVectorFromMap(hc.mToMap(mTmRes)._1, sparseValue, (nv.toLong, nv.toLong))).deep == toArray(resScala).deep)
      }
    }
  }

//  test("LagDstrContext.mTm nr != nc") {
//    val nr = 16
//    val hc: LagContext = LagContext.getLagDstrContext(nr)
//    val nc = 2
//    val sparseValue = 0.0
//    val raw = (1 to nr * nc).map(_.toDouble).toVector
//
//    val a = raw.grouped(nc).toVector
//    val aFull = Vector.tabulate(nr, nr)((r, c) => (r * 2 + 1 + c).toDouble) //  a augmented w/ columns
//    val at = a.transpose
//    val resScala = mult(a, at)
//
//    // the hpi result 
//    // start w/ transpose end up w/ transpose
//    val aFullGpi = hc.mFromSeqOfSeq(aFull, sparseValue)
//    //    val aGpi = hc.mFromSeqOfSeq(a, sparseValue)
//    val aGpi = hc.mSlice(aFullGpi, colRangeOpt = Option(0, 2))
//    //    val atGpi = hc.mFromSeqOfSeq(a, sparseValue)
//    val f = (x: Int, y: Int) => x + y
//    val g = (x: Int, y: Int) => x * y
//    object add_mul extends LagSemiring[Double] {
//      override val addition = (x: Double, y: Double) => x + y
//      val multiplication = (x: Double, y: Double) => x * y
//      val zero = 0.0
//      val one = 1.0
//    }
//    val mTm = hc.mTm(add_mul, aGpi, aGpi)
//
//    val mTmRes = hc.mToVectorOfVector(mTm).transpose
//
//    assert(toArray(mTmRes).deep == toArray(resScala).deep)
//
//  }
  test("LagDstrContext.mTv") {
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
  //
  //  test("LagDstrContext.mZip") {
  //    val nv = 10
  //    val hc: LagContext = LagContext.getLagDstrContext(nv)
  //    val nr = nv
  //    val nc = nv
  //    val sparseValueDouble = 0.0
  //    val sparseValueInt = 0
  //    val mA = hc.mFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble), sparseValueDouble)
  //    val mB = hc.mFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toInt), sparseValueInt)
  //
  //    val f = (a: Double, b: Int) => Tuple3(a, b, (a + b).toInt)
  //    val mZip = hc.mZip(f, mA, mB)
  //
  //    val mZipRes = hc.mToVectorOfVector(mZip)
  //
  //    (0 until nr).map { r =>
  //      (0 until nc).map { c =>
  //        {
  //          val v = r * nc + c
  //          assert(mZipRes(r)(c) == Tuple3(v.toDouble, v.toInt, (2 * v).toDouble))
  //        }
  //      }
  //    }
  //  }
  //
  test("LagDstrContext.vMap") {
    val sparseValueDouble: Double = 0.0
    val sparseValueInt: Int = 0
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vMap", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        // vector
        val vMap = Map((0 until graphSize).map { r => (r.toLong, r.toDouble) }: _*)
        val v = hc.vFromMap[Double](vMap, sparseValueDouble)

        val doubleItToInt = (x: Double) => {
          (2.0 * x).toInt
        }
        // vMap
        val u = hc.vMap(doubleItToInt, v)

        // back to map
        val (uvm, uvmSparseValue) = hc.vToMap(u)
        assert(uvmSparseValue == sparseValueInt)

        // compare
        val uvma = LagContext.vectorFromMap(uvm, sparseValueInt, graphSize)

        val vva = Vector.tabulate(graphSize)(r => (r * 2).toInt)
        assert(uvma.corresponds(vva)(_ == _))
        //  }
      }
    }
  }
  //  test("LagDstrContext.vMap 1") {
  //    val nv = 10
  //    val hc: LagContext = LagContext.getLagDstrContext(nv)
  //    val size = nv
  //    val u = hc.vReplicate(1)
  //    val f = (a: Int) => a.toDouble * 2.0
  //    val v = hc.vMap(f, u)
  //    val vRes = hc.vToVector(v)
  //    val vScala = (0 until size).map { a => 2.0 }.toVector
  //    assert(vScala.corresponds(vRes)(_ == _))
  //  }
  //  test("LagDstrContext.vMap 2") {
  //    val nv = 10
  //    val hc: LagContext = LagContext.getLagDstrContext(nv)
  //    val size = nv
  //    val u = hc.vIndices(0)
  //    val f = (a: Int) => a.toDouble * 2.0
  //    val v = hc.vMap(f, u)
  //    val vRes = hc.vToVector(v)
  //    val vScala = (0 until size).map { a => 2.0 * a }.toVector
  //    assert(vScala.corresponds(vRes)(_ == _))
  //  }
  //  test("LagDstrContext.vZip") {
  //    val nv = 10
  //    val hc: LagContext = LagContext.getLagDstrContext(nv)
  //    val size = nv
  //    val offset = 100
  //    val u = hc.vIndices(0)
  //    val v = hc.vIndices(offset)
  //    val f = (a: Int, b: Int) => a + b
  //    val w = hc.vZip(f, u, v)
  //    val wRes = hc.vToVector(w)
  //    val vScala = (0 until size).map { a => a + a + offset }.toVector
  //    assert(wRes.size == size)
  //    assert(vScala.corresponds(wRes)(_ == _))
  //  }
  test("LagDstrContext.vZip") {
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
  test("LagDstrContext.vEquiv eq indices") {
    val offset = 0
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vEquiv eq indices", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        // vectors
        val u = hc.vIndices(0)
        val v = hc.vIndices(offset)
        assert(hc.vEquiv(u, v))
        assert(hc.vEquiv(v, u))
      }
    }
  }
  test("LagDstrContext.vEquiv ne indices") {
    val offset = 100
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vEquiv ne indices", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        // vectors
        val u = hc.vIndices(0)
        val v = hc.vIndices(offset)
        assert(!hc.vEquiv(u, v))
        assert(!hc.vEquiv(v, u))
      }
    }
  }
  test("LagDstrContext.vEquiv eq replicate") {
    val constant = 0
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vEquiv eq replicate", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        // vectors
        val u = hc.vReplicate(constant)
        val v = hc.vReplicate(constant)
        assert(hc.vEquiv(u, v))
        assert(hc.vEquiv(v, u))
      }
    }
  }
  test("LagDstrContext.vEquiv ne replicate") {
    val constant = 100
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vEquiv ne replicate", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        // vectors
        val u = hc.vReplicate(0)
        val v = hc.vReplicate(constant)
        assert(!hc.vEquiv(u, v))
        assert(!hc.vEquiv(v, u))
      }
    }
  }
  test("LagDstrContext.vZipWithIndex") {
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

  test("LagDstrContext.vReduceWithIndex") {
    val sparseValueInt = 0
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vReduceWithIndex", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)

        val testIndex = 3
        val vTest = Range(0, graphSize).toVector.map {
          x =>
            if (x == testIndex)
              (x.toLong, -1)
            else
              (x.toLong, x)
        }.toMap
        val v = hc.vFromMap(vTest, sparseValueInt)
        val f = (v: (Int, Long), c: (Int, Long)) => {
          if (v._1 < c._1)
            (v._1, v._2)
          else
            (c._1, c._2)
        }
        val c = (l: (Int, Long), r: (Int, Long)) => {
          if (l._1 < r._1) l else r
        }

        val res = hc.vReduceWithIndex(f, c, (Int.MaxValue,Long.MaxValue), v)
        assert(res._2 == testIndex.toLong)
      }
    }
  }
}

