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
import scala.collection.mutable.{Map => MMap}
import com.ibm.lagraph._

class LagSmpGpiSuite extends FunSuite with Matchers {

//  def hadamardProduct[A](a: Vector[Vector[A]], b: Vector[Vector[A]])(implicit n: Numeric[A]) = {
//    import n._
//    a.zip(b).map {
//      case (ar, br) => ar.zip(br).map { case (av, bv) => av + bv }
//    }
//  }
  test("LagSmpContext.mHm") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val nr = nv
    val nc = nv
    val sparseValue = 0.0
    val mA =
      hc.mFromMap(
        LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble),
                                   sparseValue),
        sparseValue)
    val mB =
      hc.mFromMap(
        LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble),
                                   sparseValue),
        sparseValue)

    val f = (a: Double, b: Double) => a + b
    //    object _mul extends LagSemiring[Double] {
    //      val addition = null
    //      val multiplication = (x: Double, y: Double) => x + y
    //      val zero = 0.0
    //      val one = 1.0
    //    }
    val _mul = LagSemiring.min_plus[Double]
    val mHm = hc.mHm(_mul, mA, mB)

    val (vvm, vvs) = hc.mToMap(mHm)
    val mHmRes = LagContext.vectorOfVectorFromMap(vvm, vvs, (nr, nc))

    (0 until nr).map { r =>
      (0 until nc).map { c =>
        assert(mHmRes(r)(c) == (2 * (r * nc + c)).toDouble)
      }
    }
  }
  def outerproduct[A](
      a: Vector[Vector[A]],
      acol: Int,
      b: Vector[Vector[A]],
      brow: Int)(implicit n: Numeric[A]): Vector[Vector[A]] = {
    import n._
    val bw = b.zipWithIndex
    a.map { ar =>
      bw.withFilter { case (br, bri) => bri == brow }.flatMap {
        case (br, bri) => br.map { _ * ar(acol) }
      }
    }
  }
  test("LagSmpContext.mOp") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val sparseValue = 0.0
    val nA = Vector.tabulate(nv, nv)((r, c) => (r * nv + c).toDouble)
    val nB = Vector.tabulate(nv, nv)((r, c) => (r * nv + c).toDouble)
    val mA =
      hc.mFromMap(LagContext.mapFromSeqOfSeq(nA, sparseValue), sparseValue)
    val mB =
      hc.mFromMap(LagContext.mapFromSeqOfSeq(nB, sparseValue), sparseValue)
    //    }
    val _mul = LagSemiring.plus_times[Double]

    //    val mOp = hc.mOp(_mul, mA, 0L, mB, 0L)
    //    val mOpRes = hc.mToVectorOfVector(mOp)
    //    val mOpAct = outerproduct(nA, 0, nB, 0)
    //    println("XXXXXXX")
    //    println(mA)
    //    println(mB)
    //    println(mHmRes)
    //    println(mHmAct)
    //    println("XXXXXXX")
    (0 until nv).map { oc =>
      (0 until nv).map { or =>
        {
          val mOp = hc.mOp(_mul, mA, oc.toLong, mB, or.toLong)
          val (vvm, vvs) = hc.mToMap(mOp)
          val mOpRes = LagContext.vectorOfVectorFromMap(vvm, vvs, (nv, nv))
          val mOpAct = outerproduct(nA, oc, nB, or)
          (0 until nv).map { r =>
            (0 until nv).map { c =>
              assert(mOpRes(r)(c) == mOpAct(r)(c))
            }
          }
        }
      }
    }
  }

  test("LagSmpContext.mMap") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val nr = nv
    val nc = nv
    val sparseValue = 0.0
    val mA =
      hc.mFromMap(
        LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble),
                                   sparseValue),
        sparseValue)

    val f = (a: Double) => 2.0 * a
    val mMap = hc.mMap(f, mA)

    val (vvm, vvs) = hc.mToMap(mMap)
    val mMapRes = LagContext.vectorOfVectorFromMap(vvm, vvs, (nr, nc))

    (0 until nr).map { r =>
      (0 until nc).map { c =>
        assert(mMapRes(r)(c) == (2 * (r * nc + c)).toDouble)
      }
    }
  }

  def mult[A](
      a: Vector[Vector[A]],
      b: Vector[Vector[A]])(implicit n: Numeric[A]): Vector[Vector[A]] = {
    import n._
    for (row <- a)
      yield
        for (col <- b.transpose)
          yield row zip col map Function.tupled(_ * _) reduceLeft (_ + _)
  }
  def toArray[A: ClassTag](a: Vector[Vector[A]]): Array[Array[A]] = {
    a.map(x => x.toArray).toArray
  }

  test("LagSmpContext.mTm nr == nc") {
    val graphSize = 10
    val hc: LagContext = LagContext.getLagSmpContext(graphSize)
    val nv = graphSize

    val nr = nv
    val nc = nv
    val nA = Vector.tabulate(nv, nv)((r, c) => r * nv + c + 1.0)
    val nB = Vector.tabulate(nv, nv)((r, c) => r * nv + c + 101.0)
    val sparseValue = 0.0
    val mA =
      hc.mFromMap(LagContext.mapFromSeqOfSeq(nA, sparseValue), sparseValue)
    val mB =
      hc.mFromMap(LagContext.mapFromSeqOfSeq(nB, sparseValue), sparseValue)
    val sr = LagSemiring.plus_times[Double]
    val mTmRes = hc.mTm(sr, mA, mB)

    val resScala = mult(nA, nB)
    assert(
      toArray(LagContext.vectorOfVectorFromMap(
        hc.mToMap(mTmRes)._1,
        sparseValue,
        (nv.toLong, nv.toLong))).deep == toArray(resScala).deep)

  }

  //  test("LagSmpContext.mTm nr != nc") {
  //    val nr = 16
  //    val hc: LagContext = LagContext.getLagSmpContext(nr)
  //    val nc = 2
  //    val sparseValue = 0.0
  //    val raw = (1 to nr * nc).map(_.toDouble).toVector
  //
  //    val a = raw.grouped(nc).toVector
  //    //  a augmented w/ columns
  //    val aFull = Vector.tabulate(nr, nr)((r, c) => (r * 2 + 1 + c).toDouble)
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
  //    //    object add_mul extends LagSemiring[Double] {
  //    //      override val addition = (x: Double, y: Double) => x + y
  //    //      val multiplication = (x: Double, y: Double) => x * y
  //    //      val zero = 0.0
  //    //      val one = 1.0
  //    //    }
  //    val add_mul = LagSemiring.plus_times[Double]
  //    val mTm = hc.mTm(add_mul, aGpi, aGpi)
  //
  //    val mTmRes = hc.mToVectorOfVector(mTm).transpose
  //
  //    assert(toArray(mTmRes).deep == toArray(resScala).deep)
  //
  //  }

  test("LagSmpContext.mTv") {
    val nv = 3
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val sparseValueInt = 0
    val mv = Vector.tabulate(nv, nv)((r, c) => c * nv + r)
    val m = hc.mFromMap(LagContext.mapFromSeqOfSeq(mv, sparseValueInt), sparseValueInt)
    val v = hc.vFromSeq(Range(0, nv).toVector, sparseValueInt)
    //    object add_mul extends LagSemiring[Int] {
    //      override val addition = (x: Int, y: Int) => x + y
    //      val multiplication = (x: Int, y: Int) => x * y
    //      val zero = 0
    //      val one = 1
    //    }
    val add_mul = LagSemiring.plus_times[Int]
    val u = hc.mTv(add_mul, m, v)
    val ua = Vector(15, 18, 21)
    assert(ua.corresponds(hc.vToVector(u))(_ == _))
  }

  //  test("LagSmpContext.mZip") {
  //    val nv = 10
  //    val hc: LagContext = LagContext.getLagSmpContext(nv)
  //    val nr = nv
  //    val nc = nv
  //    val sparseValueDouble = 0.0
  //    val sparseValueInt = 0
  //    val mA = hc.mFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) =>
  //      (r * nc + c).toDouble), sparseValueDouble)
  //    val mB = hc.mFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) =>
  //      (r * nc + c).toInt), sparseValueInt)
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

  test("LagSmpContext.mSparseZipWithIndex") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val nr = nv
    val nc = nv
    val sparseValueDouble = 0.0
    val mA =
      hc.mFromMap(
        LagContext.mapFromSeqOfSeq(Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble),
                                   sparseValueDouble),
        sparseValueDouble)

    val f = (a: Double, b: (Long, Long)) => Tuple2(a, b)
    val mSparseZipWithIndex =
      hc.mSparseZipWithIndex(f, mA, Tuple2(sparseValueDouble, (0L, 0L)))

    val (vvm, vvs) = hc.mToMap(mSparseZipWithIndex)
    val mZipWithIndexRes = LagContext.vectorOfVectorFromMap(vvm, vvs, (nr, nc))

    (0 until nr).map { r =>
      (0 until nc).map { c =>
        {
          val v = r * nc + c
          assert(mZipWithIndexRes(r)(c) == Tuple2(v.toDouble, (r, c)))
        }
      }
    }
  }
  test("LagSmpContext.vMap 1") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val size = nv
    val u = hc.vReplicate(1)
    val f = (a: Int) => a.toDouble * 2.0
    val v = hc.vMap(f, u)
    val vRes = hc.vToVector(v)
    val vScala = (0 until size).map { a =>
      2.0
    }.toVector
    assert(vScala.corresponds(vRes)(_ == _))
  }
  test("LagSmpContext.vMap 2") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val size = nv
    val u = hc.vIndices(0)
    val f = (a: Long) => a.toDouble * 2.0
    val v = hc.vMap(f, u)
    val vRes = hc.vToVector(v)
    val vScala = (0 until size).map { a =>
      2.0 * a
    }.toVector
    assert(vScala.corresponds(vRes)(_ == _))
  }
  test("LagSmpContext.vZip") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val size = nv
    val offset = 100
    val u = hc.vIndices(0)
    val v = hc.vIndices(offset)
    val f = (a: Long, b: Long) => a + b
    val w = hc.vZip(f, u, v)
    val wRes = hc.vToVector(w)
    val vScala = (0 until size).map { a =>
      a + a + offset
    }.toVector
    assert(wRes.size == size)
    assert(vScala.corresponds(wRes)(_ == _))
  }
  test("LagSmpContext.vZipWithIndex") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val size = nv
    val u = hc.vIndices(0)
    val f = (a: Long, b: Long) => (a, b)
    val w = hc.vZipWithIndex(f, u)
    val wRes = hc.vToVector(w)
    val vScala = (0 until size).map { a =>
      Tuple2(a, a)
    }.toVector
    assert(wRes.size == size)
    assert(vScala.corresponds(wRes)(_ == _))
  }
  test("LagSmpContext.vEquiv eq indices") {
    val offset = 0
    for (graphSize <- List(16, 32)) {
      val hc: LagContext = LagContext.getLagSmpContext(graphSize)
      // vectors
      val u = hc.vIndices(0)
      val v = hc.vIndices(offset)
      assert(hc.vEquiv(u, v))
      assert(hc.vEquiv(v, u))
    }
  }
  test("LagSmpContext.vEquiv ne indices") {
    val offset = 100
    for (graphSize <- List(16, 32)) {
      val hc: LagContext = LagContext.getLagSmpContext(graphSize)
      // vectors
      val u = hc.vIndices(0)
      val v = hc.vIndices(offset)
      assert(!hc.vEquiv(u, v))
      assert(!hc.vEquiv(v, u))
    }
  }
  test("LagSmpContext.vEquiv eq replicate") {
    val constant = 0
    for (graphSize <- List(16, 32)) {
      val hc: LagContext = LagContext.getLagSmpContext(graphSize)
      // vectors
      val u = hc.vReplicate(constant)
      val v = hc.vReplicate(constant)
      assert(hc.vEquiv(u, v))
      assert(hc.vEquiv(v, u))
    }
  }
  test("LagSmpContext.vEquiv ne replicate") {
    val constant = 100
    for (graphSize <- List(16, 32)) {
      val hc: LagContext = LagContext.getLagSmpContext(graphSize)
      // vectors
      val u = hc.vReplicate(0)
      val v = hc.vReplicate(constant)
      assert(!hc.vEquiv(u, v))
      assert(!hc.vEquiv(v, u))
    }
  }
  test("LagSmpContext.vEquiv 7") {
    val constant = 7
    for (graphSize <- List(8, 16, 32)) {
      val hc: LagContext = LagContext.getLagSmpContext(graphSize)
      // vectors
      val u = hc.vReplicate(constant)
      val v = hc.vReplicate(constant)
      assert(hc.vEquiv(u, v))
      assert(hc.vEquiv(v, u))
    }
  }
  test("LagSmpContext.vReduceWithIndex") {
    val sparseValueInt = 0
    for (graphSize <- List(8, 16, 32)) {
      val hc: LagContext = LagContext.getLagSmpContext(graphSize)

      val testIndex = 3
      val vTest = Range(0, graphSize).toVector.map { x =>
        if (x == testIndex) -1 else x
      }
      val v = hc.vFromSeq(vTest, sparseValueInt)
      def f(v: (Int, Long), c: Long): Long = {
        if (v._1 < c) {
          v._2
        } else {
          c
        }
      }
      def c(l: Long, r: Long): Long = {
        Math.min(l, r)
      }
      val res = hc.vReduceWithIndex(f, c, Long.MaxValue, v)
      assert(res == testIndex.toLong)
    }
  }
}
