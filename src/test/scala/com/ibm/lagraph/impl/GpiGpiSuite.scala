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
import com.ibm.lagraph._

class GpiGpiSuite extends FunSuite with Matchers {

  test("GpiOps.gpi_replicate") {
    val sparseValue: Int = 0
    val size = 100
    val x = 11
    val u = GpiOps.gpi_replicate(size, x)
    assert(u.size == size)
    assert(u(size - 1) == 11)
    assert(u.sparseValue == x)
    assert(u.denseCount == 0)
    val uu = u.updated(50, 0)
    assert(uu.denseCount == 1)
    assert(GpiGpiSuite.getVectorType(uu) == "SparseVector")
    val ue = u.extend(10 * size, 0)
    assert(GpiGpiSuite.getVectorType(ue) == "SparseVector")
    val di = ue.denseIterator.next()
    assert((100, 0) == di)

    val uv = u.toVector
    assert(uv(size - 1) == 11)
    assert(uv.length == 100)
  }
  test("GpiOps.gpi_replicate2") {
    val sparseValue: Int = 0
    val size = 100
    val x = 11
    val u = GpiOps.gpi_replicate(size, x)
    assert(u.size == size)
    assert(u(size - 1) == 11)
    assert(u.sparseValue == 11)
    assert(u.denseCount == 0)
    val uu = u.updated(50, 0)
    assert(uu.denseCount == 1)
    assert(GpiGpiSuite.getVectorType(uu) == "SparseVector")
    val di = uu.denseIterator.next()
    assert((50, 0) == di)

    val uv = u.toVector
    assert(uv(size - 1) == 11)
    assert(uv.length == 100)
  }

  test("GpiOps.gpireduce") {
    //    val u = GpiOps.gpi_replicate(size, x, sparseValue)
    val u = GpiOps.gpi_replicate(10, 1)
    def f(vs: Int, t2: Double): Double = vs.toDouble + t2
    //    val r = u.gpireduce(f, 0.0)
    def c(l: Double, r: Double): Double = l + r
    val r = GpiOps.gpi_reduce(f, c, 0.0, u)
    assert(r.isInstanceOf[Double])
    assert(r == 10.0)
    assert(GpiGpiSuite.getVectorType(u) == "SparseVector")
    val u2 = u.extend(u.size * 10, 1)
    assert(GpiGpiSuite.getVectorType(u2) == "SparseVector")
    //    val r2 = u2.gpireduce(f, 0.0)
    val r2 = GpiOps.gpi_reduce(f, c, 0.0, u2)
    assert(r2.isInstanceOf[Double])
    assert(r2 == 110.0)
  }

  test("GpiOps.gpimap") {
    //    val u = GpiOps.gpi_replicate(size, x, sparseValue)
    val u = GpiOps.gpi_replicate(5, 1)
    def f(vs: Int): Double = { vs.toDouble * 2.0 }
    //    val m = u.gpimap(f, 0.0)
    val m = GpiOps.gpi_map(f, u, Option(0.0))
    assert(GpiGpiSuite.getVectorType(m) == "DenseVector")
    val mm = Vector(2.0, 2.0, 2.0, 2.0, 2.0)
    assert(mm.corresponds(m.toVector)(_ == _))
  }

  test("GpiOps.gpi_indices") {
    val u = GpiOps.gpi_indices(5, 0)
    val um = Vector(0, 1, 2, 3, 4)
    assert(um.corresponds(u.toVector)(_ == _))
  }

  test("GpiOps.gpi_reduce") {
    val u = GpiOps.gpi_indices(5, 0)
    def f(vs: Long, sum: Double): Double = { vs.toDouble + sum }
    def c(l: Double, r: Double): Double = l + r
    val sum = GpiOps.gpi_reduce(f, c, 0.0, u)
    assert(sum == 10)
  }

  test("GpiOps.gpi_m_times_v") {
    val mv = Vector.tabulate(3, 3)((r, c) => c * 3 + r)
    val m = GpiSparseRowMatrix.fromVector(mv, 0)
    val v = GpiAdaptiveVector.fromSeq(Range(0, 3).toVector, 0)
    def f(x: Int, y: Int): Int = { x + y }
    def g(x: Int, y: Int): Int = { x * y }
    val u = GpiOps.gpi_m_times_v(f, g, f, 0, m, v, Option(0), Option(0))
    val ua = Vector(15, 18, 21)
    assert(ua.corresponds(u.toVector)(_ == _))
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

  test("GpiOps.gpi_m_times_m") {
    // the matrix
    val nv = 16
    val raw = (1 to nv * nv).toVector
    val a = raw.grouped(nv).toVector
    // the scala result
    val resScala = mult(a, a)

    // the gpi result
    // start w/ transpose end up w/ transpose
    val at = a.transpose
    val aGpi = GpiSparseRowMatrix.fromVector(a, 0)
    val atGpi = GpiSparseRowMatrix.fromVector(at, 0)
    def f(x: Int, y: Int): Int = { x + y }
    def g(x: Int, y: Int): Int = { x * y }
    val x = GpiOps.gpi_m_times_m(f, g, f, 0, aGpi, atGpi, Option(0), Option(0))
    val resGpi = GpiSparseRowMatrix.toVector(x).transpose

    assert(toArray(resGpi).deep == toArray(resScala).deep)

  }

  test("GpiSparseRowMatrix") {
    val frm: Map[Int, Map[Int, Double]] = Map(1 -> Map(1 -> 1.0))
    val srmFrm = GpiSparseRowMatrix.fromMapOfMap(frm, 0.0, 3, 3)
    val tv = srmFrm(1)(1)
    assert(tv.isInstanceOf[Double])
    assert(tv == 1.0)
    val mapFsrm = GpiSparseRowMatrix.toMapOfMap(srmFrm)
    assert(mapFsrm.size == 1)
    assert(mapFsrm(1).size == 1)
    val tv2 = mapFsrm(1)(1)
    assert(tv2.isInstanceOf[Double])
    assert(tv2 == 1.0)
  }

  //  test("GraphGenerators.generateRandomEdges") {
  //    val src = 5
  //    val numEdges10 = 10
  //    val numEdges20 = 20
  //    val maxVertexId = 100
  //
  //    val edges10 = GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId)
  //    assert(edges10.length == numEdges10)
  //
  //    val correctSrc = edges10.forall(e => e.srcId == src)
  //    assert(correctSrc)
  //
  //    val correctWeight = edges10.forall(e => e.attr == 1)
  //    assert(correctWeight)
  //
  //    val correctRange = edges10.forall(e => e.dstId >= 0 && e.dstId <= maxVertexId)
  //    assert(correctRange)
  //
  //    val edges20 = GraphGenerators.generateRandomEdges(src, numEdges20, maxVertexId)
  //    assert(edges20.length == numEdges20)
  //
  //    val edges10_round1 =
  //      GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId, seed = 12345)
  //    val edges10_round2 =
  //      GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId, seed = 12345)
  //    assert(edges10_round1.zip(edges10_round2).forall { case (e1, e2) =>
  //      e1.srcId == e2.srcId && e1.dstId == e2.dstId && e1.attr == e2.attr
  //    })
  //
  //    val edges10_round3 =
  //      GraphGenerators.generateRandomEdges(src, numEdges10, maxVertexId, seed = 3467)
  //    assert(!edges10_round1.zip(edges10_round3).forall { case (e1, e2) =>
  //      e1.srcId == e2.srcId && e1.dstId == e2.dstId && e1.attr == e2.attr
  //    })
  //  }
  //
  //  test("GraphGenerators.sampleLogNormal") {
  //    val mu = 4.0
  //    val sigma = 1.3
  //    val maxVal = 100
  //
  //    val trials = 1000
  //    for (i <- 1 to trials) {
  //      val dstId = GraphGenerators.sampleLogNormal(mu, sigma, maxVal)
  //      assert(dstId < maxVal)
  //    }
  //
  //    val dstId_round1 = GraphGenerators.sampleLogNormal(mu, sigma, maxVal, 12345)
  //    val dstId_round2 = GraphGenerators.sampleLogNormal(mu, sigma, maxVal, 12345)
  //    assert(dstId_round1 == dstId_round2)
  //
  //    val dstId_round3 = GraphGenerators.sampleLogNormal(mu, sigma, maxVal, 789)
  //    assert(dstId_round1 != dstId_round3)
  //  }
  test("GpiOps.gpi_zip2") {
    val sparseValue: Int = 0
    val size = 10
    val x = 11
    val u = GpiOps.gpi_replicate(size, x)
    def f(x: Int, y: Int): Int = { x + y }
    val v = GpiOps.gpi_zip(f, u, u)
    assert(GpiGpiSuite.getVectorType(v) == "SparseVector")
    assert(v.size == size)
    assert(v.sparseValue == 22)
    assert(v.denseCount == 0)

    val vv = v.toVector
    assert(vv(size - 1) == 22)
    assert(vv.length == 10)
  }
  test("GpiOps.gpi_transpose dense") {

    // the matrix
    val nv = 16
    val raw = (1 to nv * nv).toVector
    val a = raw.grouped(nv).toVector

    // scala transpose
    val at = a.transpose
    val aGpi = GpiSparseRowMatrix.fromVector(a, 0)
    val atGpi = GpiOps.gpi_transpose(aGpi)
    val atGpiRes = GpiSparseRowMatrix.toVector(atGpi)
    assert(toArray(at).deep == toArray(atGpiRes).deep)
  }
  test("GpiOps.gpi_transpose sparse zero") {

    // the matrix
    val nv = 16
    val raw = Vector.fill[Int](nv * nv)(0)

    val a = raw.grouped(nv).toVector
    // scala transpose
    val at = a.transpose
    val aGpi = GpiSparseRowMatrix.fromVector(a, 0)
    val atGpi = GpiOps.gpi_transpose(aGpi)
    val atGpiRes = GpiSparseRowMatrix.toVector(atGpi)
    assert(toArray(at).deep == toArray(atGpiRes).deep)
  }
  test("GpiOps.gpi_transpose sparse") {

    // the matrix
    val nv = 16
    val raw = Vector.fill[Int](nv * nv)(0).zipWithIndex.map {
      case (v, i) => if (i == 4) 1 else 0
    }

    val a = raw.grouped(nv).toVector
    // scala transpose
    val at = a.transpose
    val aGpi = GpiSparseRowMatrix.fromVector(a, 0)
    val atGpi = GpiOps.gpi_transpose(aGpi)
    val atGpiRes = GpiSparseRowMatrix.toVector(atGpi)
    assert(toArray(at).deep == toArray(atGpiRes).deep)
  }
}
object GpiGpiSuite {
  def getVectorType[T](u: GpiAdaptiveVector[T]): String = {
    u match {
      case (sv: GpiSparseVector[T]) => {
        "SparseVector"
      }
      case (sv: GpiDenseVector[T]) => {
        "DenseVector"
      }
      case (_) => {
        "NoMatch"
      }
    }
  }
}
