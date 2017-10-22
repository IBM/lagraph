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

import scala.reflect.{ClassTag, classTag}
import scala.{specialized => spec}

import scala.collection.mutable.{ArrayBuffer, Map => MMap, Builder}

import com.ibm.lagraph._

sealed protected trait AdaptiveVectorToBuffer {

  def asDenseBuffer[VS: ClassTag](v: GpiAdaptiveVector[VS]): GpiBuffer[VS] =
    v match {
      case GpiDenseVector(is, sv, _, _) => is
      case GpiSparseVector(m, v, sz, _) => toDenseBuffer(m, v, sz)
    }

  def toDenseBuffer[VS: ClassTag](rv: (GpiBuffer[Int], GpiBuffer[VS]),
                                  sparseValue: VS,
                                  size: Int): GpiBuffer[VS] = {
    val t0 = System.nanoTime()
    val res = GpiBuffer.rvSparseBuffersToDenseBuffer(rv, sparseValue, size)
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    // x    println("AdaptiveVector: toDenseBuffer: stop: time: >%.3f< s".format(t01))
    res
  }

  def asSparseBuffers[VS: ClassTag](v: GpiAdaptiveVector[VS]): (GpiBuffer[Int], GpiBuffer[VS]) =
    v match {
      case GpiDenseVector(is, sv, _, _) => toSparseBuffers(is, sv, v.denseCount)
      case GpiSparseVector(rv, _, _, _) => rv
    }

  def toSparseBuffers[VS: ClassTag](iseq: GpiBuffer[VS],
                                    sparseValue: VS,
                                    denseCount: Int): (GpiBuffer[Int], GpiBuffer[VS]) = {
    val t0 = System.nanoTime()
    val res =
      GpiBuffer.rvDenseBufferToSparseBuffer(iseq, denseCount, sparseValue)
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    // x    println("AdaptiveVector: toSparseBuffers: stop: time: >%.3f< s".format(t01))
    res
  }
}

// ********
// loosely based on algebird AdaptiveVector
// https://github.com/twitter/algebird/blob/develop/algebird-core
// /src/main/scala/com/twitter/algebird/AdaptiveVector.scala
sealed trait GpiAdaptiveVector[VS] {
  def length: Int = size
  def sparseValue: VS
  // number of items that are not sparse
  def denseCount: Int
  def size: Int
  def threshold: Double
  def apply(i: Int): VS
  // return new AV w/ updated value
  def updated(i: Int, v: VS): GpiAdaptiveVector[VS]
  // Grow by adding count sparse values to the end
  def extend(count: Int, value: VS): GpiAdaptiveVector[VS]
  // iterator of indices and values of all non-sparse values
  def denseIterator: Iterator[(Int, VS)]
  // ****
  def toVector: Vector[VS]
}
// ********
object GpiAdaptiveVector extends AdaptiveVectorToBuffer with Serializable {
  // if denseCount >= DefaultThreshold * size then use dense representation
  val DefaultThreshold = 0.25
  //  val DefaultThreshold = 0.10
  def fillWithSparse[VS: ClassTag](size: Int,
                                   threshold: Double = GpiAdaptiveVector.DefaultThreshold)(
      sparseValue: VS): GpiAdaptiveVector[VS] =
    GpiSparseVector[VS]((GpiBuffer.empty[Int], GpiBuffer.empty[VS]), sparseValue, size, threshold)
  def fromSeq[VS: ClassTag](
      v: Seq[VS],
      sparseValue: VS,
      threshold: Double = GpiAdaptiveVector.DefaultThreshold): GpiAdaptiveVector[VS] = {
    if (v.size == 0) {
      fillWithSparse[VS](0)(sparseValue)
    } else {
      val denseCount = v.count { _ != sparseValue }
      val sz = v.size
      if (denseCount < sz * threshold) {
        GpiSparseVector[VS](toSparseBuffers(GpiBuffer(v.toArray), sparseValue, denseCount),
                            sparseValue,
                            sz,
                            threshold)
      } else {
        GpiDenseVector[VS](GpiBuffer(v.toArray), sparseValue, denseCount, threshold)
      }
    }
  }
  // TODO sparseValue not used, maybe it should be an option?
  def wrapDenseArray[VS: ClassTag](a: Array[VS], sparseValue: VS): GpiAdaptiveVector[VS] = {
    GpiDenseVector[VS](GpiBuffer(a), sparseValue, a.size, 0.0)
  }
  def fromRvSeq[VS: ClassTag](
      rseq: Seq[Int],
      vseq: Seq[VS],
      sparseValue: VS,
      sizeOfDense: Int,
      threshold: Double = GpiAdaptiveVector.DefaultThreshold): GpiAdaptiveVector[VS] = {
    require(rseq.length == vseq.length)
    val denseCount = rseq.length
    if (rseq.size == 0) {
      fillWithSparse[VS](sizeOfDense)(sparseValue)
    } else {
      if (denseCount < sizeOfDense * threshold) {
        GpiSparseVector[VS](GpiBuffer.rvSeqToSparseBuffers(rseq, vseq, sparseValue, denseCount),
                            sparseValue,
                            sizeOfDense,
                            threshold)
      } else {
        GpiDenseVector[VS](GpiBuffer.rvSeqToDenseBuffer(rseq, vseq, sparseValue, sizeOfDense),
                           sparseValue,
                           denseCount,
                           threshold)
      }
    }
  }
  def fromMap[VS: ClassTag](
      m: Iterable[(Int, VS)],
      sparseValue: VS,
      sizeOfDense: Int,
      threshold: Double = GpiAdaptiveVector.DefaultThreshold): GpiAdaptiveVector[VS] = {
    if (m.size == 0) {
      fillWithSparse[VS](sizeOfDense)(sparseValue)
    } else {
      //      val maxIdx = m.keys.max
      val maxIdx = m.reduceLeft { (x, y) =>
        if (x._1 > y._1) x else y
      }._1
      require(maxIdx < sizeOfDense,
              "Max key (" + maxIdx + ") exceeds valid for size (" + sizeOfDense + ")")
      val denseCount = m.count { _._2 != sparseValue }
      if (denseCount < sizeOfDense * threshold) {
        val (rseq, cseq) = m.toSeq.sortBy(_._1).unzip
        GpiSparseVector[VS](GpiBuffer.rvSeqToSparseBuffers(rseq, cseq, sparseValue, denseCount),
                            sparseValue,
                            sizeOfDense,
                            threshold)
      } else {
        val (rseq, cseq) = m.toSeq.unzip
        GpiDenseVector[VS](GpiBuffer.rvSeqToDenseBuffer(rseq, cseq, sparseValue, sizeOfDense),
                           sparseValue,
                           denseCount,
                           threshold)
      }
    }
  }
  def toVector[VS: ClassTag](v: GpiAdaptiveVector[VS]): Vector[VS] = {
    GpiAdaptiveVector.asDenseBuffer(v).toVector
  }
  def toMap[VS: ClassTag](v: GpiAdaptiveVector[VS]): Map[Int, VS] = {
    val rv = GpiAdaptiveVector.asSparseBuffers(v)
    (rv._1 zip rv._2).toVector.toMap
  }
  def concatenateToDense[VS: ClassTag](vs: Seq[GpiAdaptiveVector[VS]]): GpiAdaptiveVector[VS] = {
    def concat(sz: Int, i: Int, va: Seq[GpiAdaptiveVector[VS]], vf: Array[VS]): Array[VS] =
      i match {
        case `sz` => vf
        case _ =>
          concat(sz, i + 1, va, vf ++ GpiAdaptiveVector.asDenseBuffer(va(i)).toArray)
      }
    GpiAdaptiveVector.wrapDenseArray(concat(vs.size, 0, vs, Array[VS]()), vs(0).sparseValue)
  }
  def toDense[VS: ClassTag](v: GpiAdaptiveVector[VS]): GpiAdaptiveVector[VS] = {
    if (v.isInstanceOf[GpiSparseVector[VS]]) {
      val iSeq = GpiBuffer.rvSparseBuffersToDenseBuffer(v.asInstanceOf[GpiSparseVector[VS]].rv,
                                                        v.sparseValue,
                                                        v.size)
      GpiDenseVector(iSeq, v.sparseValue, v.denseCount, 0.0)
    } else {
      v
    }
  }

  // ****
  // gpi ops

  /**
    * Applies a binary operator to a start value and all elements of this
    *  vector, going left to right.
    *
    *  @tparam T1 the input vector type.
    *  @tparam T2 the result type of the binary operator.
    *  @param z the start value.
    *  @param f the binary operator.
    */
  def gpi_reduce[@spec(Int) VS: ClassTag, @spec(Int) T2: ClassTag](
      f: (VS, T2) => T2,
      c: (T2, T2) => T2, // not used for SMP
      zero: T2,
      u: GpiAdaptiveVector[VS],
      stats: Option[GpiAdaptiveVector.Stat] = None): T2 = {
    u match {
      case uSparse: GpiSparseVector[VS] => {
        //        println ("AdaptiveVector: gpi_reduce: start: uSparse")
        val (pt1, ops1) = uSparse.rv._2.foldLeft(zero) {
          case (x, y) => f(x, y)
        }
        val res =
          if (f(u.sparseValue, zero) == zero) {
            (pt1, ops1)
          } else {
            var pt2 = pt1
            val sparses = u.size - u.denseCount
            for (i <- 0 until sparses)
              pt2 = f(u.sparseValue, pt2)
            (pt2, ops1 + sparses)
          }
        if (stats.isDefined) stats.get.incrementAdd(res._2)
        res._1
      }
      case uDense: GpiDenseVector[VS] => {
        // x        println("AdaptiveVector: gpi_reduce: start: uDense")
        val res = uDense.iseq.foldLeft(zero) { case (x, y) => f(x, y) }
        if (stats.isDefined) stats.get.incrementAdd(res._2)
        res._1
      }
    }
  }
  def gpi_equiv[VS: ClassTag](u: GpiAdaptiveVector[VS],
                              v: GpiAdaptiveVector[VS],
                              stats: Option[GpiAdaptiveVector.Stat] = None): Boolean = {
    val res =
      (u, v) match {
        case (uSparse: GpiSparseVector[VS], vSparse: GpiSparseVector[VS]) => {
          GpiBuffer.gpiCompareSparseSparse(uSparse.rv,
                                           vSparse.rv,
                                           uSparse.length,
                                           uSparse.sparseValue,
                                           vSparse.sparseValue)
        }
        case (uSparse: GpiSparseVector[VS], vDense: GpiDenseVector[VS]) => {
          GpiBuffer.gpiCompareSparseDense(uSparse.rv,
                                          vDense.iseq,
                                          uSparse.length,
                                          uSparse.sparseValue,
                                          vDense.sparseValue)
        }
        case (uDense: GpiDenseVector[VS], vSparse: GpiSparseVector[VS]) => {
          GpiBuffer.gpiCompareDenseSparse(uDense.iseq,
                                          vSparse.rv,
                                          uDense.length,
                                          uDense.sparseValue,
                                          vSparse.sparseValue)
        }
        case (uDense: GpiDenseVector[VS], vDense: GpiDenseVector[VS]) => {
          GpiBuffer.gpiCompareDenseDense(uDense.iseq,
                                         vDense.iseq,
                                         uDense.length,
                                         uDense.sparseValue,
                                         vDense.sparseValue)
        }
      }
    if (stats.isDefined) stats.get.incrementCmp(res._2)
    res._1
  }

  /**
    * Creates a new vector by applying a unary operator to all elements of the input vector.
    *
    *  @tparam T1 the input vector type.
    *  @tparam T2 the output vector type.
    *  @param f the unary operator.
    *  @param u the input vector
    *
    */
  def gpi_map[@spec(Int) VS: ClassTag, @spec(Int) T2: ClassTag](
      f: (VS) => T2,
      u: GpiAdaptiveVector[VS],
      sparseValue: T2,
      thresholdOpt: Option[Double] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T2] = {
    val threshold =
      if (thresholdOpt.isDefined) thresholdOpt.get else u.threshold
    val xVS = classTag[VS]
    val xT2 = classTag[T2]

    val ((bC, denseCountC, ops), utype) =
      u match {
        case uSparse: GpiSparseVector[VS] => {
          (GpiBuffer.gpiMapSparseBuffersToDenseBuffer(uSparse.rv,
                                                      uSparse.length,
                                                      uSparse.sparseValue,
                                                      sparseValue,
                                                      f),
           "sparse")
        }
        case uDense: GpiDenseVector[VS] => {
          (GpiBuffer.gpiMapDenseBufferToDenseBuffer(uDense.iseq, sparseValue, f), "dense")
        }
      }
    if (stats.isDefined) stats.get.increment(f, ops)

    val t0 = System.nanoTime()
    val res = if (denseCountC < u.length * threshold) {
      // x      println("AdaptiveVector: gpi_map: start denseToSparse: VS: >%s<, T2: >%s<".
      //          format(xVS, xT2))
      GpiSparseVector[T2](GpiAdaptiveVector.toSparseBuffers(bC, sparseValue, denseCountC),
                          sparseValue,
                          u.length,
                          threshold)
    } else {
      // x      println("AdaptiveVector: gpi_map: start denseToDense: VS: >%s<, T2: >%s<".
      //          format(xVS, xT2))
      GpiDenseVector[T2](bC, sparseValue, denseCountC, threshold)
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    // x    println("AdaptiveVector: gpi_map: stop: utype: >%s<, time: >%.3f< s".
    //        format(utype, t01))
    res
  }

  /**
    * Creates a new vector by applying a binary operator to pairs formed by
    *  combining two input vector
    *
    *  @tparam T1 first input vector type.
    *  @tparam T2 second input vector type.
    *  @tparam T3 output vector type.
    *  @param f the binary operator.
    *  @param u first input vector.
    *  @param v second input vector.
    *  @param sparseValue determines sparsity of new vector.
    */
  def gpi_zip[@spec(Int) VS: ClassTag, @spec(Int) T2: ClassTag, @spec(Int) T3: ClassTag](
      f: (VS, T2) => T3,
      u: GpiAdaptiveVector[VS],
      v: GpiAdaptiveVector[T2],
      sparseValue: T3,
      thresholdOpt: Option[Double] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T3] = {
    val threshold =
      if (thresholdOpt.isDefined) thresholdOpt.get else u.threshold
    val xVS = classTag[VS]
    val xT2 = classTag[T2]
    val xT3 = classTag[T3]
//    println("WHAT!", u, v)
    require(u.length == v.length,
            "gpi_zip: attempt to zip two unequal length vectors, u.length: >%d<, v.length: >%d<"
              .format(u.length, v.length))
    val ftype = f match {
      case sg: LagSemigroup[_] => {
        // TODO TODO need to fix this!
        if (sg.annihilator.isEmpty) "notspecified"
        else {
          val aga = sg.annihilator.get
          if (aga == u.sparseValue && aga == v.sparseValue) {
            if (false) {
              println(
                "gpi_zip: multiplication:  >%s< >%s< >%s<"
                  .format(u.sparseValue, v.sparseValue, aga)) }
            "multiplication"
          } else {
            if (false) {
              println(
                "gpi_zip: notspecified:  >%s< >%s< >%s<"
                  .format(u.sparseValue, v.sparseValue, aga)); }
            "notspecified"
          }
        }
      }
      case _ => "notspecified"
    }
    //    println("gpi_zip: FTYPE: >%s<".format(ftype))
    ftype match {
      case "multiplication" => {
        (u, v) match {
          case (uSparse: GpiSparseVector[VS], vSparse: GpiSparseVector[T2]) => {
            //            println("AdaptiveVector: gpi_zip: Multiplication, uSparse, vSparse")
            val (rv, denseCountC, ops) =
              GpiBuffer.gpiZipSparseSparseToSparse(uSparse.rv,
                                                   vSparse.rv,
                                                   uSparse.length,
                                                   uSparse.sparseValue,
                                                   vSparse.sparseValue,
                                                   sparseValue,
                                                   f)
            if (stats.isDefined) stats.get.increment(f, ops)
            GpiSparseVector[T3](rv, sparseValue, u.length, threshold)
          }
          case (uSparse: GpiSparseVector[VS], vDense: GpiDenseVector[T2]) => {
            // x            println("AdaptiveVector: gpi_zip: Multiplication, uSparse, vDense")
            val (rv, denseCountC, ops) =
              GpiBuffer.gpiZipSparseDenseToSparse(uSparse.rv,
                                                  vDense.iseq,
                                                  uSparse.length,
                                                  uSparse.sparseValue,
                                                  vDense.sparseValue,
                                                  sparseValue,
                                                  f)
            if (stats.isDefined) stats.get.increment(f, ops)
            GpiSparseVector[T3](rv, sparseValue, u.length, threshold)
          }
          case (uDense: GpiDenseVector[VS], vSparse: GpiSparseVector[T2]) => {
            //            println("AdaptiveVector: gpi_zip: Multiplication, uDense, vSparse")
            val (rv, denseCountC, ops) =
              GpiBuffer.gpiZipDenseSparseToSparse(uDense.iseq,
                                                  vSparse.rv,
                                                  uDense.length,
                                                  uDense.sparseValue,
                                                  vSparse.sparseValue,
                                                  sparseValue,
                                                  f)
            if (stats.isDefined) stats.get.increment(f, ops)
            GpiSparseVector[T3](rv, sparseValue, u.length, threshold)
          }
          case (uDense: GpiDenseVector[VS], vDense: GpiDenseVector[T2]) => {
            // x            println("AdaptiveVector: gpi_zip: Multiplication, uDense, vDense")
            val (vC, denseCountC, ops) =
              GpiBuffer.gpiZipDenseDenseToDense(uDense.iseq,
                                                vDense.iseq,
                                                uDense.length,
                                                uDense.sparseValue,
                                                vDense.sparseValue,
                                                sparseValue,
                                                f)
            if (stats.isDefined) stats.get.increment(f, ops)
            // cover case where dense becomes sparse TODO unit test
            if (denseCountC < uDense.length * threshold) {
              GpiSparseVector[T3](GpiAdaptiveVector.toSparseBuffers(vC, sparseValue, denseCountC),
                                  sparseValue,
                                  uDense.length,
                                  threshold)
            } else {
              GpiDenseVector[T3](vC, sparseValue, denseCountC, threshold)
            }
          }
        }
      }
      case "notspecified" => {
        val (vC, denseCountC, ops) = (u, v) match {
          case (uSparse: GpiSparseVector[VS], vSparse: GpiSparseVector[T2]) => {
            // x            println("AdaptiveVector: gpi_zip: _, uSparse, vSparse")
            GpiBuffer.gpiZipSparseSparseToDense(uSparse.rv,
                                                vSparse.rv,
                                                uSparse.length,
                                                uSparse.sparseValue,
                                                vSparse.sparseValue,
                                                sparseValue,
                                                f)
          }
          case (uSparse: GpiSparseVector[VS], vDense: GpiDenseVector[T2]) => {
            // x            println("AdaptiveVector: gpi_zip: _, uSparse, vDense")
            GpiBuffer.gpiZipSparseDenseToDense(uSparse.rv,
                                               vDense.iseq,
                                               uSparse.length,
                                               uSparse.sparseValue,
                                               vDense.sparseValue,
                                               sparseValue,
                                               f)
          }
          case (uDense: GpiDenseVector[VS], vSparse: GpiSparseVector[T2]) => {
            // x            println("AdaptiveVector: gpi_zip: _, uDense, vSparse")
            GpiBuffer.gpiZipDenseSparseToDense(uDense.iseq,
                                               vSparse.rv,
                                               uDense.length,
                                               uDense.sparseValue,
                                               vSparse.sparseValue,
                                               sparseValue,
                                               f)
          }
          case (uDense: GpiDenseVector[VS], vDense: GpiDenseVector[T2]) => {
            // x            println("AdaptiveVector: gpi_zip: _, uDense, vDense")
            GpiBuffer.gpiZipDenseDenseToDense(uDense.iseq,
                                              vDense.iseq,
                                              uDense.length,
                                              uDense.sparseValue,
                                              vDense.sparseValue,
                                              sparseValue,
                                              f)
          }
        }
        if (stats.isDefined) stats.get.increment(f, ops)
        val t0 = System.nanoTime()
        val res = if (denseCountC < vC.length * threshold) {
          // x  println("AdaptiveVector: gpi_zip: start denseToSparse: VS: >%s<, T2: >%s<, T3: >%s<"
          //      .format(xVS, xT2, xT3))
          GpiSparseVector[T3](GpiAdaptiveVector.toSparseBuffers(vC, sparseValue, denseCountC),
                              sparseValue,
                              vC.length,
                              threshold)
        } else {
          // x println(
          //   "AdaptiveVector: gpi_zip: start sparseToDense: VS: >%s<, T2: >%s<, T3: >%s<".
          //   format(xVS, xT2, xT3))
          GpiDenseVector[T3](vC, sparseValue, denseCountC, threshold)
        }
        val t1 = System.nanoTime()
        val t01 = LagUtils.tt(t0, t1)
        // x        println("AdaptiveVector: gpi_zip: stop: time: >%.3f< s".format(t01))
        res
      }
    }
  }
  def gpi_zip_with_index_special[@spec(Int) VS: ClassTag, @spec(Int) T3: ClassTag](
      f: (VS, Long) => T3,
      u: GpiAdaptiveVector[VS],
      base: Long,
      zeroSourceOpt: Option[VS] = None,
      zeroTargetOpt: Option[T3] = None,
      thresholdOpt: Option[Double] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T3] = {
    // no target zero specified, just grab one
    val sparseValue = zeroTargetOpt.getOrElse(f(u(0), 0L))
    val threshold =
      if (thresholdOpt.isDefined) thresholdOpt.get else u.threshold
    val xVS = classTag[VS]
    val xT3 = classTag[T3]
    u match {
      case uSparse: GpiSparseVector[VS] => {
        if (zeroSourceOpt.isDefined && zeroSourceOpt.get == uSparse.sparseValue) {
          val (rv, denseCountC, ops) =
            // x            println("AdaptiveVector: gpi_zip_with_index: _, uSparse, base")
            GpiBuffer.gpiZipSparseWithIndexToSparse(uSparse.rv,
                                                    uSparse.length,
                                                    base,
                                                    uSparse.sparseValue,
                                                    sparseValue,
                                                    f)
          if (stats.isDefined) stats.get.increment(f, ops)
          GpiSparseVector[T3](rv, sparseValue, u.length, threshold)
        } else {
          // x println("AdaptiveVector: gpi_zip_with_index: zero not sparse_, uSparse, base")
          val (vC, denseCountC, ops) =
            GpiBuffer.gpiZipSparseWithIndexToDense(uSparse.rv,
                                                   uSparse.length,
                                                   base,
                                                   uSparse.sparseValue,
                                                   sparseValue,
                                                   f)
          if (stats.isDefined) stats.get.increment(f, ops)
          // cover case where dense becomes sparse TODO unit test
          if (denseCountC < uSparse.length * threshold) {
            GpiSparseVector[T3](GpiAdaptiveVector.toSparseBuffers(vC, sparseValue, denseCountC),
                                sparseValue,
                                uSparse.length,
                                threshold)
          } else {
            GpiDenseVector[T3](vC, sparseValue, denseCountC, threshold)
          }
        }
      }
      case uDense: GpiDenseVector[VS] => {
        val (vC, denseCountC, ops) =
          // x            println("AdaptiveVector: gpi_zip_with_index: _, uDense, base")
          GpiBuffer.gpiZipDenseWithIndexToDense(uDense.iseq,
                                                uDense.length,
                                                base,
                                                uDense.sparseValue,
                                                sparseValue,
                                                f)
        if (stats.isDefined) stats.get.increment(f, ops)
        // cover case where dense becomes sparse TODO unit test
        if (denseCountC < uDense.length * threshold) {
          GpiSparseVector[T3](GpiAdaptiveVector.toSparseBuffers(vC, sparseValue, denseCountC),
                              sparseValue,
                              uDense.length,
                              threshold)
        } else {
          GpiDenseVector[T3](vC, sparseValue, denseCountC, threshold)
        }
      }
    }
  }
  def gpi_zip_with_index[@spec(Int) VS: ClassTag, @spec(Int) T3: ClassTag](
      f: (VS, Long) => T3,
      u: GpiAdaptiveVector[VS],
      base: Long,
      zeroTargetOpt: Option[T3] = None,
      thresholdOpt: Option[Double] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T3] = {
    // no target zero specified, just grab one
    val sparseValue = zeroTargetOpt.getOrElse(f(u(0), 0L))
    val threshold =
      if (thresholdOpt.isDefined) thresholdOpt.get else u.threshold
    val xVS = classTag[VS]
    val xT3 = classTag[T3]
    u match {
      case uSparse: GpiSparseVector[VS] => {
//        if (zeroSourceOpt.isDefined && zeroSourceOpt.get == uSparse.sparseValue) {
//          val (rv, denseCountC, ops) =
//            // x            println("AdaptiveVector: gpi_zip_with_index: _, uSparse, base")
//            GpiBuffer.gpiZipSparseWithIndexToSparse(
//              uSparse.rv,
//              uSparse.length,
//              base,
//              uSparse.sparseValue,
//              sparseValue,
//              f)
//          if (stats.isDefined) stats.get.increment(f, ops)
//          GpiSparseVector[T3](rv, sparseValue, u.length, threshold)
//        } else {
//          // x println("AdaptiveVector: gpi_zip_with_index: zero not sparse_, uSparse, base")
        val (vC, denseCountC, ops) =
          GpiBuffer.gpiZipSparseWithIndexToDense(uSparse.rv,
                                                 uSparse.length,
                                                 base,
                                                 uSparse.sparseValue,
                                                 sparseValue,
                                                 f)
        if (stats.isDefined) stats.get.increment(f, ops)
        // cover case where dense becomes sparse TODO unit test
        if (denseCountC < uSparse.length * threshold) {
          GpiSparseVector[T3](GpiAdaptiveVector.toSparseBuffers(vC, sparseValue, denseCountC),
                              sparseValue,
                              uSparse.length,
                              threshold)
        } else {
          GpiDenseVector[T3](vC, sparseValue, denseCountC, threshold)
        }
//        }
      }
      case uDense: GpiDenseVector[VS] => {
        val (vC, denseCountC, ops) =
          // x            println("AdaptiveVector: gpi_zip_with_index: _, uDense, base")
          GpiBuffer.gpiZipDenseWithIndexToDense(uDense.iseq,
                                                uDense.length,
                                                base,
                                                uDense.sparseValue,
                                                sparseValue,
                                                f)
        if (stats.isDefined) stats.get.increment(f, ops)
        // cover case where dense becomes sparse TODO unit test
        if (denseCountC < uDense.length * threshold) {
          GpiSparseVector[T3](GpiAdaptiveVector.toSparseBuffers(vC, sparseValue, denseCountC),
                              sparseValue,
                              uDense.length,
                              threshold)
        } else {
          GpiDenseVector[T3](vC, sparseValue, denseCountC, threshold)
        }
      }
    }
  }
  def gpi_zip_with_index_matrix[@spec(Int) VS: ClassTag, @spec(Int) T3: ClassTag](
      f: (VS, (Long, Long)) => T3,
      u: GpiAdaptiveVector[VS],
      rowIndex: Long,
      base: Long,
      zeroTargetOpt: Option[T3] = None,
      thresholdOpt: Option[Double] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T3] = {

    val visitDiagonalsOpt = None
    // no target zero specified, just grab one
    val sparseValue = zeroTargetOpt.getOrElse(f(u(0), (0L, 0L)))
    val threshold =
      if (thresholdOpt.isDefined) thresholdOpt.get else u.threshold
    val xVS = classTag[VS]
    val xT3 = classTag[T3]
    u match {
      case uSparse: GpiSparseVector[VS] => {
//        if (zeroSourceOpt.isDefined && zeroSourceOpt.get == uSparse.sparseValue) {
//          val (rv, denseCountC, ops) =
//            // x            println("AdaptiveVector: gpi_zip_with_index_matrix: _, uSparse, base")
//            GpiBuffer.gpiZipSparseWithIndexToSparseMatrix(
//              uSparse.rv,
//              uSparse.length,
//              rowIndex,
//              base,
//              visitDiagonalsOpt,
//              uSparse.sparseValue,
//              sparseValue,
//              f)
//          if (stats.isDefined) stats.get.increment(f, ops)
//          // cover case where sparse becomes dense, eg due
//          //   to insertion of diagonal element TODO unit test
//          if (denseCountC < uSparse.length * threshold) {
//            GpiSparseVector[T3](rv, sparseValue, u.length, threshold)
//          } else {
//            GpiDenseVector[T3](GpiAdaptiveVector.toDenseBuffer(rv, sparseValue, u.length),
//              sparseValue, denseCountC, threshold)
//          }
//        } else {
        // x println("AdaptiveVector: gpi_zip_with_index_matrix: zero not sparse_, uSparse, base")
        val (vC, denseCountC, ops) =
          GpiBuffer.gpiZipSparseWithIndexToDenseMatrix(uSparse.rv,
                                                       uSparse.length,
                                                       rowIndex,
                                                       base,
                                                       uSparse.sparseValue,
                                                       sparseValue,
                                                       f)
        if (stats.isDefined) stats.get.increment(f, ops)
        // cover case where dense becomes sparse TODO unit test
        if (denseCountC < uSparse.length * threshold) {
          GpiSparseVector[T3](GpiAdaptiveVector.toSparseBuffers(vC, sparseValue, denseCountC),
                              sparseValue,
                              uSparse.length,
                              threshold)
        } else {
          GpiDenseVector[T3](vC, sparseValue, denseCountC, threshold)
        }
//        }
      }
      case uDense: GpiDenseVector[VS] => {
        val (vC, denseCountC, ops) =
          // x            println("AdaptiveVector: gpi_zip_with_index_matrix: _, uDense, base")
          GpiBuffer.gpiZipDenseWithIndexToDenseMatrix(uDense.iseq,
                                                      uDense.length,
                                                      rowIndex,
                                                      base,
                                                      uDense.sparseValue,
                                                      sparseValue,
                                                      f)
        if (stats.isDefined) stats.get.increment(f, ops)
        // cover case where dense becomes sparse TODO unit test
        if (denseCountC < uDense.length * threshold) {
          GpiSparseVector[T3](GpiAdaptiveVector.toSparseBuffers(vC, sparseValue, denseCountC),
                              sparseValue,
                              uDense.length,
                              threshold)
        } else {
          GpiDenseVector[T3](vC, sparseValue, denseCountC, threshold)
        }
      }
    }
  }

  def gpi_zip_with_index_matrix_special[@spec(Int) VS: ClassTag, @spec(Int) T3: ClassTag](
      f: (VS, (Long, Long)) => T3,
      u: GpiAdaptiveVector[VS],
      rowIndex: Long,
      base: Long,
      visitDiagonalsOpt: Option[VS],
      zeroSourceOpt: Option[VS] = None,
      zeroTargetOpt: Option[T3] = None,
      thresholdOpt: Option[Double] = None,
      stats: Option[GpiAdaptiveVector.Stat] = None): GpiAdaptiveVector[T3] = {
    // no target zero specified, just grab one
    val sparseValue = zeroTargetOpt.getOrElse(f(u(0), (0L, 0L)))
    val threshold =
      if (thresholdOpt.isDefined) thresholdOpt.get else u.threshold
    val xVS = classTag[VS]
    val xT3 = classTag[T3]
    u match {
      case uSparse: GpiSparseVector[VS] => {
        if (zeroSourceOpt.isDefined && zeroSourceOpt.get == uSparse.sparseValue) {
          val (rv, denseCountC, ops) =
            // x            println("AdaptiveVector: gpi_zip_with_index_matrix: _, uSparse, base")
            GpiBuffer.gpiZipSparseWithIndexToSparseMatrix(uSparse.rv,
                                                          uSparse.length,
                                                          rowIndex,
                                                          base,
                                                          visitDiagonalsOpt,
                                                          uSparse.sparseValue,
                                                          sparseValue,
                                                          f)
          if (stats.isDefined) stats.get.increment(f, ops)
          // cover case where sparse becomes dense, eg due to insertion of diagonal element
          // TODO need unit test
          if (denseCountC < uSparse.length * threshold) {
            GpiSparseVector[T3](rv, sparseValue, u.length, threshold)
          } else {
            GpiDenseVector[T3](GpiAdaptiveVector.toDenseBuffer(rv, sparseValue, u.length),
                               sparseValue,
                               denseCountC,
                               threshold)
          }
        } else {
          // x println("AdaptiveVector: gpi_zip_with_index_matrix: zero not sparse_, uSparse, base")
          val (vC, denseCountC, ops) =
            GpiBuffer.gpiZipSparseWithIndexToDenseMatrix(uSparse.rv,
                                                         uSparse.length,
                                                         rowIndex,
                                                         base,
                                                         uSparse.sparseValue,
                                                         sparseValue,
                                                         f)
          if (stats.isDefined) stats.get.increment(f, ops)
          // cover case where dense becomes sparse TODO unit test
          if (denseCountC < uSparse.length * threshold) {
            GpiSparseVector[T3](GpiAdaptiveVector.toSparseBuffers(vC, sparseValue, denseCountC),
                                sparseValue,
                                uSparse.length,
                                threshold)
          } else {
            GpiDenseVector[T3](vC, sparseValue, denseCountC, threshold)
          }
        }
      }
      case uDense: GpiDenseVector[VS] => {
        val (vC, denseCountC, ops) =
          // x            println("AdaptiveVector: gpi_zip_with_index_matrix: _, uDense, base")
          GpiBuffer.gpiZipDenseWithIndexToDenseMatrix(uDense.iseq,
                                                      uDense.length,
                                                      rowIndex,
                                                      base,
                                                      uDense.sparseValue,
                                                      sparseValue,
                                                      f)
        if (stats.isDefined) stats.get.increment(f, ops)
        // cover case where dense becomes sparse TODO unit test
        if (denseCountC < uDense.length * threshold) {
          GpiSparseVector[T3](GpiAdaptiveVector.toSparseBuffers(vC, sparseValue, denseCountC),
                              sparseValue,
                              uDense.length,
                              threshold)
        } else {
          GpiDenseVector[T3](vC, sparseValue, denseCountC, threshold)
        }
      }
    }
  }

  class Stat {
    val stats: MMap[String, Any] = MMap()
    def get[T](key: String): T = {
      stats(key).asInstanceOf[T]
    }
    def put[T](key: String, value: T): Unit = {
      stats(key) = value
    }

    def clear: Unit = {
      stats("add") = 0L
      stats("mul") = 0L
      stats("cmp") = 0L
      stats("other") = 0L
    }

    // http://stackoverflow.com/questions/2657940/
    //   how-can-i-take-any-function-as-input-for-my-scala-wrapper-method
    // increment
    def increment(f: => Any, value: Long): Unit = {
      f match {
        case sg: LagSemigroup[_] => {
          if (sg.annihilator.isEmpty) incrementAdd(value)
          else incrementMul(value)
        }
        case _ => {
          incrementOther(value)
        }
      }
    }

    // add
    stats("add") = 0L
    def getAdd: Long = {
      get[Long]("add")
    }
    def incrementAdd(value: Long): Unit = {
      stats("add") = get[Long]("add") + value
    }

    // mul
    stats("mul") = 0L
    def getMul: Long = {
      get[Long]("mul")
    }
    def incrementMul(value: Long): Unit = {
      stats("mul") = get[Long]("mul") + value
    }

    // cmp
    stats("cmp") = 0L
    def getCmp: Long = {
      get[Long]("cmp")
    }
    def incrementCmp(value: Long): Unit = {
      stats("cmp") = get[Long]("cmp") + value
    }

    // other
    stats("other") = 0L
    def getOther: Long = {
      get[Long]("other")
    }
    def incrementOther(value: Long): Unit = {
      stats("other") = get[Long]("other") + value
    }
    override def toString(): String = {
      "stats: add: >%d<, mul: >%d<, cmp: >%d<, other: >%d<".format(getAdd, getMul, getCmp, getOther)
    }
  }
  object Stat {
    def Stat(): Stat = { new Stat() }
  }

}
final case class GpiDenseVector[VS: ClassTag](iseq: GpiBuffer[VS],
                                              override val sparseValue: VS,
                                              override val denseCount: Int,
                                              override val threshold: Double)
    extends GpiAdaptiveVector[VS] {
  override def size: Int = iseq.size
  override def apply(idx: Int): VS = iseq(idx)
  override def updated(idx: Int, v: VS): GpiAdaptiveVector[VS] = {
    val oldIsSparse = if (iseq(idx) == sparseValue) 1 else 0
    val newIsSparse = if (v == sparseValue) 1 else 0
    val newDenseCount = denseCount - newIsSparse + oldIsSparse
    val newIseq = iseq.updated(idx, v)
    if (denseCount < size * threshold) {
      GpiSparseVector[VS](GpiAdaptiveVector.toSparseBuffers(newIseq, sparseValue, newDenseCount),
                          sparseValue,
                          size,
                          threshold)
    } else {
      GpiDenseVector[VS](newIseq, sparseValue, newDenseCount, threshold)
    }
  }
  override def extend(cnt: Int, value: VS): GpiAdaptiveVector[VS] = {
    val newSize = size + cnt
    if (value == sparseValue)
      if (denseCount < newSize * threshold) {
        GpiSparseVector[VS](GpiAdaptiveVector.toSparseBuffers(iseq, sparseValue, denseCount),
                            sparseValue,
                            newSize,
                            threshold)
      } else {
        val newIseq = iseq.extend(cnt, sparseValue)
        GpiDenseVector[VS](newIseq, sparseValue, denseCount, threshold)
      } else {
      val newIseq = iseq.extend(cnt, sparseValue)
      GpiDenseVector[VS](newIseq, sparseValue, denseCount, threshold)
    }

  }
  // TODO potential performance issue
  override def toVector: Vector[VS] = {
    GpiAdaptiveVector.toVector(this)
  }

  override def denseIterator: Iterator[(Int, VS)] =
    iseq.toList.view.zipWithIndex
      .filter { _._1 != sparseValue }
      .map { _.swap }
      .iterator

  // TODO revisit equals e.g. role of threshold
  override def equals(o: Any): Boolean = o match {
  case that: GpiDenseVector[VS] => {
    if (this.hashCode == that.hashCode) {
      true
    } else if (that.size != this.size) {
      false
    } else if (that.sparseValue != this.sparseValue) {
      false
    } else if (that.threshold != this.threshold) {
      false
    } else {this.iseq == that.iseq }
  }
  case _ => false
}

}
private case object GpiSparseVector extends AdaptiveVectorToBuffer {}
final case class GpiSparseVector[VS: ClassTag](rv: (GpiBuffer[Int], GpiBuffer[VS]),
                                               override val sparseValue: VS,
                                               override val size: Int,
                                               override val threshold: Double)
    extends GpiAdaptiveVector[VS] {
  override def denseCount: Int = rv._1.size
  override def apply(i: Int): VS = {
    require(i >= 0 && i < size, "Index out of range")
    val r = GpiBuffer.binarySearchValue(rv._1, i)
    if (r == None) sparseValue else rv._2(r.get)
  }
  override def updated(indx: Int, v: VS): GpiAdaptiveVector[VS] = {
    val (r, cursor) = GpiBuffer.binarySearch(rv._1, indx)
    if (v == sparseValue) {
      if (r == None) this
      else {
        val newRv = GpiBuffer.rvDeleteItemFromSparseBuffers(rv, r.get)
        GpiSparseVector(newRv, sparseValue, size, threshold)
      }
    } else {
      if (r == None) {
        val newRv =
          GpiBuffer.rvInsertItemInSparseBuffers(rv, cursor.get, indx, v)
        val sv = GpiSparseVector(newRv, sparseValue, size, threshold)
        if (denseCount + 1 < size * threshold) {
          sv
        } else {
          GpiDenseVector[VS](GpiAdaptiveVector.toDenseBuffer(sv.rv, sparseValue, size),
                             sparseValue,
                             denseCount + 1,
                             threshold) }
      } else {
        val newRv = GpiBuffer.rvUpdateItemInSparseBuffers(rv, r.get, v)
        GpiSparseVector(newRv, sparseValue, size, threshold)
      }
    }
  }
  override def extend(delta: Int, value: VS): GpiAdaptiveVector[VS] = {
    val newSize = delta + size
    if (value == sparseValue) {
      GpiSparseVector(rv, sparseValue, newSize, threshold)
    } else {
      val (newRv, densecount) = GpiBuffer
        .rvExtendSparseBuffersWithNonSparseValue(rv, size, delta, value)
      if (denseCount < newSize * threshold) {
        GpiSparseVector(newRv, sparseValue, newSize, threshold)
      } else {
        GpiDenseVector[VS](GpiAdaptiveVector.toDenseBuffer(newRv, sparseValue, newSize),
                           sparseValue,
                           denseCount,
                           threshold)
      }
    }
  }
  // TODO potential performance issue
  override def toVector: Vector[VS] = {
    GpiAdaptiveVector.toVector(this)
  }
  private lazy val sortedList = (rv._1.toList zip rv._2.toList)
  override def denseIterator: Iterator[(Int, VS)] = sortedList.iterator

  // TODO revisit equals e.g. role of threshold
  override def equals(o: Any): Boolean = o match {
    case that: GpiSparseVector[VS] => {
      if (this.hashCode == that.hashCode) {
        true
      } else if (that.size != this.size) {
        false
      } else if (that.sparseValue != this.sparseValue) {
        false
      } else if (that.threshold != this.threshold) {
        false
      } else {
        this.rv._1 == that.rv._1 && this.rv._2 == that.rv._2
      }
    }
    case _ => false
  }

}

// ********
// MATRIX
// ****
// abstract

/**
  * Functions to create or convert GpiAdaptiveVectors representing a sparse row matrix
  */
object GpiSparseRowMatrix extends Serializable {
  def toString[T](m: GpiAdaptiveVector[GpiAdaptiveVector[T]]): String = {
    val ab = ArrayBuffer.fill(m.size, m(0).size)(m(0).sparseValue)
    var r = 0
    for (r <- 0 until m.size) {
      val rv = m(r)
      var c = 0
      for (c <- 0 until rv.size) {
        ab(r)(c) = rv(c)
      }
    }
    val aab = ab.toArray
    aab.deep.mkString("\n").replaceAll("ArrayBuffer", "")
  }

  def fromVector[T: ClassTag](vMatrix: Seq[Seq[T]],
                              sparseValue: T): GpiAdaptiveVector[GpiAdaptiveVector[T]] = {
    val mm = MMap[Int, GpiAdaptiveVector[T]]()
    val nrow = vMatrix.size
    val ncol = vMatrix(0).size
    vMatrix.zipWithIndex.foreach {
      case (v, r) => mm(r) = GpiAdaptiveVector.fromSeq(v, sparseValue)
    }
    val em = GpiAdaptiveVector.fillWithSparse[T](ncol)(sparseValue)
    GpiAdaptiveVector.fromMap(mm.toMap, em, nrow)
  }

  def toVector[T: ClassTag](aa: GpiAdaptiveVector[GpiAdaptiveVector[T]]): Vector[Vector[T]] = {
    val sparseValue = aa.sparseValue
    val cl = Seq.newBuilder[Vector[T]]
    aa match {
      case gaa: GpiAdaptiveVector[GpiAdaptiveVector[T]] => {
        cl.sizeHint(aa.size)
        val rv = GpiAdaptiveVector.toVector(gaa)
        rv.foreach { r =>
          cl += GpiAdaptiveVector.toVector(r)
        }
        cl.result.toVector
      }
    }
  }
  // http://stackoverflow.com/questions/8415812/
  //   collection-mutable-openhashmap-vs-collection-mutable-hashmap
  def fromMap[T: ClassTag](mMatrix: Iterable[((Int, Int), T)],
                           sparseValue: T,
                           nrow: Int,
                           ncol: Int): GpiAdaptiveVector[GpiAdaptiveVector[T]] = {
    //    val rohm = new OpenHashMap[Int, OpenHashMap[Int, T]](nrow)
    val rohm = MMap[Int, MMap[Int, T]]()
    //    // TODO not 10 put some powerlaw calc
    //    (0 until nrow) map { i => rohm(i) = new OpenHashMap[Int, T](10) }
    (0 until nrow) map { i =>
      rohm(i) = MMap[Int, T]()
    }
    mMatrix map { case (key, value) => rohm(key._1)(key._2) = value }

    val sohm = MMap[Int, GpiAdaptiveVector[T]]()
    //    val sohm = new OpenHashMap[Int, GpiAdaptiveVector[T]](nrow)
    rohm map {
      case (r, m) => sohm(r) = GpiAdaptiveVector.fromMap(m, sparseValue, ncol)
    }
    val em = GpiAdaptiveVector.fillWithSparse[T](ncol)(sparseValue)
    GpiAdaptiveVector.fromMap(sohm, em, nrow)
  }

  def fromMapOfMap[T: ClassTag](mMatrix: Iterable[(Int, Iterable[(Int, T)])],
                                sparseValue: T,
                                nrow: Int,
                                ncol: Int): GpiAdaptiveVector[GpiAdaptiveVector[T]] = {
    val mm = MMap[Int, GpiAdaptiveVector[T]]()
    mMatrix.map {
      case (k, v) => mm(k) = GpiAdaptiveVector.fromMap(v, sparseValue, ncol)
    }
    val em = GpiAdaptiveVector.fillWithSparse[T](ncol)(sparseValue)
    GpiAdaptiveVector.fromMap(mm.toMap, em, nrow)
  }

  def toMap[T: ClassTag](aa: GpiAdaptiveVector[GpiAdaptiveVector[T]]): Map[(Int, Int), T] = {
    val mm = MMap[(Int, Int), T]()
    aa match {
      case gaa: GpiAdaptiveVector[GpiAdaptiveVector[T]] => {
        val mas = GpiAdaptiveVector.toMap(gaa)
        mas.map {
          case (r, rm) =>
            GpiAdaptiveVector.toMap(rm).map { case (c, v) => mm((r, c)) = v }
        }
        mm.toMap
      }
      case _ => throw new RuntimeException("cant handle native GpiBmat")
    }
  }

  def toMapOfMap[T: ClassTag](
      aa: GpiAdaptiveVector[GpiAdaptiveVector[T]]): Map[Int, Map[Int, T]] = {
    val mm = MMap[Int, Map[Int, T]]()
    aa match {
      case gaa: GpiAdaptiveVector[GpiAdaptiveVector[T]] => {
        GpiAdaptiveVector.toMap(gaa).map {
          case (r, v) => mm(r) = GpiAdaptiveVector.toMap(v)
        }
        //          val mas = GpiAdaptiveVector.toMap(gaa)
        //          val sortedList = mas.toList.sortBy { _._1 }
        //          sortedList.foreach { case (k, v) => mm(k) = GpiAdaptiveVector.toMap(v) }
        mm.toMap
      }
      case _ => throw new RuntimeException("cant handle native GpiBmat")
    }
  }

  // TODO optimize for dense matrices
  def transpose[T: ClassTag](
      aa: GpiAdaptiveVector[GpiAdaptiveVector[T]]): GpiAdaptiveVector[GpiAdaptiveVector[T]] = {

    aa match {
      case gaa: GpiAdaptiveVector[GpiAdaptiveVector[T]] => {
        val mm = MMap[(Int, Int), T]()
        val m = toMap(gaa)
        m.map { case (k, v) => mm((k._2, k._1)) = v }
        val sparseValue = gaa(0).sparseValue
        val nrow = gaa.size
        val ncol = gaa(0).size
        fromMap(mm.toMap, sparseValue, ncol, nrow)
      }
      case _ => throw new RuntimeException("cant handle native GpiBmat")
    }
  }
  def slice[T: ClassTag](aa: GpiAdaptiveVector[GpiAdaptiveVector[T]],
                         rowRange: (Int, Int),
                         colRange: (Int, Int)): GpiAdaptiveVector[GpiAdaptiveVector[T]] = {
    def accum(list: List[((Int, Int), T)], item: ((Int, Int), T)) = {
      val k = item._1
      val v = item._2
      val r = k._1
      val c = k._2
      if (r >= rowRange._1 && r < rowRange._2 && c >= colRange._1 && c < colRange._2) {
        ((r - rowRange._1, c - colRange._1), v) :: list
      } else {
        list
      }
    }
    val sparseValue = aa(0).sparseValue
    val nrow = rowRange._2 - rowRange._1
    val ncol = colRange._2 - colRange._1
    fromMap(toMap(aa).foldLeft(List.empty[((Int, Int), T)])(accum), sparseValue, nrow, ncol)
  }
}
// scalastyle:on println
