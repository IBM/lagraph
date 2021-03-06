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
import scala.reflect.{ClassTag, classTag}
import scala.collection.mutable.{Map => MMap}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.ibm.lagraph._

class LagSemiringBellmanFordSuite extends FunSuite with Matchers {

  def DEBUG: Boolean = false

  def srTests[T](sr: LagSemiring[T],
                 ts: List[T],
                 validIn: Option[Function[T, Boolean]] = None): Unit = {

    val validDef = (x: T) => true
    val valid = validIn.getOrElse(validDef)
    val zero = sr.zero
    val one = sr.one

    //    val validefd = validIn.getOrElse(validDef )
    if (DEBUG) println("1) check that (T, addition, zero) is positive monoid w/ identity zero")
    if (DEBUG) println("1a) associative: a addition ( b addition c) = (a addition b) addition c")
    for (a <- ts)
      if (valid(a)) {
        for (b <- ts)
          if (valid(b)) {
            for (c <- ts)
            {
              if (valid(c)) {
                assert(
                  sr.addition(a, (sr.addition(b, c)))
                    == sr.addition(sr.addition(a, b), c))
              }
            }
          }
      }
    if (DEBUG) println("1b) commutative: a addition b = b addition a")
    for (a <- ts) {
      if (valid(a)) {
        for (b <- ts) {
          if (valid(b)) assert(sr.addition(a, b) == sr.addition(b, a))
        }
      }
    }
    if (DEBUG) println("1c) identity: a addition zero = zero addition a = a")
    for (a <- ts)
      if (valid(a)) {
        assert(sr.addition(zero, a) == a)
        assert(sr.addition(a, zero) == a)
      }
    if (DEBUG) println("2) (S, multiplication, one) is a monoid w/ identity one")
    if (DEBUG) {
      println(
        "2a) associative: a multiplication ( b multiplication c) = " +
        "( a multiplication b) multiplication c")
    }
    for (a <- ts) {
      if (valid(a)) {
        for (b <- ts) {
          if (valid(b)) {
            for (c <- ts) {
              if (valid(c)) {
                assert(
                  sr.multiplication(a, (sr.multiplication(b, c)))
                    == sr.multiplication(sr.multiplication(a, b), c))
              }
            }
          }
        }
      }
    }
    if (DEBUG) println("2b) identity: a multiplication one = one multiplication a = a")
    for (a <- ts)
      if (valid(a)) {
        assert(sr.multiplication(one, a) == a)
        assert(sr.multiplication(a, one) == a)
      }
    if (DEBUG) println("3) times multiplication over addition")
    if (DEBUG) {
      println(
        "3a) a multiplication (b addition c) = (a multiplication b) addition (a multiplication c)")
    }
    if (DEBUG) {
      println(
        "3b) (b addition c) multiplication a = (b multiplication a) addition (c multiplication a)")
    }
    for (a <- ts) {
      if (valid(a)) {
        for (b <- ts) {
          if (valid(b)) {
            for (c <- ts) {
              if (valid(c)) {
                assert(
                  sr.multiplication(a, sr.addition(b, c))
                    == sr.addition(sr.multiplication(a, b), sr.multiplication(a, c)))
                assert(
                  sr.multiplication(sr.addition(b, c), a)
                    == sr.addition(sr.multiplication(b, a), sr.multiplication(c, a)))
              }
            }
          }
        }
      }
    }
    if (DEBUG) println("4) zero is an annihilator under multiplication")
    for (a <- ts)
      if (valid(a)) {
        assert(sr.multiplication(zero, a) == zero)
        assert(sr.multiplication(a, zero) == zero)
      }

  }
  // *********

  type PathType = Tuple3[Float, Long, Long]
  val PathTypeInf = Tuple3(Float.MaxValue, Long.MaxValue, Long.MaxValue)
  trait PathTypeOrdering extends Ordering[PathType] {
    def compare(ui: PathType, vi: PathType): Int = {
      val w1 = ui._1; val h1 = ui._2; val p1 = ui._3
      val w2 = vi._1; val h2 = vi._2; val p2 = vi._3
      if (w1 < w2) {
        -1
      } else if ((w1 == w2) && (h1 < h2)) {
        -1
      } else if ((w1 == w2) && (h1 == h2) && (p1 < p2)) {
        -1
      } else {
        1
      }
    }
  }
  trait PathTypeAsNumeric extends LagSemiringAsNumeric[PathType] with PathTypeOrdering {
    val nodeNil: Long = -1L
    def plus(ui: PathType, vi: PathType): PathType = {

      def fpinf(x: Float, y: Float): Float =
        if (x == Float.MaxValue || y == Float.MaxValue) {
          Float.MaxValue
        } else {
          x + y
        }
      def lpinf(x: Long, y: Long): Long =
        if (x == Long.MaxValue || y == Long.MaxValue) {
          Long.MaxValue
        } else {
          x + y
        }

      val _zero = fromInt(0)
      val w1 = ui._1; val h1 = ui._2; val p1 = ui._3
      val w2 = vi._1; val h2 = vi._2; val p2 = vi._3
      if (p2 != _zero._3) { // for p2 annihilator (not) // p2 maybe identity
        // p2 not annihilator
        if (p1 == _zero._3) { // for p1 annihilator (is) (never happens)
          // p1 is annihilator (is)
          (fpinf(w1, w2), lpinf(h1, h2), p2)
        } else // for p1 annihilator (not)
        // p1 not annihilator
        if (p2 != nodeNil) { // p2 not identity // original
          (fpinf(w1, w2), lpinf(h1, h2), p2) // original
        //        else if (p1 != nodeInf) // p2 is identity // original
        //          (pinf(w1,w2), pinf(h1,h2), p2) //original // broke for p2 identity
        } else { // p2 is identity
          (fpinf(w1, w2), lpinf(h1, h2), p1) // original
        }
      } else {// for p2 annihilator (is) (never happens)
        (fpinf(w1, w2), lpinf(h1, h2), p1)
      //      // p2 is annihilator (never happens) // dm
      //      if (p1 == _zero._3) // for annihilator (is) // dm
      //        // p1 is annihilator // dm
      //        (pinf(w1,w2), pinf(h1,h2), _zero._3) // both annihilator // dm
      //      else // dm
      //        (pinf(w1,w2), pinf(h1,h2), p1) // dm
      }
    }
    def times(x: PathType, y: PathType): PathType = min(x, y)
    def fromInt(x: Int): PathType = x match {
      case 0 => ((0.0).toFloat, 0L, nodeNil)
      case 1 => (Float.MaxValue, Long.MaxValue, Long.MaxValue)
      case other =>
        throw new RuntimeException("fromInt for: >%d< not implemented".format(other))
    }
  }
  implicit object PathTypeAsNumeric extends PathTypeAsNumeric
  //  val PathTypeInf = Tuple2(floatInf, longInf)
  //  val PathTypeMinf = Tuple2(floatMinf, longMinf)
  // *********
  // bellmanford test constants
  object BfCons {
    val nodeInf: Long = Long.MaxValue
    val nodeNil: Long = -1L
    val hopInf: Long = Long.MaxValue
    val hopZero: Long = 0L
    val wInf: Float = Float.MaxValue
    val wZero: Float = (0.0).toFloat
  }

  test("testBellmanFord") {
    val bf = LagSemiring.min_plus[PathType](PathTypeInf)
//    val bf = SemiringLibrary.shortpath_min_plus
    //    bf.zero
    //    bf.one

    //    bf.wInf
    //    bf.wZero
    //    bf.hopInf
    //    bf.hopZero
    //    bf.nodeInf
    //    bf.nodeNil

    val zero = bf.zero
    val one = bf.one
    val fm99: Float = (-99.0).toFloat
    val f99: Float = (99.0).toFloat
    val f199: Float = (199.0).toFloat
    val l0: Long = 0L
    val l99: Long = 99L
    val l199: Long = 199L
    val fws = List(fm99, BfCons.wZero, BfCons.wInf, f99, f199)
    val fhs = List(BfCons.hopZero, BfCons.hopInf, l99, l199)
    val fns = List(BfCons.nodeNil, BfCons.nodeInf, l0, l99, l199)
    var fs = List[Tuple3[Float, Long, Long]]()
    for (fw <- fws)
      for (fh <- fhs)
        for (fn <- fns)
          fs = fs ::: List(Tuple3(fw, fh, fn))

    val validBf = (whn: Tuple3[Float, Long, Long]) => {
      val w = whn._1
      val h = whn._2
      val n = whn._3
      // self path (0, 0, nil)
      var checkh1 = true; var checkh2 = true; var checkh3 = true
      // if no hops then weight must be zero and parent must be nil
      checkh1 =
        if (h == BfCons.hopZero) (w == BfCons.wZero) && (n == BfCons.nodeNil)
        else true
      // if weight is zero then hops must be zero and parent must be nil
      //      checkh2 = if (w == BfCons.wZero) (h == BfCons.hopZero) &&
      //        (n == BfCons.nodeNil) else true
      // if parent is nil then hops must be zero and weight must be zero
      checkh3 =
        if (n == BfCons.nodeNil) (w == BfCons.wZero) && (h == BfCons.hopZero)
        else true
      //
      // infinity checks (inf, inf, inf) for no path
      var checki1 = true; var checki2 = true; var checki3 = true
      checki1 = if (w == zero._1) (h == zero._2) && (n == zero._3) else true
      checki2 = if (h == zero._2) (w == zero._1) && (n == zero._3) else true
      checki3 = if (n == zero._3) (w == zero._1) && (h == zero._2) else true
      //      println(checkh1,checkh2,checkh3,checki1,checki2,checki3)
      val passed = checkh1 && checkh2 && checkh3 && checki1 && checki2 && checki3
      val wstr =
        if (w == zero._1) "inf"
        else if (w == BfCons.wZero) "zero"
        else "%s".format(w)
      val hstr =
        if (h == zero._2) "inf"
        else if (h == BfCons.hopZero) "zero"
        else "%s".format(h)
      val nstr =
        if (n == zero._3) "inf"
        else if (n == BfCons.nodeNil) "nil"
        else "%s".format(n)
      // if (!passed) println("Rejected: >%s<".format(Tuple3(wstr, hstr, nstr)))
      //   else println("Passed: >%s<".format(Tuple3(wstr, hstr, nstr)))
      passed
    }
    srTests(bf, fs, Option(validBf))

    // functional
    // verify addition path weights appropriately
    val ueqv = Tuple3(BfCons.wZero, BfCons.hopZero, BfCons.nodeNil)
    val nopath = Tuple3(BfCons.wInf, BfCons.hopInf, BfCons.nodeInf)
    val x = (a: Double) => a.toFloat

    val p121 = Tuple3(x(1.0), 2L, 1L)
    val p222 = Tuple3(x(2.0), 2L, 2L)
    val p232 = Tuple3(x(2.0), 3L, 2L)
    val p223 = Tuple3(x(2.0), 2L, 3L)
    val p22n = Tuple3(x(2.0), 2L, BfCons.nodeNil)
    val p341 = Tuple3(x(3.0), 4L, 1L)
    val p342 = Tuple3(x(3.0), 4L, 2L)

    assert(bf.addition(p121, p222) == p121)
    assert(bf.addition(p222, p232) == p222)
    assert(bf.addition(p222, p223) == p222)
    assert(bf.addition(p222, ueqv) == ueqv)
    assert(bf.addition(p222, nopath) == p222)
    assert(bf.addition(ueqv, nopath) == ueqv)
    assert(bf.addition(nopath, nopath) == nopath)
    assert(bf.addition(p222, p222) == p222)
    assert(bf.addition(ueqv, ueqv) == ueqv)

    assert(bf.multiplication(p121, p222) == p342)
    assert(bf.multiplication(p121, p22n) == p341)

    // multiplicative identity
    assert(bf.multiplication(ueqv, one) == ueqv)
    assert(bf.multiplication(nopath, one) == nopath)
    assert(bf.multiplication(p22n, one) == p22n)
    assert(bf.multiplication(p222, one) == p222)
    assert(bf.multiplication(one, ueqv) == ueqv)
    assert(bf.multiplication(one, nopath) == nopath)
    assert(bf.multiplication(one, p22n) == p22n)
    assert(bf.multiplication(one, p222) == p222)

    // additive identity
    assert(bf.addition(ueqv, zero) == ueqv)
    assert(bf.addition(nopath, zero) == nopath)
    assert(bf.addition(p22n, zero) == p22n)
    assert(bf.addition(p222, zero) == p222)
    assert(bf.addition(zero, ueqv) == ueqv)
    assert(bf.addition(zero, nopath) == nopath)
    assert(bf.addition(zero, p22n) == p22n)
    assert(bf.addition(zero, p222) == p222)

    // annihilator
    assert(bf.multiplication(ueqv, zero) == zero)
    assert(bf.multiplication(nopath, zero) == zero)
    assert(bf.multiplication(p22n, zero) == zero)
    assert(bf.multiplication(p222, zero) == zero)
    assert(bf.multiplication(zero, ueqv) == zero)
    assert(bf.multiplication(zero, nopath) == zero)
    assert(bf.multiplication(zero, p22n) == zero)
    assert(bf.multiplication(zero, p222) == zero)

  }
}
// scalastyle:on println
