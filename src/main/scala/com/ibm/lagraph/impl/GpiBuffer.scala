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

import annotation.tailrec
import scala.math.{min, max}
import scala.{specialized => spec}
import com.ibm.lagraph.LagUtils

final class GpiBuffer[@spec(Int) A: ClassTag](val elems: Array[A], val length: Int)
    extends Serializable {

  def this(elems: Array[A]) = {
    this(elems, elems.length)
  }

  override def equals(o: Any): Boolean = o match {
    case that: GpiBuffer[A] => {
      if (this.hashCode == that.hashCode) {
        true
      } else if (that.length != this.length) {
        false
      } else {
        var doesmatch = true
        var i = 0
        val k = this.length
        while (doesmatch && i < k) {
          if (this.elems(i) != that.elems(i)) {
            doesmatch = false
          }
          i += 1
        }
        doesmatch
      }
    }
    case _ => false
  }
  override def hashCode: Int = elems.hashCode

  //  def length = elems.length

  def toList: List[A] = {
    @tailrec
    def f(i: Int, as: List[A]): List[A] =
      if (i < 0) as else f(i - 1, apply(i) :: as)
    f(length - 1, Nil)
  }
  def toVector: Vector[A] = Vector(toArray: _*)

  def foreach(f: Function[A, Unit]): Unit = {
    var i = 0
    while (i < length) {
      f(apply(i))
      i += 1
    }
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append("GpiBuffer(")
    if (length > 0) {
      sb.append(apply(0))
      var i = 1
      while (i < length) {
        sb.append(",")
        sb.append(apply(i))
        i += 1
      }
    }
    sb.append(")")
    sb.toString
  }

  // ****
  def toArray: Array[A] = {
    val as = Array.ofDim[A](length)
    System.arraycopy(elems, 0, as, 0, length)
    as
  }
  def apply(i: Int): A = elems(i)
  def map[@spec(Int) B: ClassTag](f: A => B): GpiBuffer[B] = {
    val bs = Array.ofDim[B](this.length)
    var i = 0
    val k = this.length
    while (i < k) {
      bs(i) = f(this(i))
      i += 1
    }
    new GpiBuffer(bs)
  }

  def foldLeft[@spec(Int) B: ClassTag](z: B)(f: (A, B) => B): (B, Int) = {
    var result = z
    this.foreach(x => result = f(x, result))
    (result, this.length)
  }

  def count(p: A => Boolean): Int = {
    var cnt = 0
    this.foreach(x => if (p(x)) cnt += 1)
    cnt
  }
  def size: Int = length
  def isEmpty: Boolean = length == 0
  def last: A = {
    if (isEmpty) throw new UnsupportedOperationException("GpiBuffer: empty")
    apply(length - 1)
  }

  def zip[@spec(Int) B](that: GpiBuffer[B]): GpiBuffer[(A, B)] = {
    val len = this.length min that.length
    val bs = Array.ofDim[(A, B)](len)

    var i = 0
    while (i < len) {
      bs(i) = ((this(i), that(i)))
      i += 1
    }
    new GpiBuffer(bs)
  }

  def zipWithIndex(): GpiBuffer[(A, Int)] = {
    val bs = Array.ofDim[(A, Int)](this.length)

    var i = 0
    val k = this.length
    while (i < k) {
      bs(i) = ((this(i), i))
      i += 1
    }
    new GpiBuffer(bs)
  }
  def updated(index: Int, elem: A): GpiBuffer[A] = {
    val bs = Array.ofDim[A](this.length)
    var i = 0
    val k = index
    while (i < k) {
      bs(i) = this(i)
      i += 1
    }
    bs(i) = elem
    i += 1
    val l = this.length
    while (i < l) {
      bs(i) = this(i)
      i += 1
    }
    new GpiBuffer(bs)
  }
  def inserted(index: Int, elem: A): GpiBuffer[A] = {
    val bs = Array.ofDim[A](this.length + 1)
    var i = 0
    val k = index
    while (i < k) {
      bs(i) = this(i)
      i += 1
    }
    var j = i
    bs(j) = elem
    j += 1
    val l = this.length
    while (i < l) {
      bs(j) = this(i)
      i += 1
      j += 1
    }
    new GpiBuffer(bs)
  }

  def extend(cnt: Int, elem: A): GpiBuffer[A] = {
    val bs = Array.ofDim[A](this.length + cnt)
    var i = 0
    val k = this.length
    while (i < k) {
      bs(i) = this(i)
      i += 1
    }
    val m = this.length + cnt
    while (i < m) {
      bs(i) = elem
      i += 1
    }
    new GpiBuffer(bs)
  }

  @throws(classOf[java.io.IOException])
  private def sizeofElem(): Int = {
    val obj = elems
    val byteOutputStream: java.io.ByteArrayOutputStream =
      new java.io.ByteArrayOutputStream();
    val objectOutputStream: java.io.ObjectOutputStream =
      new java.io.ObjectOutputStream(byteOutputStream);
    objectOutputStream.writeObject(obj);
    objectOutputStream.flush();
    objectOutputStream.close();
    return byteOutputStream.toByteArray().length;
  }
}

// TODO deal with boxing
object GpiBuffer {

  def rvSparseBuffersToDenseBuffer[@spec(Int) B: ClassTag](rv: (GpiBuffer[Int], GpiBuffer[B]),
                                                           sparseValue: B,
                                                           size: Int): GpiBuffer[B] = {
    val t0 = System.nanoTime()
    val dbs = Array.fill(size)(sparseValue)
    var i = 0
    val k = rv._1.length
    while (i < k) {
      dbs(rv._1(i)) = rv._2(i)
      i += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: rvSparseBuffersToDenseBuffer: time: >%.3f< s".format(t01))
    }
    GpiBuffer(dbs)
  }

  def rvSeqToSparseBuffers[@spec(Int) B: ClassTag](
      rseq: Seq[Int],
      vseq: Seq[B],
      sparseValue: B,
      denseCount: Int): (GpiBuffer[Int], GpiBuffer[B]) = {
    val t0 = System.nanoTime()
    require(rseq.length == vseq.length)
    val rbs = Array.ofDim[Int](denseCount)
    val vbs = Array.ofDim[B](denseCount)
    var i = 0
    var j = 0
    while (i < rseq.length) {
      val v = vseq(i)
      if (v != sparseValue) {
        rbs(j) = rseq(i)
        vbs(j) = v
        j += 1
      }
      i += 1
    }
    require(j == denseCount)
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: rvSeqToSparseBuffers: time: >%.3f< s".format(t01))
    }
    (GpiBuffer(rbs), GpiBuffer(vbs))
  }

  def rvSeqToDenseBuffer[@spec(Int) B: ClassTag](rseq: Seq[Int],
                                                 vseq: Seq[B],
                                                 sparseValue: B,
                                                 len: Int): GpiBuffer[B] = {
    val t0 = System.nanoTime()
    require(rseq.length == vseq.length)
    val vbs = Array.fill(len)(sparseValue)
    var i = 0
    while (i < rseq.length) {
      vbs(rseq(i)) = vseq(i)
      i += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: rvSeqToDenseBuffer: time: >%.3f< s".format(t01))
    }
    GpiBuffer(vbs)
  }

  def rvDenseBufferToSparseBuffer[@spec(Int) B: ClassTag](
      dbs: GpiBuffer[B],
      denseCount: Int,
      sparseValue: B): (GpiBuffer[Int], GpiBuffer[B]) = {
    val t0 = System.nanoTime()

    val rbs = Array.ofDim[Int](denseCount)
    val vbs = Array.ofDim[B](denseCount)
    var i = 0
    var j = 0
    while (i < dbs.length) {
      if (dbs(i) != sparseValue) {
        rbs(j) = i
        vbs(j) = dbs(i)
        j += 1
      }
      i += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    // x    println("GpiBuffer: rvDenseBufferToSparseBuffer: time: >%.3f< s".format(t01))
    (GpiBuffer(rbs), GpiBuffer(vbs))
  }

  def gpiMapDenseBufferToDenseBuffer[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
      //  def gpiMapDenseBufferToDenseBuffer[@spec(Int) A: ClassTag, @spec(Int) B](
      dbs: GpiBuffer[A],
      sparseValue: B,
      f: A => B): (GpiBuffer[B], Int, Int) = {
    //    f: A => B)(implicit eA: ClassTag[A],  eB:ClassTag[B]): (GpiBuffer[B], Int, Int) = {
    val xA = classTag[A]
    val xB = classTag[B]
    val t0 = System.nanoTime()
    val bs = Array.ofDim[B](dbs.length)
    var i = 0
    val k = dbs.length
    var newDenseCount = 0
    val ta = System.nanoTime()
    val t0a = LagUtils.tt(t0, ta)
    var xMax = 0.0
    var xMin = 9999999.0
    while (i < k) {
      val xt0 = System.nanoTime()
      bs(i) = f(dbs(i))
      if (bs(i) != sparseValue) newDenseCount += 1
      i += 1
      val xt1 = System.nanoTime()
      val xt01 = LagUtils.tt(xt0, xt1)
      if (xt01 > xMax) xMax = xt01
      if (xt01 < xMin) xMin = xt01
    }
    // x    println("    GpiBuffer:
    //      gpiMapDenseBufferToDenseBuffer: t0a: >%.3f<, k: >%d<, newDenseCount: >%d<, A:>%s<,
    //      B:>%s<, xMin: >%.6f<, xMax: >%.6f<".format(t0a, k, newDenseCount, xA, xB, xMin, xMax))
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    // x    println("GpiBuffer: gpiMapDenseBufferToDenseBuffer: time: >%.3f< s".format(t01))
    (GpiBuffer(bs), newDenseCount, k)
  }

  def gpiMapSparseBuffersToDenseBuffer[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      len: Int,
      sparseValueA: A,
      sparseValueB: B,
      f: A => B): (GpiBuffer[B], Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val vB = Array.ofDim[B](len)
    var iiA = 0
    var iB = 0
    var iiO = 0
    var denseCountB = 0
    val fofsparse = f(sparseValueA)
    while (iB < len) {
      if (iiA < lenA && iB == rvA._1(iiA)) {
        vB(iB) = f(rvA._2(iiA))
        iiA += 1
      } else {
        vB(iB) = fofsparse
      }
      if (vB(iB) != sparseValueB) denseCountB += 1
      iB += 1
    }
    require(iiA == lenA)
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    // x    println("GpiBuffer: gpiMapSparseBuffersToDenseBuffer: time: >%.3f<,
    //      denseCountB: >%d<, ops: >%d<".format(t01, denseCountB, iiA))
    (GpiBuffer(vB), denseCountB, iiA)
  }

  def gpiMapSparseBuffersToSparseBuffers[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
      rv: (GpiBuffer[Int], GpiBuffer[A]),
      sparseValue: B,
      f: A => B): (GpiBuffer[Int], GpiBuffer[B], Int) = {
    val t0 = System.nanoTime()
    val len = rv._1.length
    val rs = Array.ofDim[Int](len)
    val vs = Array.ofDim[B](len)
    var i = 0
    var j = 0
    val k = len
    while (i < k) {
      vs(j) = f(rv._2(i))
      if (vs(j) != sparseValue) {
        rs(j) = rv._1(i)
        j += 1
      }
      i += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    // x    println("GpiBuffer: gpiMapSparseBuffersToSparseBuffers: time: >%.3f< s".format(t01))
    (GpiBuffer(rs, j), GpiBuffer(vs, j), k)
  }

  def gpiZipSparseSparseToDense[@spec(Int) A: ClassTag,
                                @spec(Int) B: ClassTag,
                                @spec(Int) C: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      rvB: (GpiBuffer[Int], GpiBuffer[B]),
      len: Int,
      sparseValueA: A,
      sparseValueB: B,
      sparseValueC: C,
      f: (A, B) => C): (GpiBuffer[C], Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val lenB = rvB._1.length
    val vC = Array.ofDim[C](len)
    var iiA = 0
    var iiB = 0
    var iC = 0
    var denseCountC = 0
    while (iC < len) {
      iC match {
        case n if iiA < lenA && iiB < lenB && n == rvA._1(iiA) && n == rvB._1(iiB) => {
          vC(n) = f(rvA._2(iiA), rvB._2(iiB))
          iiA += 1
          iiB += 1
        }
        case n if iiA < lenA && n == rvA._1(iiA) => {
          vC(n) = f(rvA._2(iiA), sparseValueB)
          iiA += 1
        }
        case n if iiB < lenB && n == rvB._1(iiB) => {
          vC(n) = f(sparseValueA, rvB._2(iiB))
          iiB += 1
        }
        case _ => {
          vC(iC) = f(sparseValueA, sparseValueB)
        }
      }
      if (vC(iC) != sparseValueC) denseCountC += 1
      iC += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipSparseSparseToDense: time: >%.3f< s".format(t01))
    }
    (GpiBuffer(vC), denseCountC, len)
  }

  def gpiZipSparseSparseToSparse[@spec(Int) A: ClassTag,
                                 @spec(Int) B: ClassTag,
                                 @spec(Int) C: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      rvB: (GpiBuffer[Int], GpiBuffer[B]),
      len: Int,
      sparseValueA: A,
      sparseValueB: B,
      sparseValueC: C,
      f: (A, B) => C): ((GpiBuffer[Int], GpiBuffer[C]), Int, Int) = {
    require(sparseValueA == sparseValueB)
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val lenB = rvB._1.length
    if (lenA < lenB) {
      val rvCr = Array.ofDim[Int](lenA)
      val rvCv = Array.ofDim[C](lenA)
      var iiA = 0
      var iiB = 0
      var iiC = 0
      var iiO = 0
      while (iiA < lenA && iiB < lenB) {
        if (rvA._1(iiA) < rvB._1(iiB)) {
          iiA += 1
        } else if (rvA._1(iiA) > rvB._1(iiB)) {
          iiB += 1
        } else {
          rvCr(iiC) = rvA._1(iiA)
          rvCv(iiC) = f(rvA._2(iiA), rvB._2(iiB))
          iiO += 1
          if (rvCv(iiC) != sparseValueC) iiC += 1
          iiA += 1
          iiB += 1
        }
      }
      val t1 = System.nanoTime()
      val t01 = LagUtils.tt(t0, t1)
      if (false) {
        println(
          "GpiBuffer: gpiZipSparseSparseToSparse: lenA < lenB: time: >%.3f< s"
            .format(t01))
      }
      ((GpiBuffer(rvCr, iiC), GpiBuffer(rvCv, iiC)), iiC, iiO)
    } else {
      val rvCr = Array.ofDim[Int](lenB)
      val rvCv = Array.ofDim[C](lenB)
      var iiA = 0
      var iiB = 0
      var iiC = 0
      var iiO = 0
      while (iiA < lenA && iiB < lenB) {
        if (rvB._1(iiB) < rvA._1(iiA)) {
          iiB += 1
        } else if (rvB._1(iiB) > rvA._1(iiA)) {
          iiA += 1
        } else {
          rvCr(iiC) = rvB._1(iiB)
          rvCv(iiC) = f(rvA._2(iiA), rvB._2(iiB))
          iiO += 1
          if (rvCv(iiC) != sparseValueC) iiC += 1
          iiA += 1
          iiB += 1
        }
      }
      val t1 = System.nanoTime()
      val t01 = LagUtils.tt(t0, t1)
      if (false) {
        println(
          "GpiBuffer: gpiZipSparseSparseToSparse: lenA >= lenB: time: >%.3f< s"
            .format(t01))
      }
      ((GpiBuffer(rvCr, iiC), GpiBuffer(rvCv, iiC)), iiC, iiO)
    }
  }

  def gpiZipSparseDenseToDense[@spec(Int) A: ClassTag,
                               @spec(Int) B: ClassTag,
                               @spec(Int) C: ClassTag](rvA: (GpiBuffer[Int], GpiBuffer[A]),
                                                       vB: GpiBuffer[B],
                                                       len: Int,
                                                       sparseValueA: A,
                                                       sparseValueB: B,
                                                       sparseValueC: C,
                                                       f: (A, B) => C): (GpiBuffer[C], Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val lenB = vB.length
    val vC = Array.ofDim[C](len)
    var iiA = 0
    var iB = 0
    var iC = 0
    var denseCountC = 0
    while (iC < len) {
      if (iiA < lenA && iC == rvA._1(iiA)) {
        vC(iC) = f(rvA._2(iiA), vB(iB))
        iiA += 1
      } else {
        vC(iC) = f(sparseValueA, vB(iB))
      }
      if (vC(iC) != sparseValueC) denseCountC += 1
      iC += 1
      iB += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipSparseDenseToDense: time: >%.3f< s".format(t01))
    }
    (GpiBuffer(vC), denseCountC, len)
  }

  def gpiZipDenseSparseToDense[@spec(Int) A: ClassTag,
                               @spec(Int) B: ClassTag,
                               @spec(Int) C: ClassTag](vA: GpiBuffer[A],
                                                       rvB: (GpiBuffer[Int], GpiBuffer[B]),
                                                       len: Int,
                                                       sparseValueA: A,
                                                       sparseValueB: B,
                                                       sparseValueC: C,
                                                       f: (A, B) => C): (GpiBuffer[C], Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = vA.length
    val lenB = rvB._1.length
    val vC = Array.ofDim[C](lenA)
    var iA = 0
    var iiB = 0
    var iC = 0
    var denseCountC = 0
    while (iC < lenA) {
      if (iiB < lenB && iC == rvB._1(iiB)) {
        vC(iC) = f(vA(iA), rvB._2(iiB))
        iiB += 1
      } else {
        vC(iC) = f(vA(iA), sparseValueB)
      }
      if (vC(iC) != sparseValueC) denseCountC += 1
      iC += 1
      iA += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipDenseSparseToDense: time: >%.3f< s".format(t01))
    }
    (GpiBuffer(vC), denseCountC, lenA)
  }

  def gpiZipDenseSparseToSparse[@spec(Int) A: ClassTag,
                                @spec(Int) B: ClassTag,
                                @spec(Int) C: ClassTag](
      vA: GpiBuffer[A],
      rvB: (GpiBuffer[Int], GpiBuffer[B]),
      len: Int,
      sparseValueA: A,
      sparseValueB: B,
      sparseValueC: C,
      f: (A, B) => C): ((GpiBuffer[Int], GpiBuffer[C]), Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = vA.length
    val lenB = rvB._1.length
    val rvCr = Array.ofDim[Int](lenB)
    val rvCv = Array.ofDim[C](lenB)
    var iA = 0
    var iiB = 0
    var iiC = 0
    while (iiB < lenB) {
      rvCr(iiC) = rvB._1(iiB)

      rvCv(iiC) = f(vA(rvB._1(iiB)), rvB._2(iiB))
      if (rvCv(iiC) != sparseValueC) iiC += 1
      iiB += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipDenseSparseToSparse: time: >%.3f< s".format(t01))
    }
    ((GpiBuffer(rvCr, iiC), GpiBuffer(rvCv, iiC)), iiC, lenB)
  }

  def gpiZipSparseDenseToSparse[@spec(Int) A: ClassTag,
                                @spec(Int) B: ClassTag,
                                @spec(Int) C: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      vB: GpiBuffer[B],
      len: Int,
      sparseValueA: A,
      sparseValueB: B,
      sparseValueC: C,
      f: (A, B) => C): ((GpiBuffer[Int], GpiBuffer[C]), Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val lenB = vB.length
    val rvCr = Array.ofDim[Int](lenA)
    val rvCv = Array.ofDim[C](lenA)
    var iiA = 0
    var iB = 0
    var iiC = 0
    while (iiA < lenA) {
      rvCr(iiC) = rvA._1(iiA)

      rvCv(iiC) = f(rvA._2(iiA), vB(rvA._1(iiA)))
      if (rvCv(iiC) != sparseValueC) iiC += 1
      iiA += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipSparseDenseToSparse: time: >%.3f< s".format(t01))
    }
    ((GpiBuffer(rvCr, iiC), GpiBuffer(rvCv, iiC)), iiC, lenA)
  }

  def gpiZipDenseDenseToDense[@spec(Int) A: ClassTag,
                              @spec(Int) B: ClassTag,
                              @spec(Int) C: ClassTag](vA: GpiBuffer[A],
                                                      vB: GpiBuffer[B],
                                                      len: Int,
                                                      sparseValueA: A,
                                                      sparseValueB: B,
                                                      sparseValueC: C,
                                                      f: (A, B) => C): (GpiBuffer[C], Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = vA.length
    val lenB = vB.length
    val vC = Array.ofDim[C](len)
    var iA = 0
    var iB = 0
    var iC = 0
    var denseCountC = 0
    while (iC < len) {
      vC(iC) = f(vA(iA), vB(iB))
      if (vC(iC) != sparseValueC) denseCountC += 1
      iC += 1
      iA += 1
      iB += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipDenseDenseToDense: time: >%.3f< s".format(t01))
    }
    (GpiBuffer(vC), denseCountC, len)
  }

  // ****
  // ** zip with index vector
  def gpiZipSparseWithIndexToSparse[@spec(Int) A: ClassTag, @spec(Int) C: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      len: Int,
      base: Long,
      sparseValueA: A,
      sparseValueC: C,
      f: (A, Long) => C): ((GpiBuffer[Int], GpiBuffer[C]), Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val rvCr = Array.ofDim[Int](lenA)
    val rvCv = Array.ofDim[C](lenA)
    var iiA = 0
    var iiC = 0
    while (iiA < lenA) {
      rvCr(iiC) = rvA._1(iiA)
      rvCv(iiC) = f(rvA._2(iiA), rvCr(iiC).toLong + base)
      if (rvCv(iiC) != sparseValueC) iiC += 1
      iiA += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipSparseWithIndexToSparse: time: >%.3f< s".format(t01))
    }
    ((GpiBuffer(rvCr, iiC), GpiBuffer(rvCv, iiC)), iiC, lenA)
  }

  def gpiZipSparseWithIndexToDense[@spec(Int) A: ClassTag, @spec(Int) C: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      len: Int,
      base: Long,
      sparseValueA: A,
      sparseValueC: C,
      f: (A, Long) => C): (GpiBuffer[C], Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val vC = Array.ofDim[C](len)
    var iiA = 0
    var iC = 0
    var denseCountC = 0
    while (iC < len) {
      if (iiA < lenA && iC == rvA._1(iiA)) {
        vC(iC) = f(rvA._2(iiA), iC.toLong + base)
        iiA += 1
      } else {
        vC(iC) = f(sparseValueA, iC.toLong + base)
      }
      if (vC(iC) != sparseValueC) denseCountC += 1
      iC += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipSparseWithIndexToDense: time: >%.3f< s".format(t01))
    }
    (GpiBuffer(vC), denseCountC, len)
  }

  def gpiZipDenseWithIndexToDense[@spec(Int) A: ClassTag, @spec(Int) C: ClassTag](
      vA: GpiBuffer[A],
      len: Int,
      base: Long,
      sparseValueA: A,
      sparseValueC: C,
      f: (A, Long) => C): (GpiBuffer[C], Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = vA.length
    val vC = Array.ofDim[C](len)
    var iA = 0
    var iC = 0
    var denseCountC = 0
    while (iC < len) {
      vC(iC) = f(vA(iA), iC.toLong + base)
      if (vC(iC) != sparseValueC) denseCountC += 1
      iC += 1
      iA += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipDenseWithIndexToDense: time: >%.3f< s".format(t01))
    }
    (GpiBuffer(vC), denseCountC, len)
  }

  // ****
  // ** zip with index matrix
  def gpiZipSparseWithIndexToSparseMatrix[@spec(Int) A: ClassTag, @spec(Int) C: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      len: Int,
      rowIndex: Long,
      base: Long,
      visitDiagonalsOpt: Option[A],
      sparseValueA: A,
      sparseValueC: C,
      f: (A, (Long, Long)) => C): ((GpiBuffer[Int], GpiBuffer[C]), Int, Int) = {
    val visitDiagonals = visitDiagonalsOpt.isDefined
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val rvCr =
      if (visitDiagonals) Array.ofDim[Int](lenA + 1) else Array.ofDim[Int](lenA)
    val rvCv =
      if (visitDiagonals) Array.ofDim[C](lenA + 1) else Array.ofDim[C](lenA)
    var iiA = 0
    var iiC = 0
    var diagonalProcessed = false
    if ((rowIndex < base) || (rowIndex >= (base + len))) {
      // don't bother if diagonal doesn't fall in this block
      diagonalProcessed = true
    }
    while (iiA < lenA) {
      val abscol = rvA._1(iiA).toLong + base
      if (visitDiagonals && !diagonalProcessed && (abscol == rowIndex)) {
        // landed on diagonal, ie it is not sparse
        diagonalProcessed = true
        rvCr(iiC) = rvA._1(iiA)
        rvCv(iiC) = f(rvA._2(iiA), (rowIndex, abscol))
        if (rvCv(iiC) != sparseValueC) iiC += 1
        iiA += 1
      } else if (visitDiagonals && !diagonalProcessed && (abscol > rowIndex)) {
        // jumped over diagonal, deal with it
        diagonalProcessed = true
        rvCr(iiC) = (rowIndex - base).toInt
        rvCv(iiC) = f(visitDiagonalsOpt.get, (rowIndex, rowIndex))
        if (rvCv(iiC) != sparseValueC) iiC += 1
        // now deal with next value
        rvCr(iiC) = rvA._1(iiA)
        rvCv(iiC) = f(rvA._2(iiA), (rowIndex, abscol))
        if (rvCv(iiC) != sparseValueC) iiC += 1
        iiA += 1
      } else {
        // not visiting diagonals or diagonal already handled or diagonal not reached
        if (rowIndex == abscol) diagonalProcessed = true
        rvCr(iiC) = rvA._1(iiA)
        rvCv(iiC) = f(rvA._2(iiA), (rowIndex, abscol))
        if (rvCv(iiC) != sparseValueC) iiC += 1
        iiA += 1
      }
    }
    if (visitDiagonals && !diagonalProcessed) {
      // diagonal in block and never reached it, deal w/ that now
      diagonalProcessed = true
      rvCr(iiC) = (rowIndex - base).toInt
      rvCv(iiC) = f(visitDiagonalsOpt.get, (rowIndex, rowIndex))
      if (rvCv(iiC) != sparseValueC) iiC += 1
    }
    require(!visitDiagonals || diagonalProcessed)
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipSparseWithIndexToSparseMatrix: time: >%.3f< s".format(t01))
    }
    ((GpiBuffer(rvCr, iiC), GpiBuffer(rvCv, iiC)), iiC, iiC)
  }

  def gpiZipSparseWithIndexToDenseMatrix[@spec(Int) A: ClassTag, @spec(Int) C: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      len: Int,
      rowIndex: Long,
      base: Long,
      sparseValueA: A,
      sparseValueC: C,
      f: (A, (Long, Long)) => C): (GpiBuffer[C], Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val vC = Array.ofDim[C](len)
    var iiA = 0
    var iC = 0
    var denseCountC = 0
    while (iC < len) {
      if (iiA < lenA && iC == rvA._1(iiA)) {
        vC(iC) = f(rvA._2(iiA), (rowIndex, iC.toLong + base))
        iiA += 1
      } else {
        vC(iC) = f(sparseValueA, (rowIndex, iC.toLong + base))
      }
      if (vC(iC) != sparseValueC) denseCountC += 1
      iC += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipSparseWithIndexToDenseMatrix: time: >%.3f< s".format(t01))
    }
    (GpiBuffer(vC), denseCountC, len)
  }

  def gpiZipDenseWithIndexToDenseMatrix[@spec(Int) A: ClassTag, @spec(Int) C: ClassTag](
      vA: GpiBuffer[A],
      len: Int,
      rowIndex: Long,
      base: Long,
      sparseValueA: A,
      sparseValueC: C,
      f: (A, (Long, Long)) => C): (GpiBuffer[C], Int, Int) = {
    val t0 = System.nanoTime()
    val lenA = vA.length
    val vC = Array.ofDim[C](len)
    var iA = 0
    var iC = 0
    var denseCountC = 0
    while (iC < len) {
      vC(iC) = f(vA(iA), (rowIndex, iC.toLong + base))
      if (vC(iC) != sparseValueC) denseCountC += 1
      iC += 1
      iA += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiZipDenseWithIndexToDenseMatrix: time: >%.3f< s".format(t01))
    }
    (GpiBuffer(vC), denseCountC, len)
  }

  def rvExtendSparseBuffersWithNonSparseValue[@spec(Int) A: ClassTag](
      rv: (GpiBuffer[Int], GpiBuffer[A]),
      origsize: Int,
      deltasize: Int,
      value: A): ((GpiBuffer[Int], GpiBuffer[A]), Int) = {
    val t0 = System.nanoTime()
    require(rv._1.length == rv._2.length)
    val rvl = rv._1.length
    val denseCount = rvl + deltasize
    val rbs = Array.ofDim[Int](denseCount)
    val vbs = Array.ofDim[A](denseCount)
    var i = 0
    while (i < rvl) {
      rbs(i) = rv._1(i)
      vbs(i) = rv._2(i)
      i += 1
    }
    var j = origsize
    while (i < denseCount) {
      rbs(i) = j
      vbs(i) = value
      i += 1
      j += 1
    }
    require(i == denseCount)
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: rvExtendSparseBuffers: time: >%.3f< s".format(t01))
    }
    ((GpiBuffer(rbs), GpiBuffer(vbs)), denseCount)
  }

  def rvDeleteItemFromSparseBuffers[@spec(Int) B: ClassTag](
      rv: (GpiBuffer[Int], GpiBuffer[B]),
      indx: Int): (GpiBuffer[Int], GpiBuffer[B]) = {
    val t0 = System.nanoTime()

    def difba[@spec(Int) A: ClassTag](src: GpiBuffer[A], index: Int) = {
      val bs = Array.ofDim[A](src.length)
      var i = 0
      val k = index
      while (i < k) {
        bs(i) = src(i)
        i += 1
      }
      var j = i
      i += 1
      val l = src.length
      while (i < l) {
        bs(j) = src(i)
        i += 1
      }
      bs
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: rvDeleteItemFromSparseBuffers: time: >%.3f< s".format(t01))
    }
    (GpiBuffer(difba(rv._1, indx)), GpiBuffer(difba(rv._2, indx)))
  }

  def rvUpdateItemInSparseBuffers[@spec(Int) B: ClassTag](
    rv: (GpiBuffer[Int], GpiBuffer[B]),
    indx: Int,
    elem: B): (GpiBuffer[Int], GpiBuffer[B]) =
    (rv._1, rv._2.updated(indx, elem))

  def rvInsertItemInSparseBuffers[@spec(Int) B: ClassTag](
    rv: (GpiBuffer[Int], GpiBuffer[B]),
    cursor: Int,
    indx: Int,
    elem: B): (GpiBuffer[Int], GpiBuffer[B]) =
    (rv._1.inserted(cursor, indx), rv._2.inserted(cursor, elem))

  // ****
  def apply[@spec(Int) A: ClassTag](as: Array[A]): GpiBuffer[A] = new GpiBuffer(as)
  def apply[@spec(Int) A: ClassTag](as: Array[A], length: Int): GpiBuffer[A] =
    new GpiBuffer(as, length)
  def empty[@spec(Int) A: ClassTag]: GpiBuffer[A] = new GpiBuffer(Array.empty[A])
  private def ofDim[@spec(Int) A: ClassTag](n: Int) =
    new GpiBuffer(Array.ofDim[A](n))
  private def fill[@spec(Int) A: ClassTag](n: Int)(a: A) =
    new GpiBuffer(Array.fill(n)(a))

  def binarySearchValue(ds: GpiBuffer[Int], key: Int): Option[Int] =
    binarySearch(ds, key)._1

  def binarySearch(ds: GpiBuffer[Int], key: Int): (Option[Int], Option[Int]) = {
    val t0 = System.nanoTime()
    @tailrec
    def fr(lo: Int, hi: Int): (Option[Int], Option[Int]) = {
      if (lo > hi) {
        (None, Some(lo))
      } else {
        val mid: Int = lo + (hi - lo) / 2
        ds(mid) match {
          case mv if (mv == key) => (Some(mid), None)
          case mv if (mv <= key) => fr(mid + 1, hi)
          case _ => fr(lo, mid - 1)
        }
      }
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) { println("GpiBuffer: binarySearch: time: >%.3f< s".format(t01)) }
    fr(0, ds.size - 1)
  }
  // ****
  // new compare

  def gpiCompareSparseSparse[@spec(Int) T: ClassTag](rvA: (GpiBuffer[Int], GpiBuffer[T]),
                                                     rvB: (GpiBuffer[Int], GpiBuffer[T]),
                                                     len: Int,
                                                     sparseValueA: T,
                                                     sparseValueB: T): (Boolean, Int) =
    if (sparseValueA != sparseValueB) {
      gpiCompareSparseSparseToDense(rvA, rvB, len, sparseValueA, sparseValueB)
    } else {
      gpiCompareSparseSparseToSparse(rvA, rvB, len, sparseValueA, sparseValueB)
    }

  private def gpiCompareSparseSparseToDense[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      rvB: (GpiBuffer[Int], GpiBuffer[B]),
      len: Int,
      sparseValueA: A,
      sparseValueB: B): (Boolean, Int) = {
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val lenB = rvB._1.length
    var iiA = 0
    var iiB = 0
    var iC = 0
    var matched = true
    while (iC < len && matched) {
      iC match {
        case n if iiA < lenA && iiB < lenB && n == rvA._1(iiA) && n == rvB._1(iiB) => {
          matched = rvA._2(iiA) == rvB._2(iiB)
          iiA += 1
          iiB += 1
        }
        case n if iiA < lenA && n == rvA._1(iiA) => {
          matched = rvA._2(iiA) == sparseValueB
          iiA += 1
        }
        case n if iiB < lenB && n == rvB._1(iiB) => {
          matched = sparseValueA == rvB._2(iiB)
          iiB += 1
        }
        case _ => {
          matched = sparseValueA == sparseValueB
        }
      }
      iC += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiCompareSparseSparseToDense: time: >%.3f< s".format(t01))
    }
    (matched, iC)
  }

  private def gpiCompareSparseSparseToSparse[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      rvB: (GpiBuffer[Int], GpiBuffer[B]),
      len: Int,
      sparseValueA: A,
      sparseValueB: B): (Boolean, Int) = {
    require(sparseValueA == sparseValueB)
    def skipSparse[T](curi: Int,
                      len: Int,
                      bufs: (GpiBuffer[Int], GpiBuffer[T]),
                      sparseValue: T): Int = {
      if (curi == len || bufs._2(curi) != sparseValue) curi
      else skipSparse(curi + 1, len, bufs, sparseValue)
    }
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val lenB = rvB._1.length
    var matched = true
    if (lenA < lenB) {
      var iiA = skipSparse(0, lenA, rvA, sparseValueA)
      var iiB = skipSparse(0, lenB, rvB, sparseValueB)
      var iiC = 0 // TODO only for stats, potential performance impact?
      while (iiA < lenA && iiB < lenB && matched) {
        if (rvA._1(iiA) < rvB._1(iiB)) {
          iiA = skipSparse(iiA + 1, lenA, rvA, sparseValueA)
        } else if (rvA._1(iiA) > rvB._1(iiB)) {
          iiB = skipSparse(iiB + 1, lenB, rvB, sparseValueB)
        } else {
          matched = rvA._2(iiA) == rvB._2(iiB)
          iiA = skipSparse(iiA + 1, lenA, rvA, sparseValueA)
          iiB = skipSparse(iiB + 1, lenB, rvB, sparseValueB)
          iiC += 1
        }
      }
      val t1 = System.nanoTime()
      val t01 = LagUtils.tt(t0, t1)
      if (false) {
        println(
          "GpiBuffer: gpiCompareSparseSparseToSparse: lenA < lenB: time: >%.3f< s"
            .format(t01))
        }
      ((matched && iiA == lenA && iiB == lenB), iiC)
    } else {
      var iiA = skipSparse(0, lenA, rvA, sparseValueA)
      var iiB = skipSparse(0, lenB, rvB, sparseValueB)
      var iiC = 0 // TODO only for stats, potential performance impact?
      while (iiA < lenA && iiB < lenB && matched) {
        if (rvB._1(iiB) < rvA._1(iiA)) {
          iiB = skipSparse(iiB + 1, lenB, rvB, sparseValueB)
        } else if (rvB._1(iiB) > rvA._1(iiA)) {
          iiA = skipSparse(iiA + 1, lenA, rvA, sparseValueA)
        } else {
          matched = rvA._2(iiA) == rvB._2(iiB)
          iiA = skipSparse(iiA + 1, lenA, rvA, sparseValueA)
          iiB = skipSparse(iiB + 1, lenB, rvB, sparseValueB)
          iiC += 1
        }
      }
      val t1 = System.nanoTime()
      val t01 = LagUtils.tt(t0, t1)
      if (false) {
        println(
          "GpiBuffer: gpiCompareSparseSparseToSparse: lenA >= lenB: time: >%.3f< s"
            .format(t01))
        }
      ((matched && iiA == lenA && iiB == lenB), iiC)
    }
  }

  def gpiCompareSparseDense[@spec(Int) T: ClassTag](rvA: (GpiBuffer[Int], GpiBuffer[T]),
                                                    vB: GpiBuffer[T],
                                                    len: Int,
                                                    sparseValueA: T,
                                                    sparseValueB: T): (Boolean, Int) =
    if (sparseValueA != sparseValueB) {
      gpiCompareSparseDenseToDense(rvA, vB, len, sparseValueA, sparseValueB)
    } else {
      gpiCompareSparseDenseToDense(rvA, vB, len, sparseValueA, sparseValueB)
//    gpiCompareSparseDenseToSparse(rvA, vB, len, sparseValueA, sparseValueB) BROKE
    }

  private def gpiCompareSparseDenseToDense[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
      rvA: (GpiBuffer[Int], GpiBuffer[A]),
      vB: GpiBuffer[B],
      len: Int,
      sparseValueA: A,
      sparseValueB: B): (Boolean, Int) = {
    val t0 = System.nanoTime()
    val lenA = rvA._1.length
    val lenB = vB.length
    var iiA = 0
    var iB = 0
    var iC = 0
    var denseCountC = 0
    var matched = true
    while (iC < len && matched) {
      if (iiA < lenA && iC == rvA._1(iiA)) {
        matched = rvA._2(iiA) == vB(iB)
        iiA += 1
      } else {
        matched = sparseValueA == vB(iB)
      }
      iC += 1
      iB += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiCompareSparseDenseToDense: time: >%.3f< s".format(t01))
    }
    (matched, iC)
  }

//  private def gpiCompareSparseDenseToSparse[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
//    rvA: (GpiBuffer[Int], GpiBuffer[A]),
//    vB: GpiBuffer[B],
//    len: Int,
//    sparseValueA: A,
//    sparseValueB: B): (Boolean, Int) = {
//    val t0 = System.nanoTime()
//    val lenA = rvA._1.length
//    val lenB = vB.length
//    var iiA = 0
//    var iB = 0
//    var matched = true
//    while (iiA < lenA && matched) {
//      matched = rvA._2(iiA) == vB(rvA._1(iiA))
//      iiA += 1
//    }
//    val t1 = System.nanoTime()
//    val t01 = LagUtils.tt(t0, t1)
//    if (false) println("GpiBuffer: gpiCompareSparseDenseToSparse: time: >%.3f< s".format(t01))
//    (matched, iiA)
//  }

  def gpiCompareDenseSparse[@spec(Int) T: ClassTag](vA: GpiBuffer[T],
                                                    rvB: (GpiBuffer[Int], GpiBuffer[T]),
                                                    len: Int,
                                                    sparseValueA: T,
                                                    sparseValueB: T): (Boolean, Int) =
    if (sparseValueA != sparseValueB) {
      gpiCompareDenseSparseToDense(vA, rvB, len, sparseValueA, sparseValueB)
    }
    else {
      gpiCompareDenseSparseToDense(vA, rvB, len, sparseValueA, sparseValueB)
//    gpiCompareDenseSparseToSparse(vA, rvB, len, sparseValueA, sparseValueB)
    }

  private def gpiCompareDenseSparseToDense[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
      vA: GpiBuffer[A],
      rvB: (GpiBuffer[Int], GpiBuffer[B]),
      len: Int,
      sparseValueA: A,
      sparseValueB: B): (Boolean, Int) = {
    val t0 = System.nanoTime()
    val lenA = vA.length
    val lenB = rvB._1.length
    var iA = 0
    var iiB = 0
    var iC = 0
    var matched = true
    while (iC < lenA && matched) {
      if (iiB < lenB && iC == rvB._1(iiB)) {
        matched = vA(iA) == rvB._2(iiB)
        iiB += 1
      } else {
        matched = vA(iA) == sparseValueB
      }
      iC += 1
      iA += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiCompareDenseSparseToDense: time: >%.3f< s".format(t01))
    }
    (matched, iC)
  }

//  private def gpiCompareDenseSparseToSparse[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
//    vA: GpiBuffer[A],
//    rvB: (GpiBuffer[Int], GpiBuffer[B]),
//    len: Int,
//    sparseValueA: A,
//    sparseValueB: B): (Boolean, Int) = {
//    val t0 = System.nanoTime()
//    val lenA = vA.length
//    val lenB = rvB._1.length
//    var iA = 0
//    var iiB = 0
//    var matched = true
//    while (iiB < lenB && matched) {
//      matched = vA(rvB._1(iiB)) == rvB._2(iiB)
//      iiB += 1
//    }
//    val t1 = System.nanoTime()
//    val t01 = LagUtils.tt(t0, t1)
//    if (false) println("GpiBuffer: gpiCompareDenseSparseToSparse: time: >%.3f< s".format(t01))
//    (matched, iiB)
//  }

  def gpiCompareDenseDense[@spec(Int) T: ClassTag](vA: GpiBuffer[T],
                                                   vB: GpiBuffer[T],
                                                   len: Int,
                                                   sparseValueA: T,
                                                   sparseValueB: T): (Boolean, Int) =
    gpiCompareDenseDenseToDense(vA, vB, len, sparseValueA, sparseValueB)

  private def gpiCompareDenseDenseToDense[@spec(Int) A: ClassTag, @spec(Int) B: ClassTag](
      vA: GpiBuffer[A],
      vB: GpiBuffer[B],
      len: Int,
      sparseValueA: A,
      sparseValueB: B): (Boolean, Int) = {
    val t0 = System.nanoTime()
    val lenA = vA.length
    val lenB = vB.length
    var iA = 0
    var iB = 0
    var iC = 0
    var matched = true
    while (iC < len && matched) {
      matched = vA(iA) == vB(iB)
      iC += 1
      iA += 1
      iB += 1
    }
    val t1 = System.nanoTime()
    val t01 = LagUtils.tt(t0, t1)
    if (false) {
      println("GpiBuffer: gpiCompareDenseDenseToDense: time: >%.3f< s".format(t01))
    }
    (matched, iC)
  }
  def main(args: Array[String]): Unit = {

    val data = Array(1, 2, 3, 4, 5, 6, 7, 8)
    val b = GpiBuffer(data)
    b.foreach(println)

    val c = GpiBuffer(data)
    println("c: >%s<".format(c.toArray.mkString("[", ",", "]")))
    val d = c.updated(2, 99)
    println("d: >%s<".format(d.toArray.mkString("[", ",", "]")))
    val e = c.updated(0, 99)
    println("e: >%s<".format(e.toArray.mkString("[", ",", "]")))
    val f = c.updated(7, 99)
    println("f: >%s<".format(f.toArray.mkString("[", ",", "]")))

    val result = b.foldLeft(100)((a, b) => a + b)
    println("foldleft: >%d<".format(result))

    val size = b.size
    println("size: >%d<".format(size))

    val notThreeCount = b.count { _ != 3 }
    println("count: >%d<".format(notThreeCount))

    val dataz = Array(100, 200, 300, 400, 500, 600, 700, 800)
    val z = GpiBuffer(dataz)
    val zz = b.zip(z)
    println("zz: >%s<".format(zz.toArray.mkString("[", ",", "]")))
    val zwi = b.zipWithIndex()
    println("zwi: >%s<".format(zwi.toArray.mkString("[", ",", "]")))

    val mr = b.map(x => 2 * x)
    println("mr: >%s<".format(mr.toArray.mkString("[", ",", "]")))

    val be = b.extend(3, 999)
    println("be: >%s<".format(be.toArray.mkString("[", ",", "]")))

    val datad = Array(1, 2, 0, 0, 0, 6, 7)
    val u = GpiBuffer(datad)
    val rv = GpiBuffer.rvDenseBufferToSparseBuffer(u, 4, 0)
    println("rv._1: >%s<".format(rv._1.toArray.mkString("[", ",", "]")))
    println("rv._1: >%s<".format(rv._2.toArray.mkString("[", ",", "]")))

    // ****

    assert(data.length > 4)
    assert(b.length == data.length)
    //    assert(b.toArray == data)
    data.zipWithIndex.foreach { case (x, i) => assert(b(i) == x) }
    //    data.zipWithIndex.foreach{ case (x, i) => assert(b.get(i) == Some(x)) }

    //    val s = b.slice(2, 4)
    //    assert(s.length == 2)
    //    assert(s.toArray == Array(data(2), data(3)))

  }
  //  @throws(classOf[java.io.IOException])
  //  def sizeof(obj: Object): Int = {
  //    def byteOutputStream: java.io.ByteArrayOutputStream = new java.io.ByteArrayOutputStream();
  //    def objectOutputStream: java.io.ObjectOutputStream =
  //      new java.io.ObjectOutputStream(byteOutputStream);
  //    objectOutputStream.writeObject(obj);
  //    objectOutputStream.flush();
  //    objectOutputStream.close();
  //    return byteOutputStream.toByteArray().length;
  //  }
}
// scalastyle:on println
