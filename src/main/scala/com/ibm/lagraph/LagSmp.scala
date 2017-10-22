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
package com.ibm.lagraph
import scala.reflect.ClassTag
import scala.collection.mutable.{Map => MMap, Builder}
import com.ibm.lagraph.impl.{GpiAdaptiveVector, GpiSparseRowMatrix, GpiOps}

import com.ibm.lagraph.impl.{LagSmpVector, LagSmpMatrix}

/**
  * The (non-distributed) LAG context
  *
  *  The context is used for creating, importing, exporting, and manipulating
  *  matrices and vectors in a pure Scala environment.
  *
  *  A LAG context defines the dimension of the problem, ie number of vertices.
  *
  *  A LAG context may be used for any number of matrices and vectors.
  *
  *  Matrices and vectors in one context cannot be mixed with matrices and
  *  vectors in a different context.
  */
final case class LagSmpContext(override val graphSize: Long) extends LagContext {

  // *****
  // import / export
  // these are require specialized contexts may offer additional methods
  override def vFromMap[T: ClassTag](m: Map[Long, T], sparseValue: T): LagVector[T] = {
    val mtoint = m.map { case (k, v) => (k.toInt -> v) }
    LagSmpVector(this, GpiAdaptiveVector.fromMap(mtoint, sparseValue, graphSize.toInt))
  }
  override def vFromSeq[T: ClassTag](v: Seq[T], sparseValue: T): LagVector[T] = {
    assert(v.size == graphSize)
    LagSmpVector(this, GpiAdaptiveVector.fromSeq(v, sparseValue))
  }
  private[lagraph] override def vToMap[T: ClassTag](v: LagVector[T]): (Map[Long, T], T) = v match {
    case va: LagSmpVector[T] => {
      val mfromint = GpiAdaptiveVector.toMap(va.v)
      (mfromint.map { case (k, v) => (k.toLong -> v) }, va.v.sparseValue)
    }
  }
  private[lagraph] override def vToVector[T: ClassTag](v: LagVector[T]): Vector[T] = v match {
    case va: LagSmpVector[T] =>
      GpiAdaptiveVector.toVector(va.v)
  }
  override def mFromMap[T: ClassTag](mMap: Map[(Long, Long), T], sparseValue: T): LagMatrix[T] = {
    val nrow = graphSize
    val ncol = graphSize
    val mtoint = mMap.map { case (k, v) => ((k._1.toInt, k._2.toInt) -> v) }
    LagSmpMatrix(this,
                 mMap,
                 GpiSparseRowMatrix.fromMap(mtoint, sparseValue, nrow.toInt, ncol.toInt))
  }

  private[lagraph] override def mToMap[T: ClassTag](m: LagMatrix[T]): (Map[(Long, Long), T], T) =
    m match {
      case ma: LagSmpMatrix[_] => {
        val aa = ma.vov
        aa match {
          case gaa: GpiAdaptiveVector[GpiAdaptiveVector[T]] => {
            //          (GpiSparseRowMatrix.toMap(gaa), gaa(0).sparseValue)
            (GpiSparseRowMatrix.toMap(gaa).map {
              case (k, v) => ((k._1.toLong, k._2.toLong) -> v)
            }, gaa(0).sparseValue)
          }
          case _ => throw new RuntimeException("cant handle native GpiBmat")
        }
      }
    }
  // ****************
  // ****************
  // ****************
  // ****************
  // ****************
  // factory
  override def vIndices(start: Long = 0L, sparseValueOpt: Option[Long] = None): LagVector[Long] = {
    LagSmpVector(this, GpiOps.gpi_indices(graphSize, start))
  }
  override def vReplicate[T: ClassTag](x: T, sparseValueOpt: Option[T] = None): LagVector[T] = {
    LagSmpVector(this, GpiOps.gpi_replicate(graphSize, x, sparseValueOpt))
  }
  // experimental
  override def mIndices(start: (Long, Long)): LagMatrix[(Long, Long)] = {
    require(start._1 > 0)
    val end = (start._1 + graphSize, start._2 + graphSize)
    mFromMap[(Long, Long)](Map((start._1 until end._1).map { r =>
      (start._2 until end._2).map { c =>
        ((r - start._1, c - start._1), (r, c))
      }
    }.flatten: _*), (0, 0))
  }
  // experimental
  override def mReplicate[T: ClassTag](x: T): LagMatrix[T] = {
    val size = (graphSize, graphSize)
    mFromMap[T](Map((0L until size._1).map { r =>
      (0L until size._2).map { c =>
        ((r, c), x)
      }
    }.flatten: _*), x)
  }

  // *****
  // vector function
  private[lagraph] override def vReduce[T1: ClassTag, T2: ClassTag](f: (T1, T2) => T2,
                                                                    c: (T2, T2) => T2,
                                                                    z: T2,
                                                                    u: LagVector[T1]): T2 =
    u match {
      case ua: LagSmpVector[_] =>
        GpiOps.gpi_reduce(f, c, z, ua.v)
    }
  private[lagraph] override def vMap[T1: ClassTag, T2: ClassTag](f: (T1) => T2,
                                                                 u: LagVector[T1]): LagVector[T2] =
    u match {
      case ua: LagSmpVector[_] =>
        LagSmpVector(this, GpiOps.gpi_map(f, ua.v))
    }
  private[lagraph] override def vZip[T1: ClassTag, T2: ClassTag, T3: ClassTag](
      f: (T1, T2) => T3,
      u: LagVector[T1],
      v: LagVector[T2]): LagVector[T3] = (u, v) match {
    case (ua: LagSmpVector[_], va: LagSmpVector[_]) =>
      LagSmpVector(this, GpiOps.gpi_zip(f, ua.v, va.v))
  }
  private[lagraph] override def vZipWithIndex[T1: ClassTag, T2: ClassTag](
      f: (T1, Long) => T2,
      u: LagVector[T1],
      sparseValue: Option[T2] = None): LagVector[T2] = u match {
    case ua: LagSmpVector[_] =>
      LagSmpVector(this, GpiOps.gpi_zip(f, ua.v, GpiOps.gpi_indices(u.size, 0)))
  }
  private[lagraph] override def vReduceWithIndex[T1: ClassTag, T2: ClassTag](
      f: ((T1, Long), T2) => T2,
      c: (T2, T2) => T2,
      z: T2,
      u: LagVector[T1]): T2 = u match {
    case ua: LagSmpVector[T1] => {
      def f2(a: T1, b: Long): (T1, Long) = { (a, b) }
      val uinds = GpiOps.gpi_zip(f2, ua.v, GpiOps.gpi_indices(u.size, 0))
      GpiOps.gpi_reduce(f, c, z, uinds)
    }
  }
  // *****
  private[lagraph] override def mMap[T1: ClassTag, T2: ClassTag](f: (T1) => T2,
                                                                 m: LagMatrix[T1]): LagMatrix[T2] =
    m match {
      case ma: LagSmpMatrix[_] => {
        val sparseValue = f(ma.vov(0).sparseValue)
        mFromMap(ma.rcvMap.map { case (k, v) => (k, f(v)) }.filter {
          case (k, v) => v != sparseValue
        }, sparseValue)
      }
    }
  private[lagraph] override def mZip[T1: ClassTag, T2: ClassTag, T3: ClassTag](
      f: (T1, T2) => T3,
      m: LagMatrix[T1],
      n: LagMatrix[T2]): LagMatrix[T3] = (m, n) match {
    case (ma: LagSmpMatrix[_], na: LagSmpMatrix[_]) => {
      def perrow(mv: GpiAdaptiveVector[T1], nv: GpiAdaptiveVector[T2]): GpiAdaptiveVector[T3] = {
        GpiOps.gpi_zip(f, mv, nv)
      }
      val vovZip = GpiOps.gpi_zip(perrow, ma.vov, na.vov)
      val mapZip = GpiSparseRowMatrix.toMap(vovZip).map {
        case (k, v) => ((k._1.toLong, k._2.toLong) -> v)
      }
      LagSmpMatrix(this, mapZip, vovZip)
    }
  }
  private[lagraph] override def mZipWithIndex[T1: ClassTag, T2: ClassTag](
      f: (T1, (Long, Long)) => T2,
      m: LagMatrix[T1]): LagMatrix[T2] = m match {
    case ma: LagSmpMatrix[T1] => {
      mFromMap(ma.rcvMap.map { case (k, v) => (k, f(v, k)) }, f(ma.vov(0).sparseValue, (0, 0)))
    }
  }
  private[lagraph] override def mSparseZipWithIndex[T1: ClassTag, T2: ClassTag](
      f: (T1, (Long, Long)) => T2,
      m: LagMatrix[T1],
      targetSparseValue: T2,
      visitDiagonalsOpt: Option[T1] = None): LagMatrix[T2] = m match {
    case ma: LagSmpMatrix[T1] => {
      val rcvMap = if (visitDiagonalsOpt.isDefined) {
        val diagSeq = (0L until ma.size._1).map { d =>
          ((d, d), visitDiagonalsOpt.get)
        }
        (ma.rcvMap.toSeq.flatMap {
          case (k, v) => if (k._1 != k._2) Some((k, v)) else None
        } ++ diagSeq).toMap
      } else ma.rcvMap
      mFromMap(rcvMap.map { case (k, v) => (k, f(v, k)) }, targetSparseValue)
    }
  }
  // *****
  // gpi semiring
  // mTv addition.multiplication
  // a matrix
  // u vector
  // returns vector
  private[lagraph] override def mTv[T: ClassTag](sr: LagSemiring[T],
                                                 m: LagMatrix[T],
                                                 u: LagVector[T]): LagVector[T] = (m, u) match {
    case (ma: LagSmpMatrix[_], ua: LagSmpVector[_]) => {
      LagSmpVector(
        this,
        GpiOps.gpi_m_times_v(sr.addition, sr.multiplication, sr.addition, sr.zero, ma.vov, ua.v))
    }
  }
  //  // mTv "addition.multiplication"
  //  override def mTm[T: ClassTag](
  //    sr: LagSemiring[T],
  //    m: LagMatrix[T],
  //    n: LagMatrix[T]): LagMatrix[T] = (m, n) match {
  //    case (ma: LagSmpMatrix[_], na: LagSmpMatrix[_]) => {
  //      LagSmpMatrix(
  //        this,
  //        GpiOps.gpi_m_times_m(
  //          sr.addition,
  //          sr.multiplication,
  //          sr.zero,
  //          ma.vov,
  //          na.vov))
  //    }
  //  }
  // mTv "addition.multiplication"
  // experimental
  private[lagraph] override def mTm[T: ClassTag](sr: LagSemiring[T],
                                                 m: LagMatrix[T],
                                                 n: LagMatrix[T]): LagMatrix[T] = (m, n) match {
    case (ma: LagSmpMatrix[T], na: LagSmpMatrix[T]) => {
      require(ma.vov(0).sparseValue == na.vov(0).sparseValue,
              "mTm does not currently support disparate sparsities")
      val msSparse = ma.vov(0).sparseValue
      val vovMtM = GpiOps.gpi_m_times_m(sr.addition,
                                        sr.multiplication,
                                        sr.addition,
                                        sr.zero,
                                        ma.vov,
                                        na.transpose.asInstanceOf[LagSmpMatrix[T]].vov,
                                        Option(msSparse),
                                        Option(msSparse))
      val mapMtM = GpiSparseRowMatrix.toMap(vovMtM).map {
        case (k, v) => ((k._1.toLong, k._2.toLong) -> v)
      }
      LagSmpMatrix(this, mapMtM, vovMtM).transpose
    }
  }
  // Hadagard ".multiplication"
  private[lagraph] override def mHm[T: ClassTag](
                                                 // // TODO maybe sr OR maybe f: (T1, T2) => T3?
                                                 // f: (T, T) => T,
                                                 sr: LagSemiring[T],
                                                 m: LagMatrix[T],
                                                 n: LagMatrix[T]): LagMatrix[T] = {
    mZip(sr.multiplication, m, n)
    //    mZip(f, m, n)
  }
  // OuterProduct
  private[lagraph] def mOp[T: ClassTag](f: LagSemiring[T],
                                        m: LagMatrix[T],
                                        mCol: Long,
                                        n: LagMatrix[T],
                                        nRow: Long): LagMatrix[T] =
    (m, n) match {
      case (ma: LagSmpMatrix[T], na: LagSmpMatrix[T]) => {
        assert(ma.vov(0).sparseValue == na.vov(0).sparseValue)
        assert(f.multiplication.annihilator.isDefined)
        assert(f.multiplication.annihilator.get == ma.vov(0).sparseValue)
        val sparseValue = ma.vov(0).sparseValue
        val mac = ma.rcvMap.toSeq.filter { case (k, v) => k._2 == mCol }.map {
          case (k, v) => Tuple2(k._1, v)
        }
        val nar = na.rcvMap.toSeq.filter { case (k, v) => k._1 == nRow }.map {
          case (k, v) => Tuple2(k._2, v)
        }
        val cp = for (r <- mac; c <- nar) yield (r, c)
        val rcvMap = cp.map {
          case (p1, p2) =>
            Tuple2(Tuple2(p1._1, p2._1), f.multiplication.op(p1._2, p2._2))
        }.toMap
        mFromMap(rcvMap, sparseValue)
      }
    }

  // *******
  // matrix mechanics
  //  override def mTranspose[T: ClassTag](
  //    m: LagMatrix[T]): LagMatrix[T] = m match {
  //    case ma: LagSmpMatrix[_] => {
  //      LagSmpMatrix(this, GpiSparseRowMatrix.transpose(ma.vov))
  //    }
  //  }
  private[lagraph] override def mTranspose[T: ClassTag](m: LagMatrix[T]): LagMatrix[T] = m match {
    case ma: LagSmpMatrix[_] => {
      val sparseValue = ma.vov(0).sparseValue
      mFromMap(ma.rcvMap.map { case (k, v) => ((k._2, k._1), v) }, sparseValue)
    }
  }
  private[lagraph] def vFromMrow[T: ClassTag](m: LagMatrix[T], mRow: Long): LagVector[T] =
    m match {
      case ma: LagSmpMatrix[_] =>
        LagSmpVector(this, ma.vov(mRow.toInt))
    }
  private[lagraph] def vFromMcol[T: ClassTag](m: LagMatrix[T], mCol: Long): LagVector[T] =
    m match {
      case ma: LagSmpMatrix[_] =>
        vFromMap(ma.rcvMap.filter { case (k, v) => k._2 == mCol }.map {
          case (k, v) => (k._1, v)
        }, ma.vov(0).sparseValue)
    }

  // *******
  // equivalence
  private[lagraph] override def vEquiv[T: ClassTag](u: LagVector[T], v: LagVector[T]): Boolean =
    (u, v) match {
      case (ua: LagSmpVector[T], va: LagSmpVector[T]) => {
        GpiOps.gpi_equiv(ua.v, va.v)
      }
    }
}
