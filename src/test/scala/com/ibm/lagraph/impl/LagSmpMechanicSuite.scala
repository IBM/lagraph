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

class LagSmpMechanicSuite extends FunSuite with Matchers {

  test("LagSmpContext.mTranspose") {
    val nr = 10
    val hc: LagContext = LagContext.getLagSmpContext(nr)
    var nc = 10
    val vMatrix = Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble)
    val sparseValue = 0.0
    val m =
      hc.mFromMap(LagContext.mapFromSeqOfSeq(vMatrix, sparseValue), sparseValue)
    val mT = m.transpose

    val (vvm, vvs) = hc.mToMap(mT)
    val mTres = LagContext.vectorOfVectorFromMap(vvm, vvs, (nr, nc))
    assert(mTres.size == nr)
    mTres.zipWithIndex.map {
      case (vr, r) => {
        assert(vr.size == nc)
        vr.zipWithIndex.map {
          case (vc, c) => {
            assert(vc == (c * nr + r).toDouble)
          }
        }
      }
    }
  }

  test("LagSmpContext.vFromMrow") {
    val nr = 10
    val hc: LagContext = LagContext.getLagSmpContext(nr)
    var nc = 10
    val vMatrix = Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble)
    val sparseValue = 0.0
    val m =
      hc.mFromMap(LagContext.mapFromSeqOfSeq(vMatrix, sparseValue), sparseValue)
    val testRow = 1
    val v = hc.vFromMrow(m, testRow)

    val vRes = hc.vToVector(v)
    assert(vRes.size == nr)
    vRes.zipWithIndex.map {
      case (v, r) => {
        assert(v == (testRow * nr + r).toDouble)
      }
    }
  }

  test("LagSmpContext.vFromMcol") {
    val nr = 10
    val hc: LagContext = LagContext.getLagSmpContext(nr)
    var nc = 10
    val vMatrix = Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble)
    val sparseValue = 0.0
    val m =
      hc.mFromMap(LagContext.mapFromSeqOfSeq(vMatrix, sparseValue), sparseValue)
    val testCol = 1
    val v = hc.vFromMcol(m, testCol)

    val vRes = hc.vToVector(v)
    assert(vRes.size == nr)
    vRes.zipWithIndex.map {
      case (v, c) => {
//        println(c, v)
        assert(v == (c * nr + testCol).toDouble)
      }
    }
  }

  //  test("LagSmpContext.mTranspose w/ slice") {
  //    val nr = 10
  //    val hc: LagContext = LagContext.getLagSmpContext(nr)
  //    var nc = 10
  //    val vMatrix = Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble)
  //    val sparseValue = 0.0
  //    val m = hc.mFromSeqOfSeq(vMatrix, sparseValue)
  //    nc = 1
  //    val ms = hc.mSlice(m, rowRangeOpt=Option(Tuple2(0,nc)))
  //    val mT = hc.mTranspose(ms)
  //    val mTres = hc.mToVectorOfVector(mT)
  //    assert (mTres.size==nr)
  //    mTres.zipWithIndex.map {
  //      case (vr, r) => {
  //        assert(vr.size == nc)
  //        vr.zipWithIndex.map {
  //          case (vc, c) => {
  //            assert(vc == (r).toDouble)
  //          }
  //        }
  //      }
  //    }
  //  }

  //  test("LagSmpContext.mSlice") {
  //    val nv = 10
  //    val hc: LagContext = LagContext.getLagSmpContext(nv)
  //    val nr = nv
  //    val nc = nv
  //    val sparseValue = 0.0
  //    val rRange = (2L, 5L)
  //    val cRange = (0, nc)
  //    val cSparseIndex = 0
  //    val vMatrix = Vector.tabulate(nr, nc)((r, c) => (r * nc + c).toDouble)
  //    val m = hc.mFromSeqOfSeq(vMatrix, sparseValue)
  //    val mS = hc.mSlice(m, rowRangeOpt=Option(rRange))
  //    val mSres = hc.mToVectorOfVector(mS)
  //    assert (mSres.size == (rRange._2 - rRange._1))
  //    mSres.zipWithIndex.map {
  //      case (vr, r) => {
  //        assert(vr.size == (cRange._2 - cRange._1))
  //        vr.zipWithIndex.map {
  //          case (vc, c) => assert(vc == ((r + rRange._1) * nc + c + cRange._1).toDouble)
  //        }
  //      }
  //    }
  //  }
}
