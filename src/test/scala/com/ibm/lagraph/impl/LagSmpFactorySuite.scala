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

class LagSmpFactorySuite extends FunSuite with Matchers {

  test("LagSmpContext.vIndices") {
    val nv = 8
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val start = 2
    val end = start + nv
    val v = hc.vIndices(start)
    val vRes = hc.vToVector(v)
//    assert(vRes.isInstanceOf[Vector[Int]])
    assert(vRes.size == (end - start))
    (start until end).map { r =>
      assert(vRes(r - start) == r)
    }
  }
  test("LagSmpContext.vReplicate") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val singleValue: Double = 99.0
    val v = hc.vReplicate(singleValue)
    val vRes = hc.vToVector(v)
    assert(vRes.isInstanceOf[Vector[Double]])
    assert(vRes.size == nv)
    (0 until nv).map { r =>
      assert(vRes(r) == singleValue)
    }
  }
  test("LagSmpContext.mIndices") {
    val nv = 8
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val start = (2L, 2L)
    val end = (start._1 + nv, start._2 + nv)
    val m = hc.mIndices(start)
    val (vvm, vvs) = hc.mToMap(m)
    val mRes = LagContext.vectorOfVectorFromMap(vvm, vvs, (nv, nv))
    assert(mRes.size == (end._1 - start._1))
    mRes.zipWithIndex.map {
      case (vr, r) => {
        assert(vr.size == (end._2 - start._2))
        vr.zipWithIndex.map {
          case (vc, c) => assert(vc == (start._1 + r, start._2 + c))
        }
      }
    }
  }
  test("LagSmpContext.mReplicate") {
    val nv = 10
    val hc: LagContext = LagContext.getLagSmpContext(nv)
    val size = (nv, nv)
    val singleValue: Double = 99.0
    val m = hc.mReplicate(singleValue)
    val (vvm, vvs) = hc.mToMap(m)
    val mRes = LagContext.vectorOfVectorFromMap(vvm, vvs, (nv, nv))
    assert(mRes.size == size._1)
    mRes.zipWithIndex.map {
      case (vr, r) => {
        assert(vr.size == size._2)
        vr.zipWithIndex.map {
          case (vc, c) => assert(vc == singleValue)
        }
      }
    }
  }
}
