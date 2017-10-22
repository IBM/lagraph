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

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import org.scalatest.Matchers
import scala.reflect.ClassTag
import scala.collection.mutable.{Map => MMap}
import com.ibm.lagraph._

class LagDstrFactorySuite extends FunSuite with Matchers with SharedSparkContext {

  val denseGraphSizes = List(1 << 4, 1 << 5)
  //  val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 29, 1 << 30)
  val sparseGraphSizes = List(1 << 16, 1 << 17, 1 << 26, 1 << 27)
  val nblocks = List(1 << 0, 1 << 1, 1 << 2, 1 << 3)

  test("test initializing spark context") {
    val hc: LagContext = LagContext.getLagDstrContext(sc, 1 << 3, 1)
    val list = nblocks
    val rdd = sc.parallelize(list)
    assert(rdd.count === list.length)
  }

  test("LagDstrContext.vIndices") {
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.vIndices", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val start = 2
        val end = start + hc.graphSize
        val v = hc.vIndices(start)
        val vRes = hc.vToVector(v)
        assert(v.size == hc.graphSize)
        assert(vRes.size == (end - start))
        (start until end.toInt).map { r =>
          assert(vRes(r - start) == r)
        }
      }
    }
  }

  test("LagDstrContext.mIndices") {
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.mIndices", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val start = (2L, 2L)
        val m = hc.mIndices(start)
        val (mResMap, sparseValue) = hc.mToMap(m)
        val mRes =
          LagContext.vectorOfVectorFromMap(mResMap, sparseValue, m.size)
        val end = (start._1 + graphSize, start._2 + graphSize)
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
    }
  }
  test("LagDstrContext.mReplicate") {
    for (graphSize <- denseGraphSizes) {
      for (nblock <- nblocks) {
        println("LagDstrContext.mReplicate", graphSize, nblock)
        val hc: LagContext = LagContext.getLagDstrContext(sc, graphSize, nblock)
        val singleValue: Double = 99.0
        val m = hc.mReplicate(singleValue)
        val (mResMap, sparseValue) = hc.mToMap(m)
        val mRes =
          LagContext.vectorOfVectorFromMap(mResMap, sparseValue, m.size)
        mRes.zipWithIndex.map {
          case (vr, r) => {
            assert(vr.size == graphSize)
            vr.zipWithIndex.map {
              case (vc, c) => assert(vc == singleValue)
            }
          }
        }
      }
    }
  }
}
// scalastyle:on println
