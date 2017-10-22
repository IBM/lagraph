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
import com.ibm.lagraph._

class GpiBufferSuite extends FunSuite {
  val DEBUG = false

  def f(x: Int, y: Int): Int = { if (x == 0 || y == 0) 0 else x + y }
  val dataDense = (1 to 100).toArray
  val len = dataDense.length
  val vA = GpiBuffer(dataDense)
  val sparseValueA = 0
  val sparseValueB = 0
  val sparseValueC = 0

  test("zipDenseDense00") {
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipDenseDenseToDense(vA, vA, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 100)
    assert(vR.length == denseCountResult)
    BufferSuite.checkDense(vR, (0 to 99 toList) zip (2 to 102 by 2 toList))
  }
  test("zipDenseDense01") {
    val dataDenseB = (-1 to -100 by -1).toArray
    val vB = GpiBuffer(dataDenseB)
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipDenseDenseToDense(vA, vB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 0)
    BufferSuite.checkDense(vR, (0 to 99 toList) zip List.fill(100)(0))
  }

  test("zipDenseSparse00") {
    val rvSr = Array(0)
    val rvSv = Array(-1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToSparse(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 0)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List())
  }

  test("zipDenseSparse01") {
    val rvSr = Array(0)
    val rvSv = Array(1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToSparse(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 1)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(0,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((0, 2)))
  }

  test("zipDenseSparse02") {
    val rvSr = Array(1)
    val rvSv = Array(1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToSparse(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 1)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,3)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 3)))
  }

  test("zipDenseSparse03") {
    val rvSr = Array(1, 2)
    val rvSv = Array(1, 1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToSparse(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 2)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,3),(2,4)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 3), (2, 4)))
  }

  test("zipDenseSparse04") {
    val rvSr = Array(1, 2, 4)
    val rvSv = Array(1, 1, 1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToSparse(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 3)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,3),(2,4),(4,6)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 3), (2, 4), (4, 6)))
  }

  test("zipDenseSparse05") {
    val rvSr = Array(1, 2, 4, 98)
    val rvSv = Array(1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToSparse(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 4)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,3),(2,4),(4,6),(98,100)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 3), (2, 4), (4, 6), (98, 100)))
  }

  test("zipDenseSparse06") {
    val rvSr = Array(1, 2, 4, 97, 99)
    val rvSv = Array(1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToSparse(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 5)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,3),(2,4),(4,6),(97,99),(99,101)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 3), (2, 4), (4, 6), (97, 99), (99, 101)))
  }

  test("zipDenseSparse down") {
    val rvSr = Array(1, 2, 4, 97, 99)
    val rvSv = Array(1, 1, 1, -98, 1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToSparse(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 4)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,3),(2,4),(4,6),(99,101)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 3), (2, 4), (4, 6), (99, 101)))
  }

  // zipSparseSparse

  test("zipSparseSparse00: lenA < lenB") {
    val rvSAr = Array(1)
    val rvSAv = Array(1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(2, 99)
    val rvSBv = Array(1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 0)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  []")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List())
  }

  test("zipSparseSparse01: lenA < lenB") {
    val rvSAr = Array(1)
    val rvSAv = Array(1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 99)
    val rvSBv = Array(1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 1)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 2)))
  }

  test("zipSparseSparse02: lenA < lenB") {
    val rvSAr = Array(1, 5)
    val rvSAv = Array(1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 3, 5, 99)
    val rvSBv = Array(1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 2)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
  }

  test("zipSparseSparse03: lenA < lenB") {
    val rvSAr = Array(1, 5, 7)
    val rvSAv = Array(1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 3, 5, 6, 99)
    val rvSBv = Array(1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 2)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 2), (5, 2)))
  }

  test("zipSparseSparse04: lenA < lenB") {
    val rvSAr = Array(1, 5, 7, 50)
    val rvSAv = Array(1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 3, 5, 6, 50, 99)
    val rvSBv = Array(1, 1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 3)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 2), (5, 2), (50, 2)))
  }

  test("zipSparseSparse05: lenA < lenB") {
    val rvSAr = Array(1, 5, 7, 50, 99)
    val rvSAv = Array(1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 3, 5, 6, 50, 77, 99)
    val rvSBv = Array(1, 1, 1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 4)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2),(99,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 2), (5, 2), (50, 2), (99, 2)))
  }

  test("zipSparseSparse06: lenA < lenB") {
    val rvSAr = Array(1, 5, 7, 50, 99)
    val rvSAv = Array(1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 3, 5, 6, 50, 77, 98)
    val rvSBv = Array(1, 1, 1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 3)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 2), (5, 2), (50, 2)))
  }

  // zipSparseSparse:flip

  test("zipSparseSparse00: lenB < lenA") {
    val rvSBr = Array(1)
    val rvSBv = Array(1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(2, 99)
    val rvSAv = Array(1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 0)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  []")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List())
  }

  test("zipSparseSparse01: lenB < lenA") {
    val rvSBr = Array(1)
    val rvSBv = Array(1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 99)
    val rvSAv = Array(1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 1)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 2)))
  }

  test("zipSparseSparse02: lenB < lenA") {
    val rvSBr = Array(1, 5)
    val rvSBv = Array(1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 3, 5, 99)
    val rvSAv = Array(1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 2)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List())
  }

  test("zipSparseSparse03: lenB < lenA") {
    val rvSBr = Array(1, 5, 7)
    val rvSBv = Array(1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 3, 5, 6, 99)
    val rvSAv = Array(1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 2)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 2), (5, 2)))
  }

  test("zipSparseSparse04: lenB < lenA") {
    val rvSBr = Array(1, 5, 7, 50)
    val rvSBv = Array(1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 3, 5, 6, 50, 99)
    val rvSAv = Array(1, 1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 3)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 2), (5, 2), (50, 2)))
  }

  test("zipSparseSparse05: lenB < lenA") {
    val rvSBr = Array(1, 5, 7, 50, 99)
    val rvSBv = Array(1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 3, 5, 6, 50, 77, 99)
    val rvSAv = Array(1, 1, 1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 4)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2),(99,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 2), (5, 2), (50, 2), (99, 2)))
  }

  test("zipSparseSparse06: lenB < lenA") {
    val rvSBr = Array(1, 5, 7, 50, 99)
    val rvSBv = Array(1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 3, 5, 6, 50, 77, 98)
    val rvSAv = Array(1, 1, 1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToSparse(rvA,
                                           rvB,
                                           len,
                                           sparseValueA,
                                           sparseValueB,
                                           sparseValueC,
                                           f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 3)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse((rvRr, rvRv), List((1, 2), (5, 2), (50, 2)))
  }

  test("2ndzipDenseSparse00") {
    val rvSr = Array(0)
    val rvSv = Array(-1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToDense(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 0)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  []")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List())
  }

  test("2ndzipDenseSparse01") {
    val rvSr = Array(0)
    val rvSv = Array(1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToDense(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 1)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(0,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((0, 2)))
  }

  test("2ndzipDenseSparse02") {
    val rvSr = Array(1)
    val rvSv = Array(1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToDense(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 1)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,3)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 3)))
  }

  test("2ndzipDenseSparse03") {
    val rvSr = Array(1, 2)
    val rvSv = Array(1, 1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToDense(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 2)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,3),(2,4)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 3), (2, 4)))
  }

  test("2ndzipDenseSparse04") {
    val rvSr = Array(1, 2, 4)
    val rvSv = Array(1, 1, 1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToDense(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 3)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,3),(2,4),(4,6)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 3), (2, 4), (4, 6)))
  }

  test("2ndzipDenseSparse05") {
    val rvSr = Array(1, 2, 4, 98)
    val rvSv = Array(1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToDense(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 4)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,3),(2,4),(4,6),(98,100)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 3), (2, 4), (4, 6), (98, 100)))
  }

  test("2ndzipDenseSparse06") {
    val rvSr = Array(1, 2, 4, 97, 99)
    val rvSv = Array(1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToDense(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 5)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,3),(2,4),(4,6),(97,99),(99,101)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 3), (2, 4), (4, 6), (97, 99), (99, 101)))
  }

  test("2ndzipDenseSparse down") {
    val rvSr = Array(1, 2, 4, 97, 99)
    val rvSv = Array(1, 1, 1, -98, 1)
    val rvB = (GpiBuffer(rvSr), GpiBuffer(rvSv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipDenseSparseToDense(vA, rvB, len, sparseValueA, sparseValueB, sparseValueC, f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 4)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,3),(2,4),(4,6),(99,101)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 3), (2, 4), (4, 6), (99, 101)))
  }

  // zipSparseSparse

  test("2ndzipSparseSparse00: lenA < lenB") {
    val rvSAr = Array(1)
    val rvSAv = Array(1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(2, 99)
    val rvSBv = Array(1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 0)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  []")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List())
  }

  test("2ndzipSparseSparse01: lenA < lenB") {
    val rvSAr = Array(1)
    val rvSAv = Array(1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 99)
    val rvSBv = Array(1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 1)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2)))
  }

  test("2ndzipSparseSparse02: lenA < lenB") {
    val rvSAr = Array(1, 5)
    val rvSAv = Array(1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 3, 5, 99)
    val rvSBv = Array(1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 2)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2), (5, 2)))
  }

  test("2ndzipSparseSparse03: lenA < lenB") {
    val rvSAr = Array(1, 5, 7)
    val rvSAv = Array(1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 3, 5, 6, 99)
    val rvSBv = Array(1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 2)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2), (5, 2)))
  }

  test("2ndzipSparseSparse04: lenA < lenB") {
    val rvSAr = Array(1, 5, 7, 50)
    val rvSAv = Array(1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 3, 5, 6, 50, 99)
    val rvSBv = Array(1, 1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 3)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2), (5, 2), (50, 2)))
  }

  test("2ndzipSparseSparse05: lenA < lenB") {
    val rvSAr = Array(1, 5, 7, 50, 99)
    val rvSAv = Array(1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 3, 5, 6, 50, 77, 99)
    val rvSBv = Array(1, 1, 1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 4)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2),(99,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2), (5, 2), (50, 2), (99, 2)))
  }

  test("2ndzipSparseSparse06: lenA < lenB") {
    val rvSAr = Array(1, 5, 7, 50, 99)
    val rvSAv = Array(1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val rvSBr = Array(1, 3, 5, 6, 50, 77, 98)
    val rvSBv = Array(1, 1, 1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 3)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2), (5, 2), (50, 2)))
  }

  // flip

  test("2ndzipSparseSparse00: lenB < lenA") {
    val rvSBr = Array(1)
    val rvSBv = Array(1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(2, 99)
    val rvSAv = Array(1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 0)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  []")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List())
  }

  test("2ndzipSparseSparse01: lenB < lenA") {
    val rvSBr = Array(1)
    val rvSBv = Array(1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 99)
    val rvSAv = Array(1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 1)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2)))
  }

  test("2ndzipSparseSparse02: lenB < lenA") {
    val rvSBr = Array(1, 5)
    val rvSBv = Array(1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 3, 5, 99)
    val rvSAv = Array(1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 2)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List())
  }

  test("2ndzipSparseSparse03: lenB < lenA") {
    val rvSBr = Array(1, 5, 7)
    val rvSBv = Array(1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 3, 5, 6, 99)
    val rvSAv = Array(1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 2)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2), (5, 2)))
  }

  test("2ndzipSparseSparse04: lenB < lenA") {
    val rvSBr = Array(1, 5, 7, 50)
    val rvSBv = Array(1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 3, 5, 6, 50, 99)
    val rvSAv = Array(1, 1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 3)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2), (5, 2), (50, 2)))
  }

  test("2ndzipSparseSparse05: lenB < lenA") {
    val rvSBr = Array(1, 5, 7, 50, 99)
    val rvSBv = Array(1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 3, 5, 6, 50, 77, 99)
    val rvSAv = Array(1, 1, 1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 4)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2),(99,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2), (5, 2), (50, 2), (99, 2)))
  }

  test("2ndzipSparseSparse06: lenB < lenA") {
    val rvSBr = Array(1, 5, 7, 50, 99)
    val rvSBv = Array(1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    val rvSAr = Array(1, 3, 5, 6, 50, 77, 98)
    val rvSAv = Array(1, 1, 1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val (vR, denseCountResult, ops) =
      GpiBuffer.gpiZipSparseSparseToDense(rvA,
                                          rvB,
                                          len,
                                          sparseValueA,
                                          sparseValueB,
                                          sparseValueC,
                                          f)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 3)
    assert(vR.count(_ != sparseValueC) == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(5,2),(50,2)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format(vR.toArray.zipWithIndex
          .filter { case (v, i) => if (v != sparseValueC) true else false }
          .map { case (v, i) => (i, v) }
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkDense(vR, List((1, 2), (5, 2), (50, 2)))
  }

  test("compareSparseSparse01 - simple eq") {
    val len = 100
    val sparseValueA = 0
    val rvSAr = Array(1, 5, 7, 50, 99)
    val rvSAv = Array(1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val rvSBr = Array(1, 5, 7, 50, 99)
    val rvSBv = Array(1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    assert(
      GpiBuffer
        .gpiCompareSparseSparse(rvA, rvB, len, sparseValueA, sparseValueB)
        ._1)
  }
  test("compareSparseSparse01 - simple ne") {
    val len = 100
    val sparseValueA = 0
    val rvSAr = Array(1, 5, 7, 50, 99)
    val rvSAv = Array(1, 1, 1, 1, 2)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val rvSBr = Array(1, 5, 7, 50, 99)
    val rvSBv = Array(1, 1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    assert(
      !GpiBuffer
        .gpiCompareSparseSparse(rvA, rvB, len, sparseValueA, sparseValueB)
        ._1)
  }
  test("compareSparseSparse01 - sparseValue eq") {
    val len = 8
    val sparseValueA = 1
    val rvSAr = Array(4, 5, 6, 7)
    val rvSAv = Array(0, 0, 0, 0)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val rvSBr = Array(0, 1, 2, 3)
    val rvSBv = Array(1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    assert(
      GpiBuffer
        .gpiCompareSparseSparse(rvA, rvB, len, sparseValueA, sparseValueB)
        ._1)
  }
  test("compareSparseSparse01 - sparseValue eq empty") {
    val len = 8
    val sparseValueA = 1
    val rvSAr = Array[Int]()
    val rvSAv = Array[Int]()
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 1
    val rvSBr = Array[Int]()
    val rvSBv = Array[Int]()
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    assert(
      GpiBuffer
        .gpiCompareSparseSparse(rvA, rvB, len, sparseValueA, sparseValueB)
        ._1)
  }
  test("compareSparseSparse01 - sparseValue ne empty") {
    val len = 8
    val sparseValueA = 1
    val rvSAr = Array[Int]()
    val rvSAv = Array[Int]()
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 2
    val rvSBr = Array[Int]()
    val rvSBv = Array[Int]()
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    assert(
      !GpiBuffer
        .gpiCompareSparseSparse(rvA, rvB, len, sparseValueA, sparseValueB)
        ._1)
  }
  test("compareSparseSparse01 - sparseValue eq sparse in list") {
    val len = 8
    val sparseValueA = 0
    val rvSAr = Array(0, 1, 2, 3, 4)
    val rvSAv = Array(1, 1, 1, 1, 0)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val rvSBr = Array(0, 1, 2, 3)
    val rvSBv = Array(1, 1, 1, 1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    assert(
      GpiBuffer
        .gpiCompareSparseSparse(rvA, rvB, len, sparseValueA, sparseValueB)
        ._1)
  }

  test("compareSparseSparse01 - final value ne") {
    val len = 8
    val sparseValueA = 0
    val rvSAr = Array[Int]()
    val rvSAv = Array[Int]()
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val rvSBr = Array(7)
    val rvSBv = Array(1)
    val rvB = (GpiBuffer(rvSBr), GpiBuffer(rvSBv))
    assert(
      !GpiBuffer
        .gpiCompareSparseSparse(rvA, rvB, len, sparseValueA, sparseValueB)
        ._1)
  }

  test("compareSparseDense01 - simple eq") {
    val len = 8
    val sparseValueA = 0
    val rvSAr = Array(0, 1, 2, 3, 4, 5, 6, 7)
    val rvSAv = Array(1, 1, 1, 1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val vDB = GpiBuffer(Array(1, 1, 1, 1, 1, 1, 1, 1))
    assert(
      GpiBuffer
        .gpiCompareSparseDense(rvA, vDB, len, sparseValueA, sparseValueB)
        ._1)
  }

  test("compareSparseDense01 - simple ne") {
    val len = 8
    val sparseValueA = 0
    val rvSAr = Array(0, 1, 2, 3, 4, 5, 6, 7)
    val rvSAv = Array(1, 1, 1, 1, 1, 1, 1, 99)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val vDB = GpiBuffer(Array(1, 1, 1, 1, 1, 1, 1, 1))
    assert(
      !GpiBuffer
        .gpiCompareSparseDense(rvA, vDB, len, sparseValueA, sparseValueB)
        ._1)
  }

  test("compareSparseDense01 - simple empty eq") {
    val len = 8
    val sparseValueA = 1
    val rvSAr = Array[Int]()
    val rvSAv = Array[Int]()
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val vDB = GpiBuffer(Array(1, 1, 1, 1, 1, 1, 1, 1))
    assert(
      GpiBuffer
        .gpiCompareSparseDense(rvA, vDB, len, sparseValueA, sparseValueB)
        ._1)
  }

  test("compareSparseDense01 - simple empty ne") {
    val len = 8
    val sparseValueA = 99
    val rvSAr = Array[Int]()
    val rvSAv = Array[Int]()
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val vDB = GpiBuffer(Array(1, 1, 1, 1, 1, 1, 1, 1))
    assert(
      !GpiBuffer
        .gpiCompareSparseDense(rvA, vDB, len, sparseValueA, sparseValueB)
        ._1)
  }

  test("compareSparseDense01 - sparse in dense") {
    val len = 8
    val sparseValueA = 0
    val rvSAr = Array[Int](1, 3, 4, 5, 6)
    val rvSAv = Array[Int](1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val vDB = GpiBuffer(Array(0, 1, 0, 1, 1, 1, 1, 0))
    assert(
      GpiBuffer
        .gpiCompareSparseDense(rvA, vDB, len, sparseValueA, sparseValueB)
        ._1)
  }

  test("compareDenseSparse01 - simple eq") {
    val len = 8
    val sparseValueA = 0
    val rvSAr = Array(0, 1, 2, 3, 4, 5, 6, 7)
    val rvSAv = Array(1, 1, 1, 1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val vDB = GpiBuffer(Array(1, 1, 1, 1, 1, 1, 1, 1))
    assert(
      GpiBuffer
        .gpiCompareDenseSparse(vDB, rvA, len, sparseValueB, sparseValueA)
        ._1)
  }

  test("compareDenseSparse01 - simple ne") {
    val len = 8
    val sparseValueA = 0
    val rvSAr = Array(0, 1, 2, 3, 4, 5, 6, 7)
    val rvSAv = Array(1, 1, 1, 1, 1, 1, 1, 99)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val vDB = GpiBuffer(Array(1, 1, 1, 1, 1, 1, 1, 1))
    assert(
      !GpiBuffer
        .gpiCompareDenseSparse(vDB, rvA, len, sparseValueB, sparseValueA)
        ._1)
  }

  test("compareDenseSparse01 - simple empty eq") {
    val len = 8
    val sparseValueA = 1
    val rvSAr = Array[Int]()
    val rvSAv = Array[Int]()
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val vDB = GpiBuffer(Array(1, 1, 1, 1, 1, 1, 1, 1))
    assert(
      GpiBuffer
        .gpiCompareDenseSparse(vDB, rvA, len, sparseValueB, sparseValueA)
        ._1)
  }

  test("compareDenseSparse01 - simple empty ne") {
    val len = 8
    val sparseValueA = 99
    val rvSAr = Array[Int]()
    val rvSAv = Array[Int]()
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val vDB = GpiBuffer(Array(1, 1, 1, 1, 1, 1, 1, 1))
    assert(
      !GpiBuffer
        .gpiCompareDenseSparse(vDB, rvA, len, sparseValueB, sparseValueA)
        ._1)
  }

  test("compareDenseSparse01 - sparse in dense") {
    val len = 8
    val sparseValueA = 0
    val rvSAr = Array[Int](1, 3, 4, 5, 6)
    val rvSAv = Array[Int](1, 1, 0, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val sparseValueB = 0
    val vDB = GpiBuffer(Array(0, 1, 0, 1, 0, 1, 1, 0))
    assert(
      GpiBuffer
        .gpiCompareDenseSparse(vDB, rvA, len, sparseValueB, sparseValueA)
        ._1)
  }

  test("compareDenseDense01 - simple eq") {
    val len = 100
    val vD0 = GpiBuffer((1 to len).toArray)
    val vD1 = GpiBuffer((1 to len).toArray)
    val sparseValueVd0 = 0
    val sparseValueVd1 = 0
    assert(
      GpiBuffer
        .gpiCompareDenseDense(vD0, vD1, len, sparseValueVd0, sparseValueVd1)
        ._1)
  }
  test("compareDenseDense01 - simple neq") {
    val len = 100
    val vD0 = GpiBuffer((1 to len).toArray)
    val vD1 = GpiBuffer((2 to len + 1).toArray)
    val sparseValueVd0 = 0
    val sparseValueVd1 = 0
    assert(
      !GpiBuffer
        .gpiCompareDenseDense(vD0, vD1, len, sparseValueVd0, sparseValueVd1)
        ._1)
  }
  test("compareDenseDense01 - neq") {
    val len = 100
    val vD0 = GpiBuffer(List(1, 2, 3, 4).toArray)
    val vD1 = GpiBuffer(List(1, 2, 3, 5).toArray)
    val sparseValueVd0 = 0
    val sparseValueVd1 = 0
    assert(
      !GpiBuffer
        .gpiCompareDenseDense(vD0, vD1, len, sparseValueVd0, sparseValueVd1)
        ._1)
  }
  test("compareDenseDense01 - 1st value ne") {
    val len = 4
    val vD0 = GpiBuffer(List(1, 2, 3, 4).toArray)
    val vD1 = GpiBuffer(List(99, 2, 3, 4).toArray)
    val sparseValueVd0 = 0
    val sparseValueVd1 = 0
    assert(
      !GpiBuffer
        .gpiCompareDenseDense(vD0, vD1, len, sparseValueVd0, sparseValueVd1)
        ._1)
  }
  test("compareDenseDense01 - final value ne") {
    val len = 4
    val vD0 = GpiBuffer(List(1, 2, 3, 4).toArray)
    val vD1 = GpiBuffer(List(1, 2, 3, 5).toArray)
    val sparseValueVd0 = 0
    val sparseValueVd1 = 0
    assert(
      !GpiBuffer
        .gpiCompareDenseDense(vD0, vD1, len, sparseValueVd0, sparseValueVd1)
        ._1)
  }

  // zip*WIthIndex
  def s(x: Int, y: Long): Long = { x.toLong + y }
  test("gpiZipSparseWithIndexToSparse") {
    val rvSAr = Array(1, 3, 5, 6, 50, 77, 99)
    val rvSAv = Array(1, 1, 1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val base = 0L
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseWithIndexToSparse(rvA, len, base, sparseValueA, sparseValueC, s)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 7)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(3,4),(5,6),(6,7),(50,51),(77,78), (99,100)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse(
      (rvRr, rvRv),
      List((1, 2L), (3, 4L), (5, 6L), (6, 7L), (50, 51L), (77, 78L), (99, 100L)))
  }
  test("gpiZipSparseWithIndexToSparse 02") {
    val rvSAr = Array(1, 2, 3, 5, 6, 50, 77, 99)
    val rvSAv = Array(1, -2, 1, 1, 1, 1, 1, 1)
    val rvA = (GpiBuffer(rvSAr), GpiBuffer(rvSAv))
    val base = 0L
    val ((rvRr, rvRv), denseCountResult, ops) =
      GpiBuffer.gpiZipSparseWithIndexToSparse(rvA, len, base, sparseValueA, sparseValueC, s)
    if (DEBUG) println("// ****")
    assert(denseCountResult == 7)
    assert(rvRr.length == denseCountResult)
    if (DEBUG) println("expect  [(1,2),(3,4),(5,6),(6,7),(50,51),(77,78), (99,100)]")
    if (DEBUG) {
      println(
        "rv._1: >%s<".format((rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
          .mkString("[", ",", "]")))
    }
    BufferSuite.checkSparse(
      (rvRr, rvRv),
      List((1, 2L), (3, 4L), (5, 6L), (6, 7L), (50, 51L), (77, 78L), (99, 100L)))
  }

  // gpiZipSparseWithIndexToDense TODO
  // gpiZipDenseWithIndexToDense TODO

  object BufferSuite {
    def checkDense[VS](v: GpiBuffer[VS], ve: Seq[(VS, VS)]): Unit = {
      val x = v.toArray.zipWithIndex
        .filter { case (v, i) => if (v != sparseValueC) true else false }
        .map { case (v, i) => (i, v) }
      val matched = (x zip ve).forall {
        case (x, y) => (x._1 == y._1) && (x._2 == y._2)
      }
      assert(matched)
    }
    def checkSparse[VS](rv: (GpiBuffer[Int], GpiBuffer[VS]), ve: Seq[(VS, VS)]): Unit = {
      val (rvRr, rvRv) = rv
      val x =
        (rvRr.elems.take(rvRr.length) zip rvRv.elems.take(rvRr.length)).toArray
      val matched = (x zip ve).forall {
        case (x, y) => (x._1 == y._1) && (x._2 == y._2)
      }
      assert(matched)
    }
  }
}

// scalastyle:on println
