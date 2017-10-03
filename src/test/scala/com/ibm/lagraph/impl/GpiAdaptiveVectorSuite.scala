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
import com.ibm.lagraph._

class GpiAdaptiveVectorSuite extends FunSuite with Matchers {

  val DEBUG = false
  val sparseValue = 0

  test("prereq DefaultThreshold") {
    assert(GpiAdaptiveVector.DefaultThreshold <= 0.25)
  }

  // operations for traditional semiring w/ multiplication and addition
  object mul extends Function2[Int, Int, Int] {
    override def apply(x: Int, y: Int): Int = {
      x * y
    }
  }
  object add extends Function2[Int, Int, Int] {
    override def apply(x: Int, y: Int): Int = {
      x + y
    }
  }

  test("gpizip: add: Dense-Dense") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Dense = GpiAdaptiveVector.fromSeq(Vector(10, 11, 12, 13, 14, 15, 16, 17, 18, 19), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(add, v1Dense, v2Dense, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = List(10, 12, 14, 16, 18, 20, 22, 24, 26, 28)
    GpiAdaptiveVectorSuite.checkDenseVector(r, v1Dense.length, sparseValue, 10, re)
  }

  test("gpizip: add: Dense-Sparse") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(add, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = List(0, 1, 2, 3, 4, 5, 6, 7, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(r, v1Dense.length, sparseValue, 9, re)
  }
  test("gpizip: add: Dense-Sparse 2") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(add, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = List(0, 1, 3, 3, 4, 5, 6, 7, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(r, v1Dense.length, sparseValue, 9, re)
  }
  test("gpizip: add: Sparse-Dense") {
    val v2Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(1, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(add, v1Sparse, v2Dense, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = List(1, 1, 2, 3, 4, 5, 6, 7, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(r, v2Dense.length, sparseValue, 10, re)
  }
  test("gpizip: add: Sparse-Sparse") {
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(add, v1Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(2), List(1))
    GpiAdaptiveVectorSuite.checkSparseVector(r, v2Sparse.length, sparseValue, 1, re)
  }
  test("gpizip: add: Sparse-Sparse all zeroes") {
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(add, v1Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(r, v2Sparse.length, sparseValue, 0, re)
  }
  test("gpizip: mul: Dense-Dense") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Dense = GpiAdaptiveVector.fromSeq(Vector(10, 11, 12, 13, 14, 15, 16, 17, 18, 19), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(mul, v1Dense, v2Dense, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = List(0, 11, 24, 39, 56, 75, 96, 119, 144, 171)
    GpiAdaptiveVectorSuite.checkDenseVector(r, v1Dense.length, sparseValue, 9, re)
  }

  test("gpizip: mul: Dense-Sparse sparse all zeroes") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(mul, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(r, v1Dense.length, sparseValue, 0, re)
  }
  test("gpizip: mul: Dense-Sparse") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(mul, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(2), List(2))
    GpiAdaptiveVectorSuite.checkSparseVector(r, v1Dense.length, sparseValue, 1, re)
  }
  test("gpizip: mul: Sparse-Dense") {
    val v2Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(1, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(mul, v1Sparse, v2Dense, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(r, v2Dense.length, sparseValue, 0, re)
  }
  test("gpizip: mul: Sparse-Sparse all zeroes") {
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(mul, v1Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(r, v2Sparse.length, sparseValue, 0, re)
  }
  test("gpizip: mul: Sparse-Sparse") {
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(mul, v1Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(r, v2Sparse.length, sparseValue, 0, re)
  }
  // ****

  val dim = 10
  val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
  val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)

  test("fromSeq") {
    if (DEBUG) println("%s: apply(%d): >%d< (%d)".format("v1Dense", 5, v1Dense(5), 5))
    if (DEBUG) println("%s: apply(%d): >%d< (%d)".format("v2Sparse", 2, v2Sparse(2), 1))
    assert(v2Sparse(2) == 1)
    val res = (List(2), List(1))
    GpiAdaptiveVectorSuite.checkSparseVector(v2Sparse, v2Sparse.length, sparseValue, 1, res)
    assert(v1Dense(5) == 5)
    val red = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(v1Dense, v1Dense.length, sparseValue, 9, red)
  }

  test("updated") {
    // dense
    val uv1 = v1Dense.updated(5, 99)
    if (DEBUG) println("%s: apply(%d): >%d< (%d)".format("uv1", 5, v1Dense(5), 99))
    assert(uv1(5) == 99)
    val uv1e = List(0, 1, 2, 3, 4, 99, 6, 7, 8, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(uv1, v1Dense.length, sparseValue, 9, uv1e)
    // sparse
    val uv2 = v2Sparse.updated(5, 99)
    if (DEBUG) println("%s: apply(%d): >%d< (%d)".format("uv1", 5, v1Dense(5), 99))
    assert(uv2(5) == 99)
    val uv2e = (List(2, 5), List(1, 1))
    GpiAdaptiveVectorSuite.checkSparseVector(uv2, v2Sparse.length, sparseValue, 2, uv2e)

  }

  // ****
  test("transition: dense -> sparse") {
    val tv1 = v1Dense.updated(1, 0).updated(2, 0).updated(3, 0).updated(4, 0).updated(5, 0).updated(6, 0).updated(7, 0).updated(8, 0)
    if (DEBUG) println("%s: apply(%d): >%d< (%d)".format("tv1", 9, v1Dense(9), 9))
    val tv1e = (List(9), List(1))
    GpiAdaptiveVectorSuite.checkSparseVector(tv1, v1Dense.length, sparseValue, 1, tv1e)
  }

  test("transition: sparse -> dense") {
    val tv2 = v2Sparse.updated(0, 1).updated(1, 1).updated(2, 1).updated(3, 1).updated(4, 1).updated(5, 1).updated(6, 1).updated(7, 1).updated(8, 1)
    if (DEBUG) println("%s: apply(%d): >%d< (%d)".format("tv2", 5, v1Dense(5), 1))
    val tv2e = List(1, 1, 1, 1, 1, 1, 1, 1, 1, 0)
    if (DEBUG) println("tv2: >%s<".format(tv2))
    GpiAdaptiveVectorSuite.checkDenseVector(tv2, v2Sparse.length, sparseValue, 9, tv2e)
  }
  test("extend") {
    //    def extend(count: Int): GpiAdaptiveVector[VS] // Grow by adding count sparse values to the end
    println("extend: i need coverage")
  }
  test("denseIterator") {
    //    def denseIterator: Iterator[(Int, VS)] // iterator of indices and values of all non-sparse values
    println("denseIterator: i need coverage")
  }

  test("gpireduce - dense") {
    val v1r = GpiAdaptiveVector.gpi_reduce(add, add, 100, v1Dense)
    if (DEBUG) println("%s: reduced w/ %s: %s (%s)".format("v1r", 100, v1r, 145))
    assert(v1r == 145)
  }

  test("gpireduce - sparse") {
    val v2r = GpiAdaptiveVector.gpi_reduce(add, add, 100, v2Sparse)
    if (DEBUG) println("%s: reduced w/ %s: %s (%s)".format("v2r", 100, v2r, 101))
    assert(v2r == 101)
  }

  test("gpimap") {
    val fm1 = (x: Int) => x * 2
    val mv1 = GpiAdaptiveVector.gpi_map(fm1, v1Dense, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(0,2,4,6,8,10,12,14,16,18),0,9)", mv1)
    val mv1e = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(mv1, v1Dense.length, sparseValue, 9, mv1e)
    val mv2 = GpiAdaptiveVector.gpi_map(fm1, v2Sparse, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", mv2)
    val mv2e = (List(2), List(2))
    GpiAdaptiveVectorSuite.checkSparseVector(mv2, v2Sparse.length, sparseValue, 1, mv2e)
    val fm2 = (x: Int) => x * 0
    val mv3 = GpiAdaptiveVector.gpi_map(fm2, v1Dense, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(),GpiBuffer()),0,10)", mv3)
    val mv3e = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(mv3, v2Sparse.length, sparseValue, 0, mv3e)
    val fm3 = (x: Int) => x * 2 + 1
    val mv4 = GpiAdaptiveVector.gpi_map(fm3, v2Sparse, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(1,1,3,1,1,1,1,1,1,1),0,10)", mv4)
    val mv4e = List(1, 1, 3, 1, 1, 1, 1, 1, 1, 1)
    GpiAdaptiveVectorSuite.checkDenseVector(mv1, v1Dense.length, sparseValue, 9, mv4e)
  }

  test("gpizip:  add: etc") {
    val zv1 = GpiAdaptiveVector.gpi_zip(add, v1Dense, v1Dense, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(0,2,4,6,8,10,12,14,16,18),0,9)", zv1)
    val zv1e = List(0, 2, 4, 6, 8, 10, 12, 14, 16, 18)
    GpiAdaptiveVectorSuite.checkDenseVector(zv1, v1Dense.length, sparseValue, 9, zv1e)
    val zv2 = GpiAdaptiveVector.gpi_zip(add, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(0,1,3,3,4,5,6,7,8,9),0,9)", zv2)
    val zv2e = List(0, 1, 3, 3, 4, 5, 6, 7, 8, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(zv2, v1Dense.length, sparseValue, 9, zv2e)
    val zv3 = GpiAdaptiveVector.gpi_zip(add, v2Sparse, v1Dense, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(0,1,3,3,4,5,6,7,8,9),0,9)", zv3)
    val zv3e = List(0, 1, 3, 3, 4, 5, 6, 7, 8, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(zv3, v1Dense.length, sparseValue, 9, zv3e)
    val zv4 = GpiAdaptiveVector.gpi_zip(add, v2Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", zv4)
    val mv3e = (List(2), List(2))
    GpiAdaptiveVectorSuite.checkSparseVector(zv4, v2Sparse.length, sparseValue, 1, mv3e)
  }

  test("gpizip:  mul: etc") {
    val zv1 = GpiAdaptiveVector.gpi_zip(mul, v1Dense, v1Dense, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(0,1,4,9,16,25,36,49,64,81),0,9)", zv1)
    val zv1e = List(0, 1, 4, 9, 16, 25, 36, 49, 64, 81)
    GpiAdaptiveVectorSuite.checkDenseVector(zv1, v1Dense.length, sparseValue, 9, zv1e)
    val zv2 = GpiAdaptiveVector.gpi_zip(mul, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", zv2)
    val zv2e = (List(2), List(2))
    GpiAdaptiveVectorSuite.checkSparseVector(zv2, v1Dense.length, sparseValue, 1, zv2e)
    val zv3 = GpiAdaptiveVector.gpi_zip(mul, v2Sparse, v1Dense, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", zv3)
    val zv3e = (List(2), List(2))
    GpiAdaptiveVectorSuite.checkSparseVector(zv3, v2Sparse.length, sparseValue, 1, zv3e)
    val zv4 = GpiAdaptiveVector.gpi_zip(mul, v2Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", zv4)
    val mv3e = (List(2), List(1))
    GpiAdaptiveVectorSuite.checkSparseVector(zv4, v2Sparse.length, sparseValue, 1, mv3e)
  }

  // ****

  
  // operations for traditional semiring w/ multiplication and addition
  val sr = LagSemiring.plus_times[Int]
  val srmul = sr.multiplication
  val sradd = sr.addition

  test("gpizip:  sradd: etc") {
    val zv1 = GpiAdaptiveVector.gpi_zip(sradd, v1Dense, v1Dense, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(0,2,4,6,8,10,12,14,16,18),0,9)", zv1)
    val zv1e = List(0, 2, 4, 6, 8, 10, 12, 14, 16, 18)
    GpiAdaptiveVectorSuite.checkDenseVector(zv1, v1Dense.length, sparseValue, 9, zv1e)
    val zv2 = GpiAdaptiveVector.gpi_zip(sradd, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(0,1,3,3,4,5,6,7,8,9),0,9)", zv2)
    val zv2e = List(0, 1, 3, 3, 4, 5, 6, 7, 8, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(zv2, v1Dense.length, sparseValue, 9, zv2e)
    val zv3 = GpiAdaptiveVector.gpi_zip(sradd, v2Sparse, v1Dense, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(0,1,3,3,4,5,6,7,8,9),0,9)", zv3)
    val zv3e = List(0, 1, 3, 3, 4, 5, 6, 7, 8, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(zv3, v1Dense.length, sparseValue, 9, zv3e)
    val zv4 = GpiAdaptiveVector.gpi_zip(sradd, v2Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", zv4)
    val mv3e = (List(2), List(2))
    GpiAdaptiveVectorSuite.checkSparseVector(zv4, v2Sparse.length, sparseValue, 1, mv3e)
  }

  test("gpizip:  srmul: etc") {
    val zv1 = GpiAdaptiveVector.gpi_zip(srmul, v1Dense, v1Dense, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(0,1,4,9,16,25,36,49,64,81),0,9)", zv1)
    val zv1e = List(0, 1, 4, 9, 16, 25, 36, 49, 64, 81)
    GpiAdaptiveVectorSuite.checkDenseVector(zv1, v1Dense.length, sparseValue, 9, zv1e)
    val zv2 = GpiAdaptiveVector.gpi_zip(srmul, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", zv2)
    val zv2e = (List(2), List(2))
    GpiAdaptiveVectorSuite.checkSparseVector(zv2, v1Dense.length, sparseValue, 1, zv2e)
    val zv3 = GpiAdaptiveVector.gpi_zip(srmul, v2Sparse, v1Dense, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", zv3)
    val zv3e = (List(2), List(2))
    GpiAdaptiveVectorSuite.checkSparseVector(zv3, v2Sparse.length, sparseValue, 1, zv3e)
    val zv4 = GpiAdaptiveVector.gpi_zip(srmul, v2Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", zv4)
    val mv3e = (List(2), List(1))
    GpiAdaptiveVectorSuite.checkSparseVector(zv4, v2Sparse.length, sparseValue, 1, mv3e)
  }

  test("gpizip: sradd: Dense-Dense") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Dense = GpiAdaptiveVector.fromSeq(Vector(10, 11, 12, 13, 14, 15, 16, 17, 18, 19), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(sradd, v1Dense, v2Dense, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = List(10, 12, 14, 16, 18, 20, 22, 24, 26, 28)
    GpiAdaptiveVectorSuite.checkDenseVector(r, v1Dense.length, sparseValue, 10, re)
  }

  test("gpizip: sradd: Dense-Sparse sparse all zeroes") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(sradd, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = List(0, 1, 2, 3, 4, 5, 6, 7, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(r, v1Dense.length, sparseValue, 9, re)
  }
  test("gpizip: sradd: Dense-Sparse") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(sradd, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = List(0, 1, 3, 3, 4, 5, 6, 7, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(r, v1Dense.length, sparseValue, 9, re)
  }
  test("gpizip: sradd: Sparse-Dense") {
    val v2Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(1, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(sradd, v1Sparse, v2Dense, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = List(1, 1, 2, 3, 4, 5, 6, 7, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(r, v2Dense.length, sparseValue, 10, re)
  }
  test("gpizip: sradd: Sparse-Sparse") {
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(sradd, v1Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(2), List(1))
    GpiAdaptiveVectorSuite.checkSparseVector(r, v2Sparse.length, sparseValue, 1, re)
  }
  test("gpizip: sradd: Sparse-Sparse all zeroes") {
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(sradd, v1Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(r, v2Sparse.length, sparseValue, 0, re)
  }
  test("gpizip: srmul: Dense-Dense") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Dense = GpiAdaptiveVector.fromSeq(Vector(10, 11, 12, 13, 14, 15, 16, 17, 18, 19), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(srmul, v1Dense, v2Dense, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = List(0, 11, 24, 39, 56, 75, 96, 119, 144, 171)
    GpiAdaptiveVectorSuite.checkDenseVector(r, v1Dense.length, sparseValue, 9, re)
  }

  test("gpizip: srmul: Dense-Sparse sparse all zeroes") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(srmul, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(r, v1Dense.length, sparseValue, 0, re)
  }
  test("gpizip: srmul: Dense-Sparse") {
    val v1Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(srmul, v1Dense, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(2), List(2))
    GpiAdaptiveVectorSuite.checkSparseVector(r, v1Dense.length, sparseValue, 1, re)
  }
  test("gpizip: srmul: Sparse-Dense") {
    val v2Dense = GpiAdaptiveVector.fromSeq(Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(1, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(srmul, v1Sparse, v2Dense, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(r, v2Dense.length, sparseValue, 0, re)
  }
  test("gpizip: srmul: Sparse-Sparse") {
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(srmul, v1Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(r, v2Sparse.length, sparseValue, 0, re)
  }
  test("gpizip: srmul: Sparse-Sparse all zeroes") {
    val v2Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val v1Sparse = GpiAdaptiveVector.fromSeq(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), sparseValue)
    val r = GpiAdaptiveVector.gpi_zip(srmul, v1Sparse, v2Sparse, sparseValue)
    if (DEBUG) println("r: >%s<".format(r))
    val re = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(r, v2Sparse.length, sparseValue, 0, re)
  }

  test("gpimap: old semiring") {
    object fm1 extends Function[Int, Int] {
      override def apply(x: Int): Int = {
        x * 2
      }
    }
    val mv1 = GpiAdaptiveVector.gpi_map(fm1, v1Dense, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(0,2,4,6,8,10,12,14,16,18),0,9)", mv1)
    val mv1e = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    GpiAdaptiveVectorSuite.checkDenseVector(mv1, v1Dense.length, sparseValue, 9, mv1e)
    val mv2 = GpiAdaptiveVector.gpi_map(fm1, v2Sparse, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(2),GpiBuffer(2)),0,10)", mv2)
    val mv2e = (List(2), List(2))
    GpiAdaptiveVectorSuite.checkSparseVector(mv2, v2Sparse.length, sparseValue, 1, mv2e)
    object fm2 extends Function[Int, Int] {
      override def apply(x: Int): Int = {
        x * 0
      }
    }
    val mv3 = GpiAdaptiveVector.gpi_map(fm2, v1Dense, sparseValue)
    if (DEBUG) println("GpiSparseVector((GpiBuffer(),GpiBuffer()),0,10)", mv3)
    val mv3e = (List(), List())
    GpiAdaptiveVectorSuite.checkSparseVector(mv3, v2Sparse.length, sparseValue, 0, mv3e)
    object fm3 extends Function[Int, Int] {
      override def apply(x: Int): Int = {
        x * 2 + 1
      }
    }
    val mv4 = GpiAdaptiveVector.gpi_map(fm3, v2Sparse, sparseValue)
    if (DEBUG) println("GpiDenseVector(GpiBuffer(1,1,3,1,1,1,1,1,1,1),0,10)", mv4)
    val mv4e = List(1, 1, 3, 1, 1, 1, 1, 1, 1, 1)
    GpiAdaptiveVectorSuite.checkDenseVector(mv1, v1Dense.length, sparseValue, 9, mv4e)
  }
}
object GpiAdaptiveVectorSuite {

  def checkSparseVector[VS](
    v: GpiAdaptiveVector[VS],
    length: Int,
    sparseValue: VS,
    denseCount: Int,
    expected: (Seq[VS], Seq[VS])): Unit = {
    assert(v.length == length)
    assert(v.sparseValue == sparseValue)
    assert(v.denseCount == denseCount)
    assert(v.isInstanceOf[GpiSparseVector[VS]])
    v match {
      case sv: GpiSparseVector[VS] => {
        (sv.rv._1.toVector zip expected._1).forall { case (va, ve) => va == ve }
      }
      case _ => throw new RuntimeException("checkSparseVector: no match")
    }
  }
  def checkDenseVector[VS](
    v: GpiAdaptiveVector[VS],
    length: Int,
    sparseValue: VS,
    denseCount: Int,
    expected: Seq[VS]): Unit = {
    assert(v.length == length)
    assert(v.sparseValue == sparseValue)
    assert(v.denseCount == denseCount)
    assert(v.isInstanceOf[GpiDenseVector[VS]])
    v match {
      case dv: GpiDenseVector[VS] => {
        (dv.toVector zip expected).forall { case (va, ve) => va == ve }
      }
      case _ => throw new RuntimeException("checkDenseVector: no match")
    }
  }
}

