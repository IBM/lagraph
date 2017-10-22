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

import scala.reflect.ClassTag
import com.ibm.lagraph._
import org.apache.spark.rdd.RDD

// ********
// Dstr MATRIX
/**
  * The Distributed Matrix
  *
  *  TODO: Hide my scaladoc somehow, maybe move me to com.ibm.lagraph.impl?
  */
final case class LagDstrMatrix[T: ClassTag](override val hc: LagContext,
                                            val rcvRdd: RDD[((Long, Long), T)],
                                            val dstrBmat: GpiDstrBmat[T])
    extends LagMatrix[T](hc, (dstrBmat.nrow, dstrBmat.ncol)) {
  //  override def size = (dstrBmat.nrow, dstrBmat.ncol)
  override lazy val _transpose: LagMatrix[T] = hc match {
    case hca: LagDstrContext => {
      hca.mFromRcvRdd(rcvRdd.map { case (k, v) => ((k._2, k._1), v) }, dstrBmat.sparseValue)
    } // this.hc.mTranspose(this)
  }
}
// ********
// Dstr VECTOR
/**
  * The Distributed Vector
  *
  */
final case class LagDstrVector[T: ClassTag](override val hc: LagContext,
                                            val dstrBvec: GpiDstrBvec[T])
    extends LagVector[T](hc, dstrBvec.nrow) {
  //  override def size = dstrBvec.nrow // TODO Int->Long
}

// ********
// SMP MATRIX
final case class LagSmpMatrix[T: ClassTag](override val hc: LagContext,
                                           rcvMap: Map[(Long, Long), T],
                                           vov: GpiAdaptiveVector[GpiAdaptiveVector[T]])
    extends LagMatrix[T](hc, (vov.size, vov(0).size)) {
  //  override def size = (vov.size, vov(0).size)
  override lazy val _transpose: LagMatrix[T] = hc match {
    case hca: LagSmpContext => {
      hca.mFromMap(rcvMap.map { case (k, v) => ((k._2, k._1), v) }, vov(0).sparseValue)
    }
  }
  //    LagSmpMatrix(this.hc, rcvMap.map{case(k:(Long, Long), v:T) =>
  //      ((k._2,k._1),v)}, GpiSparseRowMatrix.transpose(vov))
  // hc.mTranspose(this.asInstanceOf[LagMatrix])

}
// ********
// SMP VECTOR
// ****
// LagAdaptiveVector
// MATRIX
final case class LagSmpVector[T: ClassTag](override val hc: LagContext, v: GpiAdaptiveVector[T])
    extends LagVector[T](hc, v.size.toLong) {
  //  override def size = v.size.toLong
}
