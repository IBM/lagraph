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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{Map => MMap}
import scala.reflect.ClassTag
import scala.reflect.classTag

import org.apache.spark.SparkContext

/**
  * A semigroup for use in LAG.
  *
  *  @tparam T the type of the elements operated on by this semigroup.
  *
  *  @constructor return a semigroup with a function, an operation,
  *  an optional annihilator for the operation, and optional identity for the operation.
  *  @param op the semigroup function, the function must respect semiring
  *  properties for annihilator (if specified) and identity (if specified)
  *  @param annihilator an optional annihilator for the function
  *  @param identity an optional identity for the fucntion
  */
case class LagSemigroup[T](op: Function2[T, T, T],
                           annihilator: Option[T] = None,
                           identity: Option[T] = None)
    extends Function2[T, T, T] {

  /** Apply the body of this function to the arguments. */
  override def apply(a: T, b: T): T = { op(a, b) }
}

/** Companion object for LagSemigroup. Handles +/- infinity. */
object LagSemigroup {
  // a registry of infinity values for basic scala types
  val infRegistry = Map[ClassTag[_], Any](classTag[Float] -> Float.MaxValue,
                                          classTag[Double] -> Double.MaxValue,
                                          classTag[Int] -> Int.MaxValue,
                                          classTag[Long] -> Long.MaxValue)
  def infinity[T](tt: ClassTag[T]): T =
    LagSemigroup.infRegistry(tt).asInstanceOf[T]
  // a registry of minus infinity values for basic scala types
  val minfRegistry = Map[ClassTag[_], Any](classTag[Float] -> Float.MinValue,
                                           classTag[Double] -> Double.MinValue,
                                           classTag[Int] -> Int.MinValue,
                                           classTag[Long] -> Long.MinValue)
  def minfinity[T](tt: ClassTag[T]): T =
    LagSemigroup.minfRegistry(tt).asInstanceOf[T]
}

/**
  * Provides implementations for Numeric that are not required
  * for LagSemiring
  *
  *  @tparam T the type operated on by this semiring.
  */
trait LagSemiringAsNumeric[T] extends Numeric[T] {
  def minus(x: T, y: T): T = throw new RuntimeException("minus not required")
  def quot(x: T, y: T): T = throw new RuntimeException("quot not required")
  def rem(x: T, y: T): T = throw new RuntimeException("rem not required")
  def negate(x: T): T = throw new RuntimeException("negate not required")
  def toInt(x: T): Int = throw new RuntimeException("toInt not required")
  def toLong(x: T): Long = throw new RuntimeException("toLong not required")
  def toFloat(x: T): Float = throw new RuntimeException("toFloat not required")
  def toDouble(x: T): Double =
    throw new RuntimeException("toDouble not required")
}

/**
  * A semiring, that is, an abstraction of addition and multiplication in the context of LAG
  *
  *  @tparam T the type of the elements operated on by this semiring.
  */
trait LagSemiring[T] extends Serializable {

  implicit protected def tt: ClassTag[T]
  //  require(addition.identity == multiplication.annihilator) // from definition of semiring
  /** Addition for this semiring, must meet all semiring requirements for addition */
  val addition: LagSemigroup[T]

  /** Multiplication for this semiring, must meet all semiring requirements for multiplication */
  val multiplication: LagSemigroup[T]

  /** The zero for this semiring, must meet all semiring requirements for zero */
  def zero: T = multiplication.annihilator.get

  /** the one for this semiring, must meet all semiring requirements for one */
  def one: T = multiplication.identity.get
}

/**
  * Companion object to the LagSemiring class.
  *  Provides a factory for LagSemeiring.
  *  Also, provides templated versions of well know semirings.
  */
object LagSemiring {

  /**
    * Creates a semiring.
    *
    *  @tparam T the underlying type of the semigroup.
    *
    *  @param addition, must meet all semiring requirements for addition
    *  @param multiplication, must meet all semiring requirements for multiplication
    *  @param zero, must meet all semiring requirements for zero
    *  @param one , must meet all semiring requirements for one
    *  @return a semiring w/ specified properties
    */
  def apply[T: ClassTag](addition: Function2[T, T, T],
                         multiplication: Function2[T, T, T],
                         zero: T,
                         one: T): LagSemiring[T] = {
    val additionSg = LagSemigroup(addition, identity = Option(zero))
    val multiplicationSg =
      LagSemigroup(multiplication, annihilator = Option(zero), identity = Option(one))
    //    new LagSemiring(additionSg, multiplicationSg)
    new LagSemiring[T] {
      protected def tt = reflect.classTag[T]; val addition = additionSg;
      val multiplication = multiplicationSg
    }
  }

  /**
    * The plus.times semiring. If the underlying type is not a Scala primitive
    * type (e.g., Float, Double, Int, or Long, then user must define an implicit
    * object that extends LagSemiringAsNumeric[T].
    *
    *  @tparam T the underlying type.
    *
    */
  def plus_times[T: ClassTag](implicit num: Numeric[T]): LagSemiring[T] = {
    val zero = num.zero
    val one = num.one
    val addition = (a: T, b: T) => num.plus(a, b)
    val multiplication = (a: T, b: T) => num.times(a, b)
    val additionSg = LagSemigroup[T](addition, identity = Option(zero))
    val multiplicationSg =
      LagSemigroup[T](multiplication, annihilator = Option(zero), identity = Option(one))
    LagSemiring[T](additionSg, multiplicationSg, zero, one)
  }

  /**
    * The or.and semiring. If the underlying type is not a Scala primitive
    * type (e.g., Float, Double, Int, or Long, then user must define an
    * implicit object that extends LagSemiringAsNumeric[T].
    *
    *  @tparam T the underlying type.
    *
    */
  def or_and: LagSemiring[Boolean] = {
    val zero = false
    val one = true
    val addition = (a: Boolean, b: Boolean) => a || b
    val multiplication = (a: Boolean, b: Boolean) => a && b
    val additionSg = LagSemigroup[Boolean](addition, identity = Option(zero))
    val multiplicationSg =
      LagSemigroup[Boolean](multiplication, annihilator = Option(zero), identity = Option(one))
    LagSemiring[Boolean](additionSg, multiplicationSg, zero, one)
  }

  /**
    * The min.plus semiring for an underlying user-defined type.
    *  The user must define an implicit object that extends both Numeric[T] and Ordering[T]
    *
    *  @tparam T the underlying type.
    *
    */
  def min_plus[T: ClassTag](infinity: T)(implicit num: Numeric[T]): LagSemiring[T] = {
    val zero = infinity
    val one = num.zero
    val addition = (a: T, b: T) => num.min(a, b)
    val multiplication = (a: T, b: T) =>
      if (a == zero || b == zero) { zero }
      else { num.plus(a, b) }
    val additionSg = LagSemigroup[T](addition, identity = Option(zero))
    val multiplicationSg =
      LagSemigroup[T](multiplication, annihilator = Option(zero), identity = Option(one))
    LagSemiring[T](additionSg, multiplicationSg, zero, one)
  }

  /**
    * The min.plus semiring for an underlying type of Float, Double, Int, or Long.
    *
    *  @tparam T the underlying type (Float, Double, Int, or Long)
    *
    */
  def min_plus[T: ClassTag](implicit num: Numeric[T]): LagSemiring[T] = {
    val tt = classTag[T]
    val infinity = LagSemigroup.infRegistry(tt).asInstanceOf[T]
    min_plus[T](infinity)
  }

  /**
    * The min.times semiring for an underlying user-defined type.
    *  The user must define an implicit object that extends both Numeric[T] and Ordering[T]
    *
    *  @tparam T the underlying type.
    *
    */
  def min_times[T: ClassTag](infinity: T)(implicit num: Numeric[T]): LagSemiring[T] = {
    val zero = infinity
    val one = num.one
    val addition = (a: T, b: T) => num.min(a, b)
    val multiplication = (a: T, b: T) =>
      if (a == zero || b == zero) { zero } else { num.times(a, b) }
    val additionSg = LagSemigroup[T](addition, identity = Option(zero))
    val multiplicationSg =
      LagSemigroup[T](multiplication, annihilator = Option(zero), identity = Option(one))
    LagSemiring[T](additionSg, multiplicationSg, zero, one)
  }

  /**
    * The min.times semiring for an underlying type of Float, Double, Int, or Long.
    *
    *  @tparam T the underlying type (Float, Double, Int, or Long)
    *
    */
  def min_times[T: ClassTag](implicit num: Numeric[T]): LagSemiring[T] = {
    val tt = classTag[T]
    val infinity = LagSemigroup.infRegistry(tt).asInstanceOf[T]
    min_times[T](infinity)
  }

  /**
    * The max.times semiring for an underlying user-defined type.
    *  The user must define an implicit object that extends both Numeric[T] and Ordering[T]
    *
    *  @tparam T the underlying type.
    *
    */
  def max_times[T: ClassTag](infinity: T)(implicit num: Numeric[T]): LagSemiring[T] = {
    val zero = infinity
    val one = num.one
    val addition = (a: T, b: T) => num.max(a, b)
    val multiplication = (a: T, b: T) =>
      if (a == zero || b == zero) { zero } else { num.times(a, b) }
    val additionSg = LagSemigroup[T](addition, identity = Option(zero))
    val multiplicationSg =
      LagSemigroup[T](multiplication, annihilator = Option(zero), identity = Option(one))
    LagSemiring[T](additionSg, multiplicationSg, zero, one)
  }

  /**
    * The max.times semiring for an underlying type of Float, Double, Int, or Long.
    *
    *  @tparam T the underlying type (Float, Double, Int, or Long)
    *
    */
  def max_times[T: ClassTag](implicit num: Numeric[T]): LagSemiring[T] = {
    val tt = classTag[T]
    val infinity = LagSemigroup.minfRegistry(tt).asInstanceOf[T]
    max_times[T](infinity)
  }

  /**
    * The nop.min semiring for an underlying user-defined type.
    *  The user must define an implicit object that extends both Numeric[T] and Ordering[T]
    *  "nop" means that the addition function in the semiring is not used.
    *
    *  @tparam T the underlying type.
    *
    *  @param infinity value representing infinity for the underlying type
    *  @param minfinity value representing minus infinity for the underlying type
    *
    */
  def nop_min[T: ClassTag](infinity: T, minfinity: T)(implicit num: Numeric[T]): LagSemiring[T] = {
    val tt = classTag[T]
    val zero = minfinity
    val one = infinity
    val multiplication = (a: T, b: T) => num.min(a, b)
    val multiplicationSg =
      LagSemigroup[T](multiplication, annihilator = Option(zero), identity = Option(one))
    LagSemiring[T](null, multiplicationSg, zero, one)
  }

  /**
    * The nop.min semiring for an underlying type of Float, Double, Int, or Long.
    *  "nop" means that the addition function in the semiring is not used.
    *
    *  @tparam T the underlying type.
    *
    */
  def nop_min[T: ClassTag](implicit num: Numeric[T]): LagSemiring[T] = {
    val tt = classTag[T]
    val infinity = LagSemigroup.infRegistry(tt).asInstanceOf[T]
    val minfinity = LagSemigroup.minfRegistry(tt).asInstanceOf[T]
    nop_min[T](infinity, minfinity)
  }

  /**
    * The nop.plus semiring for an underlying user-defined type.
    *  The user must define an implicit object that extends both Numeric[T] and Ordering[T]
    *  "nop" means that the addition function in the semiring is not used.
    *
    *  @tparam T the underlying type.
    *
    *  @param infinity value representing infinity for the underlying type
    *  @param minfinity value representing minus infinity for the underlying type
    *
    */
  def nop_plus[T: ClassTag](infinity: T)(implicit num: Numeric[T]): LagSemiring[T] = {
    val zero = infinity
    val one = num.zero
    val multiplication = (a: T, b: T) =>
      if (a == zero || b == zero) { zero }
      else { num.plus(a, b) }
    val multiplicationSg =
      LagSemigroup[T](multiplication, annihilator = Option(zero), identity = Option(one))
    LagSemiring[T](null, multiplicationSg, zero, one)
  }

  /**
    * The nop.plus semiring for an underlying type of Float, Double, Int, or Long.
    *  "nop" means that the addition function in the semiring is not used.
    *
    *  @tparam T the underlying type.
    *
    */
  def nop_plus[T: ClassTag](implicit num: Numeric[T]): LagSemiring[T] = {
    val tt = classTag[T]
    val infinity = LagSemigroup.infRegistry(tt).asInstanceOf[T]
    nop_plus[T](infinity)
  }
}

/**
  * A vector. In LAG, all Vectors are created using an instance of 'LagContext'.
  *
  *  @param T the type of the vertex
  *  @param hc the LagContext
  *  @param size the length of the vector
  */
abstract class LagVector[T: ClassTag] protected (val hc: LagContext, val size: Long)
    extends Serializable {

  /** The length of this vector, equivalent to `size`. */
  val length = size
  //  def length = size
  //  /** The size of this vector, equivalent to `length`. */
  //  def size: Long
  //  /** The context under which this vector was created */
  //  def hc: LagContext
  // *****
  // import / export
  /**
    * Convert this vector to a map.
    *
    *  @param v input vector.
    *  @return a Tuple2: (Map[row, value[T]) representation of the matrix, sparse value)
    */
  def toMap: (Map[Long, T], T) =
    hc.vToMap(this)

  /**
    * Convert a vector to a scala.collection.immutable.Vector.
    *
    *  @param v input vector.
    *  @return a scala.collection.immutable.Vector representation of this vector
    */
  def toVector: Vector[T] =
    hc.vToVector(this)
  // *****
  // vector functions
  /**
    * Using functions f and c and an starting value, reduce this vector to scalar
    *  @tparam T2 the type of the result
    *
    *  @param f binary reduction operator
    *  @param c binary combiner operator
    *  @param z the start value
    *  @param u the vector to be reduced
    *  @return the result of the reduction
    */
  def reduce[T2: ClassTag](f: (T, T2) => T2,
                           c: (T2, T2) => T2, // TODO need help from eknath
                           z: T2): T2 =
    hc.vReduce(f, c, z, this)

  /**
    * Return a vector representing the application of a function to all elements of this vector
    *
    *  @tparam T2 the element type of the returned vector
    *
    *  @param f the function to be applied to each element
    *  @param u the input vector
    *  @return resultant vector
    */
  def map[T2: ClassTag](f: (T) => T2): LagVector[T2] =
    hc.vMap(f, this)

  /**
    * Return a vector representing the application of a binary operator to
    * the pairs of elements formed by zipping together this vector with
    * another vector
    *
    *  @tparam T2 the element type of the other vector
    *  @tparam T3 the element type of the returned vector
    *
    *  @param f the function to be applied to each pair of elements
    *  @param v the other vector
    *  @return resultant vector
    */
  def zip[T2: ClassTag, T3: ClassTag](f: (T, T2) => T3, v: LagVector[T2]): LagVector[T3] =
    hc.vZip(f, this, v)

  /**
    * Return a vector representing the application of a binary operator
    * to pairs of elements formed by zipping together this vector with
    * an (implicit) index vector
    *
    *  @tparam T2 the element type of the returned vector
    *
    *  @param f the function to be applied to each pair of elements
    *  @param u the first input vector
    *  @param sparseValueOpt option to control sparsity of output vector, if not sepecified
    *         f(u.sparseValue, 0) will be used
    *  @return resultant vector
    */
  def zipWithIndex[T2: ClassTag](f: (T, Long) => T2,
                                 sparseValueOpt: Option[T2] = None): LagVector[T2] =
    hc.vZipWithIndex(f, this, sparseValueOpt)

  /**
    * Using functions f and c and an starting value along w/ an (implicit)
    * index vector, reduce this vector to scalar. This function is similar
    * to 'vZip' except the reduction function takes a pair elements formed
    * by zipping togehter the input vector with an index vector.
    *
    *  @tparam T1 the type of the elements in the input vector
    *  @tparam T2 the type of the result
    *
    *  @param f binary reduction operator
    *  @param c binary combiner operator
    *  @param z the start value
    *  @param u the vector to be reduced
    *  @return the result of the reduction
    */
  def reduceWithIndex[T2: ClassTag](f: ((T, Long), T2) => T2,
                                    c: (T2, T2) => T2, // TODO need help from eknath
                                    z: T2): T2 =
    hc.vReduceWithIndex(f, c, z, this)
  // *******
  // equivalence
  /**
    * Test if this vector is equivalent to another vector
    *
    *  @param r the other vector
    *
    *  @return true if l is equivalent to r; false otherwise.
    */
  def equiv(r: LagVector[T]): Boolean =
    hc.vEquiv(this, r)
  // ********
  /**
    * Use a formatting function to return a String representation of this vector.
    *
    *  @param formatter function to covert an element of type T to a String
    *
    *  @return a string representation of this vector
    */
  def toString(formatter: (T) => String): String =
    hc.vToString(this, formatter)

  /**
    * Access an element in this vector
    *
    *  @param r the position in this vector
    *
    *  @return the rth element of this vector
    */
  def ele(r: Long): (Option[T], Long) =
    hc.vEle(this, r)

  /**
    * Return a vector representing this vector with one element modified.
    *
    *  @param r the position of the element to be modified
    *  @param v the modified value
    *
    *  @return resultant vector
    */
  def set(r: Long, v: T): LagVector[T] =
    hc.vSet(this, r, v)

  /**
    * Find the index of a minimum value in this vector
    *
    *  @return a pair containing the minimum value and its index
    */
  def argmin(implicit n: Numeric[T]): (T, Long) =
    hc.vArgmin(this)

}

/**
  * An adjacency matrix. In LAG, all Matrices are created using an instance of 'LagContext'.
  *
  *  @tparam T the type of elements in the matrix
  *
  *  @param hc the LagContext
  *  @param size A (Long, Long) pair representing the dimension of the matrix
  */
abstract class LagMatrix[T: ClassTag] protected (val hc: LagContext, val size: (Long, Long))
    extends Serializable {

  /**
    * A (row, col) pair representing the dimension of the matrix, equivalent to `size`.
    */
  val length = size
  //  def length = size
  //  def size: (Long, Long)
  //  def hc: LagContext
  protected val _transpose: LagMatrix[T]
  // *****
  // import / export
  /**
    * Convert this matrix to a map.
    *
    *  @param m input matrix.
    *  @return a Tuple2: (map representation of the matrix, sparse value)
    */
  def toMap: (Map[(Long, Long), T], T) =
    hc.mToMap(this)

  // *****
  // matrix function
  // *****
  // experimental
  def map[T2: ClassTag](f: (T) => T2, m: LagMatrix[T]): LagMatrix[T2] =
    hc.mMap(f, this)
  // experimental
  def zip[T2: ClassTag, T3: ClassTag](f: (T, T2) => T3,
                                      m: LagMatrix[T],
                                      n: LagMatrix[T2]): LagMatrix[T3] =
    hc.mZip(f, m, n)

  /**
    * Transform a matrix.
    *
    *  The primary purpose of this function is to transform a canonical
    *  adjacency matrix into an adjacency matrix suitable for a particular
    *  LAG algorithm. All non-sparse entries and (optionally) the diagonal
    *  are transformed using an input function. Sparse values in the input
    *  matrix are directly transformed into 'targetSparseValue'. If
    *  visitDiagonalsOpt is not specified or specified as None then diagonal
    *  elements are only transformed if they are non-sparse.
    *
    *  @tparam T2 the type of the result
    *
    *  @param f binary operator that performs the transforms an element of
    *           the input matrix and its indices into an element at the
    *           same location in the output matrix
    *  @param targetSparseValue sparse elements in the input matrix are
    *                           transformed into elements with this value
    *  @param visitDiagonalsOpt if not 'None", diaganol elements are
    *                           transformed using 'visitDiagonalsOpt.get'
    *                           as input to 'f'
    *
    *  @return a matrix resulting from the transformation
    */
  @deprecated("Not in scope", "0.1.0")
  def sparseZipWithIndex[T2: ClassTag](f: (T, (Long, Long)) => T2,
                                       targetSparseValue: T2,
                                       visitDiagonalsOpt: Option[T] = None): LagMatrix[T2] =
    hc.mSparseZipWithIndex(f, this, targetSparseValue, visitDiagonalsOpt)

  /**
    * Creates a matrix by applying a binary operator to pairs of elements formed by zipping together
    *  this matrix with an (implicit) index matrix
    *
    *  @tparam T2 the type of the resultant matrix
    *
    *  @param f binary operator that performs the transforms an element of
    *           the input matrix and its indices into an element at the
    *           same location in the output matrix
    *
    *  @return the resultant matrix
    */
  def zipWithIndex[T2: ClassTag](f: (T, (Long, Long)) => T2): LagMatrix[T2] =
    hc.mZipWithIndex(f, this)

  /**
    * Multiplies this matrix times a vector.
    *
    *  @param sr the semiring used in the multiplication
    *  @param u the input vector
    *
    *  @return the resultant vector
    */
  def tV(sr: LagSemiring[T], u: LagVector[T]): LagVector[T] =
    hc.mTv(sr, this, u)

  /**
    * Multiplies a matrix times another matrix.
    *
    *  @param sr the semiring used in the multiplication
    *  @param n the other matrix
    *
    *  @return the resultant matrix
    */
  def tM(sr: LagSemiring[T], n: LagMatrix[T]): LagMatrix[T] =
    hc.mTm(sr, this, n)
  // Hadagard .multiplication
  /**
    * Hadagard multiplication between this matrix and another matrix
    *
    *  @param sr the semiring used in the multiplication, not that only semiring multiplication
    *            is used in the Hadagard product
    *  @param n the other matrix
    *
    *  @return the resultant matrix
    */
  def hM( // TODO maybe combine w/ vZip
         sr: LagSemiring[T],
         //    f: (T, T) => T,  // TODO maybe sr OR maybe f: (T1, T2) => T3?
         n: LagMatrix[T]): LagMatrix[T] =
    hc.mHm(sr, this, n)

  /**
    * Outer product of a column from this matrix and a row from another matrix
    *
    *  @param f the semiring used in the multiplication, not that only semiring multiplication
    *            is used in the outer product
    *  @param mCol the column of 'm'
    *  @param n the second input matrix
    *  @param nRow the row of 'n'
    *
    *  @return the resultant matrix
    */
  def oP(f: LagSemiring[T], mCol: Long, n: LagMatrix[T], nRow: Long): LagMatrix[T] =
    hc.mOp(f, this, mCol, n, nRow)
  // *******
  // matrix mechanics
  /**
    * Return a matrix representing the transpose this matrix.
    *
    *  @return resultant matrix
    */
  def transpose: LagMatrix[T] =
    _transpose

  /**
    * Return a vector representing a row in this matrix.
    *
    *  @param mRow the row in this matrix
    *
    *  @return resultant vector
    */
  def vFromRow(mRow: Long): LagVector[T] =
    hc.vFromMrow(this, mRow)

  /**
    * Return a vector representing a column in this matrix.
    *
    *  @param mCol the column in this matrix
    *
    *  @return resultant vector
    */
  def vFromCol(mCol: Long): LagVector[T] =
    hc.vFromMcol(this, mCol)
  // ********
  /**
    * Use a formatting function to return a String representation of this matrix.
    *
    *  @param formatter function to covert an element of type T to a String
    *
    *  @return a string representation of the matrix
    */
  def toString(formatter: (T) => String): String =
    hc.mToString(this, formatter)
}

/**
  * The LAG context
  *
  *  The context is used for creating, importing, exporting, and manipulating
  *  matrices and vectors.
  *
  *  A LAG context defines the dimension of the problem, ie number of vertices.
  *
  *  A LAG context may be used for any number of matrices and vectors.
  *
  *  Matrices and vectors in one context cannot be mixed with matrices and vectors
  *  in a different context.
  */
trait LagContext {

  /** The dimension of the Adjacency Matrix will be graphSize X graphSize */
  val graphSize: Long
  // vector factory
  /**
    * Return a vector with base-0 indices of type Long, optionally offset by start.
    *
    * The resultant vector will possess a sparse value of 0L
    *
    *  @param start optional offset
    *  @return resultant vector
    */
  def vIndices(start: Long = 0L, sparseValueOpt: Option[Long] = None): LagVector[Long]

  /**
    * Return a vector, where every element has a value of x.
    *
    *  @tparam T the type of the elements in the vector
    *
    *  @param x value for assignment
    *  @param sparseValueOpt option to control sparsity of resultant vector, if not specified
    *         x will be used
    *  @return resultant vector
    */
  def vReplicate[T: ClassTag](x: T, sparseValueOpt: Option[T] = None): LagVector[T]
  // experimental
  // TODO Do we need this?
  /**
    * Return a matrix with base-0 indices of type Long, optionally offset by start.
    *
    * TODO: Experimental: Do we need this?
    *
    * The resultant vector will possess a sparse value of 0L
    *
    *  @param start optional (row, column) offset
    *  @return resultant matrix
    */
  def mIndices(start: (Long, Long)): LagMatrix[(Long, Long)]
  // experimental
  /**
    * Return a matrix, where every element has a value of x.
    *
    * TODO: Experimental: Do we need this?
    *
    *  @tparam T the type of the elements in the matrix
    *
    *  @param x value for assignment
    *  @param sparseValueOpt option to control sparsity of resultant matrix, if not specified
    *         x will be used
    *  @return resultant matrix
    */
  def mReplicate[T: ClassTag](x: T): LagMatrix[T]

  // *****
  // import / export
  // TODO should sparseValue be optional for mFromSeqOfSeq, vFromSeq
  /**
    * Return a matrix representing a Scala Map.
    *
    *  @tparam T the type of the elements in the matrix
    *
    *  @param mMatrix Map[(row[Long],col[Long]), value[T]] representation of a matrix.
    *  @param sparseValue sparse value for unspecified (row, col) pairs.
    *  @return resultant matrix
    */
  def mFromMap[T: ClassTag](mMatrix: Map[(Long, Long), T], sparseValue: T): LagMatrix[T]

  /**
    * Convert a matrix to a map.
    *
    *  @tparam T the type of the elements in the matrix
    *
    *  @param m input matrix.
    *  @return a Tuple2: (map representation of the matrix, sparse value)
    */
  private[lagraph] def mToMap[T: ClassTag](m: LagMatrix[T]): (Map[(Long, Long), T], T)

  /**
    * Return a vector representing a Scala Map.
    *
    *  @tparam T the type of the elements in the vector
    *
    *  @param m Map[row[Long], value[T]] representation of a vector.
    *  @return resultant vector
    */
  def vFromMap[T: ClassTag](m: Map[Long, T], sparseValue: T): LagVector[T]

  /**
    * Return a vector representing a Scala Seq.
    *
    *  @tparam T the type of the elements in the vector
    *
    *  @param v Seq representation of a vector.
    *  @return resultant vector
    */
  def vFromSeq[T: ClassTag](v: Seq[T], sparseValue: T): LagVector[T]

  /**
    * Convert a vector to a map.
    *
    *  @tparam T the type of the elements in the vector
    *
    *  @param v input vector.
    *  @return a Tuple2: (Map[row, value[T]) representation of the matrix, sparse value)
    */
  private[lagraph] def vToMap[T: ClassTag](v: LagVector[T]): (Map[Long, T], T)

  /**
    * Convert a vector to a scala.collection.immutable.Vector.
    *
    *  @tparam T the type of the elements in the vector
    *
    *  @param v input vector.
    *  @return a scala.collection.immutable.Vector representation of the vector
    */
  private[lagraph] def vToVector[T: ClassTag](v: LagVector[T]): Vector[T]

  // *****
  // vector function
  /**
    * Using functions f and c and an starting value, reduce a vector to scalar
    *
    *  @tparam T1 the type of the elements in the input vector
    *  @tparam T2 the type of the result
    *
    *  @param f binary reduction operator
    *  @param c binary combiner operator
    *  @param z the start value
    *  @param u the vector to be reduced
    *  @return the result of the reduction
    */
  private[lagraph] def vReduce[T1: ClassTag, T2: ClassTag](
      f: (T1, T2) => T2,
      c: (T2, T2) => T2, // TODO need help from eknath
      z: T2,
      u: LagVector[T1]): T2

  /**
    * Builds a new vector by applying a function to all elements of the input vector
    *
    *  @tparam T1 the element type of the input vector
    *  @tparam T2 the element type of the returned vector
    *
    *  @param f the function to be applied to each element
    *  @param u the input vector
    *  @return a new vector resulting from the application of the function
    *          to each element in the input vector
    */
  private[lagraph] def vMap[T1: ClassTag, T2: ClassTag](f: (T1) => T2,
                                                        u: LagVector[T1]): LagVector[T2]

  /**
    * Builds a new vector by applying a binary operator to pairs of elements
    * formed by zipping together two input vectors
    *
    *  @tparam T1 the element type of the first input vector
    *  @tparam T2 the element type of the second input vector
    *  @tparam T3 the element type of the returned vector
    *
    *  @param f the function to be applied to each pair of elements
    *  @param u the first input vector
    *  @param v the second input vector
    *  @return a new vector resulting from the application of the function to each pair of elements.
    */
  private[lagraph] def vZip[T1: ClassTag, T2: ClassTag, T3: ClassTag](
      f: (T1, T2) => T3,
      u: LagVector[T1],
      v: LagVector[T2]): LagVector[T3]

  /**
    * Builds a new vector by applying a binary operator to pairs of elements
    * formed by zipping together the input vector with an index vector
    *
    *  @tparam T1 the element type of the first input vector
    *  @tparam T2 the element type of the returned vector
    *
    *  @param f the function to be applied to each pair of elements
    *  @param u the first input vector
    *  @param sparseValueOpt option to control sparsity of output vector, if not sepecified
    *         f(u.sparseValue, 0) will be used
    *  @return a new vector resulting from the application of the function to each pair of elements.
    */
  private[lagraph] def vZipWithIndex[T1: ClassTag, T2: ClassTag](
      f: (T1, Long) => T2,
      u: LagVector[T1],
      sparseValueOpt: Option[T2] = None): LagVector[T2]

  /**
    * Using functions f and c and an starting value along w/ an internal index
    * vector, reduce a vector to scalar. This function is similar to 'vZip' except
    * the reduction function takes a pair elements formed by
    * zipping togehter the input vector with an index vector.
    *
    *  @tparam T1 the type of the elements in the input vector
    *  @tparam T2 the type of the result
    *
    *  @param f binary reduction operator
    *  @param c binary combiner operator
    *  @param z the start value
    *  @param u the vector to be reduced
    *  @return the result of the reduction
    */
  private[lagraph] def vReduceWithIndex[T1: ClassTag, T2: ClassTag](
      f: ((T1, Long), T2) => T2,
      c: (T2, T2) => T2, // TODO need help from eknath
      z: T2,
      u: LagVector[T1]): T2

  // *****
  // experimental
  private[lagraph] def mMap[T1: ClassTag, T2: ClassTag](f: (T1) => T2,
                                                        m: LagMatrix[T1]): LagMatrix[T2]
  // experimental
  private[lagraph] def mZip[T1: ClassTag, T2: ClassTag, T3: ClassTag](
      f: (T1, T2) => T3,
      m: LagMatrix[T1],
      n: LagMatrix[T2]): LagMatrix[T3]

  /**
    * Transform a matrix.
    *
    *  The primary purpose of this function is to transform a canonical
    *  adjacency matrix into an adjacency matrix suitable for a particular
    *  LAG algorithm. All non-sparse entries and (optionally) the diagonal
    *  are transformed using an input function. Sparse values in the input
    *  matrix are directly transformed into 'targetSparseValue' If
    *  visitDiagonalsOpt is not specified or specified as None then diagonal
    *  elements are only transformed if they are non-sparse.
    *
    *  @tparam T1 the element type of the input matrix
    *  @tparam T2 the type of the result
    *
    *  @param f binary operator that performs the transforms an element of
    *           the input matrix and its indices into an element at the same
    *           location in the output matrix
    *  @param m the input matrix
    *  @param targetSparseValue sparse elements in the input matrix are
    *                           transformed into elements with this value
    *  @param visitDiagonalsOpt if not 'None", diaganol elements are transformed
    *                           using 'visitDiagonalsOpt.get' as input to 'f'
    *
    *  @return a new matrix resulting from the transformation
    */
  private[lagraph] def mSparseZipWithIndex[T1: ClassTag, T2: ClassTag](
      f: (T1, (Long, Long)) => T2,
      m: LagMatrix[T1],
      targetSparseValue: T2,
      visitDiagonalsOpt: Option[T1] = None): LagMatrix[T2]

  private[lagraph] def mZipWithIndex[T1: ClassTag, T2: ClassTag](f: (T1, (Long, Long)) => T2,
                                                                 m: LagMatrix[T1]): LagMatrix[T2]

  /**
    * Multiplies a matrix times a vector.
    *
    *  @tparam T the element type of the input matrix and input vector
    *
    *  @param sr the semiring used in the multiplication
    *  @param m the input matrix
    *  @param u the input vector
    *
    *  @return a new vector resulting from the multiplication
    */
  private[lagraph] def mTv[T: ClassTag](sr: LagSemiring[T],
                                        m: LagMatrix[T],
                                        u: LagVector[T]): LagVector[T]
  // mTv addition.multiplication
  // experimental
  private[lagraph] def mTm[T: ClassTag](sr: LagSemiring[T],
                                        m: LagMatrix[T],
                                        n: LagMatrix[T]): LagMatrix[T]
  // Hadagard .multiplication
  /**
    * Hadagard multiplication
    *
    *  @tparam T the element type of the input matrices
    *
    *  @param sr the semiring used in the multiplication, not that only semiring multiplication
    *            is used in the Hadagard product
    *  @param m the first input matrix
    *  @param n the second input matrix
    *
    *  @return a new matrix resulting from the multiplication
    */
  private[lagraph] def mHm[T: ClassTag]( // TODO maybe combine w/ vZip
                                        sr: LagSemiring[T],
                                        //    f: (T, T) => T,  // TODO maybe sr
                                        //    OR maybe f: (T1, T2) => T3?
                                        m: LagMatrix[T],
                                        n: LagMatrix[T]): LagMatrix[T]

  /**
    * Outer product of a matrix column and a matrix row
    *
    *  @tparam T the element type of the input matrix
    *
    *  @param f the semiring used in the multiplication, not that only semiring multiplication
    *            is used in the outer product
    *  @param m the first input matrix
    *  @param mCol the column of 'm'
    *  @param n the second input matrix
    *  @param nRow the row of 'n'
    *
    *  @return a new matrix resulting from the outer product
    */
  private[lagraph] def mOp[T: ClassTag](f: LagSemiring[T],
                                        m: LagMatrix[T],
                                        mCol: Long,
                                        n: LagMatrix[T],
                                        nRow: Long): LagMatrix[T]

  // *******
  // matrix mechanics
  /**
    * Transpose a matrix.
    *
    *  @tparam T the element type of the input matrix
    *
    *  @param m the input matrix
    *
    *  @return a new matrix representing the transpose
    */
  private[lagraph] def mTranspose[T: ClassTag](m: LagMatrix[T]): LagMatrix[T]

  /**
    * Create a vector from a matrix row.
    *
    *  @tparam T the element type of the input matrix
    *
    *  @param m the input matrix
    *  @param mRow the row of m
    *
    *  @return a new vector representing mth row of m
    */
  private[lagraph] def vFromMrow[T: ClassTag](m: LagMatrix[T], mRow: Long): LagVector[T]

  /**
    * Create a vector from a matrix column.
    *
    *  @tparam T the element type of the input matrix
    *
    *  @param m the input matrix
    *  @param mCol the column of m
    *
    *  @return a new vector representing mth column of m
    */
  private[lagraph] def vFromMcol[T: ClassTag](m: LagMatrix[T], mCol: Long): LagVector[T]

  // *******
  // equivalence
  /**
    * The equality method for vectors
    *
    *  @tparam T the element type of the input vectors
    *
    *  @param l the first input vector
    *  @param r the second input vector
    *
    *  @return true if l is equivalent to r; false otherwise.
    */
  private[lagraph] def vEquiv[T: ClassTag](l: LagVector[T], r: LagVector[T]): Boolean
  // ********
  /**
    * Converts a matrix to a string.
    *
    *  @tparam T the element type of the matrix
    *
    *  @param m the matrix
    *
    *  @return a string representation of the matrix
    */
  private[lagraph] def mToString[T: ClassTag](m: LagMatrix[T], formatter: (T) => String): String = {
    val (ms, sv) = mToMap(m)
    LagContext.vectorOfVectorToString(LagContext.vectorOfVectorFromMap(ms, sv, m.size), formatter)
  }

  /**
    * Converts a vector to a string.
    *
    *  @tparam T the element type of the vector
    *
    *  @param v the vector
    *
    *  @return a string representation of the vector
    */
  private[lagraph] def vToString[T: ClassTag](v: LagVector[T], formatter: (T) => String): String = {
    val (vs, sv) = vToMap(v)
    LagContext.vectorToString(LagContext.vectorFromMap(vs, sv, v.size), formatter)
  }

  /**
    * Access an element in a vector
    *
    *  @tparam T the element type of the vector
    *
    *  @param u the vector
    *
    *  @param r the position in the vector
    *
    *  @return the rth element of the vector
    */
  private[lagraph] def vEle[T: ClassTag](u: LagVector[T], r: Long): (Option[T], Long) = {
    val f = (tl: (T, Long), tr: (Option[T], Long)) =>
      if (tr._2 == r) (tr._1, r)
      else if (tl._2 == r) (Option(tl._1), r)
      else (None, -1L)
    val c = (tl: (Option[T], Long), tr: (Option[T], Long)) => if (tl._1.isDefined) tl else tr
    vReduceWithIndex(f, c, (None, -1L), u)
  }

  /**
    * Create a new vector by modifying an element of an input vector
    *
    *  @tparam T the element type of the vector
    *
    *  @param u the vector
    *  @param r the position in the vector
    *  @param v the new value
    *
    *  @return a new vector representing the modified input vector
    */
  private[lagraph] def vSet[T: ClassTag](u: LagVector[T], r: Long, v: T) = {

    val set = (sv: T, sr: Long) => if (sr == r) v else sv
    vZipWithIndex(set, u)
  }

  /**
    * Find the index of a minimum value in a vector
    *
    *  @tparam T the element type of the vector
    *
    *  @param v the vector
    *
    *  @return the index of a minimum value in a vector
    */
  private[lagraph] def vArgmin[T: ClassTag](v: LagVector[T])(implicit n: Numeric[T]): (T, Long) = {
    val f = (tl: (T, Long), tr: (Option[T], Long)) => {
      if (tr._1.isEmpty) (Option(tl._1), tl._2)
      else if (n.lt(tl._1, tr._1.get)) (Option(tl._1), tl._2)
      else tr
    }
    val c = (tl: (Option[T], Long), tr: (Option[T], Long)) => {
      if (tr._1.isEmpty) tl
      else if (tl._1.isEmpty) tr
      else if (n.lt(tl._1.get, tr._1.get)) tl
      else tr
    }
    val res = vReduceWithIndex(f, c, (None, -1L), v)
    (res._1.get, res._2)
  }

}

/**
  * Companion object for LagContext.
  *  In addition to general utility functions, also
  *  contains functions that facilitate import and export of vectors and matrices.
  */
object LagContext {

  /**
    * Creates a lag context that runs in a single JVM
    *
    *  @param graphSize dimension of the graph
    */
  def getLagSmpContext(graphSize: Long): LagSmpContext =
    LagSmpContext(graphSize)

  /**
    * Creates a lag context that runs distributively in Spark
    *
    *  @param sc the SparkContext
    *  @param graphSize dimension of the graph
    *  @param nblock blocking factor for graph
    */
  def getLagDstrContext(sc: SparkContext, graphSize: Long, nblock: Int): LagDstrContext =
    LagDstrContext(sc, graphSize, nblock)

  /**
    * Converts a scala Vector to a string.
    *
    *  @tparam T the element type of the vector
    *
    *  @param a the vector
    *  @param tToStr a functor to convert an element of the vector to a string
    *
    *  @return a string representation of the vector
    */
  def vectorToString[T](a: Vector[T], tToStr: (T) => String): String = {
    val as = a.map { v =>
      tToStr(v)
    }
    as.mkString("{\n", ",", "\n}")
  }

  /**
    * Converts a scala Vector of Vectors representing a matrix to a string
    *
    *  @tparam T the element type of the Vector
    *
    *  @param a the Vector of Vectors, where the outer Vector corresponds to rows and
    *           the inner Vector corresponds to columns
    *  @param tToStr a functor to convert an element of the matrix to a string
    *
    *  @return a string representation of the matrix
    */
  def vectorOfVectorToString[T](a: Vector[Vector[T]], tToStr: (T) => String): String = {
    val as = a.map { r =>
      {
        r.map { v =>
          tToStr(v)
        }
      }
    }
    val str = for (l <- as) yield l.mkString("{", ",", "}")
    str.mkString("{\n", ",\n", "\n}")
  }

  /**
    * Converts a matrix represented by a scala map and sparse value into a matrix represented
    *  by a scala Vector of Vectors
    *
    *  @tparam T the element type of the matrix
    *
    *  @param m the scala map representation of the matrix
    *  @param sparseValue the sparse value of the map representation
    *
    *  @return a new scala Vector of Vectors (outer Vector corresponds to rows and
    *          the inner Vector corresponds to columns) representing the matrix
    */
  def vectorOfVectorFromMap[T](m: Map[(Long, Long), T],
                               sparseValue: T,
                               size: (Long, Long)): Vector[Vector[T]] = {
    val nrow = size._1
    val ncol = size._2
    require(nrow < Int.MaxValue, "LagContext type failure")
    require(ncol < Int.MaxValue, "LagContext type failure")
    val cl = Vector.tabulate(nrow.toInt)(r => ArrayBuffer.fill[T](ncol.toInt)(sparseValue))
    m.map { case (k, v) => cl(k._1.toInt)(k._2.toInt) = v }
    Vector.tabulate(nrow.toInt)(r => cl(r).toVector)
  }

  /**
    * Converts a vector represented by a scala map and sparse value into a vector represented
    *  by a scala Vector
    *
    *  @tparam T the element type of the matrix
    *
    *  @param m the scala map representation of the vector
    *  @param sparseValue the sparse value of the map representation
    *
    *  @return a new scala Vector representing the vector
    */
  def vectorFromMap[T](m: Map[Long, T], sparseValue: T, size: Long): Vector[T] = {
    require(size < Int.MaxValue, "LagContext type failure")
    val cl = ArrayBuffer.fill[T](size.toInt)(sparseValue)
    m.map { case (k, v) => cl(k.toInt) = v }
    cl.toVector
  }

  /**
    * Using a user-specified sparse value, converts a matrix represented by a scala Seq of Seq
    *  into a map representing the matrix.
    *
    *  @tparam T the element type of the matrix
    *
    *  @param vMatrix the scala Seq of Seq representation of the matrix
    *         (outer Seq corresponds to rows and
    *         the inner Seq corresponds to columns)
    *  @param sparseValue the sparse value to be used it the conversion.
    *
    *  @return a new scala map representing the matrix
    */
  def mapFromSeqOfSeq[T: ClassTag](vMatrix: Seq[Seq[T]], sparseValue: T): Map[(Long, Long), T] = {
    val mo = MMap[(Long, Long), T]()
    vMatrix.zipWithIndex.map {
      case (rv, r) =>
        rv.zipWithIndex.filter { case (v, c) => v != sparseValue }.map {
          case (v, c) => mo((r.toLong, c.toLong)) = v
        }
    }
    mo.toMap
  }
}
