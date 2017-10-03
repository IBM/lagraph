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

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
//import collection.immutable._
import scala.collection.mutable.{ Map => MMap, ArrayBuffer }
import com.ibm.lagraph.impl.{ GpiDstr, GpiBvec, GpiBmat, GpiAdaptiveVector, GpiOps, GpiDstrBmat, GpiDstrBvec, GpiSparseRowMatrix }
import com.ibm.lagraph.impl.{ LagDstrVector, LagDstrMatrix }

/**
 * The Distributed LAG context
 *
 *  The context is used for creating, importing, exporting, and manipulating
 *  matrices and vectors in a distributed (i.e., Spark) environment.
 *
 *  A LAG context defines the dimension of the problem, ie number of vertices.
 *
 *  The distributed LAG context also specifies the blocking for vectors and matrices.
 *
 *  A LAG context may be used for any number of matrices and vectors.
 *
 *  Matrices and vectors in one context cannot be mixed with matrices and vectors in a different context.
 */
final case class LagDstrContext(@transient sc: SparkContext, override val graphSize: Long, nblock: Int, DEBUG: Boolean = false)
    extends LagContext {
  require(graphSize > 0, "number of vertices: >%s< must be > 0".format(graphSize))
  val dstr = GpiDstr(sc, nblock, graphSize)
  // for constructing blocked vectors and matrices
  val coordRdd = sc.parallelize(List.range(0, dstr.clipN + 1), numSlices = dstr.clipN + 1)
  coordRdd.setName("lagDstrContextcoordRdd")

  /**
   * Create a matrix from an RDD.
   *
   *  @tparam T the type of the elements in the matrix
   *
   *  @param rcvRdd RDD[(row,col),value[T]] representation of a matrix.
   *  @param sparseValue sparse value for non-specified (row, col) pairs.
   *  @return a matrix created from the input map
   */
  def mFromRcvRdd[T: ClassTag](rcvRdd: RDD[((Long, Long), T)], sparseValue: T): LagMatrix[T] = {
    val (dstrRdd, nBlocksLoaded, times) = dstr.dstrBmatAdaptiveFromRcvRdd(sc, rcvRdd, sparseValue)
    LagDstrMatrix(this, rcvRdd, dstrRdd)
  }

  // *****
  // import / export
  // these are require specialized contexts may offer additional methods
  override def vFromMap[T: ClassTag](
    m: Map[Long, T],
    sparseValue: T): LagVector[T] = {
    LagDstrVector(this, dstr.dstrBvecFromMap(
      sc,
      m.map { case (r, v) => (r.toLong, v) },
      sparseValue))
  }
  override def vFromSeq[T: ClassTag]( // TODO not recommended for large scale
    v: Seq[T],
    sparseValue: T): LagVector[T] = {
    LagDstrVector(this, dstr.dstrBvecFromMap(
      sc,
      v.zipWithIndex.map { case (v, r) => (r.toLong, v) }.toMap, // TODO LONG ISSUE
      sparseValue))
  }
  private[lagraph] override def vToMap[T: ClassTag](
    v: LagVector[T]): (Map[Long, T], T) = v match {
    case va: LagDstrVector[T] =>
      //      (dstr.dstrBvecToRvRdd(va.dstrBvec).collect().map { case (k, v) => (k, v) }(collection.breakOut): Map[Long, T], // TODO LONG ISSUE
      (dstr.dstrBvecToRvRdd(va.dstrBvec).collect().toMap, va.dstrBvec.sparseValue)
    //      (dstr.dstrBvecToRvRdd(va.dstrBvec).collectAsMap.toMap.map { case (k, v) => (k.toInt, v) }, // TODO LONG ISSUE
    //      va.dstrBvec.sparseValue)
  }
  private[lagraph] override def vToVector[T: ClassTag](
    v: LagVector[T]): Vector[T] = v match {
    case va: LagDstrVector[T] => { // TODO drop?
      val cl = ArrayBuffer.fill[T](v.size.toInt)(va.dstrBvec.sparseValue)
      //      dstr.dstrBvecToRvRdd(va.dstrBvec).collectAsMap.toMap.map { case (k, v) => cl(k.toInt) = v }
      dstr.dstrBvecToRvRdd(va.dstrBvec).collect().map { case (k, v) => cl(k.toInt) = v } // TODO LONG ISSUE
      //      dstr.dstrBvecToRvRdd(va.dstrBvec).collectAsMap.toMap.map { case (k, v) => cl(k.toInt) = v }
      cl.toVector
    }
  }
  /**
   * Convert a distributed vector to an RDD.
   *
   * Only relevant to a LagDstrContext
   *
   *  @tparam T the type of the elements in the vector
   *
   *  @param v input vector.
   *  @param sparseValue a value to be considered sparse
   *  @return a Tuple2: (RDD[row[Long], value[T]] representation of the matrix, sparse value)
   */
  def vToRvRdd[T: ClassTag](
    v: LagVector[T]): (RDD[(Long, T)], T) = v match {
    case va: LagDstrVector[T] => { // TODO drop?
      val vecRdd = va.dstrBvec.vecRdd
      (dstr.dstrBvecToRvRdd(va.dstrBvec), va.dstrBvec.sparseValue)
    }
  }

  override def mFromMap[T: ClassTag](mMap: Map[(Long, Long), T], sparseValue: T): LagMatrix[T] = {
    //    val nrow = size._1
    //    val ncol = size._2
    val nrow = graphSize
    val ncol = graphSize
    val rcv = mMap.view.map { case (k, v) => ((k._1, k._2), v) }.toList
    val rcvRdd = sc.parallelize(rcv, dstr.partitions)
    val (dstrRdd, nBlocksLoaded, times) = dstr.dstrBmatAdaptiveFromRcvRdd(sc, rcvRdd, sparseValue)
    LagDstrMatrix(this, rcvRdd, dstrRdd)
  }

  private[lagraph] override def mToMap[T: ClassTag](m: LagMatrix[T]): (Map[(Long, Long), T], T) = m match {
    case ma: LagDstrMatrix[T] => {
      val stridel = dstr.stride.toLong
      val cl = Seq.newBuilder[((Long, Long), T)]
      ma.dstrBmat.matRdd.collect.map {
        case (k, v) => {
          //        cl.sizeHint(aa.size) TODO
          val ar = k._1
          val ac = k._2
          v.loadMapBuilder(cl, ar.toLong * stridel, ac.toLong * stridel)
          //          v match {
          //            case avga: GpiBmatAdaptive[T] => {
          //              val itr = avga.a.denseIterator
          //              while (itr.hasNext) {
          //                val (ir, rv) = itr.next()
          //                val itc = rv.denseIterator
          //                while (itc.hasNext) {
          //                  val (ic, v) = itc.next()
          //                  cl += Tuple2(((ar * dstr.stride + ir).toInt, (ac * dstr.stride + ic).toInt), v) // LONG ISSUE
          //                }
          //              }
          //            }
          //          }
        }
      }
      (cl.result.toMap, ma.dstrBmat.sparseValue)
    }
  }

  private[lagraph] def mToRcvRdd[T](
    m: LagMatrix[T]): (RDD[((Long, Long), T)], T) = m match {
    case ma: LagDstrMatrix[T] => {
      def f(input: ((Int, Int), GpiBmat[T])): List[((Long, Long), T)] = {
        val rblockl = input._1._1.toLong
        val cblockl = input._1._2.toLong
        val stridel = dstr.stride.toLong
        val bmata = input._2
        //        val bmata = bmat
        //        bmat match {
        //          case bmata: GpiBmatAdaptive[T] => {
        //            val stridel = dstr.stride.toLong
        bmata.a.denseIterator.toList.flatMap {
          case (lr, r) => {
            r.denseIterator.toList.map {
              case (lc, v) => {
                val gr = (rblockl * stridel) + lr.toLong
                val gc = (cblockl * stridel) + lc.toLong
                ((gr, gc), v)
              }
            }
          }
        }
        //          }
        //        }
      }
      (ma.dstrBmat.matRdd.flatMap(f), ma.dstrBmat.sparseValue)
    }
  }
  // ****************
  // ****************
  // ****************
  // ****************
  // ****************
  // factory
  override def vIndices(
    start: Long = 0L,
    sparseValueOpt: Option[Long] = None): LagVector[Long] = {
    //    val end = start + graphSize
    LagDstrVector(this, dstr.dstr_indices(sc, start, sparseValueOpt))
  }
  override def vReplicate[T: ClassTag](
    x: T,
    sparseValueOpt: Option[T] = None): LagVector[T] = {
    //    LagDstrVector(dstr.dstr_replicate(sc, graphSize, x))
    LagDstrVector(this, dstr.dstr_replicate(sc, x, sparseValueOpt))
  }
  // experimental
  override def mIndices(
    start: (Long, Long)): LagMatrix[(Long, Long)] = {
    require(start._1 > 0)
    val end = (start._1 + graphSize, start._2 + graphSize)
    mFromMap[(Long, Long)](
      Map((start._1 until end._1).map { r => (start._2 until end._2).map { c => ((r - start._1, c - start._1), (r, c)) } }.flatten: _*),
      (0, 0))
  }
  // experimental
  override def mReplicate[T: ClassTag](
    x: T): LagMatrix[T] = {
    val size = (graphSize, graphSize)
    mFromMap[T](
      Map((0L until size._1).map { r => (0L until size._2).map { c => ((r, c), x) } }.flatten: _*),
      x)
  }

  // *****
  // vector function
  private[lagraph] override def vReduce[T1: ClassTag, T2: ClassTag](
    f: (T1, T2) => T2,
    c: (T2, T2) => T2,
    z: T2,
    u: LagVector[T1]): T2 = u match {
    case ua: LagDstrVector[T1] =>
      dstr.dstr_reduce(f, c, z, ua.dstrBvec)
  }
  private[lagraph] override def vMap[T1: ClassTag, T2: ClassTag](
    f: (T1) => T2,
    u: LagVector[T1]): LagVector[T2] = u match {
    case ua: LagDstrVector[T1] =>
      LagDstrVector(this, dstr.dstr_map(f, ua.dstrBvec))
  }
  private[lagraph] override def vZip[T1: ClassTag, T2: ClassTag, T3: ClassTag](
    f: (T1, T2) => T3,
    u: LagVector[T1],
    v: LagVector[T2]): LagVector[T3] = (u, v) match {
    case (ua: LagDstrVector[T1], va: LagDstrVector[T2]) =>
      LagDstrVector(this, dstr.dstr_zip(f, ua.dstrBvec, va.dstrBvec))
  }
  private[lagraph] override def vZipWithIndex[T1: ClassTag, T2: ClassTag](
    f: (T1, Long) => T2,
    u: LagVector[T1],
    sparseValueOpt: Option[T2] = None): LagVector[T2] = u match {
    case ua: LagDstrVector[_] =>
      LagDstrVector(this, dstr.dstr_zip(f, ua.dstrBvec, dstr.dstr_indices(sc, 0L), sparseValueOpt)) // TODO use buildin
    //      LagDstrVector(dstr.dstr_zip(f, ua.dstrBvec, dstr.dstr_indices(sc, 0, u.size)))
  }
  private[lagraph] override def vReduceWithIndex[T1: ClassTag, T2: ClassTag]( // TODO needs to be optimized
    f: ((T1, Long), T2) => T2,
    c: (T2, T2) => T2,
    z: T2,
    u: LagVector[T1]): T2 = u match {
    case ua: LagDstrVector[_] => {
      def f2(a: T1, b: Long): (T1, Long) = { (a, b) }
      //      val xxx = dstr.dstr_indices(sc, 0L)
      //      xxx.vecRdd.collect().foreach { case (k, v) => println("INDS: (%s,%s): %s".format(k._1, k._2, v)) }
      val uinds = dstr.dstr_zip(f2, ua.dstrBvec, dstr.dstr_indices(sc, 0L))
      //      uinds.vecRdd.collect().foreach { case (k, v) => println("UINDS: (%s,%s): %s".format(k._1, k._2, v)) }
      dstr.dstr_reduce(f, c, z, uinds)
      //      LagDstrVector(this, dstr.dstr_zip(f, ua.dstrBvec, dstr.dstr_indices(sc, 0L))) // TODO use buildin
      //      LagDstrVector(dstr.dstr_zip(f, ua.dstrBvec, dstr.dstr_indices(sc, 0, u.size)))
    }
    //    case ua: LagSmpVector[T1] => {
    //      def f2(a:T1,b:Long):(T1, Long) = {(a,b)}
    //      val uinds = GpiOps.gpi_zip(f2, ua.v, GpiOps.gpi_indices(u.size, 0))
    //      GpiOps.gpi_reduce(f, z, uinds)
    //    }
  }

  // *****
  private[lagraph] override def mMap[T1: ClassTag, T2: ClassTag](
    f: (T1) => T2,
    m: LagMatrix[T1]): LagMatrix[T2] = m match {
    case ma: LagDstrMatrix[_] => {
      //      def perrow(v: GpiAdaptiveVector[T1]): GpiAdaptiveVector[T2] = {
      //        GpiOps.gpi_map(f, v)
      //      }
      //      LagDstrMatrix(dstr.dstr_map(perrow, ma.dstrBmat))
      throw new NotImplementedError("mMap: TBD")
    }
  }
  private[lagraph] override def mZip[T1: ClassTag, T2: ClassTag, T3: ClassTag](
    f: (T1, T2) => T3,
    m: LagMatrix[T1],
    n: LagMatrix[T2]): LagMatrix[T3] = (m, n) match {
    case (ma: LagDstrMatrix[T1], na: LagDstrMatrix[T2]) => {
      val sparseValue = f(ma.dstrBmat.sparseValue, na.dstrBmat.sparseValue)
      val dstrBmat = {
        def perblock(bmats: (GpiBmat[T1], GpiBmat[T2])): GpiBmat[T3] = {
          val (bmatma, bmatna) = bmats
          //          val maBmat = bmats._1
          //          val naBmat = bmats._2
          //          val (bmatma, bmatna) = (maBmat, naBmat)
          //          (maBmat, naBmat) match {
          //            case (bmatma: GpiBmatAdaptive[T1], bmatna: GpiBmatAdaptive[T2]) => {
          def perrow(mv: GpiAdaptiveVector[T1], nv: GpiAdaptiveVector[T2]): GpiAdaptiveVector[T3] = {
            GpiOps.gpi_zip(f, mv, nv)
          }
          val vovZip = GpiOps.gpi_zip(perrow, bmatma.a, bmatna.a)
          //              GpiBmatAdaptive(vovZip)
          GpiBmat(vovZip)
          //            }
          //          }
        }
        val matRdd = ma.dstrBmat.matRdd.join(na.dstrBmat.matRdd).mapValues { case (mb, nb) => perblock(mb, nb) }
        GpiDstrBmat(
          ma.dstrBmat.dstr,
          matRdd,
          ma.dstrBmat.nrow,
          ma.dstrBmat.ncol,
          sparseValue)
      }
      val rcvRdd = {
        //*****
        require(ma.dstrBmat.dstr.stride == na.dstrBmat.dstr.stride)
        val stridel = ma.dstrBmat.dstr.stride.toLong
        def perblock(input: ((Int, Int), GpiBmat[T3])): List[((Long, Long), T3)] = {
          val rblockl = input._1._1.toLong
          val cblockl = input._1._2.toLong
          val mata = input._2
          //          val mata = mato
          //          mato match {
          //            case mata: GpiBmatAdaptive[T3] => {
          if (mata.a.denseCount == 0) {
            List()
          } else {
            val xxx = mata.a.denseIterator.toList.map {
              case (ri: Int, r) => {
                r.denseIterator.toList.map {
                  case (ci: Int, v) => {
                    ((rblockl * stridel + ri.toLong, cblockl * stridel + ci.toLong), v)
                  }
                }
              }
            }
            xxx.reduceLeft(_ ::: _)
          }
          //            }
          //          }
        }
        dstrBmat.matRdd.flatMap(perblock)
      }
      LagDstrMatrix(m.hc, rcvRdd, dstrBmat)

    }
  }
  private[lagraph] override def mSparseZipWithIndex[T1: ClassTag, T2: ClassTag](
    f: (T1, (Long, Long)) => T2,
    m: LagMatrix[T1],
    targetSparseValue: T2,
    visitDiagonalsOpt: Option[T1] = None): LagMatrix[T2] = m match {
    case ma: LagDstrMatrix[T1] => {
      val rcvRdd = if (visitDiagonalsOpt.isDefined) {
        val diagRdd = sc.parallelize((0L until m.size._1), dstr.partitions).map { d => ((d, d), visitDiagonalsOpt.get) }
        ma.rcvRdd.flatMap { case (k, v) => if (k._1 != k._2) Some((k, v)) else None } ++ diagRdd
      } else ma.rcvRdd
      mFromRcvRdd(
        rcvRdd.mapPartitions({ iter: Iterator[((Long, Long), T1)] => for (i <- iter) yield (i._1, f(i._2, i._1)) }, preservesPartitioning = false),
        targetSparseValue)
    }
  }
  private[lagraph] override def mZipWithIndex[T1: ClassTag, T2: ClassTag](
    f: (T1, (Long, Long)) => T2,
    m: LagMatrix[T1]): LagMatrix[T2] = m match {
    case ma: LagDstrMatrix[T1] => {
      val msSparse = f(ma.dstrBmat.sparseValue, (0, 0))

      def perblock(index: Int, iter: Iterator[((Int, Int), GpiBmat[T1])]): Iterator[((Int, Int), GpiBmat[T2])] = {
        val (k, v) = iter.next()
        //        require (k == dstr.matrixPartitionIndexToKey(index))
        val (r, c) = k
        val rBase = if (r >= dstr.clipN) dstr.clipN.toLong * dstr.stride.toLong else r.toLong * dstr.stride.toLong
        val cBase = if (c >= dstr.clipN) dstr.clipN.toLong * dstr.stride.toLong else c.toLong * dstr.stride.toLong
        def perrow(mv: GpiAdaptiveVector[T1], rlocal: Long): GpiAdaptiveVector[T2] = {
          GpiOps.gpi_zip_with_index_matrix(f, mv, rlocal, cBase, sparseValueT3Opt = Option(msSparse))
        }
        val vSparse = GpiAdaptiveVector.fillWithSparse[T2](if (c == dstr.clipN) dstr.vecStride else dstr.vecClipStride)(msSparse)
        val vovZip = GpiOps.gpi_zip_with_index_vector(perrow, v.a, base = rBase, sparseValueT3Opt = Option(vSparse))
        List(((r, c), GpiBmat(vovZip))).toIterator
      }
      val matRdd = ma.dstrBmat.matRdd.mapPartitionsWithIndex(perblock, true)

      val dstrBmat = GpiDstrBmat(ma.dstrBmat.dstr, matRdd, ma.dstrBmat.ncol, ma.dstrBmat.nrow, msSparse)
      val rcvRdd = dstr.dstrBmatAdaptiveToRcvRdd(sc, dstrBmat)
      LagDstrMatrix(m.hc, rcvRdd, dstrBmat)
    }
  }
  //********
  private[lagraph] def mTv[T: ClassTag](
    sr: LagSemiring[T],
    lm: LagMatrix[T],
    lv: LagVector[T]): LagVector[T] = (lm, lv) match {
    case (ldm: LagDstrMatrix[T], ldu: LagDstrVector[T]) => {
      require(ldm.dstrBmat.sparseValue == ldu.dstrBvec.sparseValue,
        "mTv does not currently support disparate sparsities: matrix sparseValue: >%s<, vector sparseValue: >%s<".format(ldm.dstrBmat.sparseValue, ldu.dstrBvec.sparseValue))
      val vsSparse = ldu.dstrBvec.sparseValue
      val A = ldm.dstrBmat
      val u = ldu.dstrBvec
      //********
      // distribution functor
      def distributeVector(
        kv: ((Int, Int), GpiBvec[T])): List[((Int, Int), (Int, GpiBvec[T]))] = {
        val empty = List[((Int, Int), (Int, GpiBvec[T]))]()
        val (r, c) = kv._1
        val gxa = kv._2.asInstanceOf[GpiBvec[T]]
        def recurse(
          rdls: List[((Int, Int), (Int, GpiBvec[T]))],
          targetBlock: Int): List[((Int, Int), (Int, GpiBvec[T]))] = if (targetBlock == dstr.clipN + 1) rdls else {
          //****
          recurse(((targetBlock, r), (c, gxa)) :: rdls, targetBlock + 1)
        }
        recurse(empty, 0)
      }
      //      u.vecRdd.collect().foreach { case (k, v) => println("u.vecRdd: (%s,%s): %s".format(k._1, k._2, v.u.toVector)) }
      val srb = u.vecRdd.flatMap(distributeVector)
      //      srb.collect().foreach { case (k, v) => println("srb: (%s,%s): >%s< %s".format(k._1, k._2, v._1, v._2.u.toVector)) }
      //********
      // calculation functor
      def calculate(
        kv: ((Int, Int), (Iterable[GpiBmat[T]], Iterable[(Int, GpiBvec[T])]))) = {
        val r = kv._1._1
        val c = kv._1._2
        val stride = dstr.getVectorStride(r, c)
        val (vclipn, vstride, vclipstride) = dstr.getVectorStrides(c)
        //        val cPartial = kv._2._1.iterator.next()
        //        val cPartial = GpiAdaptiveVector.fillWithSparse[T](stride)(sr.zero)
        //        val cAa = kv._2._1.iterator.next().asInstanceOf[GpiBmatAdaptive[T]].a
        val cAa = kv._2._1.iterator.next().a
        val sortedU = Array.fill[GpiAdaptiveVector[T]](vclipn + 1)(null)
        //        println("Z for (%s, %s)".format(r, c))
        val it = kv._2._2.iterator
        while (it.hasNext) {
          val (k, v) = it.next()
          //          println("ZZZZ", k, v.u.toVector)
          sortedU(k) = v.u
        }
        //        println("ZEN")
        //        println(sortedU.foreach(println))
        val xxx = GpiAdaptiveVector.concatenateToDense(sortedU)
        //        println("SORTEDU: >%s<".format(xxx.toVector))
        val blockResult = GpiOps.gpi_m_times_v(sr.addition, sr.multiplication, sr.addition, sr.zero, cAa, xxx, Option(vsSparse), Option(vsSparse))

        ((r, c), blockResult)
      }
      //      A.matRdd.collect().foreach { case (k, v) => println("A.matRdd: (%s,%s): %s".format(k._1, k._2, GpiSparseRowMatrix.toString(v.asInstanceOf[GpiBmatAdaptive[T]].a))) }
      //      val xxx = A.matRdd.cogroup(srb)
      //      def debugXxx(
      //        kv: ((Int, Int), (Iterable[GpiBmat[T]], Iterable[(Int, GpiBvec[T])]))) = {
      //        println("XXXXX: %s".format(kv._1))
      //        val it1 = kv._2._1.iterator
      //        while (it1.hasNext) {
      //          val v = it1.next()
      //          println("  BMAT: %s".format(GpiSparseRowMatrix.toString(v.asInstanceOf[GpiBmatAdaptive[T]].a)))
      //        }
      //        val it2 = kv._2._2.iterator
      //        while (it2.hasNext) {
      //          val (k, v) = it2.next()
      //          println("  BVEC: >%s< %s".format(k, v.u.toVector))
      //        }
      //      }
      //      xxx.map(debugXxx).count()
      //      val partial1 = initBlockResult.cogroup(A.matRdd, srb).map(calculate)
      val partial1 = A.matRdd.cogroup(srb).map(calculate)
      //      partial1.collect().foreach { case (k, v) => println("partial1: (%s,%s): %s".format(k._1, k._2, v.toVector)) }
      // ****
      // functor to chop and send
      def chopResults(kv: ((Int, Int), GpiAdaptiveVector[T])): List[((Int, Int), GpiAdaptiveVector[T])] = {
        val (r, c) = kv._1
        val vd = GpiAdaptiveVector.toDense(kv._2)

        require(vd.isInstanceOf[com.ibm.lagraph.impl.GpiDenseVector[T]], "must be dense for this approach to work")
        val array = vd.asInstanceOf[com.ibm.lagraph.impl.GpiDenseVector[T]].iseq.elems

        //        val curVectorStride = dstr.getVectorStride(r, c)
        //        def chop(n: Int, pos: Int, choplist: List[((Int, Int), GpiAdaptiveVector[T])]): List[((Int, Int), GpiAdaptiveVector[T])] = if (n > dstr.clipN) choplist else {
        def chop(n: Int, pos: Int, choplist: List[((Int, Int), GpiAdaptiveVector[T])]): List[((Int, Int), GpiAdaptiveVector[T])] = if (pos >= array.size) choplist else {
          val curVectorStride = dstr.getVectorStride(r, n)
          val nextPos = pos + curVectorStride
          val newArray = array.slice(pos, nextPos)
          //          println("chop: (r,c): (%s,%s) -> (r,n): (%s,%s) pos: %s array.size: >%s< newArray.size: >%s<".format(r, c, r,n, pos, array.size, newArray.size))
          chop(n + 1, nextPos, ((r, n), GpiAdaptiveVector.wrapDenseArray(newArray, vsSparse)) :: choplist)
        }
        chop(0, 0, List[((Int, Int), GpiAdaptiveVector[T])]())
      }
      val choppedResults = partial1.flatMap(chopResults).partitionBy(new com.ibm.lagraph.impl.GpiBlockVectorPartitioner(dstr.clipN + 1, dstr.vecClipN + 1, dstr.remClipN + 1))
      //      choppedResults.collect().foreach { case (k, v) => println("choppedResults: (%s,%s): %s".format(k._1, k._2, v.toVector)) }
      // ****
      // functor to combine results
      def zipChoppedResults(partition: Int, iter: Iterator[((Int, Int), GpiAdaptiveVector[T])]): Iterator[((Int, Int), GpiAdaptiveVector[T])] = {
        def f(kv1: ((Int, Int), GpiAdaptiveVector[T]), kv2: ((Int, Int), GpiAdaptiveVector[T])) = {
          val result = GpiOps.gpi_zip(sr.addition, kv1._2, kv2._2, Option(sr.zero))
          (kv1._1, result)
        }
        //        println("XXXX: PART:",partition)
        //        val xxx = List(iter.reduce(f)) // .toIterator
        //        println("XXXX: PART: >%s<, LEN: >%s<".format(partition, xxx(0)._2.size))
        //        xxx.toIterator
        List(iter.reduce(f)).toIterator
      }
      val result = choppedResults.mapPartitionsWithIndex(zipChoppedResults, preservesPartitioning = true).map { case (k, v) => Tuple2(k, GpiBvec(v)) }
      //****
      LagDstrVector(
        this,
        GpiDstrBvec(dstr, result, vsSparse))
    }
  }
  private[lagraph] override def mTm[T: ClassTag](
    sr: LagSemiring[T],
    ma: LagMatrix[T],
    mb: LagMatrix[T]): LagMatrix[T] = (ma, mb) match {
    case (maa: LagDstrMatrix[T], mba: LagDstrMatrix[T]) => {
      require(maa.dstrBmat.sparseValue == mba.dstrBmat.sparseValue, "mTm does not currently support disparate sparsities")
      val msSparse = maa.dstrBmat.sparseValue
      val hcd = this
      val dstr = hcd.dstr
      val clipnp1 = dstr.clipN + 1
      // functor to initialize partial result
      def initPartial(rc: (Int, Int)) = {
        val (r, c) = rc
        val rChunk = if (r == dstr.clipN) dstr.clipStride else dstr.stride
        val cChunk = if (c == dstr.clipN) dstr.clipStride else dstr.stride
        val vSparse = GpiAdaptiveVector.fillWithSparse[T](cChunk)(sr.zero)
        ((r, c), GpiAdaptiveVector.fillWithSparse(rChunk)(vSparse))
      }
      val Partial = hcd.coordRdd.cartesian(hcd.coordRdd).map(initPartial)
      //      Partial.collect().foreach { case (k, v) => println("Partial: (%s,%s): %s".format(k._1, k._2, GpiSparseRowMatrix.toString(v))) }
      val A = maa.dstrBmat
      val BT = mba.transpose.asInstanceOf[LagDstrMatrix[T]].dstrBmat
      //      println(A)
      //      println(BT)
      // ********
      // top level
      def recurse(
        contributingIndex: Int,
        rPartial: RDD[((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])],
        rA: RDD[((Int, Int), GpiBmat[T])],
        rB: RDD[((Int, Int), GpiBmat[T])]): RDD[((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])] = if (contributingIndex == clipnp1) rPartial else {
        def selectA(
          kv: ((Int, Int), GpiBmat[T])): List[((Int, Int), GpiBmat[T])] = {
          val (r, c) = kv._1
          val gxa = kv._2.asInstanceOf[GpiBmat[T]]
          def selectIt(rd: Int,
                       dl: List[((Int, Int), GpiBmat[T])]): List[((Int, Int), GpiBmat[T])] =
            if (rd == clipnp1) dl else selectIt(rd + 1, ((rd, r), gxa) :: dl)
          val empty = List[((Int, Int), GpiBmat[T])]()
          if (c == contributingIndex) selectIt(0, empty) else empty
        }
        def selectB(
          kv: ((Int, Int), GpiBmat[T])): List[((Int, Int), GpiBmat[T])] = {
          val (c, r) = kv._1
          val gxa = kv._2.asInstanceOf[GpiBmat[T]]
          val empty = List[((Int, Int), GpiBmat[T])]()
          def selectIt(rd: Int,
                       dl: List[((Int, Int), GpiBmat[T])]): List[((Int, Int), GpiBmat[T])] =
            if (rd == clipnp1) dl else selectIt(rd + 1, ((c, rd), gxa) :: dl)
          if (r == contributingIndex) selectIt(0, empty) else empty
        }
        //********
        // permutations

        //        def dbgprint(a:RDD[((Int, Int), GpiBmatAdaptive[T])]):Unit = {
        //          val ca = a.collect()
        //          ca.foreach{case (k, v) => {println(k);println(GpiSparseRowMatrix.toString(v.a))}}
        //        }
        //        println("****")
        //        println("contributingIndex = %s".format(contributingIndex))
        //        println("sra")
        val sra = rA.flatMap(selectA)
        //        dbgprint(sra)
        val srb = rB.flatMap(selectB)
        //        println("srb")
        //        dbgprint(srb)

        //********
        // calculation functor
        def calculate(
          kv: ((Int, Int), (Iterable[GpiAdaptiveVector[GpiAdaptiveVector[T]]], Iterable[GpiBmat[T]], Iterable[GpiBmat[T]]))) = {
          val r = kv._1._1
          val c = kv._1._2
          val rChunk = if (r == dstr.clipN) dstr.clipStride else dstr.stride
          val cChunk = if (c == dstr.clipN) dstr.clipStride else dstr.stride
          val cPartial = kv._2._1.iterator.next()
          val cAa = kv._2._2.iterator.next().a
          val cBa = kv._2._3.iterator.next().a

          val iPartial = GpiOps.gpi_m_times_m(sr.addition, sr.multiplication, sr.addition, sr.zero, cAa, cBa, Option(msSparse), Option(msSparse))
          //          println("CALC1: (r,c): (%s,%s), (rChunk,cChunk): (%s,%s), ipartial: >%s<, cpartial: >%s<".format(r,c,rChunk, cChunk,
          //              GpiSparseRowMatrix.toString(iPartial),GpiSparseRowMatrix.toString(cPartial)))
          //          println("CALC3:", iPartial.size,iPartial(0).size,cPartial.size,cPartial(0).size)
          //          println("WHAT3a!",iPartial)
          //          println("WHAT3b!",cPartial)
          val updatedPartial = GpiOps.gpi_zip(GpiOps.gpi_zip(sr.addition, _: GpiAdaptiveVector[T], _: GpiAdaptiveVector[T]), iPartial, cPartial)
          //          println("CALC2: (r,c): (%s,%s), updatedPartial: >%s<".format(r,c,GpiSparseRowMatrix.toString(updatedPartial)))
          ((r, c), updatedPartial)
        }
        val nextPartial = rPartial.cogroup(sra, srb).map(calculate)
        // ********
        // recurse
        //        println("nextPartial")
        //        def dbgprint2(a:RDD[((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])]):Unit = {
        //          val ca = a.collect()
        //          ca.foreach{case (k, v) => {println(k);println(GpiSparseRowMatrix.toString(v))}}
        //        }
        //        dbgprint2(nextPartial)
        recurse(contributingIndex + 1, nextPartial, rA, rB)
      }
      val transposedResult = recurse(0, Partial, A.matRdd, BT.matRdd)

      // ********
      def toRcv(kv: ((Int, Int), GpiAdaptiveVector[GpiAdaptiveVector[T]])) = {
        val arOff = kv._1._1.toLong * dstr.stride.toLong
        val acOff = kv._1._2.toLong * dstr.stride.toLong
        val ac = kv._1._2
        var res = List[((Long, Long), T)]()
        val itr = kv._2.denseIterator
        while (itr.hasNext) {
          val (ir, rv) = itr.next()
          val itc = rv.denseIterator
          while (itc.hasNext) {
            val (ic, v) = itc.next()
            res = ((arOff + ir.toLong, acOff + ic.toLong), v) :: res
          }
        }
        res
      }
      val rcvRdd = transposedResult.flatMap(toRcv)
      hcd.mFromRcvRdd(rcvRdd.map { case (k, v) => ((k._2, k._1), v) }, msSparse)

      //      // for transpose ...
      //      val bmatRdd = transposedResult.map { case (k, v) => (k, GpiBmatAdaptive(v).asInstanceOf[GpiBmat[T]]) }
      //      LagDstrMatrix(hcd, rcvRdd, GpiDstrBmat(dstr, bmatRdd, maa.dstrBmat.nrow, maa.dstrBmat.ncol, maa.dstrBmat.sparseValue))
    }
  }
  //  override def mTm[T: ClassTag](
  //    sr: LagSemiring[T],
  //    m: LagMatrix[T],
  //    n: LagMatrix[T]): LagMatrix[T] = (m, n) match {
  //    case (ma: LagDstrMatrix[T], na: LagDstrMatrix[T]) => {
  //      //      LagDstrMatrix(dstr.dstr_m_times_m(
  //      //        sr.addition,
  //      //        sr.addition,
  //      //        sr.multiplication,
  //      //        sr.zero,
  //      //        ma.dstrBmat,
  //      //        na.dstrBmat))
  //      throw new NotImplementedError("mTm: TBD")
  //    }
  //    //  def dstr_m_times_m[T1: ClassTag, T2: ClassTag, T3: ClassTag, T4: ClassTag](
  //    //    f: (T3, T4) => T4,
  //    //    c: (T4, T4) => T4,
  //    //    g: (T2, T1) => T3,
  //    //    zero: T4,
  //    //    a: GpiDstrBvec[GpiDstrBvec[T1]],
  //    //    u: GpiDstrBvec[T2],
  //    //    sparseValueT3Opt: Option[T3] = None,
  //    //    sparseValueT4Opt: Option[T4] = None): GpiDstrBvec[T4] = {
  //    //    dstr_map(dstr_innerp(f, c, g, zero, u, _: GpiDstrBvec[T1], sparseValueT3Opt), a, sparseValueT4Opt)
  //    //  }
  //  }
  // Hadagard ".multiplication"
  private[lagraph] override def mHm[T: ClassTag](
    //    f: (T, T) => T,  // TODO maybe sr OR maybe f: (T1, T2) => T3?
    sr: LagSemiring[T],
    m: LagMatrix[T],
    n: LagMatrix[T]): LagMatrix[T] = {
    mZip(sr.multiplication, m, n)
    //    mZip(f, m, n)
  }
  // OuterProduct
  private[lagraph] def mOp[T: ClassTag](
    f: LagSemiring[T],
    m: LagMatrix[T],
    mCol: Long,
    n: LagMatrix[T],
    nRow: Long): LagMatrix[T] = (m, n) match {
    case (ma: LagDstrMatrix[T], na: LagDstrMatrix[T]) => {
      assert(ma.dstrBmat.sparseValue == na.dstrBmat.sparseValue)
      assert(f.multiplication.annihilator.isDefined)
      assert(f.multiplication.annihilator.get == ma.dstrBmat.sparseValue)
      val sparseValue = ma.dstrBmat.sparseValue
      val mac = ma.rcvRdd.filter { case (k, v) => k._2 == mCol }.map { case (k, v) => Tuple2(k._1, v) }
      val nar = na.rcvRdd.filter { case (k, v) => k._1 == nRow }.map { case (k, v) => Tuple2(k._2, v) }
      val cp = mac.cartesian(nar)
      val rcvRdd = cp.map { case (p1, p2) => Tuple2(Tuple2(p1._1, p2._1), f.multiplication.op(p1._2, p2._2)) }
      mFromRcvRdd(rcvRdd, sparseValue)
    }
  }

  // *******
  // matrix mechanics
  private[lagraph] override def mTranspose[T: ClassTag](
    m: LagMatrix[T]): LagMatrix[T] = m match {
    case ma: LagDstrMatrix[T] => {
      def perblock(iter: Iterator[((Int, Int), GpiBmat[T])]): Iterator[((Int, Int), GpiBmat[T])] = {
        require(iter.hasNext)
        val rcb = iter.next()
        require(!iter.hasNext)
        val bmata = rcb._2 // bmat for this block
        //        val bmata = bmat
        //        bmat match {
        //          case bmata: GpiBmatAdaptive[T] => {
        val at = GpiSparseRowMatrix.transpose(bmata.a)
        if (rcb._1._1 == rcb._1._2)
          List(Tuple2(Tuple2(rcb._1._1, rcb._1._2), GpiBmat(at))).toIterator
        else
          List(Tuple2(Tuple2(rcb._1._2, rcb._1._1), GpiBmat(at))).toIterator
        //          }
        //        }
      }
      val newMmap = ma.rcvRdd.map { case (k, v) => ((k._2, k._1), v) }
      val matRdd = ma.dstrBmat.matRdd.mapPartitions(perblock, preservesPartitioning = false)
      LagDstrMatrix(this, newMmap, GpiDstrBmat(ma.dstrBmat.dstr, matRdd, ma.dstrBmat.ncol, ma.dstrBmat.nrow, ma.dstrBmat.sparseValue))
    }
  }
  private[lagraph] def vFromMrow[T: ClassTag](
    m: LagMatrix[T],
    mRow: Long): LagVector[T] = m match {
    case ma: LagDstrMatrix[T] =>
      LagDstrVector(
        this,
        dstr.dstrBvecFromRowOfRcvRdd(sc, ma.rcvRdd, mRow, ma.dstrBmat.sparseValue))
  }
  private[lagraph] def vFromMcol[T: ClassTag](
    m: LagMatrix[T],
    mCol: Long): LagVector[T] = m match {
    case ma: LagDstrMatrix[T] =>
      LagDstrVector(
        this,
        dstr.dstrBvecFromColOfRcvRdd(sc, ma.rcvRdd, mCol, ma.dstrBmat.sparseValue))
  }
  // *******
  // equivalence
  private[lagraph] override def vEquiv[T: ClassTag](
    u: LagVector[T], v: LagVector[T]): Boolean = (u, v) match {
    case (ua: LagDstrVector[T], va: LagDstrVector[T]) => {
      dstr.dstr_equiv(ua.dstrBvec, va.dstrBvec)
    }
  }
}
