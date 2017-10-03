---
layout: global
title: Spark LagContext Programming Guide
description: Spark LagContext Programming Guide
---
<!--
{% comment %}
License ...
{% endcomment %}
-->

* This will become a table of contents (this text will be scraped).
{:toc}

<br/>

# Spark Shell Example

This section uses the [Bellman-Ford](algorithms-bellmanford.md)
description to demonstrate the implementation and application of an
linear algebra-based graph algorithm.

## Start Spark Shell with LAGraph

The [Spark Overview](http://spark.apache.org/docs/latest/) provides instructions for accessing Spark.
To use LAGraph with Spark Shell, the LAGraph jar can be referenced using Spark Shell's `--jars` option.
For example:

{% highlight bash %}
spark-shell --executor-memory 4G --driver-memory 4G --jars LAGraph.jar --master local[2]
{% endhighlight %}


## Setup environment

First, import classes from the LAGraph package and define a couple of functions to facilitate
printing results.

<div class="codetabs">

<div data-lang="Scala" markdown="1">
{% highlight scala %}
// some imports ...
import com.ibm.lagraph.{ LagContext, LagSemigroup, LagSemiring, LagVector }

// for verbose printing
import scala.reflect.classTag
def float2Str(f: Float): String = {
  if (f == LagSemigroup.infinity(classTag[Float])) "   inf"
  else if (f == LagSemigroup.minfinity(classTag[Float])) "   minf"
  else "%6.3f".format(f)
}
def long2Str(l: Long): String = {
  if (l == LagSemigroup.infinity(classTag[Long])) " inf"
  else if (l == LagSemigroup.minfinity(classTag[Long])) " minf"
  else "%4d".format(l)
}
def bfType2Str(d: (Float, Long, Long)): String = {
  val d1 = float2Str(d._1)
  val d2 = long2Str(d._2)
  val d3 = long2Str(d._3)
  "(%s,%s,%s)".format(d1, d2, d3)
}
{% endhighlight %}
</div>

<div data-lang="Spark Shell" markdown="1">
{% highlight scala %}
scala> // some imports ...
import com.ibm.lagraph.{ LagContext, LagSemigroup, LagSemiring, LagVector }

// for verbose printing
import scala.reflect.classTag
def float2Str(f: Float): String = {
  if (f == LagSemigroup.infinity(classTag[Float])) "   inf"
  else if (f == LagSemigroup.minfinity(classTag[Float])) "   minf"
  else "%6.3f".format(f)
}
def long2Str(l: Long): String = {
  if (l == LagSemigroup.infinity(classTag[Long])) " inf"
  else if (l == LagSemigroup.minfinity(classTag[Long])) " minf"
  else "%4d".format(l)
}
def bfType2Str(d: (Float, Long, Long)): String = {
  val d1 = float2Str(d._1)
  val d2 = long2Str(d._2)
  val d3 = long2Str(d._3)
  "(%s,%s,%s)".format(d1, d2, d3)
}
import com.ibm.lagraph.{LagContext, LagSemigroup, LagSemiring, LagVector}
import scala.reflect.classTag
float2Str: (f: Float)String
long2Str: (l: Long)String
bfType2Str: (d: (Float, Long, Long))String
{% endhighlight %}
</div>

</div>


## Create Graph from RDD

Create a simple graph in the form of an RDD where each edge is
represented as an element with an index of (src, destination) and a
value of $1.0$. Then set the parallelism and use the `SparkContext` to
obtain a
[`DstrLagContext`](./api/index.html#com.ibm.lagraph.LagDstrContext).

<div class="codetabs">

<div data-lang="Scala" markdown="1">
{% highlight scala %}
// define graph
val numv = 5L
val houseEdges = List(((1L, 0L), 20.0F), ((2L, 0L), 10.0F), ((3L, 1L), 15.0F), ((3L, 2L), 30.0F), ((4L, 2L), 50.0F), ((4L, 3L), 5.0F))
val rcvGraph = sc.parallelize(houseEdges)

// obtain a distributed context for Spark environment
val nblock = 1 // set parallelism (blocks on one axis)
val hc = LagContext.getLagDstrContext(sc, numv, nblock)

println("Input graph: >\n%s<".format(hc.mFromRcvRdd(rcvGraph, 0.0F).toString(float2Str)))
{% endhighlight %}
</div>

<div data-lang="Spark Shell" markdown="1">
{% highlight scala %}
scala> // define graph
val numv = 5L
val houseEdges = List(((1L, 0L), 20.0F), ((2L, 0L), 10.0F), ((3L, 1L), 15.0F), ((3L, 2L), 30.0F), ((4L, 2L), 50.0F), ((4L, 3L), 5.0F))
val rcvGraph = sc.parallelize(houseEdges)

// obtain a distributed context for Spark environment
val nblock = 1 // set parallelism (blocks on one axis)
val hc = LagContext.getLagDstrContext(sc, numv, nblock)

println("Input graph: >\n%s<".format(hc.mFromRcvRdd(rcvGraph, 0.0F).toString(float2Str)))
scala > numv: Long = 5
houseEdges: List[((Long, Long), Float)] = List(((1,0),20.0), ((2,0),10.0), ((3,1),15.0), ((3,2),30.0), ((4,2),50.0), ((4,3),5.0))
rcvGraph: org.apache.spark.rdd.RDD[((Long, Long), Float)] = ParallelCollectionRDD[77] at parallelize at <console>:43
nblock: Int = 1
hc: com.ibm.lagraph.LagDstrContext = LagDstrContext(org.apache.spark.SparkContext@3b4a3d17,5,1,false)
Input graph: >
{
{ 0.000, 0.000, 0.000, 0.000, 0.000},
{20.000, 0.000, 0.000, 0.000, 0.000},
{10.000, 0.000, 0.000, 0.000, 0.000},
{ 0.000,15.000,30.000, 0.000, 0.000},
{ 0.000, 0.000,50.000, 5.000, 0.000}
}<
{% endhighlight %}
</div>

</div>


## Initialize distance-only semiring

Setup a semiring to perform the distance-only version of the semiring.

<div class="codetabs">

<div data-lang="Scala" markdown="1">
{% highlight scala %}
// Initialize distance-only Bellman Ford
type PathType = Float
val PathMinPlusSr = LagSemiring.min_plus[PathType]

// for edge initialization (weight mapping)
val eInit = (v: Float, rc: Tuple2[Long, Long]) => v.toFloat // edge (weight)
// for print
val d2Str = float2Str(_)
{% endhighlight %}
</div>

<div data-lang="Spark Shell" markdown="1">
{% highlight scala %}
scala> // Initialize distance-only Bellman Ford
type PathType = Float
val PathMinPlusSr = LagSemiring.min_plus[PathType]

// for edge initialization (weight mapping)
val eInit = (v: Float, rc: Tuple2[Long, Long]) => v.toFloat // edge (weight)
// for print
val d2Str = float2Str(_)

scala> defined type alias PathType
PathMinPlusSr: com.ibm.lagraph.LagSemiring[PathType] = com.ibm.lagraph.LagSemiring$$anon$1@187e8163
eInit: (((Long, Long), Float)) => ((Long, Long), Float) = <function1>
d2Str: Float => String = <function1>
{% endhighlight %}
</div>

</div>

## Initialize adjacency matrix for distance-only (common)

Setup the algorithm using code that is common to both the distance-only and path-augmented semirings.

<div class="codetabs">

<div data-lang="Scala" markdown="1">
{% highlight scala %}
// initialize adjacency matrix
// strip diagonal and add back in as PathMinPlusSr.one, use weight to derive PathType for edge
val diagStripped = rcvGraph.flatMap { kv => if (kv._1._1 == kv._1._2) None else Some(eInit(kv)) }
val rcvAdj = diagStripped.union(sc.range(0L, numv, 1L, rcvGraph.getNumPartitions).map { i => ((i, i), PathMinPlusSr.one) })

// use distributed context-specific utility to convert from RDD to LagMatrix
val mAdj = hc.mFromRcvRdd(rcvAdj, PathMinPlusSr.zero)

println("mAdj: >\n%s<".format(mAdj.toString(d2Str)))

// initialize vector d w/ source (input)
val source = 0L
def dInit(di: PathType, ui: Long): PathType = if (ui == source) PathMinPlusSr.one else di
val d_prev = hc.vReplicate(PathMinPlusSr.zero).zipWithIndex(dInit, Option(PathMinPlusSr.zero))
println("d_initial: >\n%s<".format(d_prev.toString(d2Str)))
{% endhighlight %}
</div>

<div data-lang="Spark Shell" markdown="1">
{% highlight scala %}
scala> // initialize adjacency matrix
// strip diagonal and add back in as PathMinPlusSr.one, use weight to derive PathType for edge
val diagStripped = rcvGraph.flatMap { kv => if (kv._1._1 == kv._1._2) None else Some((eInit(kv))) }
val rcvAdj = diagStripped.union(sc.range(0L, numv, 1L, rcvGraph.getNumPartitions).map{i => ((i, i), PathMinPlusSr.one)})

// use distributed context-specific utility to convert from RDD to LagMatrix
val mAdj = hc.mFromRcvRdd(rcvAdj, PathMinPlusSr.zero)

println("mAdj: >\n%s<".format(hc.mToString(mAdj, d2Str)))

// initialize vector d w/ source (input)
val source = 0L
def dInit(di: PathType, ui: Long): PathType = if (ui == source) PathMinPlusSr.one else di
val d_prev = hc.vZipWithIndex(dInit, hc.vReplicate(PathMinPlusSr.zero), Option(PathMinPlusSr.zero))
println("d_initial: >\n%s<".format(hc.vToString(d_prev, d2Str)))

scala> diagStripped: org.apache.spark.rdd.RDD[((Long, Long), Float)] = MapPartitionsRDD[5] at flatMap at <console>:37
rcvAdj: org.apache.spark.rdd.RDD[((Long, Long), Float)] = UnionRDD[9] at union at <console>:43
mAdj: com.ibm.lagraph.LagMatrix[PathType] =
LagDstrMatrix(LagDstrContext(org.apache.spark.SparkContext@617a034a,5,1,false),UnionRDD[9] at union at <console>:43,(0.0, 3.4028235E38, 3.4028235E38, 3.4028235E38, 3.4028235E38)
(20.0, 0.0, 3.4028235E38, 3.4028235E38, 3.4028235E38)
(10.0, 3.4028235E38, 0.0, 3.4028235E38, 3.4028235E38)
(3.4028235E38, 15.0, 30.0, 0.0, 3.4028235E38)
(3.4028235E38, 3.4028235E38, 50.0, 5.0, 0.0))
mAdj: >
{
{ 0.000,   inf,   inf,   inf,   inf},
{20.000, 0.000,   inf,   inf,   inf},
{10.000,   inf, 0.000,   inf,   inf},
{   inf,15.000,30.000, 0.000,   inf},
{   inf,   inf,50.000, 5.000, 0.000}
}<
source: Long = 0
dInit: (di: PathType, ui: Long)PathType
d_prev: com.ibm.lagraph.LagVector[PathType] = LagDstrVector(LagDstrContext(org.apache.spark.SparkContext@617a034a,5,1,false),(0.0, 3.4028235E38, 3.4028235E38, 3.4028235E38, 3.4028235E38))
d_initial: >
{
 0.000,   inf,   inf,   inf,   inf
}<
{% endhighlight %}
</div>

</div>

## Iterate for distance-only (common)

Perform the calculation using code that is common to both the distance-only and path-augmented semirings.

<div class="codetabs">

<div data-lang="Scala" markdown="1">
{% highlight scala %}
// iterate, relaxing edges
val maxiter = d_prev.size
def iterate(k: Long, d_prev: LagVector[PathType]): LagVector[PathType] =
  if (k == maxiter) d_prev else iterate(k + 1, mAdj.tV(PathMinPlusSr, d_prev))

val d_final = iterate(0L, d_prev)

// are we at a fixed point?
if (d_final.equiv(mAdj.tV(PathMinPlusSr,d_final))) {
  println("d_final: >\n%s<".format(d_final.toString(d2Str)))
} else {
  println("A negative-weight cycle exists.")
}
{% endhighlight %}
</div>

<div data-lang="Spark Shell" markdown="1">
{% highlight scala %}
scala> // loop, relaxing edges
val maxiter = d_prev.size
def iterate(k: Long, d_prev: LagVector[PathType]): LagVector[PathType] =
  if (k == maxiter) d_prev else iterate(k + 1, hc.mTv(PathMinPlusSr, mAdj, d_prev))

val d_final = iterate(0L, d_prev)

// are we at a fixed point?
if (hc.vEquiv(d_final, hc.mTv(PathMinPlusSr, mAdj, d_final))) {
  println("d_final: >\n%s<".format(hc.vToString(d_final, d2Str)))
} else {
  println("A negative-weight cycle exists.")
}

scala> maxiter: Long = 5
iterate: (k: Long, d_prev: com.ibm.lagraph.LagVector[PathType])com.ibm.lagraph.LagVector[PathType]
d_final: com.ibm.lagraph.LagVector[PathType] = LagDstrVector(LagDstrContext(org.apache.spark.SparkContext@617a034a,5,1,false),(0.0, 20.0, 10.0, 35.0, 40.0))
d_final: >
{
 0.000,20.000,10.000,35.000,40.000
}<
{% endhighlight %}
</div>

</div>


## Initialize path-augmented semiring

Setup a semiring to perform the path-augmented version of the semiring.

<div class="codetabs">

<div data-lang="Scala" markdown="1">
{% highlight scala %}
// Initialize path-augmented Bellman Ford

type PathType = Tuple3[Float, Long, Long]
val FloatInf = LagSemigroup.infinity(classTag[Float])
val LongInf = LagSemigroup.infinity(classTag[Long])
val nodeNil: Long = -1L

// Ordering for PathType
trait PathTypeOrdering extends Ordering[PathType] {
  def compare(ui: PathType, vi: PathType): Int = {
    val w1 = ui._1; val h1 = ui._2; val p1 = ui._3
    val w2 = vi._1; val h2 = vi._2; val p2 = vi._3
    if (w1 < w2) -1
    else if ((w1 == w2) && (h1 < h2)) -1
    else if ((w1 == w2) && (h1 == h2) && (p1 < p2)) -1
    else 1
  }
}

// Numeric for PathType
trait PathTypeAsNumeric extends com.ibm.lagraph.LagSemiringAsNumeric[PathType] with PathTypeOrdering {
  def plus(ui: PathType, vi: PathType): PathType = {
    def f(x: Float, y: Float): Float =
      if (x == FloatInf || y == FloatInf) FloatInf
      else x + y
    def g(x: Long, y: Long): Long =
      if (x == LongInf || y == LongInf) LongInf
      else x + y

    val _zero = fromInt(0)
    val w1 = ui._1; val h1 = ui._2; val p1 = ui._3
    val w2 = vi._1; val h2 = vi._2; val p2 = vi._3
    if (p2 != _zero._3)
      if (p1 == _zero._3) (f(w1, w2), g(h1, h2), p2)
      else if (p2 != nodeNil) (f(w1, w2), g(h1, h2), p2)
      else (f(w1, w2), g(h1, h2), p1) // original
    else (f(w1, w2), g(h1, h2), p1)
  }
  def times(x: PathType, y: PathType): PathType = min(x, y)
  def fromInt(x: Int): PathType = x match {
    case 0     => ((0.0).toFloat, 0L, nodeNil)
    case 1     => (FloatInf, LongInf, LongInf)
    case other => throw new RuntimeException("fromInt for: >%d< not implemented".format(other))
  }
}

implicit object PathTypeAsNumeric extends PathTypeAsNumeric
val PathMinPlusSr = LagSemiring.min_plus[PathType](Tuple3(FloatInf, LongInf, LongInf))

// for edge initialization (weight mapping)
val eInit = (kv: ((Long, Long), Float)) => (kv._1, (kv._2.toFloat, 1L, kv._1._2)) // edge (weight)

// for print
val d2Str = bfType2Str(_)
{% endhighlight %}
</div>

<div data-lang="Spark Shell" markdown="1">
{% highlight scala %}
scala> :paste
// Initialize path-augmented Bellman Ford

type PathType = Tuple3[Float, Long, Long]
val FloatInf = LagSemigroup.infinity(classTag[Float])
val LongInf = LagSemigroup.infinity(classTag[Long])
val nodeNil: Long = -1L

// Ordering for PathType
trait PathTypeOrdering extends Ordering[PathType] {
  def compare(ui: PathType, vi: PathType): Int = {
    val w1 = ui._1; val h1 = ui._2; val p1 = ui._3
    val w2 = vi._1; val h2 = vi._2; val p2 = vi._3
    if (w1 < w2) -1
    else if ((w1 == w2) && (h1 < h2)) -1
    else if ((w1 == w2) && (h1 == h2) && (p1 < p2)) -1
    else 1
  }
}

// Numeric for PathType
trait PathTypeAsNumeric extends com.ibm.lagraph.LagSemiringAsNumeric[PathType] with PathTypeOrdering {
  def plus(ui: PathType, vi: PathType): PathType = {
    def f(x: Float, y: Float): Float =
      if (x == FloatInf || y == FloatInf) FloatInf
      else x + y
    def g(x: Long, y: Long): Long =
      if (x == LongInf || y == LongInf) LongInf
      else x + y

    val _zero = fromInt(0)
    val w1 = ui._1; val h1 = ui._2; val p1 = ui._3
    val w2 = vi._1; val h2 = vi._2; val p2 = vi._3
    if (p2 != _zero._3)
      if (p1 == _zero._3) (f(w1, w2), g(h1, h2), p2)
      else if (p2 != nodeNil) (f(w1, w2), g(h1, h2), p2)
      else (f(w1, w2), g(h1, h2), p1) // original
    else (f(w1, w2), g(h1, h2), p1)
  }
  def times(x: PathType, y: PathType): PathType = min(x, y)
  def fromInt(x: Int): PathType = x match {
    case 0     => ((0.0).toFloat, 0L, nodeNil)
    case 1     => (FloatInf, LongInf, LongInf)
    case other => throw new RuntimeException("fromInt for: >%d< not implemented".format(other))
  }
}

implicit object PathTypeAsNumeric extends PathTypeAsNumeric
val PathMinPlusSr = LagSemiring.min_plus[PathType](Tuple3(FloatInf, LongInf, LongInf))

// for edge initialization (weight mapping)
val eInit = (kv: ((Long, Long), Float)) => (kv._1, (kv._2.toFloat, 1L, kv._1._2)) // edge (weight)

// for print
val d2Str = bfType2Str(_)
// Exiting paste mode, now interpreting.
scala> defined type alias PathType
FloatInf: Float = 3.4028235E38
LongInf: Long = 9223372036854775807
nodeNil: Long = -1
defined trait PathTypeOrdering
defined trait PathTypeAsNumeric
defined object PathTypeAsNumeric
warning: previously defined trait PathTypeAsNumeric is not a companion to object PathTypeAsNumeric.
Companions must be defined together; you may wish to use :paste mode for this.
PathMinPlusSr: com.ibm.lagraph.LagSemiring[PathType] = com.ibm.lagraph.LagSemiring$$anon$1@63b61f66
eInit: (((Long, Long), Float)) => ((Long, Long), (Float, Long, Long)) = <function1>
d2Str: ((Float, Long, Long)) => String = <function1>
{% endhighlight %}
</div>

</div>


## Initialize adjacency matrix for path-augmented (common)

Using the same code as was used for the distance-only semiring, setup the algorithm for the path-augmented semirings.

<div class="codetabs">

<div data-lang="Scala" markdown="1">
{% highlight scala %}
// initialize adjacency matrix
// strip diagonal and add back in as PathMinPlusSr.one, use weight to derive PathType for edge
val diagStripped = rcvGraph.flatMap { kv => if (kv._1._1 == kv._1._2) None else Some(eInit(kv)) }
val rcvAdj = diagStripped.union(sc.range(0L, numv, 1L, rcvGraph.getNumPartitions).map { i => ((i, i), PathMinPlusSr.one) })

// use distributed context-specific utility to convert from RDD to LagMatrix
val mAdj = hc.mFromRcvRdd(rcvAdj, PathMinPlusSr.zero)

println("mAdj: >\n%s<".format(mAdj.toString(d2Str)))

// initialize vector d w/ source (input)
val source = 0L
def dInit(di: PathType, ui: Long): PathType = if (ui == source) PathMinPlusSr.one else di
val d_prev = hc.vReplicate(PathMinPlusSr.zero).zipWithIndex(dInit, Option(PathMinPlusSr.zero))
println("d_initial: >\n%s<".format(d_prev.toString(d2Str)))
{% endhighlight %}
</div>

<div data-lang="Spark Shell" markdown="1">
{% highlight scala %}
scala> czczczczcz// initialize adjacency matrix
// strip diagonal and add back in as PathMinPlusSr.one, use weight to derive PathType for edge
val diagStripped = rcvGraph.flatMap { kv => if (kv._1._1 == kv._1._2) None else Some((eInit(kv))) }
val rcvAdj = diagStripped.union(sc.range(0L, numv, 1L, rcvGraph.getNumPartitions).map{i => ((i, i), PathMinPlusSr.one)})

// use distributed context-specific utility to convert from RDD to LagMatrix
val mAdj = hc.mFromRcvRdd(rcvAdj, PathMinPlusSr.zero)

println("mAdj: >\n%s<".format(hc.mToString(mAdj, d2Str)))

// initialize vector d w/ source (input)
val source = 0L
def dInit(di: PathType, ui: Long): PathType = if (ui == source) PathMinPlusSr.one else di
val d_prev = hc.vZipWithIndex(dInit, hc.vReplicate(PathMinPlusSr.zero), Option(PathMinPlusSr.zero))
println("d_initial: >\n%s<".format(hc.vToString(d_prev, d2Str)))

scala> diagStripped: org.apache.spark.rdd.RDD[((Long, Long), (Float, Long, Long))] = MapPartitionsRDD[82] at flatMap at <console>:49
rcvAdj: org.apache.spark.rdd.RDD[((Long, Long), (Float, Long, Long))] = UnionRDD[86] at union at <console>:53
mAdj: com.ibm.lagraph.LagMatrix[PathType] =
LagDstrMatrix(LagDstrContext(org.apache.spark.SparkContext@3b4a3d17,5,1,false),UnionRDD[86] at union at <console>:53,((0.0,0,-1), (3.4028235E38,9223372036854775807,9223372036854775807), (3.4028235E38,9223372036854775807,9223372036854775807), (3.4028235E38,9223372036854775807,9223372036854775807), (3.4028235E38,9223372036854775807,9223372036854775807))
((20.0,1,0), (0.0,0,-1), (3.4028235E38,9223372036854775807,9223372036854775807), (3.4028235E38,9223372036854775807,9223372036854775807), (3.4028235E38,9223372036854775807,9223372036854775807))
((10.0,1,0), (3.4028235E38,9223372036854775807,9223372036854775807), (0.0,0,-1), (3.4028235E38,9223372036854775807,9223372036854775807), (3.4028235E38,9223372036854775807,9223372036854775807))
((3.402...mAdj: >
{
{( 0.000,   0,  -1),(   inf, inf, inf),(   inf, inf, inf),(   inf, inf, inf),(   inf, inf, inf)},
{(20.000,   1,   0),( 0.000,   0,  -1),(   inf, inf, inf),(   inf, inf, inf),(   inf, inf, inf)},
{(10.000,   1,   0),(   inf, inf, inf),( 0.000,   0,  -1),(   inf, inf, inf),(   inf, inf, inf)},
{(   inf, inf, inf),(15.000,   1,   1),(30.000,   1,   2),( 0.000,   0,  -1),(   inf, inf, inf)},
{(   inf, inf, inf),(   inf, inf, inf),(50.000,   1,   2),( 5.000,   1,   3),( 0.000,   0,  -1)}
}<
source: Long = 0
dInit: (di: PathType, ui: Long)PathType
d_prev: com.ibm.lagraph.LagVector[PathType] = LagDstrVector(LagDstrContext(org.apache.spark.SparkContext@3b4a3d17,5,1,false),((0.0,0,-1), (3.4028235E38,9223372036854775807,9223372036854775807), (3.4028235E38,9223372036854775807,9223372036854775807), (3.4028235E38,9223372036854775807,9223372036854775807), (3.4028235E38,9223372036854775807,9223372036854775807)))
d_initial: >
{
( 0.000,   0,  -1),(   inf, inf, inf),(   inf, inf, inf),(   inf, inf, inf),(   inf, inf, inf)
}<
{% endhighlight %}
</div>

</div>


## Iterate for path-augmented (common)

Using the same code as was used for the distance-only semiring, perform the calculation for the path-augmented semiring.

<div class="codetabs">

<div data-lang="Scala" markdown="1">
{% highlight scala %}
// iterate, relaxing edges
val maxiter = d_prev.size
def iterate(k: Long, d_prev: LagVector[PathType]): LagVector[PathType] =
  if (k == maxiter) d_prev else iterate(k + 1, mAdj.tV(PathMinPlusSr, d_prev))

val d_final = iterate(0L, d_prev)

// are we at a fixed point?
if (d_final.equiv(mAdj.tV(PathMinPlusSr,d_final))) {
  println("d_final: >\n%s<".format(d_final.toString(d2Str)))
} else {
  println("A negative-weight cycle exists.")
}
{% endhighlight %}
</div>

<div data-lang="Spark Shell" markdown="1">
{% highlight scala %}
scala> // BellmanFord: Common: Loop
// loop, relaxing edges
val maxiter = d_prev.size
def iterate(k: Long, d_prev: LagVector[PathType]): LagVector[PathType] =
  if (k == maxiter) d_prev else iterate(k + 1, hc.mTv(PathMinPlusSr, mAdj, d_prev))

val d_final = iterate(0L, d_prev)

// are we at a fixed point?
if (hc.vEquiv(d_final, hc.mTv(PathMinPlusSr, mAdj, d_final))) {
  println("d_final: >\n%s<".format(hc.vToString(d_final, d2Str)))
} else {
  println("A negative-weight cycle exists.")
}

scala> maxiter: Long = 5
iterate: (k: Long, d_prev: com.ibm.lagraph.LagVector[PathType])com.ibm.lagraph.LagVector[PathType]
d_final: com.ibm.lagraph.LagVector[PathType] = LagDstrVector(LagDstrContext(org.apache.spark.SparkContext@33829b50,5,1,false),((0.0,0,-1), (20.0,1,0), (10.0,1,0), (35.0,2,1), (40.0,3,3)))
d_final: >
{
( 0.000,   0,  -1),(20.000,   1,   0),(10.000,   1,   0),(35.000,   2,   1),(40.000,   3,   3)
}<
{% endhighlight %}
</div>

</div>
