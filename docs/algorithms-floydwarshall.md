---
layout: global
title: LAGraph - Algorithms - Floyd-Warshall
displayTitle: LAGraph - Algorithms - Floyd-Warshall
description: LAGraph - Algorithms - Floyd-Warshall
---
<!--
{% comment %}
License ...
{% endcomment %}
-->
* This will become a table of contents (this text will be scraped).
{:toc}

## Notation

This page covers the [[Fineman2011Fundamental]](#references) algebraic
formulation of the Floyd-Warshall algorithm to compute the shortest
path from every vertex to every other vertex in a weighted, directed
graph.  This is the so-called All-Pairs Shortest Path problem (APSP)
problem.  Consider a directed graph $G = (V,E)$ with: no negative
weight cycles; edge weights $$ w : E \rightarrow \mathbb{R}_{\infty}
$$ where $$ \mathbb{R}_{\infty} = \mathbb{R_{\ge 0}} \cup \{ \infty \}
$$; and a weight matrix $$ \mathbf{W} : \mathbb{R}_{\infty}^{N \times
N}$$ where $\mathbf{W}(u,v) = \infty$ if $(u,v) \notin E$.  Self-loops
are ignored by setting $\mathbf{W}(v,v) = 0$ for all $v \in V$.  Path
definitions are the same as described in the [Bellman-Ford
description](algorithms-bellmanford.md#notation).


## Algorithm with a Distance-only Semiring

The implementation uses a $N \times N$ matrix $D_k$ to store the shortest paths obtained after the $k$-th iteration:

$$
\mathbf{D}_k (u,v) = 
\begin{cases}
\mathbf{W}(u,v) & k = 0 \\
min \{ \mathbf{D}_{k-1}(u,v),\mathbf{D}_{k-1}(u,k)+\mathbf{D}_{k-1}(k,v)\} & k \ge 1
\end{cases}
$$

Here is the algebraic implementation of the Floyd-Warshall algorithm:

$$
\begin{aligned}
     & \mbox {FLOYD–WARSHALL($\mathbf{A}$, $s$):} \\
     & \quad   \mathbf{D} = \mathbf{A} \\
     & \quad   \mbox{for $k = 0 $ to $N - 1$} \\
     & \qquad  \mbox{do $\mathbf{D} = \mathbf{D} \textrm{ nop.min } [\mathbf{D}(:,k) \textrm{ nop.+ } \mathbf{D}(k,:)]$}
\end{aligned}
$$

The running time is $$\mathcal{O}(N^3)$$. Since, at any instant, only $$\mathbf{D}_{k}$$ and $$\mathbf{D}_{k-1}$$ are required, the storage requirement is $$\mathcal{O}(N^2)$$.

### Implemention of the Distance-only Type

Here is the implementation of the distance-only type and the associated semirings:

```scala
// ********
// Floyd-Warshall: Distance-only Semirings: Initialize: 1st Pass
// Initialize distance-only Floyd-Warshall
type PathType = Float
val PathNopPlusSr = LagSemiring.nop_plus[PathType]
val PathNopMinSr = LagSemiring.nop_min[PathType]

// for edge initialization (weight mapping)
val eInit = (kv: ((Long, Long), Float)) => (kv._1, kv._2.toFloat) // edge (weight)
```

The semiring type of `Float` is assigned to `PathType` and reflects the
fact that only distance is being computed. The semirings are
instantiated from the generic classes
[`LagSemiring.nop_plus[T]`](./api/index.html#com.ibm.lagraph.LagSemiring$)
and
[`LagSemiring.nop_min[T]`](./api/index.html#com.ibm.lagraph.LagSemiring$)
.
Since a `Float` is a Scala `Numeric` no
additional code is necessary to implement the semiring type. The `eInit`
functor is used below in the type-independent (common) code for the
algorithm to transform the input adjacency matrix to the adjacency
matrix used in the algorithm.

### Implementation of the Distance-only Algorithm

Here is the implementation of the algorithm. It is valid for for both the distance-only computation and path-augmented computation.

```scala
// ********
// FloydWarshall: Initialize: Common
// initialize adjacency matrix
// strip diagonal and since diagonal is PathNopPlusSr.zero, since diagonal is zero, no need to add back in.
val rcvAdj = rcvGraph.flatMap { kv => if (kv._1._1 == kv._1._2) None else Some(eInit(kv)) }
// use distributed context-specific utility to convert from RDD to LagMatrix
val mAdj = hc.mFromRcvRdd(rcvAdj, PathNopPlusSr.zero)

// ********
// FloydWarshall: Iterate: Common
// iterate
@tailrec def iterate(k: Long, D: LagMatrix[PathType]): LagMatrix[PathType] = if (k == D.size._1) D else 
  iterate(k + 1, D.hM(PathNopMinSr, D.oP(PathNopPlusSr, k, D, k)))
val D = iterate(0L, mAdj)
```

The initial line of code uses the Spark RDD `flatMap` function to
transform the key value representation of the adjacency matrix into
the form required by the computation.  The iteration is performed
using tail recursion.  The implementation is for a distributed (Spark)
enviornment. For a non-distributed (pure Scala) context, the Spark
`flatMap` operation would be replaced with an equivalent
Scala `flatMap` operation.

## Algorithm with a Path-augmented Semiring

To compute the path, the distance-only semiring type is augmented just as it was
in [Bellman-Ford section on the path-augmented semiring](algorithms-bellmanford.md#algorithm-with-a-path-augmented-semiring).

### Implementation of the Path-augmented Semiring

Here is the implementation of path-augmented semiring.

```scala
// ********
// Floyd-Warshall: Path-augmented Semiring: Initialize: 2nd Pass
// Define path-augmented type and semirings

type PathType = Tuple3[Float, Long, Long]

// some handy constants
val FloatInf = LagSemigroup.infinity(classTag[Float])
val LongInf = LagSemigroup.infinity(classTag[Long])
val FloatMinf = LagSemigroup.minfinity(classTag[Float])
val LongMinf = LagSemigroup.minfinity(classTag[Long])
val NodeNil: Long = -1L

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
      else if (p2 != NodeNil) (f(w1, w2), g(h1, h2), p2)
      else (f(w1, w2), g(h1, h2), p1) // original
    else (f(w1, w2), g(h1, h2), p1)
  }
  def times(x: PathType, y: PathType): PathType = min(x, y)
  def fromInt(x: Int): PathType = x match {
    case 0     => ((0.0).toFloat, 0L, NodeNil)
    case 1     => (FloatInf, LongInf, LongInf)
    case other => throw new RuntimeException("fromInt for: >%d< not implemented".format(other))
  }
}

implicit object PathTypeAsNumeric extends PathTypeAsNumeric
val PathNopPlusSr = LagSemiring.nop_plus[PathType](Tuple3(FloatInf, LongInf, LongInf))
val PathNopMinSr = LagSemiring.nop_min[PathType](Tuple3(FloatInf, LongInf, LongInf), Tuple3(FloatMinf, LongMinf, LongMinf))

// for edge initialization (weight mapping)
val eInit = (kv: ((Long, Long), Float)) => (kv._1, (kv._2.toFloat, 1L, kv._1._2)) // edge (weight)
```

The path-augmented semiring type of `Tuple3[Float, Long, Long]` is
assigned to `PathType`.  Since the semiring $\textrm{ nop.min }$
requires a comparison, we define the trait `PathTypeOrdering` to
establish a Scala Ordering for the path-augmented type that is
consistent with mathematical formulation.  The trait `PathTypeAsNumeric`
endows the new type with the semiring properties defined in the
mathematical formulation.

The implicit object `PathTypeAsNumeric` enables
the implicit conversion of a PathTypeAsNumeric instance to a
Numeric.
The $\textrm{ nop.plus }$ semiring for the new type is assigned to `PathNopPlusSr` and the $\textrm{ nop.min }$ semiring for the new type is assigned to `PathNopMinSr`.
The input argument `(Tuple3(FloatInf, LongInf, LongInf))` to `LagSemiring.nop_plus[PathType]` and `LagSemiring.nop_min[PathType]`
is required to augment Scala
Numeric with a value for $\infty$.
The input argument `(Tuple3(FloatMinf, LongMinf, LongMinf))` to `LagSemiring.nop_min[PathType]` 
is required to augment Scala
Numeric with a value for $-\infty$.
Finally, the functor `eInit` is used to
initialize the edge weights in the adjacency matrix.

### Implementation of the Path-augmented Algorithm

The implementation of the algorithm described in [Implementation of the Distance-only Algorithm](#implementation-of-the-distance-only-algorithm)
remains unchanged for new semiring type.

# References

**[Fineman2011Fundamental]**
Fineman, Jeremy T. and Robinson, Eric, Fundamental graph algorithms, Graph Algorithms in the Language of Linear Algebra, volume 22, pages 45-58, 2011, SIAM
