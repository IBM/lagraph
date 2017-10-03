---
layout: global
title: LAGraph - Algorithms - Bellman-Ford
displayTitle: LAGraph - Algorithms - Bellman-Ford
description: LAGraph - Algorithms - Bellman-Ford
---
<!--
{% comment %}
License ...
{% endcomment %}
-->
* This will become a table of contents (this text will be scraped).
{:toc}

## Notation

This page covers the [[Fineman2011Fundamental]](#references) algebraic formulation of the
Bellman-Ford algorithm to compute shortest paths from a single source
vertex to all of the other vertices in a weighted, directed graph.
This is the so-called Single Source Shortest Path (SSSP)
problem.
Consider a directed graph $G
= (V,E)$ with edge weights $$ w : E \rightarrow \mathbb{R}_{\infty} $$
where $$ \mathbb{R}_{\infty} = \mathbb{R} \cup \{ \infty \} $$, and a
weight matrix $$ \mathbf{W} : \mathbb{R}_{\infty}^{N \times N}$$ where
$\mathbf{W}(u,v) = \infty$ if $(u,v) \notin E$.  Self-loops are
ignored by setting $\mathbf{W}(v,v) = 0$ for all $v \in V$ .  A path
$p$ from $v_0$ to $v_k$ is denoted $v_0 \overset{p}{\leadsto} v_k$ and
represents a sequence of vertices $p = \langle
v_0,v_1,\dotsc,v_k\rangle$ such that the pair $(v_{i-1},v_i)$
represents an edge $e \in E$.  The _weight_ of the path is defined to
be $w(p) = \sum_{i=1}^{k} \mathbf{W}(v_{i-1},v_{i})$.  The _size_ of the
path is said to be $k$ _hops_ and is denoted $\lvert p \rvert = k$.  The
_shortest path distance_ (or, more accurately, the _shortest path
weight_) from $u$ to $v$ is given by:

$$
\mathbf{\Delta}(u,v) = 
\begin{cases}
min \{ w(p) : u \overset{p}{\leadsto} v \} & \textrm{if a path from $u$ to $v$ exists}\\
\infty & \textrm{otherwise}
\end{cases}
$$

Using these definitions we refer to the _shortest path_ from $u$ to $v$ as a (not necessarily unique) path $u \overset{p}{\leadsto} v$ with $w(p) = \mathbf{\Delta}(u,v)$.

## Algorithm with a Distance-only Semiring

An $N \times N$ adjacency matrix $\mathbf{A}$ is used to store the
edge weights and a vector $\mathbf{d}_k$ of dimension $N$ is used to
store the shortest ($\leq k$)-hop path distances
$\mathbf{\Delta}_k(s,*)$.  Self loops are ignored by setting
$\mathbf{A}(v,v) = 0$. The shortest path distance is $$\mathbf{d} = A^{N-1}\mathbf{d}_0$$ where multiplication is defined using the
 $$\textrm{ min.+ }$$semiring, i.e.,  $$\mathbf{d}_k =
\mathbf{A} \textrm{ min.+ } \mathbf{d}_{k-1}$$, and the distance vector is initialized

$$
\mathbf{d}_0(v) = \begin{cases}
0 & \textrm{if $v = s$}\\
\infty & \textrm{otherwise}
\end{cases}
$$

Here is algebraic implementation of the Bellman–Ford algorithm from [[Fineman2011Fundamental]](#references):

$$
\begin{aligned}
     & \mbox {BELLMAN-FORD($\mathbf{A}$, $s$):} \\
     & \quad   \mathbf{d} = \infty \\
     & \quad   \mathbf{d}(s) = 0 \\
     & \quad   \mbox{for $k = 1 $ to $N - 1$} \\
     & \qquad  \mbox{do $\mathbf{d} = \mathbf{d}$ min.+ $\mathbf{A}$} \\
     & \quad   \mbox{if $\mathbf{d} \neq \mathbf{d}$ min.+ $\mathbf{A}$} \\
     & \qquad  \text{then return <A negative-weight cycle exists.> } \\
     & \quad   \mbox{return $\mathbf{d}$} \\
\end{aligned}
$$

### Implemention of the Distance-only Semiring

Here is the implementation of the distance-only type and its corresponding semiring.

```scala
// ********
// Bellman-Ford: Distance-only Semiring: Initialization: 1st Pass
// Define path-augmented type and semirings
type PathType = Float
val PathMinPlusSr = LagSemiring.min_plus[PathType]

// for edge initialization (weight mapping)
val eInit = (kv: ((Long, Long), Float)) => (kv._1, kv._2.toFloat) // edge (weight)
```

The semiring type of `Float` is assigned to `PathType` and reflects the
fact that only distance is being computed. The semiring is
instantiated from the generic class
[`LagSemiring.min_plus[T]`](./api/index.html#com.ibm.lagraph.LagSemiring$).
Since a `Float` is a Scala `Numeric` no
additional code is necessary to implement the semiring type. The `eInit`
functor is used below in the type-independent (common) code for the
algorithm to transform the input adjacency matrix to the adjacency
matrix used in the algorithm.

### Implementation of the Distance-only Algorithm

Here is the implementation of the algorithm. It is valid for 
for both the distance-only computation and
path-augmented computation.

```scala
// ********
// initialize adjacency matrix
// strip diagonal and add back in as PathMinPlusSr.one, use weight to derive PathType for edge
val diagStripped = rcvGraph.flatMap { kv => if (kv._1._1 == kv._1._2) None else Some(eInit(kv)) }
val rcvAdj = diagStripped.union(sc.range(0L, numv, 1L, rcvGraph.getNumPartitions).map { i => ((i, i), PathMinPlusSr.one) })

// use distributed context-specific utility to convert from RDD to LagMatrix
val mAdj = hc.mFromRcvRdd(rcvAdj, PathMinPlusSr.zero)

println("mAdj: >\n%s<".format(hc.mToString(mAdj, d2Str)))

// initialize vector d w/ source (input)
val source = 0L
def dInit(di: PathType, ui: Long): PathType = if (ui == source) PathMinPlusSr.one else di
val d_prev = hc.vZipWithIndex(dInit, hc.vReplicate(PathMinPlusSr.zero), Option(PathMinPlusSr.zero))
println("d_initial: >\n%s<".format(hc.vToString(d_prev, d2Str)))

// ********
// iterate, relaxing edges
val maxiter = d_prev.size
@tailrec def iterate(k: Long, d_prev: LagVector[PathType]): LagVector[PathType] =
  if (k == maxiter) d_prev else iterate(k + 1, hc.mTv(PathMinPlusSr, mAdj, d_prev))

val d_final = iterate(0L, d_prev)

// are we at a fixed point?
if (hc.vEquiv(d_final, hc.mTv(PathMinPlusSr, mAdj, d_final))) {
  println("d_final: >\n%s<".format(hc.vToString(d_final, d2Str)))
} else {
  println("A negative-weight cycle exists.")
}
```

The first two lines use standard Spark functions to transform the key
value representation of the adjacency matrix into the form required by
the computation.  The initialization of the distance vector is accomplished with an application of the
[`LagContext.vZipWithIndex`](./api/index.html#com.ibm.lagraph.LagContext)
method.  The iteration is performed using tail recursion.  The
equivalence test at the end is used to test for a fixed a fixed point
(i.e., for convergence).

This implementation is for a distributed (Spark) enviornment.  For a
non-distributed (pure Scala) context, the `RDD` operations would be
replaced with corresponding Scala `Map` operations.

## Algorithm with a Path-augmented Semiring

For the distance-only computation the shortest path vector, $\mathbf{d}$, was
used to store only the path length.
To compute the path, distance is augmented with the penultimate vertex, $\mathbf{\pi}$, and
the number of hops, $\lvert p\rvert$, in a $3\textrm{-Tuple}$ of the
form

$$
\langle w, h, \pi \rangle \in S = (\mathbb{R}_\infty \times
\mathbb{N} \times V) \cup \{ \langle \infty, \infty, \infty \rangle,
\langle 0,0,\textrm{NIL} \rangle \}
$$

where $$\mathbb{R}_\infty =
\mathbb{R} \cup \{ \infty \}$$ and $$\mathbb{N} = \{ 1,2,3,\dotsc
\}$$.
The first element in the tuple, $w \in
\mathbb{R}_\infty$, is the path weight.  The second element, $h
\in \mathbb{N}$, is the path size (number of hops).  The third element,
$\pi \in V$, is the penultimate vertex along a path.
The value $\langle \infty, \infty, \infty \rangle$ represents
no path and the value $\langle 0,0,\textrm{NIL} \rangle$ represents a
path from the vertex to itself.
Using the new
type, the adjacency matrix is defined as:

$$
\mathbf{A}(u,v) = \begin{cases}
\langle 0,0,\mathrm{NIL}\rangle & \textrm{if $u = v$}\\
\langle \mathbf{W}(u,v),1,u\rangle & \textrm{if $u \neq v, (u,v) \in E$}\\
\langle \infty,\infty,\infty\rangle & \textrm{if $(u,v) \notin E$}
\end{cases}
$$

and the initial distance vector $\mathbf{d}_0$ is:

$$
\mathbf{d}_0(v) = \begin{cases}
\langle 0,0,\mathrm{NIL}\rangle & \textrm{if $v = a$}\\
\langle \infty,\infty,\infty\rangle & \textrm{otherwise}
\end{cases}
$$

For the new type, the addition operator lmin is defined to be the lexicographic minimum:

$$
\textrm{lmin}\{\langle w_1,h_1,\pi_1\rangle,\langle w_2,h_2,\pi_2\rangle\} = 
\begin{cases}
\langle w_1,h_1,\pi_1\rangle & \textrm{if $w_1 < w_2$, or}\\
                           & \textrm{if $w_1 = w_2$ and $h_1 < h_2$, or}\\
                           & \textrm{if $w_1 = w_2$ and $h_1 < h_2$ and $\pi_1 < \pi_2$}\\
\langle w_1,h_1,\pi_2\rangle & \textrm{otherwise}
\end{cases}
$$

In [[Fineman2011Fundamental]](#references) the treatment is base-1, we use
base-0 and number the vertices $0,1,2,\dotsc,N$ with $\textrm{NIL} < v
< \infty$ for all $v \in V$.  The multiplication operator for the new
type, $+_{\textrm{rhs}}$, sums the path weight and the path size and
retains the penultimate vertex:

$$
\langle w_1,h_1,\pi_1\rangle +_{\textrm{rhs}} \langle w_2,h_2,\pi_2\rangle = 
\begin{cases}
\langle w_1+w_2,h_1+h_2,\pi_2\rangle & \textrm{if $\pi_1 \ne \infty, \pi_2\ne$NIL}\\
\langle w_1+w_2,h_1+h_2,\pi_1\rangle & \textrm{otherwise}
\end{cases}
$$

The exception for $\pi_1 = \infty$ is required to maintain the semiring's multiplicative identity.
The exception for $\pi_2\ne\textrm{NIL}$ is required for correctness.

### Implementation of the Path-augmented Semiring

Here is the implementation of path-augmented semiring.

```scala

// ********
// Bellman-Ford: Path-augmented Semiring: Initialization: 2nd Pass
// Define path-augmented type and semirings

type PathType = Tuple3[Float, Long, Long]

// some handy constants
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
```

The path-augmented semiring type of `Tuple3[Float, Long, Long]` is
assigned to `PathType`.  Since the semiring $\textrm{ min.plus }$
requires a comparison, we define the trait `PathTypeOrdering` to
establish a Scala Ordering for the path-augmented type that is
consistent with mathematical formulation.  The trait `PathTypeAsNumeric`
endows the new type with the semiring properties defined in the
mathematical formulation.  Note how Scala, when compared to other
non-functional languages, facilitates this step.

The implicit object `PathTypeAsNumeric` enables
the implicit conversion of a PathTypeAsNumeric instance to a
Numeric.
The $\textrm{ min.plus }$ semiring for the new type is assigned to `PathSemiring`.
The input argument `(Tuple3(FloatInf, LongInf, LongInf))` to `LagSemiring.min_plus[PathType]` 
is required to augment Scala
Numeric with a value for $\infty$.
Finally, the functor `eInit` is used to
initialize the edge weights in the adjacency matrix to
$\langle\mathbf{W}(u,v),1,u\rangle$.

### Implementation of the Path-augmented Algorithm

The implementation of the algorithm described in [Implementation of the Distance-only Algorithm](#implementation-of-the-distance-only-algorithm)
remains unchanged for new semiring
type.

# References

**[Fineman2011Fundamental]**
Fineman, Jeremy T. and Robinson, Eric, Fundamental graph algorithms, Graph Algorithms in the Language of Linear Algebra, volume 22, pages 45-58, 2011, SIAM
