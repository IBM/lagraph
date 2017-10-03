---
layout: global
title: LAGraph - Algorithms - Prim's
displayTitle: LAGraph - Algorithms - Prim's
description: LAGraph - Algorithms - Prim's
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
formulation of Prim's algorithm to the compute the minimum spanning
tree of a connected, undirected graph $G = (V,E)$ with edge weights
$$ w : E \rightarrow \mathbb{R}_{\infty} $$ where $$
\mathbb{R}_{\infty} = \mathbb{R_{\ge 0}} \cup \{ \infty \} $$; and a
weight matrix $$ \mathbf{W} : \mathbb{R}_{\infty}^{N \times N}$$ where
$\mathbf{W}(u,v) = \infty$ if $(u,v) \notin E$.  Self-loops are
ignored by assigning $\mathbf{W}(v,v) = 0$ for all $v \in V$ .  The
_minimum spanning tree_ $T$ of $G$ is a subgraph that is a tree which
includes all of $V$ and possesses the minimum possible number of
edges. The weight of $T$ is $$w(T)= \sum_{e \in T} \mathbf{W}(e)$$.

## Algorithm with a Tree-augmented Semiring

[[Fineman2011Fundamental]](#references) presents implementations for
both the case where the tree is calculated and the case where the tree
is not calculated. Here we present only the implementation which
includes the calculation of the tree.  The algorithm is implemented
with a $$N \times N$$ adjacency matrix $$\mathbf{A}$$ constructed by
augmenting the edge weight with the source vertex: $$\mathbf{A}(u,v) =
\langle\mathbf{W}(u,v),u\rangle$$.  The algorithm uses a set $S$ to
track the addition of edges to the spanning tree. $S$ starts with a
single, arbitrarily-chosen vertex and then incrementally growes the
set by adding a minimum weight edge.  The set $S$ is tracked with a
vector $$\mathbf{s}$$ of dimension $N$ maintained such that:

$$
\mathbf{s} (v) = 
\begin{cases}
\mathbf{\infty} & v \in S \\
0 & \textrm{otherwise}
\end{cases}
$$

An vector $$\mathbf{d}$$ of dimension $N$ is used to track the _lightest_ edge from a vertex $$v \notin S$$ to $S$:

$$
\mathbf{d} (v) = 
\begin{cases}
\langle 0, \textrm{NIL} \rangle  & v \in S \\
\langle \min_{u \in s} \textbf{W}(u,v), v \rangle & \textrm{otherwise}
\end{cases}
$$

An vector $$\mathbf{\pi}$$ of dimension $N$ is used to store the spanning tree. Here is the algebraic implementation of the Prim's algorithm:

$$
\begin{aligned}
     & \mbox {PRIMS($\mathbf{A}$):} \\
     & \quad   \mathbf{s} = 0 \\
     & \quad   \mbox{$\mathit{weight} = 0$} \\
     & \quad   \mbox{$\mathbf{s}(1) = \infty$ // select arbitrary start vertex} \\
     & \quad   \mbox{$\mathbf{d} = \mathbf{A}(1,:)$} \\
     & \quad   \mbox{$\mathbf{\pi} = \textrm{NIL}$} \\
     & \quad   \mbox{while $\mathbf{s} \ne \mathbf{\infty}$} \\
     & \qquad  \mbox{do $u = \textrm{argmin}\{\mathbf{s} + \mathbf{d} \}$} \\
     & \qquad  \quad \mbox{$\mathbf{s}(u) = \infty$} \\
     & \qquad  \quad \mbox{$\langle w, p \rangle = \mathbf{d}(u)$} \\
     & \qquad  \quad \mbox{$\mathit{weight} = \mathit{weight} + w$} \\
     & \qquad  \quad \mbox{$\mathbf{d} = \mathbf{d} \textrm{ nop.min } \mathbf{A}(u,:)$}
\end{aligned}
$$

### Implementation of the Tree-augmented Semiring

Here is the implementation of the tree-augmented type and the associated semirings:

```scala
// ********
// Prim's: Tree-augmented Semiring: Initialization
// Define path-augmented type and semirings

type PrimTreeType = Tuple2[Float, Long]

// some handy constants
val FloatInf = LagSemigroup.infinity(classTag[Float])
val LongInf = LagSemigroup.infinity(classTag[Long])
val FloatMinf = LagSemigroup.minfinity(classTag[Float])
val LongMinf = LagSemigroup.minfinity(classTag[Long])
val NodeNil: Long = -1L

// Ordering for PrimTreeType
trait PrimTreeTypeOrdering extends Ordering[PrimTreeType] {
  def compare(ui: PrimTreeType, vi: PrimTreeType): Int = {
    val w1 = ui._1; val p1 = ui._2
    val w2 = vi._1; val p2 = vi._2
    if (w1 < w2) -1
    else if ((w1 == w2) && (p1 < p2)) -1
    else if ((w1 == w2) && (p1 == p2)) 0
    else 1
  }
}

// Numeric for PrimTreeType
trait PrimTreeTypeAsNumeric extends com.ibm.lagraph.LagSemiringAsNumeric[PrimTreeType] with PrimTreeTypeOrdering {
  def plus(ui: PrimTreeType, vi: PrimTreeType): PrimTreeType = throw new RuntimeException("PrimSemiring has nop for addition: ui: >%s<, vi: >%s<".format(ui, vi))
  def times(x: PrimTreeType, y: PrimTreeType): PrimTreeType = min(x, y)
  def fromInt(x: Int): PrimTreeType = x match {
    case 0     => ((0.0).toFloat, NodeNil)
    case 1     => (FloatInf, LongInf)
    case other => throw new RuntimeException("PrimSemiring: fromInt for: >%d< not implemented".format(other))
  }
}

implicit object PrimTreeTypeAsNumeric extends PrimTreeTypeAsNumeric
val PrimSemiring = LagSemiring.nop_min[PrimTreeType](Tuple2(FloatInf, LongInf), Tuple2(FloatMinf, LongMinf))

// ****
// Need a nop_min semiring Float so add proper behavior for infinity
type FloatWithInfType = Float

// Ordering for FloatWithInfType
trait FloatWithInfTypeOrdering extends Ordering[FloatWithInfType] {
  def compare(ui: FloatWithInfType, vi: FloatWithInfType): Int = {
    compare(ui, vi)
  }
}
// Numeric for FloatWithInfType
trait FloatWithInfTypeAsNumeric extends com.ibm.lagraph.LagSemiringAsNumeric[FloatWithInfType] with FloatWithInfTypeOrdering {
  def plus(ui: FloatWithInfType, vi: FloatWithInfType): FloatWithInfType = {
    if (ui == FloatInf || vi == FloatInf) FloatInf
    else ui + vi
  }
  def times(ui: FloatWithInfType, vi: FloatWithInfType): FloatWithInfType = {
    if (ui == FloatInf || vi == FloatInf) FloatInf
    else ui + vi
  }
}
```
### Implementation of the Tree-augmented Algorithm

Here is the implementation of the algorithm.

```scala
    // ********
    // Algebraic Prim's
    // initialize adjacency matrix
    // strip diagonal
    val diagStripped = rcvGraph.flatMap
      { kv => if (kv._1._1 == kv._1._2) None else Some((kv._1, (kv._2, kv._1._1))) }
    // add diagonal of "zeros"
    val rcvAdj = diagStripped.union(sc.range(0L, numv, 1L, rcvGraph.getNumPartitions).map
      { i => ((i, i), PrimSemiring.zero) })
    // use distributed context-specific utility to convert from RDD to LagMatrix
    val mAdj = hc.mFromRcvRdd(rcvAdj, Tuple2(FloatInf, NodeNil))

    val weight_initial = 0

    // arbitrary vertex to start from
    val source = 0L

    // initial membership in spanning tree set
    val s_initial = hc.vReplicate(0.0F).set(source, FloatInf)

    // initial membership in spanning tree set
    val s_final_test = hc.vReplicate(FloatInf)

    val d_initial = mAdj.vFromRow(0)

    val pi_initial = hc.vReplicate(NodeNil)

    @tailrec
    def iterate(weight: Float,
                d: LagVector[PrimTreeType],
                s: LagVector[Float],
                pi: LagVector[Long]): (Float, LagVector[PrimTreeType], LagVector[Float], LagVector[Long]) =
      if (s.equiv(s_final_test)) (weight, d, s, pi) else {
        val u = hc.vArgmin(hc.vZip(LagSemiring.nop_plus[FloatWithInfType].multiplication,
	                           hc.vMap({ wp: PrimTreeType => wp._1 }, d),
				   s))
        val wp = hc.vEle(d, u._2)
        iterate(weight + wp._1.get._1,
	        d.zip(PrimSemiring.multiplication, mAdj.vFromRow(u._2)),
		s.set(u._2, FloatInf),
		pi.set(u._2, wp._1.get._2))
      }

    val (weight_final, d_final, s_final, pi_final) = iterate(weight_initial, d_initial, s_initial, pi_initial)
```

# References

**[Fineman2011Fundamental]**
Fineman, Jeremy T. and Robinson, Eric, Fundamental graph algorithms, Graph Algorithms in the Language of Linear Algebra, volume 22, pages 45-58, 2011, SIAM
