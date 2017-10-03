---
layout: global
title: LAGraph - Programming with Semirings
displayTitle: LAGraph - Programming with Semirings
description: LAGraph - Programming with Semirings
---
<!--
{% comment %}
License ...
{% endcomment %}
-->

LAGraph was designed with the following goals:

* The implementation should closely follow the mathematical expression of the algorithm.

* It should be easy to define custom semirings supporting both Scala Numerics and user-defined custom types.

* The interface to the distributed implementation should be no different than the interface to the non-distributed implementation.

One of the challenges in designing custom semirings is the challenge
of defining custom functions for "addition" and "multiplication"
over the custom type.  Unlike other object-oriented languages, in
Scala every value is an object instance and every operation is a
method call.  This includes function values, each function value is an
object and function types are just classes that can be inherited by
subclasses, so custom functions are easily accommodated.
Scala also has support for _Generic Classes_, which are classes
parameterized with types.  We abstract the monoids $(S,\oplus,0)$ and
$(S,\otimes,1)$ over type with the generic class named LagSemigroup to
define custom functions.

```scala
case class LagSemigroup[T](
      op: Function2[T, T, T],
      annihilator: Option[T] = None,
      identity: Option[T] = None
    ) extends Function2[T, T, T] {
  /** Apply the body of this function to the arguments. */
  override def apply(a: T, b: T): T = { op(a, b) }
```

Both $(S,\oplus,0)$ and $(S,\otimes,1)$ utilize the identity
attribute.  The annihilator attribute is used by $(S,\otimes,1)$ and
defaults to None for $(S,\oplus,0)$. The semiring is specified by the
parameterized trait named LagSemiring:

```scala
trait LagSemiring[T] extends Serializable {
  /** Addition for this semiring, must meet all semiring requirements for addition */ 
  val addition: LagSemigroup[T]
  /** Multiplication for this semiring, must meet all semiring requirements for multiplication */ 
  val multiplication: LagSemigroup[T]
  /** The "zero" for this semiring, must meet all semiring requirements for zero */ 
  def zero = multiplication.annihilator.get
  /** the "one" for this semiring, must meet all semiring requirements for one */ 
  def one = multiplication.identity.get
}
```

A semiring is constructed with LagSemigroup instances for $(S,\oplus,0)$ and $(S,\otimes,1)$.
The attribute addition contains the $(S,\oplus,0)$ instance and
the attribute multiplication contains the $(S,\otimes,1)$.
The attribute zero and is taken from the identity attribute of the $(S,\oplus,0)$ LagSemigroup instance.
The attribute one is take from the identity attribute of the $(S,\otimes,1)$ LagSemigroup instance.
By construct, the types for addition and multiplication must be the same.
Any class implementing the LagSemiring trait must enforce the following requirements on the
input LagSemigroup instances for addition and multiplication:

* the addition annihilator is None,

* the multiplication annihilator is specified, and,

* the multiplication annihilator is equal to the addition identity.

We use generic classes implementing the LagSemiring trait to define a
library of standard semirings over abstract type.  For example, we
define the standard tropical semiring as:

```scala
def min_plus[T: ClassTag](infinity: T)
      (implicit num: Numeric[T]): LagSemiring[T] = {
    val zero = infinity
    val one = num.zero
    val addition = (a: T, b: T) =>
      num.min(a, b)
    val multiplication = (a: T, b: T) =>
      if (a == zero || b == zero)
        zero
      else
        num.plus(a, b)
    val additionSg = LagSemigroup[T](
      addition,
      identity = Option(zero))
    val multiplicationSg = LagSemigroup[T](
      multiplication,
      annihilator = Option(zero),
      identity = Option(one))
    LagSemiring[T](additionSg, multiplicationSg, zero, one)
  }
```

This definition also exploits the equivalent of Haskell's type class
pattern by using Scala's implicit conversion feature.  This is
achieved in the generic class definition of the min_plus semiring by
specifying _implicit num: Numeric[T]_. This construct permits
the use of any type that has a implicit conversion to Numeric in
scope. The object
[com.ibm.lagraph.LagSemiring](api//index.html#com.ibm.lagraph.LagSemiring$)
contains a collection of
common semirings.  The semirings are named by concatenating the name
of addition operation to the name of the multiplication operation with
an underscore.

In Scala, since numeric primitives like Int, Float and Double are
first-class objects which have implicit conversions to Numeric, they
can be used to instantiate a min_plus semiring.
We can also define custom types with
implicit conversions to Numeric that can also be used for any semiring
defined in this manner.  Basically, by defining our semiring in terms
of a Scala Numeric we get not only the base numeric types but we are
also able to accommodate user-defined custom types.

