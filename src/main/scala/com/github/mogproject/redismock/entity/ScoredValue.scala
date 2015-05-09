package com.github.mogproject.redismock.entity

/**
 * value with the orderable score
 *
 * @param score score
 * @param value value
 */
case class ScoredValue[A, B](score: A, value: B)
                            (implicit orderingScore: Ordering[A], orderingValue: Ordering[B])
  extends Ordered[ScoredValue[A, B]] {

  override def compare(that: ScoredValue[A, B]): Int = {
    val x = orderingScore.compare(score, that.score)
    if (x == 0) orderingValue.compare(value, that.value) else x
  }

  override def equals(other: Any): Boolean = canEqual(other) && (other match {
    case that: ScoredValue[A, B] => value == that.value
    case _ => false
  })

  override def hashCode() = value.hashCode()
}
