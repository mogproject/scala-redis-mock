package com.github.mogproject.redismock.util

import scala.util.Try

//implicit def seqToEnhancedSeq

/**
 * Enhanced and safer indexed sequence class
 *
 * @param target base sequence
 * @tparam A type of the element
 */
class EnhancedSeq[A](target: Seq[A]) {

  private def normalized(index: Int): Int = if (index < 0) target.size + index else index

  /**
   * Get element as option.
   *
   * @param index Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
   * @return
   */
  def get(index: Int): Option[A] = Try(target(normalized(index))).toOption

  /**
   * Slice with indexes
   *
   * @param from from, negative indexes are supported
   * @param until until, negative indexes are supported
   * @return
   */
  def sliceFromUntil(from: Int, until: Int): Seq[A] = target.slice(normalized(from), normalized(until))

  /**
   * Slice with indexes including from and to
   *
   * @param from from, negative indexes are supported
   * @param to to, negative indexes are supported
   * @return
   */
  def sliceFromTo(from: Int, to: Int): Seq[A] = target.slice(normalized(from), normalized(to) + 1)
}
