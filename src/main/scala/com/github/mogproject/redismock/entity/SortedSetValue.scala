package com.github.mogproject.redismock.entity

import scala.collection.{GenTraversableOnce, SortedSet}


case class SortedSetValue(value: SORTED_SET.DataType) extends Value {
  val valueType = SORTED_SET

  val (data, index) = value

  def size: Int = index.size

  def updated(score: Double, value: Bytes): SortedSetValue = this + (score -> value)

  def +(sv: (Double, Bytes)): SortedSetValue = {
    val (score, value) = sv
    val newData = subtractData(value) + ((score, value))
    val newIndex = index.updated(value, score)
    copy(value = (newData, newIndex))
  }

  def ++(xs: GenTraversableOnce[(Double, Bytes)]): SortedSetValue = xs.foldLeft(this)(_ + _)

  def -(value: Bytes): SortedSetValue = {
    copy(value = (subtractData(value), index - value))
  }

  def --(xs: GenTraversableOnce[Bytes]): SortedSetValue = xs.foldLeft(this)(_ - _)

  /** find the 0-indexed rank of the specified value */
  def rank(value: Bytes): Option[Int] = data.zipWithIndex.find(_._1._2 == value).map(_._2)

  private def subtractData(value: Bytes): SortedSet[(Double, Bytes)] = data -- index.get(value).map((_, value))

}

object SortedSetValue {
  lazy val empty = new SortedSetValue((SortedSet.empty, Map.empty))

  def apply(xs: (Double, Bytes)*): SortedSetValue = empty ++ xs
}
