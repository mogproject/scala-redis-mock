package com.github.mogproject.redismock.entity

import com.github.mogproject.redismock.util.Bytes


case class ListValue(data: Vector[Bytes] = Vector.empty) extends Value

object ListValue extends ValueCompanion[ListValue] {
  def apply(xs: Traversable[Bytes]): ListValue = new ListValue(xs.toVector)

  override lazy val empty = ListValue()
}
