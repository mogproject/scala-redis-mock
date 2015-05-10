package com.github.mogproject.redismock.entity


case class SetValue(data: Set[Bytes] = Set.empty) extends Value

object SetValue extends ValueCompanion[SetValue] {
  override lazy val empty = SetValue()
}
