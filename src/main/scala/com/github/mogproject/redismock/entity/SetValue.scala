package com.github.mogproject.redismock.entity

import com.github.mogproject.redismock.util.Bytes


case class SetValue(data: Set[Bytes] = Set.empty) extends Value

object SetValue extends ValueCompanion[SetValue] {
  override lazy val empty = SetValue()
}
