package com.github.mogproject.redismock.entity


case class StringValue(data: Bytes = Bytes.empty) extends Value

object StringValue extends ValueCompanion[StringValue] {
  override lazy val empty = StringValue()
}
