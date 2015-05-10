package com.github.mogproject.redismock.entity


case class HashValue(data: Map[Bytes, Bytes] = Map.empty) extends Value

object HashValue extends ValueCompanion[HashValue] {
  override lazy val empty = HashValue()
}