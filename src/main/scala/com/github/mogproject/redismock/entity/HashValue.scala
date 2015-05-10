package com.github.mogproject.redismock.entity

import com.github.mogproject.redismock.util.Bytes


case class HashValue(data: Map[Bytes, Bytes] = Map.empty) extends Value

object HashValue extends ValueCompanion[HashValue] {
  override lazy val empty = HashValue()
}