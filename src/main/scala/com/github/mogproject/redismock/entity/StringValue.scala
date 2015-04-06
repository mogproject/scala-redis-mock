package com.github.mogproject.redismock.entity

case class StringValue(value: Array[Byte]) extends Value {
  val valueType = STRING

  type DataType = Array[Byte]

  override def toString = s"StringValue(${new String(value)})"
}
