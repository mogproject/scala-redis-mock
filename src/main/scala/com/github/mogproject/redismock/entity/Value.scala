package com.github.mogproject.redismock.entity

import com.redis.serialization.Format


trait Value {
  val valueType: ValueType

  val value: valueType.DataType

  def as[A <: ValueType](vt: A): A#DataType = {
    if (valueType == vt) {
      value.asInstanceOf[A#DataType]
    } else {
      throw new RuntimeException("Operation against a key holding the wrong kind of value")
    }
  }
}


case class StringValue(value: STRING.DataType) extends Value { val valueType = STRING }

case class ListValue(value: LIST.DataType) extends Value { val valueType = LIST }


object StringValue {
  def apply(value: Array[Byte]): StringValue = new StringValue(Bytes(value))

  def apply(value: Any)(implicit format: Format): StringValue = apply(format(value))
}

object ListValue {
  def apply(value: Traversable[Array[Byte]]): ListValue = new ListValue(value.map(Bytes.apply).toVector)

  def apply(value: Traversable[Any])(implicit format: Format): ListValue = apply(value.map(format.apply))
}
