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

case class SetValue(value: SET.DataType) extends Value { val valueType = SET }

case class HashValue(value: HASH.DataType) extends Value { val valueType = HASH }


object StringValue

object ListValue

object SetValue

object HashValue
