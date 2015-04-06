package com.github.mogproject.redismock.entity


trait Value {
  val valueType: ValueType

  type DataType

  val value: DataType

  def asStringValue: Array[Byte] = valueType match {
    case STRING => value.asInstanceOf[Array[Byte]]
    case _ => throw new RuntimeException("xxx")
  }
}