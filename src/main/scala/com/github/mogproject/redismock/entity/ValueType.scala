package com.github.mogproject.redismock.entity

sealed trait ValueType {
  type DataType
}

case object STRING extends ValueType { type DataType = Bytes }

case object LIST extends ValueType { type DataType = Vector[Bytes] }

case object SET extends ValueType { type DataType = Set[Bytes] }

case object SORTED_SET extends ValueType { type DataType = Set[Bytes] } // FIXME

case object HASH extends ValueType { type DataType = Map[Bytes, Bytes] }

case object BITMAP extends ValueType { type DataType = Bytes } // FIXME

case object HYPER_LOG_LOG extends ValueType { type DataType = Bytes } // FIXME
