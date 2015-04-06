package com.github.mogproject.redismock.entity

sealed trait ValueType

case object STRING extends ValueType

case object LIST extends ValueType

case object SET extends ValueType

case object SORTED_SET extends ValueType

case object HASH extends ValueType

case object BITMAP extends ValueType

case object HYPER_LOG_LOG extends ValueType
