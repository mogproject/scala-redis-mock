package com.github.mogproject.redismock.entity

import scala.reflect.ClassTag


trait Value {
  def as[A <: Value: ClassTag]: A = this match {
    case v: A => v
    case _ => throw new RuntimeException("Operation against a key holding the wrong kind of value")
  }

  def typeName: String = this match {
    case _: StringValue => "string"
    case _: ListValue => "list"
    case _: SetValue => "set"
    case _: SortedSetValue => "zset"
    case _: HashValue => "hash"
  }
}

trait ValueCompanion[A <: Value] {
  def empty: A
}
