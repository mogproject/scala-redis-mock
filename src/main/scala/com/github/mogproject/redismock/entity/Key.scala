package com.github.mogproject.redismock.entity

case class Key(k: Seq[Byte])

object Key {
  def apply(k: Array[Byte]) = new Key(k.toSeq)
}