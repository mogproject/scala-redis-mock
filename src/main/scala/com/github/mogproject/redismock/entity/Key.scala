package com.github.mogproject.redismock.entity

import com.redis.serialization.Format

/**
 * key data should be stored as vector to calculate accurate hash code
 * @param k key binary
 */
case class Key(k: Vector[Byte]) {
}

object Key {
  def apply(k: Array[Byte]) = new Key(k.toVector)

  def apply(k: Any)(implicit format: Format): Key = apply(format(k))
}