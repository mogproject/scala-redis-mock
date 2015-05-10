package com.github.mogproject.redismock.entity

import com.github.mogproject.redismock.util.Bytes
import com.redis.serialization.Format

/**
 * key data should be stored as vector to calculate accurate hash code
 * @param k key binary
 */
case class Key(k: Bytes)

object Key {
  def apply(k: Any)(implicit format: Format): Key = new Key(Bytes(k))
}