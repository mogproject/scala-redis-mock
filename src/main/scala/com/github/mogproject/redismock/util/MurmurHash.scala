package com.github.mogproject.redismock.util

import com.github.mogproject.redismock.util.Implicits._


object MurmurHash {

  private val defaultSeed = 0xadc83b19L

  def murmurHash64A(data: Seq[Byte], seed: Long = defaultSeed): Long = {
    val m = 0xc6a4a7935bd1e995L
    val r = 47

    val f: Long => Long = m.*
    val g: Long => Long = x => x ^ (x >>> r)

    val h = data.grouped(8).foldLeft(seed ^ f(data.length)) { case (y, xs) =>
      val k = xs.foldRight(0L)((b, x) => (x << 8) + b.toUnsignedLong)
      val j: Long => Long = if (xs.length == 8) f compose g compose f else identity
      f(y ^ j(k))
    }
    (g compose f compose g)(h)
  }

}
