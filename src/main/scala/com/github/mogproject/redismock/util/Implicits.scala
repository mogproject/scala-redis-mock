package com.github.mogproject.redismock.util

object Implicits {
  import scala.language.implicitConversions

  implicit def byteToEnhancedByte(x: Byte): EnhancedByte = new EnhancedByte(x)

  implicit def seqToEnhancedSeq[A](s: Seq[A]): EnhancedSeq[A] = new EnhancedSeq(s)
}
