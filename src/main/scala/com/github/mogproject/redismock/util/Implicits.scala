package com.github.mogproject.redismock.util

object Implicits {
  import scala.language.implicitConversions

  implicit def seqToEnhancedSeq[A](s: Seq[A]): EnhancedSeq[A] = new EnhancedSeq(s)
}
