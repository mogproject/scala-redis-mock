package com.github.mogproject.redismock.util

package object ops {

  implicit class IdOps[A](val self: A) extends AnyVal {
    /**
     * pipeline processing
     * @param f function
     * @tparam B return type
     * @return
     */
    final def |>[B](f: A => B): B = f(self)

    /**
     * do the side effect with the value, then return the original
     * @param f function with side effect
     * @return
     */
    final def <|(f: A => Any): A = {
      f(self)
      self
    }
  }

  implicit class BooleanOps(val self: Boolean) extends AnyVal {
    final def whenTrue(f: => Any): Boolean = self <| {
      case true => f
      case false =>
    }

    final def whenFalse(f: => Any): Boolean = self <| {
      case true =>
      case false => f
    }
  }

}
