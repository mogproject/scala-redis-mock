package com.github.mogproject.redismock

import com.redis.{Redis, EvalOperations}
import com.redis.serialization.{Format, Parse}


trait MockEvalOperations extends EvalOperations {
  self: Redis =>

  // EVAL
  // evaluates lua code on the server.
  override def evalMultiBulk[A](luaCode: String, keys: List[Any], args: List[Any])
                               (implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
    throw new UnsupportedOperationException("scripting is not supported")

  override def evalBulk[A](luaCode: String, keys: List[Any], args: List[Any])
                          (implicit format: Format, parse: Parse[A]): Option[A] =
    throw new UnsupportedOperationException("scripting is not supported")

  override def evalMultiSHA[A](shahash: String, keys: List[Any], args: List[Any])
                              (implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
    throw new UnsupportedOperationException("scripting is not supported")

  override def evalSHA[A](shahash: String, keys: List[Any], args: List[Any])
                         (implicit format: Format, parse: Parse[A]): Option[A] =
    throw new UnsupportedOperationException("scripting is not supported")

  override def scriptLoad(luaCode: String): Option[String] =
    throw new UnsupportedOperationException("scripting is not supported")

  override def scriptExists(shahash: String): Option[Int] =
    throw new UnsupportedOperationException("scripting is not supported")

  override def scriptFlush: Option[String] =
    throw new UnsupportedOperationException("scripting is not supported")

}

