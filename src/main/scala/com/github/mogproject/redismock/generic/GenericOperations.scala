package com.github.mogproject.redismock.generic

import com.github.mogproject.redismock.entity._
import com.github.mogproject.redismock.storage.Storage
import com.redis.Redis
import com.redis.serialization.Format

import scala.reflect.ClassTag


trait GenericOperations extends Storage {
  self: Redis =>

  protected def setRaw[A <: Value](key: Any, value: A)(implicit format: Format): Unit =
    currentDB.update(Key(key), value)

  protected def setRaw[A <: Value](key: Any, value: A, ttl: Option[Long])(implicit format: Format): Unit =
    currentDB.update(Key(key), value, ttl)

  protected def getRaw[A <: Value: ClassTag](key: Any)
                                            (implicit format: Format, companion: ValueCompanion[A]): Option[A] =
    currentDB.get(Key(key)).map(_.as[A])

  protected def getRawOrEmpty[A <: Value: ClassTag](key: Any)
                                                   (implicit format: Format, companion: ValueCompanion[A]): A =
    getRaw(key).getOrElse(companion.empty)

}
