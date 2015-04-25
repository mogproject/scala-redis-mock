package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity.{Bytes, HASH, Key, HashValue}
import com.github.mogproject.redismock.storage.Storage
import com.redis.{HashOperations, Redis}
import com.redis.serialization._
import com.redis.serialization.Parse.parseDefault

import scala.util.Try


trait MockHashOperations extends HashOperations with MockOperations with Storage {
  self: Redis =>

  private[this] def setRaw(key: Any, rawValue: Map[Bytes, Bytes])(implicit format: Format): Unit = {
    currentDB.update(Key(key), HashValue(rawValue))
  }

  private[this] def getRaw(key: Any)(implicit format: Format): Option[HASH.DataType] =
    currentDB.get(Key(format.apply(key))).map(_.as(HASH))

  private[this] def getRawOrEmpty(key: Any)(implicit format: Format): HASH.DataType =
    getRaw(key).getOrElse(Map.empty[Bytes, Bytes])


  override def hset(key: Any, field: Any, value: Any)(implicit format: Format): Boolean = currentDB.synchronized {
    val m = getRawOrEmpty(key)
    setRaw(key, m.updated(Bytes(field), Bytes(value)))
    m.isEmpty
  }

  override def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): Boolean = currentDB.synchronized {
    getRaw(key) match {
      case Some(_) => false
      case None =>
        setRaw(key, Map(Bytes(field) -> Bytes(value)))
        true
    }
  }

  override def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    getRaw(key).flatMap(_.get(Bytes(field))).map(_.parse(parse))

  override def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): Boolean =
    currentDB.synchronized {
      val m = getRawOrEmpty(key)
      setRaw(key, m ++ map.map { case (k, v) => Bytes(k) -> Bytes(v) }.toMap)
      m.isEmpty
    }

  override def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]): Option[Map[K, V]] =
    getRaw(key).map(m => fields.flatMap(f => m.get(Bytes(f)).map(_.parse(parseV)).map(f -> _)).toMap)

  override def hincrby(key: Any, field: Any, value: Int)(implicit format: Format): Option[Long] =
    currentDB.synchronized {
      val n = (for {
        m <- getRaw(key)
        v <- m.get(Bytes(field))
      } yield {
          Try(v.parse(parseDefault).toLong).getOrElse(throw new RuntimeException("ERR hash value is not an integer or out of range"))
        }).getOrElse(0L) + value
      hset(key, field, n)
      Some(n)
    }

  override def hincrbyfloat(key: Any, field: Any, value: Float)(implicit format: Format): Option[Float] =
    currentDB.synchronized {
      val n = (for {
        m <- getRaw(key)
        v <- m.get(Bytes(field))
      } yield {
          Try(v.parse(parseDefault).toFloat).getOrElse(throw new RuntimeException("ERR hash value is not a valid float"))
        }).getOrElse(0.0f) + value
      hset(key, field, n)
      Some(n)
    }

  override def hexists(key: Any, field: Any)(implicit format: Format): Boolean =
    getRawOrEmpty(key).contains(Bytes(field))

  override def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): Option[Long] =
    currentDB.synchronized {
      val fs = (field :: fields.toList).map(Bytes.apply)
      val x = getRawOrEmpty(key)
      val y = x.filterKeys(!fs.contains(_))
      setRaw(key, y)
      Some(x.size - y.size)
    }

  override def hlen(key: Any)(implicit format: Format): Option[Long] = getRaw(key).map(_.size)

  override def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    getRaw(key).map(_.keys.toList.map(_.parse(parse)))

  override def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    getRaw(key).map(_.values.toList.map(_.parse(parse)))

  override def hgetall[K, V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[Map[K, V]] =
    getRaw(key).map(_.map { case (k, v) => k.parse(parseK) -> v.parse(parseV) })

  // HSCAN
  // Incrementally iterate hash fields and associated values (since 2.8)
  override def hscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
    ???
}
