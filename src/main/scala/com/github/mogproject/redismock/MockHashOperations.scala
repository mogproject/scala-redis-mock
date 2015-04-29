package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity.{Bytes, HASH, Key, HashValue}
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.ops._
import com.redis.{HashOperations, Redis}
import com.redis.serialization._
import com.redis.serialization.Parse.parseDefault

import scala.util.Try


trait MockHashOperations extends HashOperations with MockOperations with Storage {
  self: Redis =>

  private def setRaw(key: Any, rawValue: Map[Bytes, Bytes])(implicit format: Format): Unit = {
    currentDB.update(Key(key), HashValue(rawValue))
  }

  private def getRaw(key: Any)(implicit format: Format): Option[HASH.DataType] =
    currentDB.get(Key(key)).map(_.as(HASH))

  private def getRawOrEmpty(key: Any)(implicit format: Format): HASH.DataType =
    getRaw(key).getOrElse(Map.empty[Bytes, Bytes])

  private def getLong(key: Any, field: Any)(implicit format: Format): Option[Long] = hget(key, field).map { v =>
    Try(v.toLong).getOrElse(throw new RuntimeException("ERR hash value is not an integer or out of range"))
  }

  private def getLongOrZero(key: Any, field: Any)(implicit format: Format): Long = getLong(key, field).getOrElse(0L)

  private def getFloat(key: Any, field: Any)(implicit format: Format): Option[Float] = hget(key, field).map { v =>
    Try(v.toFloat).getOrElse(throw new RuntimeException("ERR hash value is not a valid float"))
  }

  private def getFloatOrZero(key: Any, field: Any)(implicit format: Format): Float =
    getFloat(key, field).getOrElse(0.0f)


  override def hset(key: Any, field: Any, value: Any)(implicit format: Format): Boolean = withDB {
    getRawOrEmpty(key) <| { h => setRaw(key, h.updated(Bytes(field), Bytes(value)))} |> {_.isEmpty}
  }

  override def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): Boolean = withDB {
    !exists(key) <| { _ => setRaw(key, Map(Bytes(field) -> Bytes(value)))}
  }

  override def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    getRaw(key).flatMap(_.get(Bytes(field))).map(_.parse(parse))

  override def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): Boolean = withDB {
    val m = map.map { case (k: Any, v: Any) => Bytes(k) -> Bytes(v)}.toMap
    getRawOrEmpty(key) ++ m <| {setRaw(key, _)} |> {_.isEmpty}
  }

  override def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]): Option[Map[K, V]] =
    getRaw(key).map(m => fields.flatMap(f => m.get(Bytes(f)).map(_.parse(parseV)).map(f -> _)).toMap)

  override def hincrby(key: Any, field: Any, value: Int)(implicit format: Format): Option[Long] = withDB {
    getLongOrZero(key, field) + value <| { x => hset(key, field, x)} |> Some.apply
  }

  override def hincrbyfloat(key: Any, field: Any, value: Float)(implicit format: Format): Option[Float] = withDB {
    getFloatOrZero(key, field) + value <| { x => hset(key, field, x)} |> Some.apply
  }

  override def hexists(key: Any, field: Any)(implicit format: Format): Boolean =
    getRawOrEmpty(key).contains(Bytes(field))

  override def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): Option[Long] = withDB {
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
    getRaw(key).map(_.map { case (k, v) => k.parse(parseK) -> v.parse(parseV)})

  // HSCAN
  // Incrementally iterate hash fields and associated values (since 2.8)
  override def hscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
    ???
}
