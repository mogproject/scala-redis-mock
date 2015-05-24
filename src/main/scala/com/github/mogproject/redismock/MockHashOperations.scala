package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity._
import com.github.mogproject.redismock.generic.GenericOperations
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.Bytes
import com.github.mogproject.redismock.util.ops._
import com.redis.{HashOperations, Redis}
import com.redis.serialization._

import scala.util.Try

/**
 * HASH operations
 */
trait MockHashOperations extends HashOperations with MockOperations with Storage with GenericOperations {
  self: Redis =>

  private implicit val companion: ValueCompanion[HashValue] = HashValue

  private def set(key: Any, value: Map[Bytes, Bytes])(implicit format: Format): Unit = setRaw(key, HashValue(value))

  private def getLong(key: Any, field: Any)(implicit format: Format): Option[Long] = hget(key, field).map { v =>
    Try(v.toLong).getOrElse(throw new RuntimeException("ERR hash value is not an integer or out of range"))
  }

  private def getLongOrZero(key: Any, field: Any)(implicit format: Format): Long = getLong(key, field).getOrElse(0L)

  private def getFloat(key: Any, field: Any)(implicit format: Format): Option[Float] = hget(key, field).map { v =>
    Try(v.toFloat).getOrElse(throw new RuntimeException("ERR hash value is not a valid float"))
  }

  private def getFloatOrZero(key: Any, field: Any)(implicit format: Format): Float =
    getFloat(key, field).getOrElse(0.0f)


  /**
   * Sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created. If field
   * already exists in the hash, it is overwritten.
   *
   * @see http://redis.io/commands/hset
   */
  override def hset(key: Any, field: Any, value: Any)(implicit format: Format): Boolean = withDB {
    getRawOrEmpty(key).data <| (h => set(key, h.updated(Bytes(field), Bytes(value)))) |> (_.isEmpty)
  }

  /**
   * Sets field in the hash stored at key to value, only if field does not yet exist. If key does not exist, a new key
   * holding a hash is created. If field already exists, this operation has no effect.
   *
   * @see http://redis.io/commands/hsetnx
   */
  override def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): Boolean = withDB {
    !exists(key) <| (_ => set(key, Map(Bytes(field) -> Bytes(value))))
  }

  /**
   * Returns the value associated with field in the hash stored at key.
   *
   * @see http://redis.io/commands/hget
   */
  override def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    getRaw(key).flatMap(_.data.get(Bytes(field))).map(_.parse(parse))

  /**
   * Sets the specified fields to their respective values in the hash stored at key. This command overwrites any
   * existing fields in the hash. If key does not exist, a new key holding a hash is created.
   *
   * @see http://redis.io/commands/hmset
   */
  override def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): Boolean = withDB {
    val m = map.map { case (k: Any, v: Any) => Bytes(k) -> Bytes(v) }.toMap
    getRawOrEmpty(key).data ++ m <| (set(key, _)) |> (_.isEmpty)
  }

  /**
   * Returns the values associated with the specified fields in the hash stored at key.
   *
   * For every field that does not exist in the hash, a nil value is returned. Because a non-existing keys are treated
   * as empty hashes, running HMGET against a non-existing key will return a list of nil values.
   *
   * @see http://redis.io/commands/hmget
   */
  override def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]): Option[Map[K, V]] =
    getRaw(key).map(m => fields.flatMap(f => m.data.get(Bytes(f)).map(_.parse(parseV)).map(f -> _)).toMap)

  /**
   * Increments the number stored at field in the hash stored at key by increment. If key does not exist, a new key
   * holding a hash is created. If field does not exist the value is set to 0 before the operation is performed.
   *
   * The range of values supported by HINCRBY is limited to 64 bit signed integers.
   *
   * @see http://redis.io/commands/hincrby
   */
  override def hincrby(key: Any, field: Any, value: Int)(implicit format: Format): Option[Long] = withDB {
    getLongOrZero(key, field) + value <| (x => hset(key, field, x)) |> Some.apply
  }

  /**
   * Increment the specified field of an hash stored at key, and representing a floating point number, by the specified
   * increment. If the field does not exist, it is set to 0 before performing the operation. An error is returned if one
   * of the following conditions occur:
   *
   * - The field contains a value of the wrong type (not a string).
   * - The current field content or the specified increment are not parsable as a double precision floating point
   * number.
   *
   * The exact behavior of this command is identical to the one of the INCRBYFLOAT command, please refer to the
   * documentation of INCRBYFLOAT for further information.
   *
   * @see http://redis.io/commands/hincrbyfloat
   */
  override def hincrbyfloat(key: Any, field: Any, value: Float)(implicit format: Format): Option[Float] = withDB {
    getFloatOrZero(key, field) + value <| (x => hset(key, field, x)) |> Some.apply
  }

  /**
   * Returns if field is an existing field in the hash stored at key.
   *
   * @see http://redis.io/commands/hexists
   */
  override def hexists(key: Any, field: Any)(implicit format: Format): Boolean =
    getRawOrEmpty(key).data.contains(Bytes(field))

  /**
   * Removes the specified fields from the hash stored at key. Specified fields that do not exist within this hash are
   * ignored. If key does not exist, it is treated as an empty hash and this command returns 0.
   *
   * @see http://redis.io/commands/hdel
   */
  override def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): Option[Long] = withDB {
    val fs = (field :: fields.toList).map(Bytes.apply)
    val x = getRawOrEmpty(key).data
    val y = x.filterKeys(!fs.contains(_))
    set(key, y)
    Some(x.size - y.size)
  }

  /**
   * Returns the number of fields contained in the hash stored at key.
   *
   * @see http://redis.io/commands/hlen
   */
  override def hlen(key: Any)(implicit format: Format): Option[Long] = getRaw(key).map(_.data.size)

  /**
   * Returns all field names in the hash stored at key.
   *
   * @see http://redis.io/commands/hkeys
   */
  override def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    getRaw(key).map(_.data.keys.toList.map(_.parse(parse)))

  /**
   * Returns all values in the hash stored at key.
   *
   * @see http://redis.io/commands/hvals
   */
  override def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    getRaw(key).map(_.data.values.toList.map(_.parse(parse)))

  /**
   * Returns all fields and values of the hash stored at key. In the returned value, every field name is followed by its
   * value, so the length of the reply is twice the size of the hash.
   *
   * @see http://redis.io/commands/hgetall
   */
  override def hgetall[K, V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[Map[K, V]] =
    getRaw(key).map(_.data.map { case (k, v) => k.parse(parseK) -> v.parse(parseV) })

  /**
   * Incrementally iterate hash fields and associated values (since 2.8)
   *
   * @see http://redis.io/commands/hscan
   */
  override def hscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)
                       (implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
    genericScan(getRawOrEmpty(key).data.toSeq,
      cursor, pattern, count, (kv: (Bytes, Bytes)) => kv._1, (kv : (Bytes, Bytes)) => Seq(kv._1, kv._2))

}
