package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity._
import com.github.mogproject.redismock.generic.GenericOperations
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.Bytes
import com.github.mogproject.redismock.util.ops._
import com.github.mogproject.redismock.util.Implicits._
import com.redis._
import com.redis.serialization.Format
import com.redis.serialization.Parse

import scala.util.Try

/**
 * STRING operations
 */
trait MockStringOperations
  extends StringOperations with MockOperations with Storage with GenericOperations {

  self: Redis =>

  private implicit val companion: ValueCompanion[StringValue] = StringValue

  private def getLong(key: Any)(implicit format: Format): Option[Long] = get(key).map { v =>
    Try(v.toLong).getOrElse(throw new RuntimeException("ERR value is not an integer or out of range"))
  }

  private def getLongOrZero(key: Any)(implicit format: Format): Long = getLong(key).getOrElse(0L)

  private def getFloat(key: Any)(implicit format: Format): Option[Float] = get(key).map { v =>
    Try(v.toFloat).getOrElse(throw new RuntimeException("ERR value is not a valid float"))
  }

  private def getFloatOrZero(key: Any)(implicit format: Format): Float = getFloat(key).getOrElse(0.0f)

  private def bitBinOp(op: (Int, Int) => Int)(a: Bytes, b: Bytes): Bytes = {
    val n = math.max(a.length, b.length)
    Bytes(a.resized(n).zip(b.resized(n)) map { case (x, y) => op(x, y).toByte })
  }


  /**
   * Set key to hold the string value. If key already holds a value, it is overwritten, regardless of its type. Any
   * previous time to live associated with the key is discarded on successful SET operation.
   *
   * @see http://redis.io/commands/set
   */
  override def set(key: Any, value: Any)(implicit format: Format): Boolean = withDB {
    true whenTrue setRaw(key, StringValue(Bytes(value)), None)
  }

  /**
   * SET key value [EX seconds] [PX milliseconds] [NX|XX]
   * set the string value of a key
   * Starting with Redis 2.6.12 SET supports a set of options that modify its behavior:
   * EX seconds -- Set the specified expire time, in seconds.
   * PX milliseconds -- Set the specified expire time, in milliseconds.
   * NX -- Only set the key if it does not already exist.
   * XX -- Only set the key if it already exist.
   */
  @deprecated("Use the more typesafe variant", "2.14")
  override def set(key: Any, value: Any, nxxx: Any, expx: Any, time: Long): Boolean = {
    val onlyIfExists = nxxx match {
      case s: String => s.toLowerCase == "xx"
    }
    val t = expx match {
      case s: String => s.toLowerCase match {
        case "ex" => Seconds(time)
        case "px" => Millis(time)
      }
    }
    set(key, value, onlyIfExists, t)
  }

  /**
   * SET with options (deprecated)
   *
   * Starting with Redis 2.6.12 SET supports a set of options that modify its behavior:
   *
   * - EX seconds -- Set the specified expire time, in seconds.
   * - PX milliseconds -- Set the specified expire time, in milliseconds.
   * - NX -- Only set the key if it does not already exist.
   * - XX -- Only set the key if it already exist.
   *
   * @see http://redis.io/commands/set
   */
  override def set(key: Any, value: Any, onlyIfExists: Boolean, time: SecondsOrMillis): Boolean = withDB {
    (!exists(key) ^ onlyIfExists) whenTrue {
      setRaw(key, StringValue(Bytes(value)), Some(getTime(time)))
    }
  }

  /**
   * Get the value of key. If the key does not exist the special value nil is returned. An error is returned if the
   * value stored at key is not a string, because GET only handles string values.
   *
   * @see http://redis.io/commands/get
   */
  override def get[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    getRaw(key).map(_.data.parse(parse))

  /**
   * Atomically sets key to value and returns the old value stored at key. Returns an error when key exists but does not
   * hold a string value.
   *
   * @see http://redis.io/commands/getset
   */
  override def getset[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]): Option[A] = withDB {
    get(key) <| { _ => set(key, value) }
  }

  /**
   * Set key to hold string value if key does not exist. In that case, it is equal to SET. When key already holds a
   * value, no operation is performed. SETNX is short for "SET if N ot e X ists".
   *
   * @see http://redis.io/commands/setnx
   */
  override def setnx(key: Any, value: Any)(implicit format: Format): Boolean = withDB {
    !exists(key) whenTrue set(key, value)
  }

  /**
   * Set key to hold the string value and set key to timeout after a given number of seconds.
   *
   * @see http://redis.io/commands/setex
   */
  override def setex(key: Any, expiry: Long, value: Any)(implicit format: Format): Boolean =
    psetex(key, expiry * 1000L, value)

  /**
   * PSETEX works exactly like SETEX with the sole difference that the expire time is specified in milliseconds instead of seconds.
   *
   * @see http://redis.io/commands/psetex
   */
  override def psetex(key: Any, expiryInMillis: Long, value: Any)(implicit format: Format): Boolean =
    true whenTrue setRaw(key, StringValue(Bytes(value)), Some(expiryInMillis))

  /**
   * Increments the number stored at key by one. If the key does not exist, it is set to 0 before performing the
   * operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be
   * represented as integer. This operation is limited to 64 bit signed integers.
   *
   * @see http://redis.io/commands/incr
   */
  override def incr(key: Any)(implicit format: Format): Option[Long] = incrby(key, 1)

  /**
   * Increments the number stored at key by increment. If the key does not exist, it is set to 0 before performing the
   * operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be
   * represented as integer. This operation is limited to 64 bit signed integers.
   *
   * @see http://redis.io/commands/incrby
   */
  override def incrby(key: Any, increment: Int)(implicit format: Format): Option[Long] = withDB {
    getLongOrZero(key) + increment <| { x => set(key, x) } |> Some.apply
  }

  /**
   * Increment the string representing a floating point number stored at key by the specified increment. If the key does
   * not exist, it is set to 0 before performing the operation. An error is returned if one of the following conditions
   * occur:
   *
   * - The key contains a value of the wrong type (not a string).
   * - The current key content or the specified increment are not parsable as a double precision floating point number.
   *
   * If the command is successful the new incremented value is stored as the new value of the key (replacing the old
   * one), and returned to the caller as a string.
   *
   * Both the value already contained in the string key and the increment argument can be optionally provided in
   * exponential notation, however the value computed after the increment is stored consistently in the same format,
   * that is, an integer number followed (if needed) by a dot, and a variable number of digits representing the decimal
   * part of the number. Trailing zeroes are always removed.
   *
   * The precision of the output is fixed at 17 digits after the decimal point regardless of the actual internal
   * precision of the computation.
   *
   * @see http://redis.io/commands/incrbyfloat
   */
  override def incrbyfloat(key: Any, increment: Float)(implicit format: Format): Option[Float] = withDB {
    getFloatOrZero(key) + increment <| { x => set(key, x) } |> Some.apply
  }

  /**
   * Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the
   * operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be
   * represented as integer. This operation is limited to 64 bit signed integers.
   *
   * @see http://redis.io/commands/decr
   */
  override def decr(key: Any)(implicit format: Format): Option[Long] = incrby(key, -1)

  /**
   * Decrements the number stored at key by decrement. If the key does not exist, it is set to 0 before performing the
   * operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be
   * represented as integer. This operation is limited to 64 bit signed integers.
   *
   * @see http://redis.io/commands/decrby
   */
  override def decrby(key: Any, increment: Int)(implicit format: Format): Option[Long] = incrby(key, -increment)

  /**
   * Returns the values of all specified keys. For every key that does not hold a string value or does not exist, the
   * special value nil is returned. Because of this, the operation never fails.
   *
   * @see http://redis.io/commands/mget
   */
  override def mget[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
    Some((key :: keys.toList).map(k => get(k)))

  /**
   * Sets the given keys to their respective values. MSET replaces existing values with new values, just as regular SET.
   * See MSETNX if you don't want to overwrite existing values.
   *
   * MSET is atomic, so all given keys are set at once. It is not possible for clients to see that some of the keys were
   * updated while others are unchanged.
   *
   * @see http://redis.io/commands/mset
   */
  override def mset(kvs: (Any, Any)*)(implicit format: Format): Boolean = withDB {
    kvs.forall { case (k, v) => set(k, v) }
  }

  /**
   * Sets the given keys to their respective values. MSETNX will not perform any operation at all even if just a single
   * key already exists.
   *
   * Because of this semantic MSETNX can be used in order to set different keys representing different fields of an
   * unique logic object in a way that ensures that either all the fields or none at all are set.
   *
   * MSETNX is atomic, so all given keys are set at once. It is not possible for clients to see that some of the keys
   * were updated while others are unchanged.
   *
   * @see http://redis.io/commands/msetnx
   */
  override def msetnx(kvs: (Any, Any)*)(implicit format: Format): Boolean = withDB {
    kvs.forall { case (k, v) => setnx(k, v) }
  }

  /**
   * Overwrites part of the string stored at key, starting at the specified offset, for the entire length of value. If
   * the offset is larger than the current length of the string at key, the string is padded with zero-bytes to make
   * offset fit. Non-existing keys are considered as empty strings, so this command will make sure it holds a string
   * large enough to be able to set value at offset.
   *
   * @see http://redis.io/commands/setrange
   */
  override def setrange(key: Any, offset: Int, value: Any)(implicit format: Format): Option[Long] = withDB {
    def f(v: Bytes) = getRawOrEmpty(key).data.resized(offset).patch(offset, v, v.length)
    (Bytes(value) |> f |> Bytes.apply) <| { xs => setRaw(key, StringValue(xs)) } |> { xs => Some(xs.length) }
  }

  /**
   * Returns the substring of the string value stored at key, determined by the offsets start and end (both are
   * inclusive). Negative offsets can be used in order to provide an offset starting from the end of the string. So -1
   * means the last character, -2 the penultimate and so forth.
   *
   * The function handles out of range requests by limiting the resulting range to the actual length of the string.
   *
   * @see http://redis.io/commands/getrange
   */
  override def getrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]): Option[A] =
    getRaw(key).map(_.data.sliceFromTo(start, end)).map(xs => Bytes(xs).parse(parse))

  /**
   * Returns the length of the string value stored at key. An error is returned when key holds a non-string value.
   *
   * @see http://redis.io/commands/strlen
   */
  override def strlen(key: Any)(implicit format: Format): Option[Long] = Some(getRawOrEmpty(key).data.length)

  /**
   * If key already exists and is a string, this command appends the value at the end of the string. If key does not
   * exist it is created and set as an empty string, so APPEND will be similar to SET in this special case.
   *
   * @see http://redis.io/commands/append
   */
  override def append(key: Any, value: Any)(implicit format: Format): Option[Long] = withDB {
    getRawOrEmpty(key).data ++ Bytes(value) <| { xs => setRaw(key, StringValue(xs)) } |> { xs => Some(xs.length) }
  }

  /**
   * Returns the bit value at offset in the string value stored at key.
   *
   * When offset is beyond the string length, the string is assumed to be a contiguous space with 0 bits. When key does
   * not exist it is assumed to be an empty string, so offset is always out of range and the value is also assumed to be
   * a contiguous space with 0 bits.
   *
   * @see http://redis.io/commands/getbit
   */
  override def getbit(key: Any, offset: Int)(implicit format: Format): Option[Int] = {
    val (n, m) = (offset / 8, 7 - offset % 8)
    getRaw(key).map { v => if (v.data.length < n) 0 else (v.data(n).toUnsignedInt >> m) & 1 }
  }

  /**
   * Sets or clears the bit at offset in the string value stored at key.
   *
   * The bit is either set or cleared depending on value, which can be either 0 or 1. When key does not exist, a new
   * string value is created. The string is grown to make sure it can hold a bit at offset. The offset argument is
   * required to be greater than or equal to 0, and smaller than 2**32 (this limits bitmaps to 512MB). When the string
   * at key is grown, added bits are set to 0.
   *
   * @see http://redis.io/commands/setbit
   */
  override def setbit(key: Any, offset: Int, value: Any)(implicit format: Format): Option[Int] = withDB {
    val x = value match {
      case x: Int if x == 0 || x == 1 => x
      case _ => throw new Exception("ERR bit is not an integer or out of range")
    }
    val (n, m) = (offset / 8, 7 - offset % 8)

    val v = getRawOrEmpty(key).data
    val old = (v.getOrElse(n).toUnsignedInt >> m) & 1
    val w: Bytes = v.updated(n, (v.getOrElse(n).toUnsignedInt & (0xff ^ (1 << m)) | (x << m)).toByte, 0)
    setRaw(key, StringValue(w))
    Some(old)
  }

  /**
   * Count the number of set bits (population counting) in a string.
   *
   * By default all the bytes contained in the string are examined. It is possible to specify the counting operation
   * only in an interval passing the additional arguments start and end.
   *
   * Like for the GETRANGE command start and end can contain negative values in order to index bytes starting from the
   * end of the string, where -1 is the last byte, -2 is the penultimate, and so forth.
   *
   * Non-existent keys are treated as empty strings, so the command will return zero.
   *
   * @see http://redis.io/commands/bitcount
   */
  override def bitcount(key: Any, range: Option[(Int, Int)] = None)(implicit format: Format): Option[Int] =
    getRaw(key).map(_.data.value.map(_.popCount).sum)

  /**
   * Perform a bitwise operation between multiple keys (containing string values) and store the result in the
   * destination key.
   *
   * The BITOP command supports four bitwise operations: AND, OR, XOR and NOT, thus the valid forms to call the command
   * are:
   *
   * - BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
   * - BITOP OR destkey srckey1 srckey2 srckey3 ... srckeyN
   * - BITOP XOR destkey srckey1 srckey2 srckey3 ... srckeyN
   * - BITOP NOT destkey srckey
   *
   * As you can see NOT is special as it only takes an input key, because it performs inversion of bits so it only makes
   * sense as an unary operator.
   *
   * The result of the operation is always stored at destkey.
   *
   * @see http://redis.io/commands/bitop
   */
  override def bitop(op: String, destKey: Any, srcKeys: Any*)(implicit format: Format): Option[Int] = withDB {
    val result: Bytes = op.toUpperCase match {
      case "AND" => srcKeys.view.map(getRawOrEmpty(_).data).reduceLeft(bitBinOp(_ & _))
      case "OR" => srcKeys.view.map(getRawOrEmpty(_).data).reduceLeft(bitBinOp(_ | _))
      case "XOR" => srcKeys.view.map(getRawOrEmpty(_).data).reduceLeft(bitBinOp(_ ^ _))
      case "NOT" => Bytes(getRawOrEmpty(srcKeys(0)).data.map(x => (~x).toByte))
    }
    result <| { r => if (r.isEmpty) del(destKey) else setRaw(destKey, StringValue(r)) } |> { xs => Some(xs.length) }
  }
}
