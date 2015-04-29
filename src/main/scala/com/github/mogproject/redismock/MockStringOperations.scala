package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity.{Bytes, StringValue, Key, STRING}
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.ops._
import com.redis._
import com.redis.serialization.Format
import com.redis.serialization.Parse

import scala.util.Try

trait MockStringOperations extends StringOperations with MockOperations with Storage {
  self: Redis =>

  //
  // helper functions
  //

  private def setRaw(key: Any, value: Bytes)(implicit format: Format): Unit =
    currentDB.update(Key(key), StringValue(value))

  private def setRaw(key: Any, value: Bytes, ttl: Option[Long])(implicit format: Format): Unit =
    currentDB.update(Key(key), StringValue(value), ttl)

  private def getRaw(key: Any)(implicit format: Format): Option[Bytes] =
    currentDB.get(Key(key)).map(_.as(STRING))

  private def getRawOrEmpty(key: Any)(implicit format: Format): Bytes =
    getRaw(key).getOrElse(Bytes.empty)

  private def getTime(time: SecondsOrMillis): Long = time match {
    case Seconds(v) => v * 1000L
    case Millis(v) => v
  }

  // operations for Byte

  /** Convert Byte object to unsigned Int */
  private[this] def b2ui(b: Byte): Int = (256 + b) % 256

  def bitBinOp(op: (Int, Int) => Int)(a: Bytes, b: Bytes): Bytes = {
    val n = math.max(a.length, b.length)
    Bytes(a.resized(n).zip(b.resized(n)) map { case (x, y) => op(x, y).toByte})
  }

  /** Count number of 1-bit */
  private[this] def popByte(x: Byte): Int = {
    val s = (x & 0x11) + ((x >> 1) & 0x11) + ((x >> 2) & 0x11) + ((x >> 3) & 0x11)
    (s & 15) + (s >> 4)
  }

  //
  // operations
  //

  // SET KEY (key, value)
  // sets the key with the specified value.
  override def set(key: Any, value: Any)(implicit format: Format): Boolean =
    true whenTrue withDB {setRaw(key, Bytes(value), None)}

  // SET key value [EX seconds] [PX milliseconds] [NX|XX]
  // set the string value of a key
  // Starting with Redis 2.6.12 SET supports a set of options that modify its behavior:
  // EX seconds -- Set the specified expire time, in seconds.
  // PX milliseconds -- Set the specified expire time, in milliseconds.
  // NX -- Only set the key if it does not already exist.
  // XX -- Only set the key if it already exist.
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

  override def set(key: Any, value: Any, onlyIfExists: Boolean, time: SecondsOrMillis): Boolean = {
    (!exists(key) ^ onlyIfExists) whenTrue {setRaw(key, Bytes(value), Some(getTime(time)))}
  }

  // GET (key)
  // gets the value for the specified key.
  override def get[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] = getRaw(key).map(_.parse(parse))

  // GETSET (key, value)
  // is an atomic set this value and return the old value command.
  override def getset[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    get(key) <| { _ => set(key, value)}

  // SETNX (key, value)
  // sets the value for the specified key, only if the key is not there.
  override def setnx(key: Any, value: Any)(implicit format: Format): Boolean = !exists(key) && set(key, value)

  override def setex(key: Any, expiry: Long, value: Any)(implicit format: Format): Boolean =
    psetex(key, expiry * 1000L, value)

  override def psetex(key: Any, expiryInMillis: Long, value: Any)(implicit format: Format): Boolean =
    true whenTrue setRaw(key, Bytes(value), Some(expiryInMillis))

  // INCR (key)
  // increments the specified key by 1
  override def incr(key: Any)(implicit format: Format): Option[Long] = incrby(key, 1)

  // INCR (key, increment)
  // increments the specified key by increment
  override def incrby(key: Any, increment: Int)(implicit format: Format): Option[Long] = withDB {
    val n = get(key).map { v =>
      Try(v.toLong).getOrElse(throw new RuntimeException("ERR value is not an integer or out of range"))
    }.getOrElse(0L) + increment
    set(key, n)
    Some(n)
  }

  override def incrbyfloat(key: Any, increment: Float)(implicit format: Format): Option[Float] = withDB {
    val n = get(key).map { v =>
      Try(v.toFloat).getOrElse(throw new RuntimeException("ERR value is not a valid float"))
    }.getOrElse(0.0f) + increment
    set(key, n)
    Some(n)
  }

  // DECR (key)
  // decrements the specified key by 1
  override def decr(key: Any)(implicit format: Format): Option[Long] = incrby(key, -1)

  // DECR (key, increment)
  // decrements the specified key by increment
  override def decrby(key: Any, increment: Int)(implicit format: Format): Option[Long] = incrby(key, -increment)

  // MGET (key, key, key, ...)
  // get the values of all the specified keys.
  override def mget[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
    Some((key :: keys.toList).map(k => get(k)))

  // MSET (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Overwrite value if key exists
  override def mset(kvs: (Any, Any)*)(implicit format: Format): Boolean = kvs.forall { case (k, v) => set(k, v)}

  // MSETNX (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Noop if any key exists
  override def msetnx(kvs: (Any, Any)*)(implicit format: Format): Boolean = kvs.forall { case (k, v) => setnx(k, v)}

  // SETRANGE key offset value
  // Overwrites part of the string stored at key, starting at the specified offset,
  // for the entire length of value.
  override def setrange(key: Any, offset: Int, value: Any)(implicit format: Format): Option[Long] = {
    val a = getRawOrEmpty(key)
    val b = format(value)
    val c = a.take(offset) ++ Bytes.fill(math.max(0, offset - a.length))(0.toByte) ++ b ++ a.drop(offset + b.length)
    setRaw(key, c)
    Some(c.length)
  }

  // GETRANGE key start end
  // Returns the substring of the string value stored at key, determined by the offsets
  // start and end (both are inclusive).
  override def getrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]): Option[A] = {
    getRaw(key).map { x =>
      def f(n: Int): Int = if (n < 0) x.length + n else n
      x.slice(f(start), f(end) + 1).parse(parse)
    }
  }

  // STRLEN key
  // gets the length of the value associated with the key
  override def strlen(key: Any)(implicit format: Format): Option[Long] = Some(getRawOrEmpty(key).length)

  // APPEND KEY (key, value)
  // appends the key value with the specified value.
  override def append(key: Any, value: Any)(implicit format: Format): Option[Long] = {
    val xs = getRawOrEmpty(key)
    val ys = xs ++ format(value)
    setRaw(key, ys)
    Some(ys.length)
  }

  // GETBIT key offset
  // Returns the bit value at offset in the string value stored at key
  override def getbit(key: Any, offset: Int)(implicit format: Format): Option[Int] = {
    val (n, m) = (offset / 8, 7 - offset % 8)

    getRaw(key).map { v =>
      if (v.length < n)
        0
      else
        (b2ui(v(n)) >> m) & 1
    }
  }

  // SETBIT key offset value
  // Sets or clears the bit at offset in the string value stored at key
  override def setbit(key: Any, offset: Int, value: Any)(implicit format: Format): Option[Int] = {
    val x = value match {
      case x: Int if x == 0 || x == 1 => x
      case _ => throw new Exception("ERR bit is not an integer or out of range")
    }
    val (n, m) = (offset / 8, 7 - offset % 8)

    val v = getRawOrEmpty(key)
    val old = (b2ui(v.getOrElse(n)) >> m) & 1
    val w: Bytes = v.updated(n, (b2ui(v.getOrElse(n)) & (0xff ^ (1 << m)) | (x << m)).toByte, 0)
    setRaw(key, w)
    Some(old)
  }

  // BITCOUNT key range
  // Count the number of set bits in the given key within the optional range
  override def bitcount(key: Any, range: Option[(Int, Int)] = None)(implicit format: Format): Option[Int] =
    getRaw(key).map(_.value.map(popByte).sum)

  // BITOP op destKey srcKey...
  // Perform a bitwise operation between multiple keys (containing string values) and store the result in the destination key.
  //
  // The BITOP command supports four bitwise operations: AND, OR, XOR and NOT, thus the valid forms to call the command are:
  //
  // BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
  // BITOP OR destkey srckey1 srckey2 srckey3 ... srckeyN
  // BITOP XOR destkey srckey1 srckey2 srckey3 ... srckeyN
  // BITOP NOT destkey srckey
  override def bitop(op: String, destKey: Any, srcKeys: Any*)(implicit format: Format): Option[Int] = {
    val result: Bytes = op.toUpperCase match {
      case "AND" => srcKeys.view.map(getRawOrEmpty).reduceLeft(bitBinOp(_ & _))
      case "OR" => srcKeys.view.map(getRawOrEmpty).reduceLeft(bitBinOp(_ | _))
      case "XOR" => srcKeys.view.map(getRawOrEmpty).reduceLeft(bitBinOp(_ ^ _))
      case "NOT" => Bytes(getRawOrEmpty(srcKeys(0)).map(x => (~x).toByte))
    }
    if (result.isEmpty) {
      del(destKey)
    } else {
      setRaw(destKey, result)
    }
    Some(result.length)
  }
}
