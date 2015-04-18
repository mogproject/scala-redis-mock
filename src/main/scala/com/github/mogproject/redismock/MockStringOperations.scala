package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity.{StringValue, Key}
import com.github.mogproject.redismock.storage.Storage
import com.redis._
import com.redis.serialization.Format
import com.redis.serialization.Parse

import scala.util.Try

trait MockStringOperations extends StringOperations with MockOperations with Storage {
  self: Redis =>

  //
  // helper functinos
  //

  private[this] def set(key: Any, value: Any, ttl: Option[Long])(implicit format: Format): Boolean = {
    currentDB.update(Key(format.apply(key)), StringValue(format.apply(value)), ttl)
    true
  }

  private[this] def getRaw(key: Any)(implicit format: Format): Option[StringValue#DataType] =
    currentDB.get(Key(format.apply(key))).map(_.asStringValue)

  private[this] def getRawOrEmpty(key: Any)(implicit format: Format): StringValue#DataType =
    getRaw(key).getOrElse(Array.empty[Byte])

  // operations for Byte
  private[this] def byte2Int(b: Byte): Int = if (b < 0) 256 + b else b

  /** Return resized n-array padding with zero */
  private[this] def resizeByteArray(a: Array[Byte], n: Int): Array[Byte] =
    a.take(n) ++ Array.fill(n - a.length)(0.toByte)

  private[this] def bitBinOp(op: (Byte, Byte) => Int)(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
    val n = math.max(a.length, b.length)
    resizeByteArray(a, n) zip resizeByteArray(b, n) map { case (x, y) => op(x, y).toByte}
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
  override def set(key: Any, value: Any)(implicit format: Format): Boolean = {
    set(key, value, None)
  }

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
    if (!exists(key) ^ onlyIfExists) {
      val t = time match {
        case Seconds(v) => v * 1000L
        case Millis(v) => v
      }
      set(key, value, Some(t))
    } else {
      false
    }
  }

  // GET (key)
  // gets the value for the specified key.
  override def get[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] = {
    currentDB.get(Key(format.apply(key))).map(x => parse(x.asStringValue))
  }

  // GETSET (key, value)
  // is an atomic set this value and return the old value command.
  override def getset[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    currentDB synchronized {
      val ret = get(key)
      set(key, value)
      ret
    }

  // SETNX (key, value)
  // sets the value for the specified key, only if the key is not there.
  override def setnx(key: Any, value: Any)(implicit format: Format): Boolean = !exists(key) && set(key, value)

  override def setex(key: Any, expiry: Long, value: Any)(implicit format: Format): Boolean =
    psetex(key, expiry * 1000L, value)

  override def psetex(key: Any, expiryInMillis: Long, value: Any)(implicit format: Format): Boolean =
    set(key, value, Some(expiryInMillis))

  // INCR (key)
  // increments the specified key by 1
  override def incr(key: Any)(implicit format: Format): Option[Long] = incrby(key, 1)

  // INCR (key, increment)
  // increments the specified key by increment
  override def incrby(key: Any, increment: Int)(implicit format: Format): Option[Long] = currentDB.synchronized {
    val n = get(key).map { v =>
      Try(v.toLong).getOrElse(throw new RuntimeException("ERR value is not an integer or out of range"))
    }.getOrElse(0L) + increment
    set(key, n)
    Some(n)
  }

  override def incrbyfloat(key: Any, increment: Float)(implicit format: Format): Option[Float] =
    currentDB.synchronized {
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
  override def setrange(key: Any, offset: Int, value: Any)(implicit format: Format): Option[Long] =
    currentDB.synchronized {
      val a = getRawOrEmpty(key)
      val b = format(value)
      val c = a.take(offset) ++ Array.fill(math.max(0, offset - a.length))(0.toByte) ++ b ++ a.drop(offset + b.length)
      set(key, c)
      Some(c.length)
    }

  // GETRANGE key start end
  // Returns the substring of the string value stored at key, determined by the offsets
  // start and end (both are inclusive).
  override def getrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]): Option[A] = {
    getRaw(key).map { x =>
      def f(n: Int): Int = if (n < 0) x.length + n else n
      parse(x.slice(f(start), f(end) + 1))
    }
  }

  // STRLEN key
  // gets the length of the value associated with the key
  override def strlen(key: Any)(implicit format: Format): Option[Long] =
    Some(getRaw(key).map(_.length.toLong).getOrElse(0L))

  // APPEND KEY (key, value)
  // appends the key value with the specified value.
  override def append(key: Any, value: Any)(implicit format: Format): Option[Long] = {
    val xs = getRawOrEmpty(key)
    val ys = xs ++ format(value)
    set(key, ys)
    Some(ys.length)
  }

  // GETBIT key offset
  // Returns the bit value at offset in the string value stored at key
  override def getbit(key: Any, offset: Int)(implicit format: Format): Option[Int] = {
    val (n, m) = (offset / 8, offset % 8)

    getRaw(key).map { v =>
      if (v.length < n)
        0
      else
        (byte2Int(v(n)) >> m) & 1
    }
  }

  // SETBIT key offset value
  // Sets or clears the bit at offset in the string value stored at key
  override def setbit(key: Any, offset: Int, value: Any)(implicit format: Format): Option[Int] = {
    val x = value match {
      case x: Int if x == 0 || x == 1 => x
    }
    val (n, m) = (offset / 8, offset % 8)

    val v = getRawOrEmpty(key)
    val u = v.clone() ++ Array.fill(n + 1 - v.length)(0.toByte)
    val old = (byte2Int(u(n)) >> m) & 1
    u(n) = (byte2Int(u(n)) & (0xff ^ (1 << m)) | (x << m)).toByte
    set(key, u)
    Some(old)
  }

  // BITCOUNT key range
  // Count the number of set bits in the given key within the optional range
  override def bitcount(key: Any, range: Option[(Int, Int)] = None)(implicit format: Format): Option[Int] =
    getRaw(key).map(_.map(popByte).sum)

  // BITOP op destKey srcKey...
  // Perform a bitwise operation between multiple keys (containing string values) and store the result in the destination key.
  //
  // The BITOP command supports four bitwise operations: AND, OR, XOR and NOT, thus the valid forms to call the command are:
  //
  // BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
  // BITOP OR destkey srckey1 srckey2 srckey3 ... srckeyN
  // BITOP XOR destkey srckey1 srckey2 srckey3 ... srckeyN
  // BITOP NOT destkey srckey
  override def bitop(op: String, destKey: Any, srcKeys: Any*)(implicit format: Format): Option[Int] =
    currentDB.synchronized {
      val result: Array[Byte] = op match {
        case "AND" => srcKeys.view.map(getRawOrEmpty).reduceLeft(bitBinOp(_ & _))
        case "OR" => srcKeys.view.map(getRawOrEmpty).reduceLeft(bitBinOp(_ | _))
        case "XOR" => srcKeys.view.map(getRawOrEmpty).reduceLeft(bitBinOp(_ ^ _))
        case "NOT" => getRawOrEmpty(srcKeys(0)).map(b => (~b).toByte)
      }
      if (result.isEmpty) {
        del(destKey)
      } else {
        set(destKey, result)
      }
      Some(result.length)
    }
}
