package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity.{Bytes, LIST, Key, ListValue}
import com.github.mogproject.redismock.storage.Storage
import com.redis.{Redis, ListOperations}
import com.redis.serialization._
import com.github.mogproject.redismock.util.Implicits._
import scala.annotation.tailrec

trait MockListOperations extends ListOperations with MockOperations with Storage {
  self: Redis =>

  //
  // helper functions
  //

  private def setRaw(key: Any, rawValue: Traversable[Bytes])(implicit format: Format): Unit = {
    currentDB.update(Key(key), ListValue(rawValue.toVector))
  }

  private def getRaw(key: Any)(implicit format: Format): Option[LIST.DataType] = currentDB.get(Key(key)).map(_.as(LIST))

  private def getRawOrEmpty(key: Any)(implicit format: Format): LIST.DataType =
    getRaw(key).getOrElse(Vector.empty[Bytes])

  // LPUSH (Variadic: >= 2.4)
  // add values to the head of the list stored at key
  override def lpush(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Long] = withDB {
    val v = (value :: values.toList).map(Bytes.apply).toVector ++ getRawOrEmpty(key)
    setRaw(key, v)
    Some(v.size)
  }

  // LPUSHX (Variadic: >= 2.4)
  // add value to the head of the list stored at key
  override def lpushx(key: Any, value: Any)(implicit format: Format): Option[Long] = lpush(key, value)

  // RPUSH (Variadic: >= 2.4)
  // add values to the tail of the list stored at key
  override def rpush(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Long] = withDB {
    val v = getRawOrEmpty(key) ++ (value :: values.toList).map(Bytes.apply).toVector
    setRaw(key, v)
    Some(v.size)
  }

  // RPUSHX (Variadic: >= 2.4)
  // add value to the tail of the list stored at key
  override def rpushx(key: Any, value: Any)(implicit format: Format): Option[Long] = rpush(key, value)

  // LLEN
  // return the length of the list stored at the specified key.
  // If the key does not exist zero is returned (the same behaviour as for empty lists).
  // If the value stored at key is not a list an error is returned.
  override def llen(key: Any)(implicit format: Format): Option[Long] = Some(getRawOrEmpty(key).size)

  // LRANGE
  // return the specified elements of the list stored at the specified key.
  // Start and end are zero-based indexes.
  override def lrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] = {
    getRaw(key) map {
      _.sliceFromTo(start, end).map(_.parseOption(parse)).toList
    }
  }

  // LTRIM
  // Trim an existing list so that it will contain only the specified range of elements specified.
  override def ltrim(key: Any, start: Int, end: Int)(implicit format: Format): Boolean = withDB {
    getRaw(key).map { xs => setRaw(key, xs.slice(start, end + 1))}.isDefined
  }

  // LINDEX
  // return the especified element of the list stored at the specified key.
  // Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
  override def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]): Option[A] =
    getRaw(key).flatMap(_.get(index)).map(_.parse(parse))

  // LSET
  // set the list element at index with the new value. Out of range indexes will generate an error
  override def lset(key: Any, index: Int, value: Any)(implicit format: Format): Boolean =
    getRaw(key).map { xs =>
      if (index < 0 || xs.length <= index) {throw new IndexOutOfBoundsException("index out of range")}
      setRaw(key, xs.updated(index, Bytes(value)))
    }.isDefined

  // LREM
  // Removes the first count occurrences of elements equal to value from the list stored at key.
  // The count argument influences the operation in the following ways:
  // count > 0: Remove elements equal to value moving from head to tail.
  // count < 0: Remove elements equal to value moving from tail to head.
  // count = 0: Remove all elements equal to value.
  override def lrem(key: Any, count: Int, value: Any)(implicit format: Format): Option[Long] = {
    @tailrec
    def f(xs: List[Bytes], n: Int, v: Bytes, sofar: List[Bytes]): List[Bytes] = xs match {
      case x :: ys if x == v && n > 0 => f(ys, n - 1, v, sofar)
      case x :: ys if n > 0 => f(ys, n, v, x :: sofar)
      case _ => sofar.reverse ::: xs
    }

    withDB {
      for {
        xs <- getRaw(key)
      } yield {
        val ys = (count match {
          case _ if count < 0 => f(xs.toList.reverse, -count, Bytes(value), Nil).reverse
          case _ if count == 0 => f(xs.toList, xs.length, Bytes(value), Nil)
          case _ => f(xs.toList, count, Bytes(value), Nil)
        }).toVector
        setRaw(key, ys)
        xs.length - ys.length
      }
    }
  }

  // LPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  override def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] = withDB {
    for {
      v <- getRaw(key)
      x <- v.headOption
    } yield {
      setRaw(key, v.tail)
      x.parse(parse)
    }
  }

  // RPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  override def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] = withDB {
    for {
      v <- getRaw(key)
      x <- v.lastOption
    } yield {
      setRaw(key, v.init)
      x.parse(parse)
    }
  }

  // RPOPLPUSH
  // TBD
  override def rpoplpush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]): Option[A] = withDB {
    for {
      v <- getRaw(srcKey)
      x <- v.lastOption
    } yield {
      setRaw(srcKey, v.init)
      lpushx(dstKey, x.toArray)
      x.parse(parse)
    }
  }

  override def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(implicit format: Format, parse: Parse[A]): Option[A] = {
    @tailrec
    def loop(limit: Long): Option[A] = {
      if (limit != 0 && limit <= System.currentTimeMillis()) {
        None
      } else {
        getRaw(srcKey) match {
          case Some(bs) if bs.nonEmpty => rpoplpush(srcKey, dstKey) // TODO: reduce # of getRaw (to be once) and be atomic
          case _ => Thread.sleep(500L); loop(limit)
        }
      }
    }

    loop(if (timeoutInSeconds == 0) 0 else System.currentTimeMillis() + timeoutInSeconds * 1000L)
  }

  override def blpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[(K, V)] = {
    val ks = key #:: keys.toStream

    @tailrec
    def loop(limit: Long): Option[(K, V)] = {
      if (limit != 0 && limit <= System.currentTimeMillis()) {
        None
      } else {
        val results = ks.map(k => (k, getRawOrEmpty(k)))
        results.find(_._2.nonEmpty) match {
          case Some((k, _)) =>
            // TODO: refactor and be atomic
            Some((k, lpop(k)(format, parseV).get))
          case _ =>
            Thread.sleep(500L)
            loop(limit)
        }
      }
    }

    loop(if (timeoutInSeconds == 0) 0 else System.currentTimeMillis() + timeoutInSeconds * 1000L)
  }

  override def brpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[(K, V)] = {
    val ks = key #:: keys.toStream

    @tailrec
    def loop(limit: Long): Option[(K, V)] = {
      if (limit != 0 && limit <= System.currentTimeMillis()) {
        None
      } else {
        val results = ks.map(k => (k, getRawOrEmpty(k)))
        results.find(_._2.nonEmpty) match {
          case Some((k, _)) =>
            // TODO: refactor and be atomic
            Some((k, rpop(k)(format, parseV).get))
          case _ =>
            Thread.sleep(500L)
            loop(limit)
        }
      }
    }

    loop(if (timeoutInSeconds == 0) 0 else System.currentTimeMillis() + timeoutInSeconds * 1000L)
  }
}
