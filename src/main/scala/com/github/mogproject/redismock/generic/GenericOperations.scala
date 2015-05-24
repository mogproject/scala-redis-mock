package com.github.mogproject.redismock.generic

import com.github.mogproject.redismock.entity._
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.{StringUtil, Bytes}
import com.github.mogproject.redismock.util.ops._
import com.redis.{Millis, Seconds, SecondsOrMillis, Redis}
import com.redis.serialization.{Parse, Format}

import scala.reflect.ClassTag
import scala.util.{Try, Random}


trait GenericOperations extends Storage {
  self: Redis =>

  lazy val random = new Random(12345L)

  protected def safeAddition(a: Long, b: Long): Long =
    Some(BigInt(a) + BigInt(b)).withFilter(_.isValidLong).map(_.toLong)
      .getOrElse(throw new Exception("ERR increment or decrement would overflow"))

  protected def setRaw[A <: Value](key: Any, value: A)(implicit format: Format): Unit =
    currentDB.update(Key(key), value)

  protected def setRaw[A <: Value](key: Any, value: A, ttl: Option[Long])(implicit format: Format): Unit =
    currentDB.update(Key(key), value, ttl)

  protected def getRaw[A <: Value : ClassTag](key: Any)
                                             (implicit format: Format, companion: ValueCompanion[A]): Option[A] =
    currentDB.get(Key(key)).map(_.as[A])

  protected def getRawOrEmpty[A <: Value : ClassTag](key: Any)
                                                    (implicit format: Format, companion: ValueCompanion[A]): A =
    getRaw(key).getOrElse(companion.empty)

  protected def getTime(time: SecondsOrMillis): Long = time match {
    case Seconds(v) => v * 1000L
    case Millis(v) => v
  }

  protected def genericScan[A](xs: Traversable[Bytes],
                               cursor: Int,
                               pattern: Any,
                               count: Int)
                              (implicit format: Format, parse: Parse[A])
  : Option[(Option[Int], Option[List[Option[A]]])] =
    genericScan(xs, cursor, pattern, count, identity[Bytes], (x: Bytes) => Seq(x))

  protected def genericScan[A, B](xs: Traversable[B],
                                  cursor: Int,
                                  pattern: Any,
                                  count: Int,
                                  filterKey: B => Bytes,
                                  resultKey: B => Seq[Bytes])
                                 (implicit format: Format, parse: Parse[A])
  : Option[(Option[Int], Option[List[Option[A]]])] = {

    if (count <= 0) throw new Exception("ERR syntax error")

    val r = StringUtil.globToRegex(new String(format(pattern)))
    val ys = xs.view.filter(filterKey(_).parse(Parse.parseStringSafe).matches(r)).toSeq

    val n = ys.length
    val start = math.max(0, math.min(cursor, n - 1))
    val end = start + count
    val next = Some(end).filter(_ < n).getOrElse(0)
    val zs = ys.slice(start, end).toList.flatMap(resultKey(_).map(_.parseOption(parse)))

    (next |> Some.apply, zs |> Some.apply) |> Some.apply
  }
}
