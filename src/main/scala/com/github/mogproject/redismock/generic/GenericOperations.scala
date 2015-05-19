package com.github.mogproject.redismock.generic

import com.github.mogproject.redismock.entity._
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.Bytes
import com.github.mogproject.redismock.util.ops._
import com.redis.{Millis, Seconds, SecondsOrMillis, Redis}
import com.redis.serialization.{Parse, Format}

import scala.reflect.ClassTag


trait GenericOperations extends Storage {
  self: Redis =>

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

  protected def genericScan[A](xs: Seq[Bytes], cursor: Int, count: Int)
                              (implicit parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] = {
    if (count <= 0) throw new Exception("ERR syntax error")
    val n = xs.length
    val start = math.max(0, math.min(cursor, n - 1))
    val end = start + count
    val next = Some(end).filter(_ < n).getOrElse(0)
    val ys = xs.slice(start, end).toList.map(_.parseOption(parse))

    (next |> Some.apply, ys |> Some.apply) |> Some.apply
  }
}
