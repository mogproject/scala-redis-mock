package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity._
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.ops._
import com.redis.{SortedSetOperations, Redis}
import com.redis.serialization._

import scala.collection.SortedSet

trait MockSortedSetOperations extends SortedSetOperations with MockOperations with Storage {
  self: Redis =>

  //
  // raw functions
  //
  private def getRaw(key: Any)(implicit format: Format): Option[SortedSetValue] =
    currentDB.get(Key(key)).map(x => SortedSetValue(x.as(SORTED_SET)))

  private def getRawOrEmpty(key: Any)(implicit format: Format): SortedSetValue =
    getRaw(key).getOrElse(SortedSetValue.empty)

  private def setRaw(key: Any, value: SortedSetValue)(implicit format: Format): Unit = currentDB.update(Key(key), value)

  // ZADD (Variadic: >= 2.4)
  // Add the specified members having the specified score to the sorted set stored at key.
  override def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)
                   (implicit format: Format): Option[Long] = withDB {
    val a = getRawOrEmpty(key)
    val b = a ++ ((score, member) :: scoreVals.toList).map { case (s, m) => (s, Bytes(m))}
    setRaw(key, b)
    Some(b.size - a.size)
  }

  // ZREM (Variadic: >= 2.4)
  // Remove the specified members from the sorted set value stored at key.
  override def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): Option[Long] = withDB {
    val a = getRawOrEmpty(key)
    val b = a -- (member :: members.toList).map(Bytes.apply)
    setRaw(key, b)
    Some(a.size - b.size)
  }

  // ZINCRBY
  //
  override def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format): Option[Double] = withDB {
    for {
      v <- getRaw(key)
      x = Bytes(member)
      s <- v.index.get(x)
    } yield {
      s + incr <| { d => setRaw(key, v.updated(d, x))}
    }
  }

  // ZCARD
  //
  override def zcard(key: Any)(implicit format: Format): Option[Long] = Some(getRawOrEmpty(key).size)

  // ZSCORE
  //
  override def zscore(key: Any, element: Any)(implicit format: Format): Option[Double] =
    getRaw(key).flatMap(_.index.get(Bytes(element)))

  // ZRANGE
  //

  import com.redis.RedisClient._

  override def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                        (implicit format: Format, parse: Parse[A]): Option[List[A]] =
    getRaw(key).map(_.data |> sliceThenReverse(start, end, sortAs != ASC) |> toValues(parse))

  override def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                                 (implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]] =
    getRaw(key).map(_.data |> sliceThenReverse(start, end, sortAs != ASC) |> toValueScores(parse))

  // ZRANGEBYSCORE
  //
  override def zrangebyscore[A](key: Any,
                                min: Double = Double.NegativeInfinity,
                                minInclusive: Boolean = true,
                                max: Double = Double.PositiveInfinity,
                                maxInclusive: Boolean = true,
                                limit: Option[(Int, Int)],
                                sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    getRaw(key).map(_.data |>
      filterByRange(min, minInclusive, max, maxInclusive) |> sliceThenReverse(limit, sortAs != ASC) |>
      toValues(parse))


  override def zrangebyscoreWithScore[A](key: Any,
                                         min: Double = Double.NegativeInfinity,
                                         minInclusive: Boolean = true,
                                         max: Double = Double.PositiveInfinity,
                                         maxInclusive: Boolean = true,
                                         limit: Option[(Int, Int)],
                                         sortAs: SortOrder = ASC)
                                        (implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]] =
    getRaw(key).map(_.data |>
      filterByRange(min, minInclusive, max, maxInclusive) |> sliceThenReverse(limit, sortAs != ASC) |>
      toValueScores(parse))

  // ZRANK
  // ZREVRANK
  //
  override def zrank(key: Any, member: Any, reverse: Boolean = false)(implicit format: Format): Option[Long] =
    getRaw(key).flatMap { v => v.rank(Bytes(member)).map(_.mapWhenTrue(reverse)(v.size - _ - 1))}

  // ZREMRANGEBYRANK
  //
  override def zremrangebyrank(key: Any, start: Int = 0, end: Int = -1)(implicit format: Format): Option[Long] =
    withDB {
      getRaw(key).map { v =>
        val b = v -- sliceThenReverse(start, end, doReverse = false)(v.data).map(_._2)
        setRaw(key, b)
        v.size - b.size
      }
    }

  // ZREMRANGEBYSCORE
  //
  override def zremrangebyscore(key: Any,
                                start: Double = Double.NegativeInfinity,
                                end: Double = Double.PositiveInfinity)
                               (implicit format: Format): Option[Long] =
    withDB {
      getRaw(key).map { v =>
        val b = v -- filterByRange(start, minInclusive = true, end, maxInclusive = true)(v.data).toSeq.map(_._2)
        setRaw(key, b)
        v.size - b.size
      }
    }

  // ZUNION
  //
  override def zunionstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)
                          (implicit format: Format): Option[Long] =
    zunionstoreWeighted(dstKey, keys.map((_, 1.0)), aggregate)

  override def zunionstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)
                                  (implicit format: Format): Option[Long] = storeReduced(_ | _)(dstKey, kws, aggregate)

  // ZINTERSTORE
  //
  override def zinterstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)
                          (implicit format: Format): Option[Long] =
    zinterstoreWeighted(dstKey, keys.map((_, 1.0)), aggregate)

  override def zinterstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)
                                  (implicit format: Format): Option[Long] = storeReduced(_ & _)(dstKey, kws, aggregate)

  // ZCOUNT
  //
  override def zcount(key: Any,
                      min: Double = Double.NegativeInfinity,
                      max: Double = Double.PositiveInfinity,
                      minInclusive: Boolean = true,
                      maxInclusive: Boolean = true)
                     (implicit format: Format): Option[Long] =
    getRaw(key).map(xs => filterByRange(min, minInclusive, max, maxInclusive)(xs.data).size)

  // ZSCAN
  // Incrementally iterate sorted sets elements and associated scores (since 2.8)
  override def zscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
  // TODO: implement
    ???

  //    send("ZSCAN", key :: cursor :: ((x: List[Any]) => if(pattern == "*") x else "match" :: pattern :: x)(if(count == 10) Nil else List("count", count)))(asPair)

  //
  // helper functions
  //
  private def filterByRange(min: Double, minInclusive: Boolean, max: Double, maxInclusive: Boolean)
                           (xs: SortedSet[(Double, Bytes)]): SortedSet[(Double, Bytes)] =
    xs.range(min -> Bytes.empty, max -> Bytes.MaxValue).filter { case (s, _) =>
      (minInclusive || s != min) && (maxInclusive || s != max)
    }

  private def sliceThenReverse(start: Int, end: Int, doReverse: Boolean)
                              (xs: SortedSet[(Double, Bytes)]): List[(Double, Bytes)] = {
    val from = start + (if (start < 0) xs.size else 0)
    val until = end + 1 + (if (end < 0) xs.size else 0)
    val s = if (doReverse) xs.size - until else from
    val t = if (doReverse) xs.size - from else until
    xs.slice(s, t).toList.mapWhenTrue(doReverse)(_.reverse)
  }

  private def sliceThenReverse(limit: Option[(Int, Int)], doReverse: Boolean)
                              (xs: SortedSet[(Double, Bytes)]): List[(Double, Bytes)] = {
    val (start, end) = limit match {
      case Some((offset, count)) => (offset, offset + count - 1)
      case None => (0, -1)
    }
    sliceThenReverse(start, end, doReverse)(xs)
  }

  private def toValues[A](parse: Parse[A])(xs: List[(Double, Bytes)]): List[A] = xs.map(_._2.parse(parse))

  private def toValueScores[A](parse: Parse[A])(xs: List[(Double, Bytes)]): List[(A, Double)] =
    xs.map { case (s, v) => (v.parse(parse), s)}

  private def storeReduced(op: (Set[Bytes], Set[Bytes]) => Set[Bytes])
                          (dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate)
                          (implicit format: Format): Option[Long] = withDB {

    val (aggregateFunc, zero): ((Double, Double) => Double, Double) = aggregate match {
      case SUM => (_ + _, 0.0)
      case MIN => (math.min, Double.MaxValue)
      case MAX => (math.max, Double.MinValue)
    }

    val indexWeightSeq = kws.map { case Product2(k, w) => (getRawOrEmpty(k).index, w)}

    val values: Seq[Bytes] = indexWeightSeq.map(_._1.keySet).reduceLeft(op).toSeq
    val scores = values.map { v =>
      indexWeightSeq.foldLeft(zero) {
        case (x, (index, w)) => aggregateFunc(x, index.get(v).map(_ * w).getOrElse(zero))
      }
    }

    SortedSetValue(scores zip values: _*) <| { v => setRaw(dstKey, v) } |> { v => Some(v.size) }
  }
}

