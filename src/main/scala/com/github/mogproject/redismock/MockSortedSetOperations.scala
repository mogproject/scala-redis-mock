package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity._
import com.github.mogproject.redismock.generic.GenericOperations
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.ops._
import com.redis.RedisClient.{ASC, SortOrder, Aggregate, SUM, MIN, MAX}
import com.redis.{SortedSetOperations, Redis}
import com.redis.serialization._

import scala.collection.SortedSet

/**
 * SORTED SET operations
 */
trait MockSortedSetOperations
  extends SortedSetOperations with MockOperations with Storage with GenericOperations {

  self: Redis =>

  private implicit val companion: ValueCompanion[SortedSetValue] = SortedSetValue

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
    xs.map { case (s, v) => (v.parse(parse), s) }

  private def storeReduced(op: (Set[Bytes], Set[Bytes]) => Set[Bytes])
                          (dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate)
                          (implicit format: Format): Option[Long] = withDB {

    val (aggregateFunc, zero): ((Double, Double) => Double, Double) = aggregate match {
      case SUM => (_ + _, 0.0)
      case MIN => (math.min, Double.MaxValue)
      case MAX => (math.max, Double.MinValue)
    }

    val indexWeightSeq = kws.map { case Product2(k, w) => (getRawOrEmpty(k).index, w) }

    val values: Seq[Bytes] = indexWeightSeq.map(_._1.keySet).reduceLeft(op).toSeq
    val scores = values.map { v =>
      indexWeightSeq.foldLeft(zero) {
        case (x, (index, w)) => aggregateFunc(x, index.get(v).map(_ * w).getOrElse(zero))
      }
    }

    SortedSetValue(scores zip values: _*) <| { v => setRaw(dstKey, v) } |> { v => Some(v.size) }
  }


  /**
   * Adds all the specified members with the specified scores to the sorted set stored at key. It is possible to specify
   * multiple score / member pairs. If a specified member is already a member of the sorted set, the score is updated
   * and the element reinserted at the right position to ensure the correct ordering.
   *
   * If key does not exist, a new sorted set with the specified members as sole members is created, like if the sorted
   * set was empty. If the key exists but does not hold a sorted set, an error is returned.
   *
   * The score values should be the string representation of a double precision floating point number. +inf and -inf
   * values are valid values as well.
   *
   * @see http://redis.io/commands/zadd
   */
  override def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)
                   (implicit format: Format): Option[Long] = withDB {
    val a = getRawOrEmpty(key)
    val b = a ++ ((score, member) :: scoreVals.toList).map { case (s, m) => (s, Bytes(m)) }
    setRaw(key, b)
    Some(b.size - a.size)
  }

  /**
   * Removes the specified members from the sorted set stored at key. Non existing members are ignored.
   *
   * An error is returned when key exists and does not hold a sorted set.
   *
   * @see http://redis.io/commands/zrem
   */
  override def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): Option[Long] = withDB {
    val a = getRawOrEmpty(key)
    val b = a -- (member :: members.toList).map(Bytes.apply)
    setRaw(key, b)
    Some(a.size - b.size)
  }

  /**
   * Increments the score of member in the sorted set stored at key by increment. If member does not exist in the sorted
   * set, it is added with increment as its score (as if its previous score was 0.0). If key does not exist, a new
   * sorted set with the specified member as its sole member is created.
   *
   * An error is returned when key exists but does not hold a sorted set.
   *
   * The score value should be the string representation of a numeric value, and accepts double precision floating point
   * numbers. It is possible to provide a negative value to decrement the score.
   *
   * @see http://redis.io/commands/zincrby
   */
  override def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format): Option[Double] = withDB {
    for {
      v <- getRaw(key)
      x = Bytes(member)
      s <- v.index.get(x)
    } yield {
      s + incr <| { d => setRaw(key, v.updated(d, x)) }
    }
  }

  /**
   * Returns the sorted set cardinality (number of elements) of the sorted set stored at key.
   *
   * @see http://redis.io/commands/zcard
   */
  override def zcard(key: Any)(implicit format: Format): Option[Long] = Some(getRawOrEmpty(key).size)

  /**
   * Returns the score of member in the sorted set at key.
   *
   * If member does not exist in the sorted set, or key does not exist, nil is returned.
   *
   * @see http://redis.io/commands/zscore
   */
  override def zscore(key: Any, element: Any)(implicit format: Format): Option[Double] =
    getRaw(key).flatMap(_.index.get(Bytes(element)))

  /**
   * Returns the specified range of elements in the sorted set stored at key. The elements are considered to be ordered
   * from the lowest to the highest score. Lexicographical order is used for elements with equal score.
   *
   * @see http://redis.io/commands/zrange
   */
  override def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                        (implicit format: Format, parse: Parse[A]): Option[List[A]] =
    getRaw(key).map(_.data |> sliceThenReverse(start, end, sortAs != ASC) |> toValues(parse))

  /**
   * ZRANGE WITHSCORES
   *
   * @see http://redis.io/commands/zrange
   */
  override def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                                 (implicit format: Format, parse: Parse[A]): Option[List[(A, Double)]] =
    getRaw(key).map(_.data |> sliceThenReverse(start, end, sortAs != ASC) |> toValueScores(parse))

  /**
   * Returns all the elements in the sorted set at key with a score between min and max (including elements with score
   * equal to min or max). The elements are considered to be ordered from low to high scores.
   *
   * @see http://redis.io/commands/zrangebyscore
   */
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

  /**
   * ZRANGEBYSCORE WITHSCORES
   *
   * @see http://redis.io/commands/zrangebyscore
   */
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

  /**
   * ZRANK
   * Returns the rank of member in the sorted set stored at key, with the scores ordered from low to high. The rank (or
   * index) is 0-based, which means that the member with the lowest score has rank 0.
   *
   * @see http://redis.io/commands/zrank
   *
   *      ZREVRANK
   *      Returns the rank of member in the sorted set stored at key, with the scores ordered from high to low. The rank (or
   *      index) is 0-based, which means that the member with the highest score has rank 0.
   *
   * @see http://redis.io/commands/zrevrank
   */
  override def zrank(key: Any, member: Any, reverse: Boolean = false)(implicit format: Format): Option[Long] =
    getRaw(key).flatMap { v => v.rank(Bytes(member)).map(_.mapWhenTrue(reverse)(v.size - _ - 1)) }

  /**
   * Removes all elements in the sorted set stored at key with rank between start and stop. Both start and stop are
   * 0 -based indexes with 0 being the element with the lowest score. These indexes can be negative numbers, where they
   * indicate offsets starting at the element with the highest score. For example: -1 is the element with the highest
   * score, -2 the element with the second highest score and so forth.
   *
   * @see http://redis.io/commands/zremrangebyrank
   */
  override def zremrangebyrank(key: Any, start: Int = 0, end: Int = -1)(implicit format: Format): Option[Long] =
    withDB {
      getRaw(key).map { v =>
        val b = v -- sliceThenReverse(start, end, doReverse = false)(v.data).map(_._2)
        setRaw(key, b)
        v.size - b.size
      }
    }

  /**
   * Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
   *
   * @see http://redis.io/commands/zremrangebyscore
   */
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

  /**
   * Computes the union of numkeys sorted sets given by the specified keys, and stores the result in destination. It is
   * mandatory to provide the number of input keys (numkeys) before passing the input keys and the other (optional)
   * arguments.
   *
   * By default, the resulting score of an element is the sum of its scores in the sorted sets where it exists.
   *
   * Using the WEIGHTS option, it is possible to specify a multiplication factor for each input sorted set. This means
   * that the score of every element in every input sorted set is multiplied by this factor before being passed to the
   * aggregation function. When WEIGHTS is not given, the multiplication factors default to 1.
   *
   * With the AGGREGATE option, it is possible to specify how the results of the union are aggregated. This option
   * defaults to SUM, where the score of an element is summed across the inputs where it exists. When this option is set
   * to either MIN or MAX, the resulting set will contain the minimum or maximum score of an element across the inputs
   * where it exists.
   *
   * If destination already exists, it is overwritten.
   *
   * @see http://redis.io/commands/zunionstore
   */
  override def zunionstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)
                          (implicit format: Format): Option[Long] =
    zunionstoreWeighted(dstKey, keys.map((_, 1.0)), aggregate)

  /**
   * ZUNIONSTORE WEIGHTS
   *
   * @see http://redis.io/commands/zunionstore
   */
  override def zunionstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)
                                  (implicit format: Format): Option[Long] = storeReduced(_ | _)(dstKey, kws, aggregate)

  /**
   * Computes the intersection of numkeys sorted sets given by the specified keys, and stores the result in destination.
   * It is mandatory to provide the number of input keys (numkeys) before passing the input keys and the other
   * (optional) arguments.
   *
   * By default, the resulting score of an element is the sum of its scores in the sorted sets where it exists. Because
   * intersection requires an element to be a member of every given sorted set, this results in the score of every
   * element in the resulting sorted set to be equal to the number of input sorted sets.
   *
   * For a description of the WEIGHTS and AGGREGATE options, see ZUNIONSTORE.
   *
   * If destination already exists, it is overwritten.
   *
   * @see http://redis.io/commands/zinterstore
   */
  override def zinterstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)
                          (implicit format: Format): Option[Long] =
    zinterstoreWeighted(dstKey, keys.map((_, 1.0)), aggregate)

  /**
   * ZINTERSTORE WEIGHTS
   *
   * @see http://redis.io/commands/zunionstore
   */
  override def zinterstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)
                                  (implicit format: Format): Option[Long] = storeReduced(_ & _)(dstKey, kws, aggregate)

  /**
   * Returns the number of elements in the sorted set at key with a score between min and max.
   *
   * The min and max arguments have the same semantic as described for ZRANGEBYSCORE.
   *
   * @see http://redis.io/commands/zcount
   */
  override def zcount(key: Any,
                      min: Double = Double.NegativeInfinity,
                      max: Double = Double.PositiveInfinity,
                      minInclusive: Boolean = true,
                      maxInclusive: Boolean = true)
                     (implicit format: Format): Option[Long] =
    getRaw(key).map(xs => filterByRange(min, minInclusive, max, maxInclusive)(xs.data).size)

  /**
   * Incrementally iterate sorted sets elements and associated scores (since 2.8)
   *
   * @see http://redis.io/commands/zscan
   */
  override def zscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
  // TODO: implement
    ???

  //    send("ZSCAN", key :: cursor :: ((x: List[Any]) => if(pattern == "*") x else "match" :: pattern :: x)(if(count == 10) Nil else List("count", count)))(asPair)
}

