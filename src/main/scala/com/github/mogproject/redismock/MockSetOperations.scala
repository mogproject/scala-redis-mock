package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity._
import com.github.mogproject.redismock.generic.GenericOperations
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.ops._
import com.redis.{SetOperations, Redis}
import com.redis.serialization._

/**
 * SET operations
 */
trait MockSetOperations extends SetOperations with MockOperations with Storage with GenericOperations {
  self: Redis =>

  private implicit val companion: ValueCompanion[SetValue] = SetValue

  private def set(key: Any, value: Set[Bytes])(implicit format: Format): Unit = setRaw(key, SetValue(value))

  private def reduceRaw(op: (Set[Bytes], Set[Bytes]) => Set[Bytes])
                       (keys: Traversable[Any])
                       (implicit format: Format): Set[Bytes] = keys.view.map(getRawOrEmpty(_).data).reduceLeft(op)

  private def getReduced[A](op: (Set[Bytes], Set[Bytes]) => Set[Bytes])
                           (key: Any, keys: Any*)
                           (implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] = withDB {
    (reduceRaw(op)(key :: keys.toList) map (_.parseOption(parse))) |> Some.apply
  }

  private def storeReduced(op: (Set[Bytes], Set[Bytes]) => Set[Bytes])
                          (key: Any, keys: Any*)
                          (implicit format: Format): Option[Long] = withDB {
    reduceRaw(op)(keys) <| (set(key, _)) |> { xs => Some(xs.size) }
  }

  /**
   * Add the specified members to the set stored at key. Specified members that are already a member of this set are
   * ignored. If key does not exist, a new set is created before adding the specified members.
   *
   * An error is returned when the value stored at key is not a set.
   *
   * @see http://redis.io/commands/sadd
   */
  override def sadd(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Long] = withDB {
    val a = getRawOrEmpty(key).data
    val b = a ++ (value :: values.toList).map(Bytes.apply)
    set(key, b)
    Some(b.size - a.size)
  }

  /**
   * Remove the specified members from the set stored at key. Specified members that are not a member of this set are
   * ignored. If key does not exist, it is treated as an empty set and this command returns 0.
   *
   * An error is returned when the value stored at key is not a set.
   *
   * @see http://redis.io/commands/srem
   */
  override def srem(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Long] = withDB {
    val a = getRawOrEmpty(key).data
    val b = a -- (value :: values.toList).map(Bytes.apply)
    set(key, b)
    Some(a.size - b.size)
  }

  /**
   * Removes and returns one or more random elements from the set value store at key.
   *
   * This operation is similar to SRANDMEMBER, that returns one or more random elements from a set but does not remove
   * it.
   *
   * The count argument will be available in a later version and is not available in 2.6, 2.8, 3.0
   *
   * @see http://redis.io/commands/spop
   */
  override def spop[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] = withDB {
    // TODO: random choice
    for {
      s <- getRaw(key)
      x <- s.data.headOption
    } yield {
      set(key, s.data.tail)
      x.parse(parse)
    }
  }

  /**
   * Move member from the set at source to the set at destination. This operation is atomic. In every given moment the
   * element will appear to be a member of source or destination for other clients.
   *
   * If the source set does not exist or does not contain the specified element, no operation is performed and 0 is
   * returned. Otherwise, the element is removed from the source set and added to the destination set. When the
   * specified element already exists in the destination set, it is only removed from the source set.
   *
   * @see http://redis.io/commands/smove
   */
  override def smove(sourceKey: Any, destKey: Any, value: Any)(implicit format: Format): Option[Long] = withDB {
    srem(sourceKey, value) <| { ret => if (ret.exists(_ > 0)) sadd(destKey, value) }
  }

  /**
   * Returns the set cardinality (number of elements) of the set stored at key.
   *
   * @see http://redis.io/commands/scard
   */
  override def scard(key: Any)(implicit format: Format): Option[Long] = Some(getRawOrEmpty(key).data.size)

  /**
   * Returns if member is a member of the set stored at key.
   *
   * @see http://redis.io/commands/sismember
   */
  override def sismember(key: Any, value: Any)(implicit format: Format): Boolean =
    getRawOrEmpty(key).data.contains(Bytes(value))

  /**
   * Returns the members of the set resulting from the intersection of all the given sets.
   *
   * @see http://redis.io/commands/sinter
   */
  override def sinter[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    getReduced(_ & _)(key, keys: _*)

  /**
   * This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
   *
   * If destination already exists, it is overwritten.
   *
   * @see http://redis.io/commands/sinterstore
   */
  override def sinterstore(key: Any, keys: Any*)(implicit format: Format): Option[Long] =
    storeReduced(_ & _)(key, keys: _*)

  /**
   * Returns the members of the set resulting from the union of all the given sets.
   *
   * @see http://redis.io/commands/sunion
   */
  override def sunion[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    getReduced(_ | _)(key, keys: _*)

  /**
   * This command is equal to SUNION, but instead of returning the resulting set, it is stored in destination.
   *
   * If destination already exists, it is overwritten.
   *
   * @see http://redis.io/commands/sunionstore
   */
  override def sunionstore(key: Any, keys: Any*)(implicit format: Format): Option[Long] =
    storeReduced(_ | _)(key, keys: _*)

  /**
   * Returns the members of the set resulting from the difference between the first set and all the successive sets.
   *
   * @see http://redis.io/commands/sdiff
   */
  override def sdiff[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    getReduced(_ -- _)(key, keys: _*)

  /**
   * This command is equal to SDIFF, but instead of returning the resulting set, it is stored in destination.
   *
   * If destination already exists, it is overwritten.
   *
   * @see http://redis.io/commands/sdiffstore
   */
  override def sdiffstore(key: Any, keys: Any*)(implicit format: Format): Option[Long] =
    storeReduced(_ -- _)(key, keys: _*)

  /**
   * Returns all the members of the set value stored at key.
   *
   * This has the same effect as running SINTER with one argument key.
   *
   * @see http://redis.io/commands/smembers
   */
  override def smembers[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    getRawOrEmpty(key).data.map(_.parseOption(parse)) |> Some.apply

  /**
   * When called with just the key argument, return a random element from the set value stored at key.
   *
   * Starting from Redis version 2.6, when called with the additional count argument, return an array of count distinct
   * elements if count is positive. If called with a negative count the behavior changes and the command is allowed to
   * return the same element multiple times. In this case the number of returned elements is the absolute value of the
   * specified count.
   *
   * When called with just the key argument, the operation is similar to SPOP, however while SPOP also removes the
   * randomly selected element from the set, SRANDMEMBER will just return a random element without altering the original
   * set in any way.
   *
   * @see http://redis.io/commands/srandmember
   */
  override def srandmember[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
  // TODO: random choice
    getRawOrEmpty(key).data.headOption.map(_.parse(parse))

  override def srandmember[A](key: Any, count: Int)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
  // TODO: random choice
    getRawOrEmpty(key).data.take(count).toList.map(_.parseOption(parse)) |> Some.apply

  /**
   * Incrementally iterate Set elements (since 2.8)
   *
   * @see http://redis.io/commands/sscan
   */
  override def sscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
  // TODO: implement
    ???

  //send("SSCAN", key :: cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x)(if (count == 10) Nil else List("count", count)))(asPair)
}
