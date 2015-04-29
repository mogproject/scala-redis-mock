package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity.{Bytes, SET, Key, SetValue}
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.ops._
import com.redis.{SetOperations, Redis}
import com.redis.serialization._

trait MockSetOperations extends SetOperations with MockOperations with Storage {
  self: Redis =>

  private def setRaw(key: Any, rawValue: SET.DataType)(implicit format: Format): Unit =
    currentDB.update(Key(key), SetValue(rawValue))

  private def getRaw(key: Any)(implicit format: Format): Option[SET.DataType] =
    currentDB.get(Key(format.apply(key))).map(_.as(SET))

  private def getRawOrEmpty(key: Any)(implicit format: Format): SET.DataType =
    getRaw(key).getOrElse(Set.empty[Bytes])

  private def reduceRaw(op: (Set[Bytes], Set[Bytes]) => Set[Bytes])
                       (keys: Traversable[Any])
                       (implicit format: Format): Set[Bytes] = keys.view.map(getRawOrEmpty).reduceLeft(op)

  private def getReduced[A](op: (Set[Bytes], Set[Bytes]) => Set[Bytes])
                           (key: Any, keys: Any*)
                           (implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] = withDB {
    (reduceRaw(op)(key :: keys.toList) map {_.parseOption(parse)}) |> Some.apply
  }

  private def storeReduced(op: (Set[Bytes], Set[Bytes]) => Set[Bytes])
                          (key: Any, keys: Any*)
                          (implicit format: Format): Option[Long] = withDB {
    reduceRaw(op)(keys) <| {setRaw(key, _)} |> { xs => Some(xs.size)}
  }

  // SADD (VARIADIC: >= 2.4)
  // Add the specified members to the set value stored at key.
  override def sadd(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Long] = withDB {
    val a = getRawOrEmpty(key)
    val b = a ++ (value :: values.toList).map(Bytes.apply)
    setRaw(key, b)
    Some(b.size - a.size)
  }

  // SREM (VARIADIC: >= 2.4)
  // Remove the specified members from the set value stored at key.
  override def srem(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Long] = withDB {
    val a = getRawOrEmpty(key)
    val b = a -- (value :: values.toList).map(Bytes.apply)
    setRaw(key, b)
    Some(a.size - b.size)
  }

  // SPOP
  // Remove and return (pop) a random element from the Set value at key.
  override def spop[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] = withDB {
    // TODO: random choice
    val (h, t) = getRawOrEmpty(key).splitAt(1)
    h.headOption.map { x =>
      setRaw(key, t)
      x.parse(parse)
    }
  }

  // SMOVE
  // Move the specified member from one Set to another atomically.
  // Integer reply, specifically:
  //   1 if the element is moved.
  //   0 if the element is not a member of source and no operation was performed.
  override def smove(sourceKey: Any, destKey: Any, value: Any)(implicit format: Format): Option[Long] = withDB {
    srem(sourceKey, value) <| { ret => if (ret.exists(_ > 0)) sadd(destKey, value)}
  }

  // SCARD
  // Return the number of elements (the cardinality) of the Set at key.
  override def scard(key: Any)(implicit format: Format): Option[Long] = Some(getRawOrEmpty(key).size)

  // SISMEMBER
  // Test if the specified value is a member of the Set at key.
  override def sismember(key: Any, value: Any)(implicit format: Format): Boolean =
    getRawOrEmpty(key).contains(Bytes(value))

  // SINTER
  // Return the intersection between the Sets stored at key1, key2, ..., keyN.
  override def sinter[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    getReduced(_ & _)(key, keys: _*)

  // SINTERSTORE
  // Compute the intersection between the Sets stored at key1, key2, ..., keyN,
  // and store the resulting Set at dstkey.
  // SINTERSTORE returns the size of the intersection, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  override def sinterstore(key: Any, keys: Any*)(implicit format: Format): Option[Long] =
    storeReduced(_ & _)(key, keys: _*)

  // SUNION
  // Return the union between the Sets stored at key1, key2, ..., keyN.
  override def sunion[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    getReduced(_ | _)(key, keys: _*)

  // SUNIONSTORE
  // Compute the union between the Sets stored at key1, key2, ..., keyN,
  // and store the resulting Set at dstkey.
  // SUNIONSTORE returns the size of the union, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  override def sunionstore(key: Any, keys: Any*)(implicit format: Format): Option[Long] =
    storeReduced(_ | _)(key, keys: _*)

  // SDIFF
  // Return the difference between the Set stored at key1 and all the Sets key2, ..., keyN.
  override def sdiff[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    getReduced(_ -- _)(key, keys: _*)

  // SDIFFSTORE
  // Compute the difference between the Set key1 and all the Sets key2, ..., keyN,
  // and store the resulting Set at dstkey.
  override def sdiffstore(key: Any, keys: Any*)(implicit format: Format): Option[Long] =
    storeReduced(_ -- _)(key, keys: _*)

  // SMEMBERS
  // Return all the members of the Set value at key.
  override def smembers[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    getRawOrEmpty(key).map(_.parseOption(parse)) |> Some.apply

  // SRANDMEMBER
  // Return a random element from a Set
  override def srandmember[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
  // TODO: random choice
    getRawOrEmpty(key).headOption.map(_.parse(parse))

  // SRANDMEMBER
  // Return multiple random elements from a Set (since 2.6)
  override def srandmember[A](key: Any, count: Int)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
  // TODO: random choice
    getRawOrEmpty(key).take(count).toList.map(_.parseOption(parse)) |> Some.apply

  // SSCAN
  // Incrementally iterate Set elements (since 2.8)
  override def sscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
  // TODO: implement
    ???

  //send("SSCAN", key :: cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x)(if (count == 10) Nil else List("count", count)))(asPair)
}
