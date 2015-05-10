package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity._
import com.github.mogproject.redismock.generic.GenericOperations
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.Bytes
import com.redis.{Redis, ListOperations}
import com.redis.serialization._
import com.github.mogproject.redismock.util.ops._
import com.github.mogproject.redismock.util.Implicits._
import scala.annotation.tailrec

/**
 * LIST operations
 */
trait MockListOperations extends ListOperations with MockOperations with Storage with GenericOperations {
  self: Redis =>

  private implicit val companion: ValueCompanion[ListValue] = ListValue

  private def set(key: Any, value: Traversable[Bytes])(implicit format: Format): Unit = setRaw(key, ListValue(value))

  private def findFirstNonEmpty[K](keys: Seq[K])(implicit format: Format): Option[K] =
    keys.find(getRawOrEmpty(_).data.nonEmpty)

  @tailrec
  private def loopUntilFound[K, A](keys: Seq[K])(task: K => Option[A])(limit: Long): Option[A] = {
    if (limit != 0 && limit <= System.currentTimeMillis()) {
      None
    } else {
      val result = withDB {
        findFirstNonEmpty(keys).flatMap(task)
      }
      result match {
        case Some(_) => result
        case None =>
          Thread.sleep(500L)
          loopUntilFound(keys)(task)(limit)
      }
    }
  }

  private def getTimeLimit(timeoutInSeconds: Int): Long =
    if (timeoutInSeconds == 0) 0 else System.currentTimeMillis() + timeoutInSeconds * 1000L


  /**
   * Insert all the specified values at the head of the list stored at key. If key does not exist, it is created as
   * empty list before performing the push operations. When key holds a value that is not a list, an error is returned.
   *
   * It is possible to push multiple elements using a single command call just specifying multiple arguments at the end
   * of the command. Elements are inserted one after the other to the head of the list, from the leftmost element to the
   * rightmost element. So for instance the command LPUSH mylist a b c will result into a list containing c as first
   * element, b as second element and a as third element.
   *
   * http://redis.io/commands/lpush
   */
  override def lpush(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Long] = withDB {
    (value :: values.toList).map(Bytes.apply) ++ getRawOrEmpty(key).data <| (set(key, _)) |> (v => Some(v.size))
  }

  /**
   * Inserts value at the head of the list stored at key, only if key already exists and holds a list. In contrary to
   * LPUSH, no operation will be performed when key does not yet exist.
   *
   * @see http://redis.io/commands/lpushx
   */
  override def lpushx(key: Any, value: Any)(implicit format: Format): Option[Long] = lpush(key, value)

  /**
   * Insert all the specified values at the tail of the list stored at key. If key does not exist, it is created as
   * empty list before performing the push operation. When key holds a value that is not a list, an error is returned.
   *
   * It is possible to push multiple elements using a single command call just specifying multiple arguments at the end
   * of the command. Elements are inserted one after the other to the tail of the list, from the leftmost element to the
   * rightmost element. So for instance the command RPUSH mylist a b c will result into a list containing a as first
   * element, b as second element and c as third element.
   *
   * @see http://redis.io/commands/rpush
   */
  override def rpush(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Long] = withDB {
    getRawOrEmpty(key).data ++ (value :: values.toList).map(Bytes.apply) <| (set(key, _)) |> (v => Some(v.size))
  }

  /**
   * Inserts value at the tail of the list stored at key, only if key already exists and holds a list. In contrary to
   * RPUSH, no operation will be performed when key does not yet exist.
   *
   * @see http://redis.io/commands/rpushx
   */
  override def rpushx(key: Any, value: Any)(implicit format: Format): Option[Long] = rpush(key, value)

  /**
   * Returns the length of the list stored at key. If key does not exist, it is interpreted as an empty list and 0 is
   * returned. An error is returned when the value stored at key is not a list.
   *
   * @see http://redis.io/commands/llen
   */
  override def llen(key: Any)(implicit format: Format): Option[Long] = Some(getRawOrEmpty(key).data.size)

  /**
   * Returns the specified elements of the list stored at key. The offsets start and stop are zero-based indexes, with 0
   * being the first element of the list (the head of the list), 1 being the next element and so on.
   *
   * These offsets can also be negative numbers indicating offsets starting at the end of the list. For example, -1 is
   * the last element of the list, -2 the penultimate, and so on.
   *
   * @see http://redis.io/commands/lrange
   */
  override def lrange[A](key: Any, start: Int, end: Int)
                        (implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
    getRaw(key) map (_.data.sliceFromTo(start, end).map(_.parseOption(parse)).toList)

  /**
   * Trim an existing list so that it will contain only the specified range of elements specified. Both start and stop
   * are zero-based indexes, where 0 is the first element of the list (the head), 1 the next element and so on.
   *
   * For example: LTRIM foobar 0 2 will modify the list stored at foobar so that only the first three elements of the
   * list will remain.
   *
   * start and end can also be negative numbers indicating offsets from the end of the list, where -1 is the last
   * element of the list, -2 the penultimate element and so on.
   *
   * Out of range indexes will not produce an error: if start is larger than the end of the list, or start > end, the
   * result will be an empty list (which causes key to be removed). If end is larger than the end of the list, Redis
   * will treat it like the last element of the list.
   *
   * @see http://redis.io/commands/ltrim
   */
  override def ltrim(key: Any, start: Int, end: Int)(implicit format: Format): Boolean = withDB {
    getRaw(key).map { xs => set(key, xs.data.slice(start, end + 1)) }.isDefined
  }

  /**
   * Returns the element at index index in the list stored at key. The index is zero-based, so 0 means the first
   * element, 1 the second element and so on. Negative indices can be used to designate elements starting at the tail of
   * the list. Here, -1 means the last element, -2 means the penultimate and so forth.
   *
   * When the value at key is not a list, an error is returned.
   *
   * @see http://redis.io/commands/lindex
   */
  override def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]): Option[A] =
    getRaw(key).flatMap(_.data.get(index)).map(_.parse(parse))

  /**
   * Sets the list element at index to value. For more information on the index argument, see LINDEX.
   *
   * An error is returned for out of range indexes.
   *
   * @see http://redis.io/commands/lset
   */
  override def lset(key: Any, index: Int, value: Any)(implicit format: Format): Boolean =
    getRaw(key).map { xs =>
      if (index < 0 || xs.data.length <= index) {
        throw new IndexOutOfBoundsException("index out of range")
      }
      set(key, xs.data.updated(index, Bytes(value)))
    }.isDefined

  /**
   * Removes the first count occurrences of elements equal to value from the list stored at key. The count argument
   * influences the operation in the following ways:
   *
   * - count > 0: Remove elements equal to value moving from head to tail.
   * - count < 0: Remove elements equal to value moving from tail to head.
   * - count = 0: Remove all elements equal to value.
   *
   * For example, LREM list -2 "hello" will remove the last two occurrences of "hello" in the list stored at list.
   *
   * Note that non-existing keys are treated like empty lists, so when key does not exist, the command will always
   * return 0.
   *
   * @see http://redis.io/commands/lrem
   */
  override def lrem(key: Any, count: Int, value: Any)(implicit format: Format): Option[Long] = {
    @tailrec
    def f(xs: List[Bytes], n: Int, v: Bytes, sofar: List[Bytes]): List[Bytes] = xs match {
      case x :: ys if x == v && n > 0 => f(ys, n - 1, v, sofar)
      case x :: ys if n > 0 => f(ys, n, v, x :: sofar)
      case _ => sofar.reverse ::: xs
    }

    withDB {
      for {
        v <- getRaw(key)
      } yield {
        val xs = v.data
        val ys = (count match {
          case _ if count < 0 => f(xs.toList.reverse, -count, Bytes(value), Nil).reverse
          case _ if count == 0 => f(xs.toList, xs.length, Bytes(value), Nil)
          case _ => f(xs.toList, count, Bytes(value), Nil)
        }).toVector
        set(key, ys)
        xs.length - ys.length
      }
    }
  }

  /**
   * Removes and returns the first element of the list stored at key.
   *
   * @see http://redis.io/commands/lpop
   */
  override def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] = withDB {
    for {
      v <- getRaw(key)
      x <- v.data.headOption
    } yield {
      set(key, v.data.tail)
      x.parse(parse)
    }
  }

  /**
   * Removes and returns the last element of the list stored at key.
   *
   * @see http://redis.io/commands/rpop
   */
  override def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] = withDB {
    for {
      v <- getRaw(key)
      x <- v.data.lastOption
    } yield {
      set(key, v.data.init)
      x.parse(parse)
    }
  }

  /**
   * Atomically returns and removes the last element (tail) of the list stored at source, and pushes the element at the
   * first element (head) of the list stored at destination.
   *
   * For example: consider source holding the list a,b,c, and destination holding the list x,y,z. Executing RPOPLPUSH
   * results in source holding a,b and destination holding c,x,y,z.
   *
   * If source does not exist, the value nil is returned and no operation is performed. If source and destination are
   * the same, the operation is equivalent to removing the last element from the list and pushing it as first element of
   * the list, so it can be considered as a list rotation command.
   *
   * @see http://redis.io/commands/rpoplpush
   */
  override def rpoplpush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]): Option[A] = withDB {
    for {
      v <- getRaw(srcKey)
      x <- v.data.lastOption
    } yield {
      set(srcKey, v.data.init)
      lpushx(dstKey, x.toArray)
      x.parse(parse)
    }
  }

  /**
   * BRPOPLPUSH is the blocking variant of RPOPLPUSH. When source contains elements, this command behaves exactly like
   * RPOPLPUSH. When source is empty, Redis will block the connection until another client pushes to it or until timeout
   * is reached. A timeout of zero can be used to block indefinitely.
   *
   * See RPOPLPUSH for more information.
   *
   * @see http://redis.io/commands/brpoplpush
   */
  override def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)
                            (implicit format: Format, parse: Parse[A]): Option[A] =
    loopUntilFound(List(srcKey))(rpoplpush(_, dstKey))(getTimeLimit(timeoutInSeconds))

  /**
   * BLPOP is a blocking list pop primitive. It is the blocking version of LPOP because it blocks the connection when
   * there are no elements to pop from any of the given lists. An element is popped from the head of the first list that
   * is non-empty, with the given keys being checked in the order that they are given.
   *
   * @see http://redis.io/commands/blpop
   */
  override def blpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)
                          (implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[(K, V)] = {
    loopUntilFound(key :: keys.toList) { k => lpop(k)(format, parseV).map((k, _)) }(getTimeLimit(timeoutInSeconds))
  }

  /**
   * BRPOP is a blocking list pop primitive. It is the blocking version of RPOP because it blocks the connection when
   * there are no elements to pop from any of the given lists. An element is popped from the tail of the first list that
   * is non-empty, with the given keys being checked in the order that they are given.
   *
   * See the BLPOP documentation for the exact semantics, since BRPOP is identical to BLPOP with the only difference
   * being that it pops elements from the tail of a list instead of popping from the head.
   *
   * @see http://redis.io/commands/brpop
   */
  override def brpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)
                          (implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[(K, V)] = {
    loopUntilFound(key :: keys.toList) { k => rpop(k)(format, parseV).map((k, _)) }(getTimeLimit(timeoutInSeconds))
  }
}
