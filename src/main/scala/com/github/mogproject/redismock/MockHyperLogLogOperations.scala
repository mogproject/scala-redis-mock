package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity.{HyperLogLog, ValueCompanion, StringValue}
import com.github.mogproject.redismock.generic.GenericOperations
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.Bytes
import com.redis.{Redis, HyperLogLogOperations}

import scala.annotation.tailrec


/**
 * HyperLogLog operations
 */
trait MockHyperLogLogOperations extends HyperLogLogOperations with MockOperations with Storage with GenericOperations {
  self: Redis =>

  private implicit val companion: ValueCompanion[StringValue] = StringValue

  private def getAsHLL(key: Any): Option[HyperLogLog] = getRaw(key).map(v => HyperLogLog.fromBytes(v.data))

  /**
   * Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as first
   * argument.
   *
   * As a side effect of this command the HyperLogLog internals may be updated to reflect a different estimation of the
   * number of unique items added so far (the cardinality of the set).
   *
   * @note Available since 2.8.9.
   * @see http://redis.io/commands/pfadd
   */
  override def pfadd(key: Any, value: Any, values: Any*): Long = withDB {
    @tailrec
    def f(hll: HyperLogLog, updated: Boolean, values: List[Any]): (HyperLogLog, Boolean) = values match {
      case x :: xs =>
        val (h, b) = hll.add(Bytes(x))
        f(h, updated || b, xs)
      case Nil => (hll, updated)
    }

    val (hll, updated: Boolean) = f(getAsHLL(key).getOrElse(HyperLogLog()), updated = false, value :: values.toList)
    setRaw(key, StringValue(hll.toBytes))
    if (updated) 1L else 0L
  }

  /**
   * When called with a single key, returns the approximated cardinality computed by the HyperLogLog data structure
   * stored at the specified variable, which is 0 if the variable does not exist.
   *
   * When called with multiple keys, returns the approximated cardinality of the union of the HyperLogLogs passed, by
   * internally merging the HyperLogLogs stored at the provided keys into a temporary hyperLogLog.
   *
   * @note Available since 2.8.9.
   * @see http://redis.io/commands/pfcount
   */
  override def pfcount(keys: Any*): Long = withDB {
    // merge all keys
    val count = keys.toList.map(getAsHLL).flatten.foldLeft(HyperLogLog())((a, h) => a.merge(h)).count

    // if the key is just one and exists, update its cache value
    if (keys.length == 1) {
      val k = keys.head
      getAsHLL(k).foreach(h => setRaw(k, StringValue(h.setCache(count).toBytes)))
    }
    count
  }

  /**
   * Merge multiple HyperLogLog values into an unique value that will approximate the cardinality of the union of the
   * observed Sets of the source HyperLogLog structures.
   *
   * @note Available since 2.8.9.
   * @see http://redis.io/commands/pfmerge
   */
  override def pfmerge(destination: Any, sources: Any*): Boolean = withDB {
    val hll = sources.toList.map(getAsHLL).flatten.foldLeft(HyperLogLog())((a, h) => a.merge(h))
    setRaw(destination, StringValue(hll.toBytes))
    true
  }

}
