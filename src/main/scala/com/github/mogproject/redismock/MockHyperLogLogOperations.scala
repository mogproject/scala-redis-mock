package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity.StringValue
import com.github.mogproject.redismock.generic.GenericOperations
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.Bytes
import com.redis.{Redis, HyperLogLogOperations}

/**
 * HyperLogLog operations
 */
trait MockHyperLogLogOperations extends HyperLogLogOperations with MockOperations with Storage with GenericOperations {
  self: Redis =>

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
  override def pfadd(key: Any, value: Any, values: Any*): Long =
    send("PFADD", List(key, value) ::: values.toList)(asLong).get

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
  override def pfcount(keys: Any*): Long =
    send("PFCOUNT", keys.toList)(asLong).get

  /**
   * Merge multiple HyperLogLog values into an unique value that will approximate the cardinality of the union of the
   * observed Sets of the source HyperLogLog structures.
   *
   * @note Available since 2.8.9.
   * @see http://redis.io/commands/pfmerge
   */
  override def pfmerge(destination: Any, sources: Any*) =
    send("PFMERGE", List(destination) ::: sources.toList)(asBoolean)
}


case class HyperLogLog(isDense: Boolean = false, registers: Seq[Int] = Seq.fill(1 << 14)(0)) {

  def add(value: Bytes): HyperLogLog = {
    ???
  }


  def sparseBytes: Bytes = {
    ???
  }

  def denseBytes: Bytes = {
    ???
  }

  def toRedisValue: StringValue = {
    ???
  }
}