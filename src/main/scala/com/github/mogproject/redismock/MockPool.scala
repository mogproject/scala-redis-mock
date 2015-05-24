package com.github.mogproject.redismock

import com.redis.{RedisClient, RedisClientPool}
import org.apache.commons.pool._
import org.apache.commons.pool.impl._

private[redismock] class MockRedisClientFactory(val host: String,
                                                val port: Int,
                                                val database: Int = 0,
                                                val secret: Option[Any] = None,
                                                val timeout: Int = 0)
  extends PoolableObjectFactory[MockRedisClient] {

  // when we make an object it's already connected
  override def makeObject = {
    new MockRedisClient(host, port, database, secret, timeout)
  }

  // quit & disconnect
  override def destroyObject(rc: MockRedisClient): Unit = {
    rc.quit // need to quit for closing the connection
    rc.disconnect // need to disconnect for releasing sockets
  }

  // noop: we want to have it connected
  override def passivateObject(rc: MockRedisClient): Unit = {}

  override def validateObject(rc: MockRedisClient) = rc.connected

  // noop: it should be connected already
  override def activateObject(rc: MockRedisClient): Unit = {}
}

class MockRedisClientPool(override val host: String,
                          override val port: Int,
                          override val maxIdle: Int = 8,
                          override val database: Int = 0,
                          override val secret: Option[Any] = None,
                          override val timeout: Int = 0
                           ) extends RedisClientPool(host, port, maxIdle, database, secret, timeout) {
  val mockPool = new StackObjectPool(new MockRedisClientFactory(host, port, database, secret, timeout), maxIdle)

  override def withClient[T](body: RedisClient => T) = {
    val client = mockPool.borrowObject
    try {
      body(client)
    } finally {
      mockPool.returnObject(client)
    }
  }

  // close pool & free resources
  override def close() = mockPool.close
}
