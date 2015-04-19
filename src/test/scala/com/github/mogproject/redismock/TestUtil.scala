package com.github.mogproject.redismock

import com.redis.{RedisClientPool, RedisClient}

/**
 * Utilities for testing
 */
object TestUtil {

  /** set environment variable "USE_REAL_REDIS=yes" to test with real Redis */
  val useRealRedis = sys.env.get("USE_REAL_REDIS").exists(_.toLowerCase == "yes")

  def getRedisClient: RedisClient = {
    if (useRealRedis) {
      println("[INFO] Connecting to REAL redis-server...")
      new RedisClient("localhost", 6379)
    } else {
      new MockRedisClient("localhost", 6379)
    }
  }

  def getRedisClientPool: RedisClientPool = {
    if (useRealRedis) {
      println("[INFO] Connecting to REAL redis-server...")
      new RedisClientPool("localhost", 6379)
    } else {
      new MockRedisClientPool("localhost", 6379)
    }
  }

}
