package com.github.mogproject.redismock

import com.redis.{Redis, PubOperations}


trait MockPubOperations extends PubOperations {
  self: Redis =>

  override def publish(channel: String, msg: String): Option[Long] =
    throw new UnsupportedOperationException("pub operation is not supported")
}
