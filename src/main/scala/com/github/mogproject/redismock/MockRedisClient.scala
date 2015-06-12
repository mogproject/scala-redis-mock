package com.github.mogproject.redismock

import com.redis._


class MockRedisClient(override val host: String,
                      override val port: Int,
                      override val database: Int = 0,
                      override val secret: Option[Any] = None,
                      override val timeout: Int = 0
                       ) extends RedisClient with MockIO with MockRedisCommand {
  connect
}

trait MockRedisCommand extends Redis with MockOperations
with MockNodeOperations
with MockStringOperations
with MockListOperations
with MockSetOperations
with MockSortedSetOperations
with MockHashOperations
with MockEvalOperations
with MockPubOperations
with MockHyperLogLogOperations
{

}
