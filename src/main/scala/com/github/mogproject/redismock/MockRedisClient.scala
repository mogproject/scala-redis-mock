package com.github.mogproject.redismock

import com.redis._


class MockRedisClient(override val host: String,
                      override val port: Int,
                      override val database: Int = 0,
                      override val secret: Option[Any] = None,
                      override val timeout: Int = 0
                       ) extends RedisClient with MockIO with MockRedisCommand {

}

trait MockRedisCommand extends Redis with MockOperations
//with NodeOperations
with MockStringOperations
//with ListOperations
//with SetOperations
//with SortedSetOperations
//with HashOperations
//with EvalOperations
//with PubOperations
//with HyperLogLogOperations
{

}
