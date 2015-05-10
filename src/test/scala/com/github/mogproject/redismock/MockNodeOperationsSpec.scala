package com.github.mogproject.redismock

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}


class MockNodeOperationsSpec extends FunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
   describe("all commands") {
     it("should not be supported") {
       if (TestUtil.useRealRedis) {
         println("[INFO] Node tests are skipped for real redis.")
       } else {
         val r = TestUtil.getRedisClient

         the [UnsupportedOperationException] thrownBy r.save
         the [UnsupportedOperationException] thrownBy r.bgsave
         the [UnsupportedOperationException] thrownBy r.lastsave
         the [UnsupportedOperationException] thrownBy r.shutdown
         the [UnsupportedOperationException] thrownBy r.bgrewriteaof
         the [UnsupportedOperationException] thrownBy r.info
         the [UnsupportedOperationException] thrownBy r.monitor
         the [UnsupportedOperationException] thrownBy r.slaveof("")
         the [UnsupportedOperationException] thrownBy r.slaveOf("")
       }
     }
   }

 }
