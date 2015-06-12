package com.github.mogproject.redismock

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}


class MockPubOperationsSpec extends FunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
   describe("publish") {
     it("should not be supported") {
       if (TestUtil.useRealRedis) {
         println("[INFO] Pub tests are skipped for real redis.")
       } else {
         val r = TestUtil.getRedisClient

         the [UnsupportedOperationException] thrownBy r.publish("channel", "msg")
       }
     }
   }

 }
