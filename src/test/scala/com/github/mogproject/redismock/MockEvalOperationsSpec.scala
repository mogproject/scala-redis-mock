package com.github.mogproject.redismock

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, FunSpec}


class MockEvalOperationsSpec extends FunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  describe("all commands") {
    it("should not be supported") {
      if (TestUtil.useRealRedis) {
        println("[INFO] Scripting tests are skipped for real redis.")
      } else {
        val r = TestUtil.getRedisClient

        the [UnsupportedOperationException] thrownBy r.evalMultiBulk("", List(), List())
        the [UnsupportedOperationException] thrownBy r.evalBulk("", List(), List())
        the [UnsupportedOperationException] thrownBy r.evalMultiSHA("", List(), List())
        the [UnsupportedOperationException] thrownBy r.evalSHA("", List(), List())
        the [UnsupportedOperationException] thrownBy r.scriptLoad("")
        the [UnsupportedOperationException] thrownBy r.scriptExists("")
        the [UnsupportedOperationException] thrownBy r.scriptFlush
      }
    }
  }

}
