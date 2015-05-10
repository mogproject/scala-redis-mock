package com.github.mogproject.redismock.entity

import com.github.mogproject.redismock.util.Bytes
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, FunSpec}


class KeySpec extends FunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  describe("Key#apply") {
    it("should instantiate with Format") {
      import com.redis.serialization.Format.default

      Key("AB") shouldBe Key(Bytes(65, 66))
    }
  }

}
