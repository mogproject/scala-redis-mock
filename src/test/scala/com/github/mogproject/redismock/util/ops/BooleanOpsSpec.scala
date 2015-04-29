package com.github.mogproject.redismock.util.ops

import org.scalatest.{Matchers, FunSpec}

class BooleanOpsSpec extends FunSpec with Matchers {

  describe("BooleanOps#whenTrue") {
    it("should return false with no side effect") {
      var x = 0
      false whenTrue (x += 1) shouldBe false
      x shouldBe 0
    }
    it("should return true with side effect") {
      var x = 0
      true whenTrue (x += 1) shouldBe true
      x shouldBe 1
    }
  }

  describe("BooleanOps#whenFalse") {
    it("should return false with side effect") {
      var x = 0
      false whenFalse (x += 1) shouldBe false
      x shouldBe 1
    }
    it("should return true with no side effect") {
      var x = 0
      true whenFalse (x += 1) shouldBe true
      x shouldBe 0
    }
  }

}
