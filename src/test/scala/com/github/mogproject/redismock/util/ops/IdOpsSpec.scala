package com.github.mogproject.redismock.util.ops

import org.scalatest.{Matchers, FunSpec}

class IdOpsSpec extends FunSpec with Matchers {

  describe("IdOps#|>") {
    it("should pipe operations") {
      3 |> (_ * 2) shouldBe 6
      3 |> (_ * 2) |> (_ - 1) shouldBe 5
    }
  }

  describe("IdOps#<|") {
    it("should do with side effect and return the original value") {
      true <| { x => assert(x) } shouldBe true
      123 <| { x => assert(x == 123) } shouldBe 123
    }
  }

}
