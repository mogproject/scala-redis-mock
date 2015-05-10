package com.github.mogproject.redismock.util

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, FunSpec}

class EnhancedSeqSpec extends FunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  describe("EnhancedSeq#sliceFromUntil") {
    it("should work with negative indices") {
      val xs = new EnhancedSeq(Seq(10, 20, 30))
      xs.sliceFromUntil(1, -1) shouldBe Seq(20)
    }
  }

  describe("EnhancedSeq#sliceFromTo") {
    it("should work with negative indices") {
      val xs = new EnhancedSeq(Seq(10, 20, 30))
      xs.sliceFromTo(1, -1) shouldBe Seq(20, 30)
    }
  }

}
