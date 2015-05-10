package com.github.mogproject.redismock

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, FunSpec}

class MockIOSpec extends FunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  class TestIO extends MockIO {
    val host = "localhost"
    val port = 0
    val timeout = 0
  }

  describe("MockIO#connect") {
    it("should throw exception when already connected") {
      val io = new TestIO

      io.connected shouldBe false
      io.connect shouldBe true
      io.connected shouldBe true
      the [RuntimeException] thrownBy io.connect
    }
  }

  describe("MockIO#disconnect") {
    it("should clear connection") {
      val io = new TestIO

      io.connected shouldBe false
      io.connect shouldBe true
      io.connected shouldBe true
      io.disconnect shouldBe true
      io.connected shouldBe false
    }
    it("should succeed when already disconnected") {
      val io = new TestIO

      io.connected shouldBe false
      io.disconnect shouldBe true
      io.connected shouldBe false
    }
  }

  describe("MockIO#write_to_socket") {
    it("should throw exception") {
      val io = new TestIO
      the [UnsupportedOperationException] thrownBy io.write_to_socket(Array.empty)(_ => ())
    }
  }

  describe("MockIO#write") {
    it("should throw exception") {
      val io = new TestIO
      the [UnsupportedOperationException] thrownBy io.write(Array.empty)
    }
  }

  describe("MockIO#readLine") {
    it("should throw exception") {
      val io = new TestIO
      the [UnsupportedOperationException] thrownBy io.readLine
    }
  }

  describe("MockIO#readCounted") {
    it("should throw exception") {
      val io = new TestIO
      the [UnsupportedOperationException] thrownBy io.readCounted(0)
    }
  }

}
