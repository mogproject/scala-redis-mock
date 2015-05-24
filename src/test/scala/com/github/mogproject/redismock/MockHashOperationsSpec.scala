package com.github.mogproject.redismock

import org.scalacheck.Gen
import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.prop.GeneratorDrivenPropertyChecks


class MockHashOperationsSpec extends FunSpec
with Matchers
with BeforeAndAfterEach
with BeforeAndAfterAll
with GeneratorDrivenPropertyChecks {

  val r = TestUtil.getRedisClient

  override def beforeEach = {
  }

  override def afterEach = {
    r.flushdb
  }

  override def afterAll = {
    r.disconnect
  }

  describe("hset") {
    it("should set and get fields") {
      r.hset("hash1", "field1", "val")
      r.hget("hash1", "field1") should be(Some("val"))
    }

    it("should set and get maps") {
      r.hmset("hash2", Map("field1" -> "val1", "field2" -> "val2"))
      r.hmget("hash2", "field1") should be(Some(Map("field1" -> "val1")))
      r.hmget("hash2", "field1", "field2") should be(Some(Map("field1" -> "val1", "field2" -> "val2")))
      r.hmget("hash2", "field1", "field2", "field3") should be(Some(Map("field1" -> "val1", "field2" -> "val2")))
    }

    it("should increment map values") {
      r.hincrby("hash3", "field1", 1)
      r.hget("hash3", "field1") should be(Some("1"))
    }

    it("should check existence") {
      r.hset("hash4", "field1", "val")
      r.hexists("hash4", "field1") should equal(true)
      r.hexists("hash4", "field2") should equal(false)
    }

    it("should delete fields") {
      r.hset("hash5", "field1", "val")
      r.hexists("hash5", "field1") should equal(true)
      r.hdel("hash5", "field1") should equal(Some(1))
      r.hexists("hash5", "field1") should equal(false)
      r.hmset("hash5", Map("field1" -> "val1", "field2" -> "val2"))
      r.hdel("hash5", "field1", "field2") should equal(Some(2))
    }

    it("should return the length of the fields") {
      r.hmset("hash6", Map("field1" -> "val1", "field2" -> "val2"))
      r.hlen("hash6") should be(Some(2))
    }

    it("should return the aggregates") {
      r.hmset("hash7", Map("field1" -> "val1", "field2" -> "val2"))
      r.hkeys("hash7") should be(Some(List("field1", "field2")))
      r.hvals("hash7") should be(Some(List("val1", "val2")))
      r.hgetall("hash7") should be(Some(Map("field1" -> "val1", "field2" -> "val2")))
    }

    it("should increment map values by floats") {
      r.hset("hash1", "field1", 10.50f)
      r.hincrbyfloat("hash1", "field1", 0.1f) should be(Some(10.6f))
      r.hset("hash1", "field1", 5.0e3f)
      r.hincrbyfloat("hash1", "field1", 2.0e2f) should be(Some(5200f))
      r.hset("hash1", "field1", "abc")
      val thrown = the[Exception] thrownBy {r.hincrbyfloat("hash1", "field1", 2.0e2f)}
      thrown.getMessage should include("hash value is not a valid float")
    }
  }

  //
  // additional tests
  //
  describe("hscan (additional)") {
    it("should work with non-existent key") {
      r.hscan("hash-1", 0) shouldBe Some((Some(0), Some(List())))
    }
    it("should work with empty set") {
      r.hset("hash-1", "field-1", "value-1")
      r.hdel("hash-1", "field-1")

      forAll(Gen.choose(-100, 100)) { i =>
        r.hscan("hash-1", i) shouldBe Some((Some(0), Some(List())))
      }
    }
    it("should work with set with one element") {
      r.hset("hash-1", "field-1", "value-1")
      r.hscan("hash-1", 0) shouldBe Some((Some(0), Some(List(Some("field-1"), Some("value-1")))))

      // Calling SCAN with a broken, negative, out of range, or otherwise invalid cursor, will result into undefined
      // behavior but never into a crash.
      forAll(Gen.choose(-100, 100)) { i =>
        r.hscan("hash-1", i) should (be(Some((Some(0), Some(List())))) or
          be(Some((Some(0), Some(List(Some("field-1"), Some("value-1")))))))
      }
    }
    it("should get back to 0 after iteration") {
      r.hmset("hash-1", (0 to 100).map(i => s"field-${i}" -> s"value-${i}"))

      @annotation.tailrec
      def f(cursor: Int, sofar: List[List[Option[String]]], count: Int): List[List[Option[String]]] = {
        if (count <= 0)
          fail("too many loop count")
        else if (cursor == 0 && sofar.nonEmpty)
          sofar
        else
          r.hscan("hash-1", cursor) match {
            case Some((Some(x), Some(xs))) => f(x, xs :: sofar, count - 1)
            case _ => sofar
          }
      }

      val result = f(0, Nil, 100)
      result.flatten should have size 202
      result.flatten.toSet should have size 202
      result.size should be <= 11
    }
    it("should throw exception with the negative or zero count") {
      r.hset("hash-1", "field-1", "value-1")
      val t1 = the[Exception] thrownBy r.hscan("hash-1", 0, "*", 0)
      t1.getMessage shouldBe "ERR syntax error"
      val t2 = the[Exception] thrownBy r.hscan("hash-1", 0, "*", Int.MinValue)
      t2.getMessage shouldBe "ERR syntax error"
    }
  }
}
