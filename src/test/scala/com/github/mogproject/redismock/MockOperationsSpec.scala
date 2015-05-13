package com.github.mogproject.redismock

import com.redis.serialization.Parse
import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers


class MockOperationsSpec extends FunSpec
with Matchers
with BeforeAndAfterEach
with BeforeAndAfterAll {

  val r = TestUtil.getRedisClient

  override def beforeEach = {
    r.select(0)
  }

  override def afterEach = {
    r.flushall
  }

  override def afterAll = {
    r.disconnect
  }

  describe("keys") {
    it("should fetch keys") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.keys("anshin*") match {
        case Some(s: List[Option[String]]) => s.size should equal(2)
        case None => fail("should have 2 elements")
      }
    }

    it("should fetch keys with spaces") {
      r.set("anshin 1", "debasish")
      r.set("anshin 2", "maulindu")
      r.keys("anshin*") match {
        case Some(s: List[Option[String]]) => s.size should equal(2)
        case None => fail("should have 2 elements")
      }
    }
  }

  describe("randomkey") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.randomkey match {
        case Some(s: String) => s should startWith("anshin")
        case None => fail("should have 2 elements")
      }
    }
  }

  describe("rename") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.rename("anshin-2", "anshin-2-new") should equal(true)
      val thrown = the[Exception] thrownBy {r.rename("anshin-2", "anshin-2-new")}
      thrown.getMessage should equal("ERR no such key")
    }
  }

  describe("renamenx") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.renamenx("anshin-2", "anshin-2-new") should equal(true)
      r.renamenx("anshin-1", "anshin-2-new") should equal(false)
    }
  }

  describe("dbsize") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.dbsize.get should equal(2)
    }
  }

  describe("exists") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.exists("anshin-2") should equal(true)
      r.exists("anshin-1") should equal(true)
      r.exists("anshin-3") should equal(false)
    }
  }

  describe("del") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.del("anshin-2", "anshin-1").get should equal(2)
      r.del("anshin-2", "anshin-1").get should equal(0)
    }
  }

  describe("type") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.getType("anshin-2").get should equal("string")
    }
  }

  describe("expire") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.expire("anshin-2", 1000) should equal(true)
      r.ttl("anshin-2") should equal(Some(1000))
      r.expire("anshin-3", 1000) should equal(false)
    }
  }

  describe("persist") {
    it("should give") {
      r.set("key-2", "maulindu")
      r.expire("key-2", 1000) should equal(true)
      r.ttl("key-2") should equal(Some(1000))
      r.persist("key-2") should equal(true)
      r.ttl("key-2") should equal(Some(-1))
      r.persist("key-3") should equal(false)
    }
  }
  describe("sort") {
    it("should give") {
      // sort[A](key:String, limit:Option[Pair[Int, Int]] = None, desc:Boolean = false, alpha:Boolean = false, by:Option[String] = None, get:List[String] = Nil)(implicit format:Format, parse:Parse[A]):Option[List[Option[A]]] = {
      r.hset("hash-1", "description", "one")
      r.hset("hash-1", "order", "100")
      r.hset("hash-2", "description", "two")
      r.hset("hash-2", "order", "25")
      r.hset("hash-3", "description", "three")
      r.hset("hash-3", "order", "50")
      r.sadd("alltest", 1)
      r.sadd("alltest", 2)
      r.sadd("alltest", 3)
      r.sort("alltest").getOrElse(Nil) should equal(List(Some("1"), Some("2"), Some("3")))
      r.sort("alltest", Some((0, 1))).getOrElse(Nil) should equal(List(Some("1")))
      r.sort("alltest", None, true).getOrElse(Nil) should equal(List(Some("3"), Some("2"), Some("1")))
      r.sort("alltest", None, false, false, Some("hash-*->order")).getOrElse(Nil) should equal(List(Some("2"), Some("3"), Some("1")))
      r.sort("alltest", None, false, false, None, List("hash-*->description")).getOrElse(Nil) should equal(List(Some("one"), Some("two"), Some("three")))
      r.sort("alltest", None, false, false, None, List("hash-*->description", "hash-*->order")).getOrElse(Nil) should equal(List(Some("one"), Some("100"), Some("two"), Some("25"), Some("three"), Some("50")))
    }
  }
  describe("sortNStore") {
    it("should give") {
      r.sadd("alltest", 10)
      r.sadd("alltest", 30)
      r.sadd("alltest", 3)
      r.sadd("alltest", 1)

      // default serialization : return String
      r.sortNStore("alltest", storeAt = "skey").getOrElse(-1) should equal(4)
      r.lrange("skey", 0, 10).get should equal(List(Some("1"), Some("3"), Some("10"), Some("30")))

      // Long serialization : return Long
      implicit val parseLong = Parse[Long](new String(_).toLong)
      r.sortNStore[Long]("alltest", storeAt = "skey").getOrElse(-1) should equal(4)
      r.lrange("skey", 0, 10).get should equal(List(Some(1), Some(3), Some(10), Some(30)))
    }
  }

  //
  // additional tests
  //
  describe("type (additional)") {
    it("should give") {
      r.lpush("list-1", "foo", "bar")
      r.sadd("set-1", 10)
      r.hset("hash-1", "description", "one")
      r.zadd("zset-1", 1.23, 20)

      r.getType("list-x") shouldBe Some("none")
      r.getType("list-1") shouldBe Some("list")
      r.getType("set-1") shouldBe Some("set")
      r.getType("hash-1") shouldBe Some("hash")
      r.getType("zset-1") shouldBe Some("zset")
    }
  }

  describe("randkey (additional)") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.randkey match {
        case Some(s: String) => s should startWith("anshin")
        case None => fail("should have 2 elements")
      }
    }
  }

  describe("expireat (additional)") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.expireat("anshin-2", System.currentTimeMillis() / 1000 + 1000) shouldBe true
      r.ttl("anshin-2").get should (be >= 999L and be <= 1000L)
      r.expireat("anshin-3", System.currentTimeMillis() / 1000 + 1000) shouldBe false
    }
  }

  describe("pexpireat (additional)") {
    it("should give") {
      r.set("anshin-1", "debasish")
      r.set("anshin-2", "maulindu")
      r.pexpireat("anshin-2", System.currentTimeMillis() + 1000000L) shouldBe true
      r.ttl("anshin-2").get should (be >= 999L and be <= 1000L)
      r.pexpireat("anshin-3", System.currentTimeMillis() + 1000000L) shouldBe false
    }
  }

  describe("select (additional)") {
    it("should change database") {
      r.set("anshin-1", "debasish")
      r.dbsize shouldBe Some(1)
      r.select(10) shouldBe true
      r.dbsize shouldBe Some(0)
    }
  }

  describe("flushdb (additional)") {
    it("should clear current database") {
      (0 to 5).map { i =>
        r.select(i)
        r.set("anshin-1", "debasish")
        r.dbsize
      } shouldBe Seq(Some(1), Some(1), Some(1), Some(1), Some(1), Some(1))
      r.flushdb shouldBe true
      (0 to 5).map { i =>
        r.select(i)
        r.dbsize
      } shouldBe Seq(Some(1), Some(1), Some(1), Some(1), Some(1), Some(0))
    }
  }

  describe("flushall (additional)") {
    it("should clear all databases") {
      (0 to 5).map { i =>
        r.select(i)
        r.set("anshin-1", "debasish")
        r.dbsize
      } shouldBe Seq(Some(1), Some(1), Some(1), Some(1), Some(1), Some(1))
      r.flushall shouldBe true
      (0 to 5).map { i =>
        r.select(i)
        r.dbsize
      } shouldBe Seq(Some(0), Some(0), Some(0), Some(0), Some(0), Some(0))
    }
  }

  describe("move (additional)") {
    it("should throw exception when the destination is same as source") {
      r.set("anshin-1", "debasish")

      val thrown = the[Exception] thrownBy r.move("anshin-1", 0)
      thrown.getMessage shouldBe "ERR source and destination objects are the same"
    }
    it("should return false when the key is missing") {
      r.move("anshin-3", 10) shouldBe false
    }
    it("should move key to another database without keeping ttl") {
      r.set("anshin-1", "debasish")
      r.setex("anshin-2", 1000L, "maulindu")
      r.move("anshin-1", 10) shouldBe true
      r.move("anshin-2", 10) shouldBe true
      r.exists("anshin-1") shouldBe false
      r.exists("anshin-2") shouldBe false
      r.select(10)
      r.ttl("anshin-1").get shouldBe -1L
      r.ttl("anshin-2").get shouldBe -1L
    }
  }

  describe("auth (additional)") {
    it("should throw exception") {
      val thrown = the[Exception] thrownBy r.auth("secret")
      thrown.getMessage shouldBe "ERR Client sent AUTH, but no password is set"
    }
  }
}
