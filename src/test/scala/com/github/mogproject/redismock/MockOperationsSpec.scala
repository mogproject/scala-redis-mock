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

  describe("sort (additional)") {
    it("should sort list") {
      r.lpush("list-1", 0, -100, -25, 30, 456, 7, 8901)
      r.lpush("list-1", 20000000000L)
      r.lpush("list-1", -0.01, 0.01, 3.14)

      r.sort("list-1", None, desc=false, alpha=false) shouldBe Some(List(
        "-100", "-25", "-0.01", "0", "0.01", "3.14", "7", "30", "456", "8901", "20000000000").map(Some.apply))
      r.sort("list-1", None, desc=false, alpha=true) shouldBe Some(List(
        "-0.01", "-100", "-25", "0", "0.01", "20000000000", "3.14", "30", "456", "7", "8901").map(Some.apply))
      r.sort("list-1", None, desc=true, alpha=false) shouldBe Some(List(
        "20000000000", "8901", "456", "30", "7", "3.14", "0.01", "0", "-0.01", "-25", "-100").map(Some.apply))
      r.sort("list-1", None, desc=true, alpha=true) shouldBe Some(List(
        "8901", "7", "456", "30", "3.14", "20000000000", "0.01", "0", "-25", "-100", "-0.01").map(Some.apply))
    }
    it("should sort set") {
      r.sadd("set-1", 0, -100, -25, 30, 456, 7, 8901)
      r.sadd("set-1", 20000000000L)
      r.sadd("set-1", -0.01, 0.01, 3.14)

      r.sort("set-1", None, desc=false, alpha=false) shouldBe Some(List(
        "-100", "-25", "-0.01", "0", "0.01", "3.14", "7", "30", "456", "8901", "20000000000").map(Some.apply))
      r.sort("set-1", None, desc=false, alpha=true) shouldBe Some(List(
        "-0.01", "-100", "-25", "0", "0.01", "20000000000", "3.14", "30", "456", "7", "8901").map(Some.apply))
      r.sort("set-1", None, desc=true, alpha=false) shouldBe Some(List(
        "20000000000", "8901", "456", "30", "7", "3.14", "0.01", "0", "-0.01", "-25", "-100").map(Some.apply))
      r.sort("set-1", None, desc=true, alpha=true) shouldBe Some(List(
        "8901", "7", "456", "30", "3.14", "20000000000", "0.01", "0", "-25", "-100", "-0.01").map(Some.apply))
    }
    it("should sort sorted set") {
      r.zadd("zset-1", 0, 0, (0, -100), (0, -25), (0, 30), (0, 456), (0, 7), (0, 8901))
      r.zadd("zset-1", 0, 20000000000L)
      r.zadd("zset-1", 0, -0.01, (0, 0.01), (0, 3.14))

      r.sort("zset-1", None, desc=false, alpha=false) shouldBe Some(List(
        "-100", "-25", "-0.01", "0", "0.01", "3.14", "7", "30", "456", "8901", "20000000000").map(Some.apply))
      r.sort("zset-1", None, desc=false, alpha=true) shouldBe Some(List(
        "-0.01", "-100", "-25", "0", "0.01", "20000000000", "3.14", "30", "456", "7", "8901").map(Some.apply))
      r.sort("zset-1", None, desc=true, alpha=false) shouldBe Some(List(
        "20000000000", "8901", "456", "30", "7", "3.14", "0.01", "0", "-0.01", "-25", "-100").map(Some.apply))
      r.sort("zset-1", None, desc=true, alpha=true) shouldBe Some(List(
        "8901", "7", "456", "30", "3.14", "20000000000", "0.01", "0", "-25", "-100", "-0.01").map(Some.apply))
    }
    it("should return empty list when the key does not exist") {
      r.sort("xxx-1") shouldBe Some(List.empty)
    }
    it("should throw exception when illegal type") {
      r.set("str-1", 0)
      r.hset("hash-1", "field", 0)

      val t1 = the[Exception] thrownBy r.sort("str-1")
      t1.getMessage shouldBe "WRONGTYPE Operation against a key holding the wrong kind of value"

      val t2 = the[Exception] thrownBy r.sort("hash-1")
      t2.getMessage shouldBe "WRONGTYPE Operation against a key holding the wrong kind of value"
    }
    it("should work with nosort option") {
      r.lpush("list-1", 1, 23, 4, 567, 8)
      r.sort("list-1", desc=false, alpha=false, by = Some("nosort")) shouldBe Some(List(
        Some("8"), Some("567"), Some("4"), Some("23"), Some("1")))
      r.sort("list-1", desc=false, alpha=false, by = Some("")) shouldBe Some(List(
        Some("8"), Some("567"), Some("4"), Some("23"), Some("1")))
      r.sort("list-1", desc=false, alpha=false, by = Some("xxx")) shouldBe Some(List(
        Some("8"), Some("567"), Some("4"), Some("23"), Some("1")))
    }
    it("should sort by string lookup") {
      r.lpush("list-1", 1, 23, 4, 567, 8)
      r.lpush("list-2", 1, 9, 99, 23)
      r.mset("x-1-y" -> 5, "x-23-y" -> 40, "x-4-y" -> 1, "x-567-y" -> 2, "x-8-y" -> 3)

      r.sort("list-1", desc=false, alpha=false, by = Some("x-*-y")) shouldBe Some(List(
        Some("4"), Some("567"), Some("8"), Some("1"), Some("23")))
      r.sort("list-1", desc=false, alpha=true, by = Some("x-*-y")) shouldBe Some(List(
        Some("4"), Some("567"), Some("8"), Some("23"), Some("1")))
      r.sort("list-1", desc=true, alpha=false, by = Some("x-*-y")) shouldBe Some(List(
        Some("23"), Some("1"), Some("8"), Some("567"), Some("4")))
      r.sort("list-1", desc=true, alpha=true, by = Some("x-*-y")) shouldBe Some(List(
        Some("1"), Some("23"), Some("8"), Some("567"), Some("4")))

      r.sort("list-2", desc=false, alpha=true, by = Some("x-*-y")) shouldBe Some(List(
        Some("99"), Some("9"), Some("23"), Some("1")))
    }
    it("should work with string getter") {
      r.lpush("list-1", 1, 23, 4)
      r.set("x-1-y", "v1")
      r.set("x-23-y", "v23")
      r.set("x-4-y", "v4")
      r.sort("list-1", desc=false, alpha=false, get = List("x-*-y", "xxx-*", "list-1", "#", "xxx")) shouldBe Some(List(
        Some("v1"), None, None, Some("1"), None,
        Some("v4"), None, None, Some("4"), None,
        Some("v23"), None, None, Some("23"), None
      ))
    }
  }
}
