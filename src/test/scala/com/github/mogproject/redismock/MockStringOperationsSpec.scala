package com.github.mogproject.redismock

import com.redis.Seconds
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scala.collection.parallel.ForkJoinTaskSupport

class MockStringOperationsSpec extends FunSpec
with Matchers
with BeforeAndAfterEach
with BeforeAndAfterAll
with GeneratorDrivenPropertyChecks {

  val r = new MockRedisClient("localhost", 6379)

  def doParallel[A](n: Int)(f: => A) = {
    val p = (1 to n).par
    p.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(8))
    p.foreach(_ => f)
  }

  override def beforeEach = {
  }

  override def afterEach = {
    r.flushdb
  }

  override def afterAll = {
    r.disconnect
  }

  describe("set") {
    it("should set key/value pairs") {
      r.set("anshin-1", "debasish") should equal(true)
      r.set("anshin-2", "maulindu") should equal(true)
    }
  }

  describe("set if not exist") {
    it("should set key/value pairs with exclusiveness and expire") {
      r.set("amit-1", "mor", "nx", "ex", 6)
      r.get("amit-1") match {
        case Some(s: String) => s should equal("mor")
        case None => fail("should return mor")
      }
      r.del("amit-1")
    }
  }

  describe("set if exists or not") {
    it("should set key/value pairs with exclusiveness and expire") {
      r.set("amit-1", "mor", false, Seconds(6))
      r.get("amit-1") match {
        case Some(s: String) => s should equal("mor")
        case None => fail("should return mor")
      }
      Thread.sleep(6000)
      r.get("amit-1") should equal(None)
      r.del("amit-1")
    }
  }

  describe("fail to set if doesn't exist; succeed later because key doesn't exist; success later because key exists") {
    it("should fail to set key/value pairs with exclusiveness and expire") {
      r.del("amit-1")
      // first trying to set with 'xx' should fail since there is not key present
      // r.set("amit-1", "mor", "xx","ex",6)
      r.set("amit-1", "mor", true, Seconds(6))
      r.get("amit-1") match {
        case Some(s: String) => fail("should return None")
        case None =>
      }
      // second, we set if there is no key and we should succeed
      // r.set("amit-1", "mor", "nx","ex",6)
      r.set("amit-1", "mor", false, Seconds(6))
      r.get("amit-1") match {
        case Some(s: String) => s should equal("mor")
        case None => fail("should return mor")
      }

      // third, since the key is now present (if second succeeded), this would succeed too
      // r.set("amit-1", "mor", "xx","ex",6)
      r.set("amit-1", "mor", true, Seconds(6))
      r.get("amit-1") match {
        case Some(s: String) => s should equal("mor")
        case None => fail("should return mor")
      }

    }
  }

  describe("get") {
    it("should retrieve key/value pairs for existing keys") {
      r.set("anshin-1", "debasish") should equal(true)
      r.get("anshin-1") match {
        case Some(s: String) => s should equal("debasish")
        case None => fail("should return debasish")
      }
    }
    it("should fail for non-existent keys") {
      r.get("anshin-2") match {
        case Some(s: String) => fail("should return None")
        case None =>
      }
    }
  }

  describe("getset") {
    it("should set new values and return old values") {
      r.set("anshin-1", "debasish") should equal(true)
      r.get("anshin-1") match {
        case Some(s: String) => s should equal("debasish")
        case None => fail("should return debasish")
      }
      r.getset("anshin-1", "maulindu") match {
        case Some(s: String) => s should equal("debasish")
        case None => fail("should return debasish")
      }
      r.get("anshin-1") match {
        case Some(s: String) => s should equal("maulindu")
        case None => fail("should return maulindu")
      }
    }
  }

  describe("setnx") {
    it("should set only if the key does not exist") {
      r.set("anshin-1", "debasish") should equal(true)
      r.setnx("anshin-1", "maulindu") should equal(false)
      r.setnx("anshin-2", "maulindu") should equal(true)
    }
  }

  describe("setex") {
    it("should set values with expiry") {
      val key = "setex-1"
      val value = "value"
      r.setex(key, 1, value) should equal(true)
      r.get(key) match {
        case Some(s: String) => s should equal(value)
        case None => fail("should return value")
      }
      Thread.sleep(2000)
      r.get(key) match {
        case Some(_) => fail("key-1 should have expired")
        case None =>
      }
    }
  }

  describe("incr") {
    it("should increment by 1 for a key that contains a number") {
      r.set("anshin-1", "10") should equal(true)
      r.incr("anshin-1") should equal(Some(11))
    }
    it("should reset to 0 and then increment by 1 for a key that contains a diff type") {
      r.set("anshin-2", "debasish") should equal(true)
      try {
        r.incr("anshin-2")
      } catch {case ex: Throwable => ex.getMessage should startWith("ERR value is not an integer")}
    }
    it("should increment by 5 for a key that contains a number") {
      r.set("anshin-3", "10") should equal(true)
      r.incrby("anshin-3", 5) should equal(Some(15))
    }
    it("should reset to 0 and then increment by 5 for a key that contains a diff type") {
      r.set("anshin-4", "debasish") should equal(true)
      try {
        r.incrby("anshin-4", 5)
      } catch {case ex: Throwable => ex.getMessage should startWith("ERR value is not an integer")}
    }

    // additional tests
    it("should increment atomically") {
      r.set("anshin-1", "10") should equal(true)
      doParallel(100)(r.incr("anshin-1"))
      r.incr("anshin-1") should equal(Some(111))
    }
    it("should increment by 5 atomically") {
      r.set("anshin-1", "10") should equal(true)
      doParallel(100)(r.incrby("anshin-1", 5))
      r.incrby("anshin-1", 5) should equal(Some(515))
    }
  }

  describe("incrbyfloat") {
    it("should increment values by floats") {
      r.set("k1", 10.50f)
      r.incrbyfloat("k1", 0.1f) should be(Some(10.6f))
      r.set("k1", 5.0e3f)
      r.incrbyfloat("k1", 2.0e2f) should be(Some(5200f))
      r.set("k1", "abc")
      val thrown = the[Exception] thrownBy {r.incrbyfloat("k1", 2.0e2f)}
      thrown.getMessage should include("value is not a valid float")
    }

    // additional tests
    it("should increment by float atomically") {
      r.set("k1", 10.50f) should equal(true)
      doParallel(100)(r.incrbyfloat("k1", 0.1f))
      r.incrbyfloat("k1", 0.1f) match {
        case Some(x) =>
          x should be > 20.599f
          x should be < 20.6001f
        case None => fail("should return Some")
      }
    }
  }

  describe("decr") {
    it("should decrement by 1 for a key that contains a number") {
      r.set("anshin-1", "10") should equal(true)
      r.decr("anshin-1") should equal(Some(9))
    }
    it("should reset to 0 and then decrement by 1 for a key that contains a diff type") {
      r.set("anshin-2", "debasish") should equal(true)
      try {
        r.decr("anshin-2")
      } catch {case ex: Throwable => ex.getMessage should startWith("ERR value is not an integer")}
    }
    it("should decrement by 5 for a key that contains a number") {
      r.set("anshin-3", "10") should equal(true)
      r.decrby("anshin-3", 5) should equal(Some(5))
    }
    it("should reset to 0 and then decrement by 5 for a key that contains a diff type") {
      r.set("anshin-4", "debasish") should equal(true)
      try {
        r.decrby("anshin-4", 5)
      } catch {case ex: Throwable => ex.getMessage should startWith("ERR value is not an integer")}
    }

    // additional tests
    it("should decrement atomically") {
      r.set("anshin-1", "10") should equal(true)
      doParallel(100)(r.decr("anshin-1"))
      r.decr("anshin-1") should equal(Some(-91))
    }
    it("should decrement by 5 atomically") {
      r.set("anshin-1", "10") should equal(true)
      doParallel(100)(r.decrby("anshin-1", 5))
      r.decrby("anshin-1", 5) should equal(Some(-495))
    }
  }

  describe("mget") {
    it("should get values for existing keys") {
      r.set("anshin-1", "debasish") should equal(true)
      r.set("anshin-2", "maulindu") should equal(true)
      r.set("anshin-3", "nilanjan") should equal(true)
      r.mget("anshin-1", "anshin-2", "anshin-3").get should equal(List(Some("debasish"), Some("maulindu"), Some("nilanjan")))
    }
    it("should give None for non-existing keys") {
      r.set("anshin-1", "debasish") should equal(true)
      r.set("anshin-2", "maulindu") should equal(true)
      r.mget("anshin-1", "anshin-2", "anshin-4").get should equal(List(Some("debasish"), Some("maulindu"), None))
    }
  }

  describe("mset") {
    it("should set all keys irrespective of whether they exist") {
      r.mset(
        ("anshin-1", "debasish"),
        ("anshin-2", "maulindu"),
        ("anshin-3", "nilanjan")) should equal(true)
    }

    it("should set all keys only if none of them exist") {
      r.msetnx(
        ("anshin-4", "debasish"),
        ("anshin-5", "maulindu"),
        ("anshin-6", "nilanjan")) should equal(true)
      r.msetnx(
        ("anshin-7", "debasish"),
        ("anshin-8", "maulindu"),
        ("anshin-6", "nilanjan")) should equal(false)
      r.msetnx(
        ("anshin-4", "debasish"),
        ("anshin-5", "maulindu"),
        ("anshin-6", "nilanjan")) should equal(false)
    }
  }

  describe("get with spaces in keys") {
    it("should retrieve key/value pairs for existing keys") {
      r.set("anshin software", "debasish ghosh") should equal(true)
      r.get("anshin software") match {
        case Some(s: String) => s should equal("debasish ghosh")
        case None => fail("should return debasish ghosh")
      }

      r.set("test key with spaces", "I am a value with spaces")
      r.get("test key with spaces").get should equal("I am a value with spaces")
    }
  }

  describe("get with newline values") {
    it("should retrieve key/value pairs for existing keys") {
      r.set("anshin-x", "debasish\nghosh\nfather") should equal(true)
      r.get("anshin-x") match {
        case Some(s: String) => s should equal("debasish\nghosh\nfather")
        case None => fail("should return debasish")
      }
    }
  }

  describe("setrange") {
    it("should set value starting from offset") {
      r.set("key1", "hello world")
      r.setrange("key1", 6, "redis")
      r.get("key1") should equal(Some("hello redis"))

      r.setrange("key2", 6, "redis") should equal(Some(11))
      r.get("key2").get.trim should equal("redis")
      r.get("key2").get.length should equal(11) // zero padding
    }
  }

  describe("getrange") {
    it("should get value starting from start") {
      r.set("mykey", "This is a string")
      r.getrange[String]("mykey", 0, 3) should equal(Some("This"))
      r.getrange[String]("mykey", -3, -1) should equal(Some("ing"))
      r.getrange[String]("mykey", 0, -1) should equal(Some("This is a string"))
      r.getrange[String]("mykey", 10, 100) should equal(Some("string"))
    }
  }

  describe("strlen") {
    it("should return the length of the value") {
      r.set("mykey", "Hello World")
      r.strlen("mykey") should equal(Some(11))
      r.strlen("nonexisting") should equal(Some(0))
    }

    // additional tests
    it("should return the byte length of the unicode value") {
      r.set("mykey", "あいうえお")
      r.strlen("mykey") should equal(Some(15))
    }
  }

  describe("append") {
    it("should append value to that of a key") {
      r.exists("mykey") should equal(false)
      r.append("mykey", "Hello") should equal(Some(5))
      r.append("mykey", " World") should equal(Some(11))
      r.get[String]("mykey") should equal(Some("Hello World"))
    }
  }

  describe("setbit") {
    it("should set of clear the bit at offset in the string value stored at the key") {
      r.setbit("mykey", 7, 1) should equal(Some(0))
      r.setbit("mykey", 7, 0) should equal(Some(1))
      String.format("%x", new java.math.BigInteger(r.get("mykey").get.getBytes("UTF-8"))) should equal("0")
    }
  }

  describe("getbit") {
    it("should return the bit value at offset in the string") {
      r.setbit("mykey", 7, 1) should equal(Some(0))
      r.getbit("mykey", 0) should equal(Some(0))
      r.getbit("mykey", 7) should equal(Some(1))
      r.getbit("mykey", 100) should equal(Some(0))
    }
  }

  describe("bitcount") {
    it("should do a population count") {
      r.setbit("mykey", 7, 1)
      r.bitcount("mykey") should equal(Some(1))
      r.setbit("mykey", 8, 1)
      r.bitcount("mykey") should equal(Some(2))
    }
  }

  describe("bitop") {
    it("should apply logical operators to the srckeys and store the results in destKey") {
      // key1: 101
      // key2:  10
      r.setbit("key1", 0, 1)
      r.setbit("key1", 2, 1)
      r.setbit("key2", 1, 1)
      r.bitop("AND", "destKey", "key1", "key2") should equal(Some(1))
      // 101 AND 010 = 000
      (0 to 2).foreach { bit =>
        r.getbit("destKey", bit) should equal(Some(0))
      }

      r.bitop("OR", "destKey", "key1", "key2") should equal(Some(1))
      // 101 OR 010 = 111
      (0 to 2).foreach { bit =>
        r.getbit("destKey", bit) should equal(Some(1))
      }

      r.bitop("NOT", "destKey", "key1") should equal(Some(1))
      r.getbit("destKey", 0) should equal(Some(0))
      r.getbit("destKey", 1) should equal(Some(1))
      r.getbit("destKey", 2) should equal(Some(0))
    }

    // additional tests
    it("should work with non-existence keys") {
      import com.redis.serialization.Parse.Implicits.parseByteArray

      r.set("key1", Array[Byte](1, 2, 3, 4, 5, 0, 127, -1, -10, -128))

      r.bitop("NOT", "destKey", "notExist")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq.empty[Byte]))

      r.bitop("AND", "destKey", "notExist1", "notExist2")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq.empty[Byte]))
      r.bitop("AND", "destKey", "notExist", "key1")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
      r.bitop("AND", "destKey", "key1", "notExist")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))

      r.bitop("OR", "destKey", "notExist1", "notExist2")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq.empty[Byte]))
      r.bitop("OR", "destKey", "notExist", "key1")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq[Byte](1, 2, 3, 4, 5, 0, 127, -1, -10, -128)))
      r.bitop("OR", "destKey", "key1", "notExist")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq[Byte](1, 2, 3, 4, 5, 0, 127, -1, -10, -128)))

      r.bitop("XOR", "destKey", "notExist1", "notExist2")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq.empty[Byte]))
      r.bitop("XOR", "destKey", "notExist", "key1")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq[Byte](1, 2, 3, 4, 5, 0, 127, -1, -10, -128)))
      r.bitop("XOR", "destKey", "key1", "notExist")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq[Byte](1, 2, 3, 4, 5, 0, 127, -1, -10, -128)))
    }
    it("should work with arrays with different length") {
      import com.redis.serialization.Parse.Implicits.parseByteArray

      r.set("key1", Array[Byte](1, 2, 3, 4, 5, 0, 127, -1, -10, -128))
      r.set("key2", Array[Byte](1, 3))

      r.bitop("AND", "destKey", "key1", "nonExist")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0)))
      r.bitop("OR", "destKey", "key1", "nonExist")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq[Byte](1, 2, 3, 4, 5, 0, 127, -1, -10, -128)))
      r.bitop("XOR", "destKey", "key1", "nonExist")
      r.get("destKey").map(_.toSeq) should equal(Some(Seq[Byte](1, 2, 3, 4, 5, 0, 127, -1, -10, -128)))
    }

    it("should get back the original string after doube NOT") {
      import com.redis.serialization.Parse.Implicits.parseByteArray
      forAll { xs: Array[Byte] =>
        r.set("key1", xs)
        r.bitop("NOT", "key2", "key1")
        r.bitop("NOT", "key3", "key2")
        r.get("key3").map(_.toSeq) should equal(Some(xs.toSeq))
      }
    }
    it("should not care order with AND, OR, XOR") {
      import com.redis.serialization.Parse.Implicits.parseByteArray
      forAll { (xs: Array[Byte], ys: Array[Byte], zs: Array[Byte]) =>
        r.set("key1", xs)
        r.set("key2", ys)
        r.set("key3", zs)

        r.bitop("AND", "destAnd1", "key1", "key2", "key3")
        r.bitop("AND", "destAnd2", "key3", "key1", "key2")
        r.get("destAnd1").map(_.toSeq) should equal(r.get("destAnd2").map(_.toSeq))

        r.bitop("OR", "destOr1", "key1", "key2", "key3")
        r.bitop("OR", "destOr2", "key3", "key1", "key2")
        r.get("destOr1").map(_.toSeq) should equal(r.get("destOr2").map(_.toSeq))
        
        r.bitop("XOR", "destXor1", "key1", "key2", "key3")
        r.bitop("XOR", "destXor2", "key3", "key1", "key2")
        r.get("destXo1").map(_.toSeq) should equal(r.get("destXo2").map(_.toSeq))
      }
    }
  }

}
