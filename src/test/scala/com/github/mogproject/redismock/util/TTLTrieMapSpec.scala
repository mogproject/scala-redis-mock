package com.github.mogproject.redismock.util

import org.scalatest.{Matchers, FunSpec}

class TTLTrieMapSpec extends FunSpec with Matchers {
  describe("update") {
    it("should set key/value pairs without ttl") {
      val t = TTLTrieMap.empty[String, Int]
      val now = System.currentTimeMillis()

      t.update("k1", 123)
      t.update("k2", 234)
      t.store shouldEqual Map("k1" -> 123, "k2" -> 234)
      t.expireAt.size shouldEqual 0

      t.withTruncate(now + 1200) {
        t.store shouldEqual Map("k1" -> 123, "k2" -> 234)
        t.expireAt.size shouldEqual 0
      }

      t.withTruncate(now + 2200) {
        t.store shouldEqual Map("k1" -> 123, "k2" -> 234)
        t.expireAt.size shouldEqual 0
      }
    }

    it("should set key/value pairs with infinite ttl") {
      val t = TTLTrieMap.empty[String, Int]
      val now = System.currentTimeMillis()

      t.update("k1", 123, None)
      t.update("k2", 234, None)
      t.store shouldEqual Map("k1" -> 123, "k2" -> 234)
      t.expireAt.size shouldEqual 0

      t.withTruncate(now + 1000) {
        t.store shouldEqual Map("k1" -> 123, "k2" -> 234)
        t.expireAt.size shouldEqual 0
      }

      t.withTruncate(now + 2000) {
        t.store shouldEqual Map("k1" -> 123, "k2" -> 234)
        t.expireAt.size shouldEqual 0
      }
    }

    it("should set key/value pairs with ttl") {
      val t = TTLTrieMap.empty[String, Int]
      val now = System.currentTimeMillis()

      t.update("k1", 123, Some(2000L))
      t.update("k2", 234, Some(1000L))
      t.store shouldEqual Map("k1" -> 123, "k2" -> 234)
      t.expireAt.size shouldEqual 2

      t.withTruncate(now + 1200) {
        t.store shouldEqual Map("k1" -> 123)
        t.expireAt.size shouldEqual 1
      }

      t.withTruncate(now + 2200) {
        t.store shouldEqual Map.empty
        t.expireAt.size shouldEqual 0
      }
    }

    it("should overwrite old value") {
      val t = TTLTrieMap.empty[String, Int]
      val now = System.currentTimeMillis()

      t.update("k1", 123, None)
      t.update("k1", 234, Some(1000L))
      t.store shouldEqual Map("k1" -> 234)
      t.expireAt.size shouldEqual 1

      t.withTruncate(now + 1200) {
        t.store shouldEqual Map.empty
        t.expireAt.size shouldEqual 0
      }
    }

    it("should set key/value pairs with extending ttl") {
      val t = TTLTrieMap.empty[String, Int]
      val now = System.currentTimeMillis()

      t.update("k1", 123, Some(2000L))
      t.update("k2", 234, Some(1100L))

      t.store shouldEqual Map("k1" -> 123, "k2" -> 234)
      t.expireAt.size shouldEqual 2

      Thread.sleep(1000L)

      t.update("k1", 123, None)
      t.update("k2", 234, Some(1000L))
      t.store shouldEqual Map("k1" -> 123, "k2" -> 234)
      t.expireAt.size shouldEqual 1

      t.withTruncate(now + 1200) {
        t.store shouldEqual Map("k1" -> 123, "k2" -> 234)
        t.expireAt.size shouldEqual 1
      }

      t.withTruncate(now + 2200) {
        t.store shouldEqual Map("k1" -> 123)
        t.expireAt.size shouldEqual 0
      }

      t.withTruncate(now + 3200) {
        t.store shouldEqual Map("k1" -> 123)
        t.expireAt.size shouldEqual 0
      }
    }
  }

  describe("get") {
    it("should not get value when the key does not exist") {
      val t = TTLTrieMap.empty[String, Int]
      t.get("k1") shouldEqual None
    }
    it("should get value from the key") {
      val t = TTLTrieMap.empty[String, Int]
      val now = System.currentTimeMillis()

      t.update("k1", 123, None)
      t.get("k1") shouldEqual Some(123)

      t.withTruncate(now + 1200) {
        t.get("k1") shouldEqual Some(123)
      }
    }
    it("should not get value with the expired key") {
      val t = TTLTrieMap.empty[String, Int]
      val now = System.currentTimeMillis()

      t.update("k1", 123, Some(1000L))
      t.get("k1") shouldEqual Some(123)

      t.withTruncate(now + 1200) {
        t.get("k1") shouldEqual None
      }
    }
  }

  describe("remove") {
    it("should remove specified key and value") {
      val t = TTLTrieMap.empty[String, Int]

      t.update("k1", 123, None)
      t.update("k2", 234, Some(1000L))
      t.remove("k2")
      t.store shouldEqual Map("k1" -> 123)
      t.expireAt.size shouldEqual 0

      t.remove("k1")
      t.store shouldEqual Map.empty
      t.expireAt.size shouldEqual 0

      t.remove("k1")
      t.store shouldEqual Map.empty
      t.expireAt.size shouldEqual 0
    }
  }

  describe("clear") {
    it("should clear all keys") {
      val t = TTLTrieMap.empty[String, Int]

      t.update("k1", 123, None)
      t.update("k2", 234, Some(1000L))
      t.clear()
      t.store shouldEqual Map.empty
      t.expireAt.size shouldEqual 0

      t.clear()
      t.store shouldEqual Map.empty
      t.expireAt.size shouldEqual 0
    }
  }

  describe("size") {
    it("should count only living keys") {
      val t = TTLTrieMap.empty[String, Int]
      val now = System.currentTimeMillis()

      t.size shouldEqual 0

      t.update("k1", 123, None)
      t.update("k2", 234, Some(1000L))

      t.size shouldEqual 2

      t.withTruncate(now + 1200) {
        t.size shouldEqual 1
      }
    }
  }

  describe("equals") {
    it("should return false with other types") {
      TTLTrieMap.empty[String, Int] == "xyz" shouldBe false
      val t = TTLTrieMap.empty[String, Int]
      t.update("key", 10)
      val u = TTLTrieMap.empty[String, String]
      u.update("key", "10")
      t == u shouldBe false
    }
    it("should check equality with TTLTrieMap") {
      val t = TTLTrieMap.empty[String, Int]
      val u = TTLTrieMap.empty[String, Int]
      t == u shouldBe true

      val now = System.currentTimeMillis()
      t.update("k1", 10)
      t.update("k2", 20, now + 1000000L)
      t == u shouldBe false

      u.update("k2", 20, now + 1000000L)
      u.update("k1", 10)
      t == u shouldBe true

      t.update("k3", 30, 0L)  // will be removed immediately
      t == u shouldBe true
    }
  }

  describe("hashCode") {
    it("should change with values") {
      val t = TTLTrieMap.empty[String, Int]
      val x1 = t.hashCode

      t.update("k1", 10)
      val x2 = t.hashCode
      x1 == x2 shouldBe false

      val now = System.currentTimeMillis()
      t.updateExpireAt("k1", now + 1000000L)
      val x3 = t.hashCode
      x2 == x3 shouldBe false
    }
  }
}
