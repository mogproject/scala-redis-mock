package com.github.mogproject.redismock

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks


class MockHyperLogLogOperationsSpec extends FunSpec
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

  describe("pfadd") {
    it("should return one for changed estimated cardinality") {
      r.pfadd("hll-updated-cardinality", "value1") should equal(1)
    }

    it("should return zero for unchanged estimated cardinality") {
      r.pfadd("hll-nonupdated-cardinality", "value1")
      r.pfadd("hll-nonupdated-cardinality", "value1") should equal(0)
    }

    it("should return one for variadic values and estimated cardinality changes") {
      r.pfadd("hll-variadic-cardinality", "value1")
      r.pfadd("hll-variadic-cardinality", "value1", "value2") should equal(1)
    }
  }

  describe("pfcount") {
    it("should return zero for an empty") {
      r.pfcount("hll-empty") should equal(0)
    }

    it("should return estimated cardinality") {
      r.pfadd("hll-card", "value1") should equal(1)
      r.pfcount("hll-card") should equal(1)
    }

    it("should return estimated cardinality of unioned keys") {
      r.pfadd("hll-union-1", "value1")
      r.pfadd("hll-union-2", "value2")
      r.pfcount("hll-union-1", "hll-union-2") should equal(2)
    }
  }

  describe("pfmerge") {
    it("should merge existing entries") {
      r.pfadd("hll-merge-source-1", "value1")
      r.pfadd("hll-merge-source-2", "value2")
      r.pfmerge("hell-merge-destination", "hll-merge-source-1", "hll-merge-source-2")
      r.pfcount("hell-merge-destination") should equal(2)
    }
  }

  //
  // additional tests
  //
  describe("pfadd (additional)") {
    it("should be able to read as STRING data") {
      r.pfadd("hll", "A", "B", "C")
      r.getType("hll") shouldBe Some("string")

      import com.redis.serialization.Parse.Implicits.parseByteArray
      r.get("hll").map(_.toList) shouldBe Some(List(
        'H', 'Y', 'L', 'L', // magic
        1, // dense or sparse
        0, 0, 0, // not used
        0, 0, 0, 0, 0, 0, 0, 0x80, // cached cardinality, little endian
        0x51, 0x7c, 0x88, 0x5e, 0xc1, 0x80, 0x42, 0x62, 0x88, 0x4d, 0x5a).map(_.toByte))
    }
  }
}
