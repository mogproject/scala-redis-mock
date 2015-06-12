package com.github.mogproject.redismock.entity

import com.github.mogproject.redismock.util.Bytes
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks


class HyperLogLogSpec extends FunSpec
with Matchers
with BeforeAndAfterEach
with BeforeAndAfterAll
with GeneratorDrivenPropertyChecks {

  describe("HyperLogLog#fromBytes") {
    it("should build HyperLogLog instance from sparse Bytes") {
      val hll = HyperLogLog(isDense = false, Seq.fill(10)(4) ++ Seq.fill(HyperLogLog.m - 10)(0))
      HyperLogLog.fromBytes(hll.toBytes) shouldBe hll
    }
    it("should get back the original HyperLogLog") {
      forAll { xss: Seq[Seq[Byte]] =>
        val hll = xss.foldLeft(HyperLogLog()) { case (h, xs) => h.add(Bytes(xs))._1}
        HyperLogLog.fromBytes(hll.toBytes) shouldBe hll.copy(isDirty = false)
      }
    }
    it("should throw exception when the Bytes are corrupted") {
      val thrown0 = the[Exception] thrownBy {HyperLogLog.fromBytes(Bytes.empty)}
      val thrown1 = the[Exception] thrownBy {HyperLogLog.fromBytes(Bytes(Seq[Byte](
        'H', 'Y', 'L', 'L', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -128
      )))}
      val thrown2 = the[Exception] thrownBy {HyperLogLog.fromBytes(Bytes(Seq[Byte](
        'H', 'Y', 'L', 'L', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -128, 0, 0
      )))}
      val thrown3 = the[Exception] thrownBy {HyperLogLog.fromBytes(Bytes(Seq[Byte](
        'H', 'Y', 'L', 'L', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -128, 127, -1, 127, -1, 127
      )))}
      val thrown4 = the[Exception] thrownBy {HyperLogLog.fromBytes(Bytes(Seq[Byte](
        'H', 'Y', 'L', 'L', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -128, 127, -1, 127, -1, 127, -1
      )))}
      thrown0.getMessage shouldBe "WRONGTYPE Key is not a valid HyperLogLog string value."
      thrown1.getMessage shouldBe "WRONGTYPE Key is not a valid HyperLogLog string value."
      thrown2.getMessage shouldBe "WRONGTYPE Key is not a valid HyperLogLog string value."
      thrown3.getMessage shouldBe "WRONGTYPE Key is not a valid HyperLogLog string value."
      thrown4.getMessage shouldBe "WRONGTYPE Key is not a valid HyperLogLog string value."
    }
  }

  describe("HyperLogLog#count") {
    it("should estimate the cardinality by linear counting") {
      val h0 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m)(0))
      val h1 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m - 1)(0) ++ Seq.fill(1)(1))
      val h2 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m - 2)(0) ++ Seq.fill(2)(1))
      val h3 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m - 3)(0) ++ Seq.fill(3)(1))
      val h4 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m / 2)(0) ++ Seq.fill(HyperLogLog.m / 2)(1))
      val h5 = HyperLogLog(isDense = true, Seq.fill(3)(0) ++ Seq.fill(HyperLogLog.m - 3)(1))
      val h6 = HyperLogLog(isDense = true, Seq.fill(2)(0) ++ Seq.fill(HyperLogLog.m - 2)(1))
      val h7 = HyperLogLog(isDense = true, Seq.fill(1)(0) ++ Seq.fill(HyperLogLog.m - 1)(1))

      h0.count shouldBe 0
      h1.count shouldBe 1
      h2.count shouldBe 2
      h3.count shouldBe 3
      h4.count shouldBe 11356
      h5.count shouldBe 140991
      h6.count shouldBe 147634
      h7.count shouldBe 158991

    }
    it("should estimate the cardinality with bias") {
      val h1 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m)(1))
      val h2 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m)(10))
      val h3 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m)(20))
      val h4 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m)(30))

      h1.count shouldBe 0
      h2.count shouldBe 0
      h3.count shouldBe 5
      h4.count shouldBe 32452
    }
    it("should estimate the cardinality") {
      val h1 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m)(40))
      val h2 = HyperLogLog(isDense = true, Seq.fill(HyperLogLog.m)(50))

      h1.count shouldBe 34910377
      h2.count shouldBe 35748227044L
    }
    it("should read value from cache") {
      val hll = HyperLogLog(isDense = false, Seq.fill(HyperLogLog.m - 2)(0) ++ Seq.fill(2)(1), false, 2)
      hll.count shouldBe 2
    }
  }
}

