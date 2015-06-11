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
    it("should get back the original HyperLogLog") {
      forAll { xss: Seq[Seq[Byte]] =>
        val hll = xss.foldLeft(HyperLogLog()) { case (h, xs) => h.add(Bytes(xs))._1}
        HyperLogLog.fromBytes(hll.toBytes) shouldBe hll
      }
    }
  }
}

