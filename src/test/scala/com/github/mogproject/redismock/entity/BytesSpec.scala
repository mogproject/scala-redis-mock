package com.github.mogproject.redismock.entity

import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.prop.GeneratorDrivenPropertyChecks


class BytesSpec extends FunSpec
with Matchers
with GeneratorDrivenPropertyChecks
with BeforeAndAfterEach
with BeforeAndAfterAll {
  describe("Bytes#seq") {
    it("should return sequence of Byte") {
      Bytes(1, 2, 3, -1, 0).seq shouldBe Bytes(1, 2, 3, -1, 0)
    }
    it("should describe its whole data") {
      forAll { xs: Seq[Byte] =>
        Bytes(xs).seq shouldBe Bytes(xs)
      }
    }
  }

  describe("Bytes#length") {
    it("should return length of data") {
      Bytes(1, 2, 3, -1, 0).length shouldBe 5
    }
  }

  describe("Bytes#equals") {
    it("should return true when the data is same") {
      Bytes(1, 2, 3, -1, 0).equals(Bytes(1, 2, 3, -1, 0)) shouldBe true
      Bytes(1, 2, 3, -1, 0).equals(Seq[Byte](1, 2, 3, -1, 0)) shouldBe false
      Bytes(1, 2, 3, -1, 0).equals(1.23) shouldBe false
    }
  }

  describe("Bytes#compare") {
    it("should return the result of comparison") {
      Bytes().compare(Bytes()) shouldBe 0
      Bytes().compare(Bytes(0)) should be < 0
      Bytes(0).compare(Bytes()) should be > 0
    }
    it("should compare bits in unsigned") {
      Bytes(-1).compare(Bytes(0)) should be > 0
      Bytes(0, 1, 2, 3).compare(Bytes(0, 1, 2, -3)) should be < 0
    }
    it("should enable comparison with Bytes") {
      Bytes(1, 2, 3) should be < Bytes(1, 2, 4)
      Bytes(1, 2, 3) should be > Bytes(1, 2)
    }
  }

  describe("Bytes#fill") {
    it("should fill value of the specified number") {
      Bytes.fill(-1)(3.toByte) shouldBe Bytes.empty
      Bytes.fill(0)(3.toByte) shouldBe Bytes.empty
      Bytes.fill(1)(3.toByte) shouldBe Bytes(3)
      Bytes.fill(5)(3.toByte) shouldBe Bytes(3, 3, 3, 3, 3)
    }
  }

  describe("Bytes#newString") {
    it("should make string from bytes") {
      Bytes.empty.newString shouldBe ""
      Bytes(97).newString shouldBe "a"
      Bytes(97, 98, 99, 100, 101).newString shouldBe "abcde"
    }
  }

  describe("Bytes#apply") {
    it("should construct with Vector[Byte]") {
      Bytes.apply(Vector.empty[Byte]) shouldBe Bytes.empty
      Bytes.apply(Vector[Byte](1.toByte)) shouldBe Bytes(1)
      Bytes.apply(Vector[Byte](1.toByte, 2.toByte, 3.toByte)) shouldBe Bytes(1, 2, 3)
    }
    it("should construct with empty") {
      Bytes.apply() shouldBe Bytes.empty
    }
    it("should construct with Int") {
      Bytes.apply(1) shouldBe Bytes(1)
      Bytes.apply(1, 2, 3) shouldBe Bytes(1, 2, 3)
      Bytes.apply(1, 2, 3.toByte) shouldBe Bytes(1, 2, 3)
    }
    it("should construct with Byte") {
      Bytes.apply(1.toByte) shouldBe Bytes(1)
      Bytes.apply(1.toByte, 2.toByte, 3.toByte) shouldBe Bytes(1, 2, 3)
    }
  }

  describe("ByteBuilder") {
    it("should build Bytes") {
      val b = new BytesBuilder
      b += 1
      b.clear()
      b += 2
      b ++= Seq(3, 4)
      b.result() shouldBe Bytes(2, 3, 4)
    }
  }
}
