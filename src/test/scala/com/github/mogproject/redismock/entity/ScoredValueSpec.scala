package com.github.mogproject.redismock.entity

import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.prop.GeneratorDrivenPropertyChecks


class ScoredValueSpec extends FunSpec
with Matchers
with GeneratorDrivenPropertyChecks
with BeforeAndAfterEach
with BeforeAndAfterAll {
  describe("ScoredValue#compare") {
    it("should compare score and value") {
      ScoredValue(1.23, "abc").compare(ScoredValue(1.23, "abc")) shouldBe 0
      ScoredValue(1.23, "abc").compare(ScoredValue(1.23, "xyz")) should be < 0
      ScoredValue(1.23, "abc").compare(ScoredValue(1.23, "ABC")) should be > 0
      ScoredValue(1.23, "abc").compare(ScoredValue(2.34, "abc")) should be < 0
      ScoredValue(1.23, "abc").compare(ScoredValue(1.22, "xyz")) should be > 0
      ScoredValue(1.23, "abc").compare(ScoredValue(2.34, "ABC")) should be < 0
    }
  }

  describe("ScoredValue#equals") {
    it("should not care about score") {
      forAll { (a: Double, b: Double, value: String) =>
        ScoredValue(a, value) == ScoredValue(b, value) shouldBe true
      }
    }
  }

  describe("ScoredValue#hashCode") {
    it("should not care about score") {
      forAll { (a: Double, b: Double, value: String) =>
        ScoredValue(a, value).hashCode() shouldBe ScoredValue(b, value).hashCode()
      }
    }
  }

  describe("Set of ScoredValue") {
    it("should not store same value with different score") {
      Set(ScoredValue(1.23, "abc"), ScoredValue(2.34, "abc")) shouldBe Set(ScoredValue(2.34, "abc"))
    }
  }

}
