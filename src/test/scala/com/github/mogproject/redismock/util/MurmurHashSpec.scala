package com.github.mogproject.redismock.util

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, FunSpec}


class MurmurHashSpec extends FunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  describe("HashUtil#murmurHash64A") {
    it("should return 64 bit value") {
      MurmurHash.murmurHash64A("A".getBytes) shouldBe 0xfc089b66b14af040L
      MurmurHash.murmurHash64A("AB".getBytes) shouldBe 0x24fb508dc42efb7fL
      MurmurHash.murmurHash64A("B".getBytes) shouldBe 0x130dd9603dcd32a4L
      MurmurHash.murmurHash64A("BA".getBytes) shouldBe 0x25cbc673105b365dL

      MurmurHash.murmurHash64A("".getBytes) shouldBe 0xd8dfea6585bc9732L
      MurmurHash.murmurHash64A("ABCDEFGHabcdefg".getBytes) shouldBe 0x3dfb8e09231eeb32L
      MurmurHash.murmurHash64A("ABCDEFGHabcdefgh".getBytes) shouldBe 0x40055074d92b389fL
      MurmurHash.murmurHash64A("ABCDEFGHabcdefghi".getBytes) shouldBe 0xd298ffe6aa77babeL

      MurmurHash.murmurHash64A(Seq.fill[Byte](100)(0)) shouldBe 0xc9450ffd5a7bf9ccL
      MurmurHash.murmurHash64A(Seq.fill[Byte](100)(-1)) shouldBe 0x78be0c4f11cdc6d5L
    }
  }
}
