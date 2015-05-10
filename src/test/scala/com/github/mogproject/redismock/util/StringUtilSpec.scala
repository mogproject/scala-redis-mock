package com.github.mogproject.redismock.util

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, FunSpec}


class StringUtilSpec extends FunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  describe("StringUtil#globToRegex") {
    it("should make regex string") {
      StringUtil.globToRegex("a\\/$^+.()=!|.,?*") shouldBe "a\\\\\\/\\$\\^\\+\\.\\(\\)\\=\\!\\|\\.\\,..*"
    }
  }
}
