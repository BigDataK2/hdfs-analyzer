package com.pg.model

import org.scalatest.{Matchers, FunSuite}

class PathTest extends FunSuite with Matchers {

  test("should normalize constructed path") {
    Path("/a/b/").path shouldBe "/a/b"
    Path("//a/b").path shouldBe "/a/b"
    Path("/a/b").path shouldBe "/a/b"
    Path("/a//b").path shouldBe "/a/b"
  }

  test("should correctly check path prefix against other path") {
    Path("/a/b/").startsWith(Path("/a")) shouldBe true
    Path("/a/b/").startsWith(Path("/a/b/c")) shouldBe false
  }

}
