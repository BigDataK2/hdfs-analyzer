package com.pg.model

import org.scalatest.{Matchers, FunSuite}


class HdfsObjectTest extends FunSuite with Matchers {

  test("should check if hdfs object is contained within any of given paths") {
    makeHdfsObject("/a/plik.txt").isContainedWithin(List(Path("/"))) shouldBe true
    makeHdfsObject("/a/plik.txt").isContainedWithin(List(Path("/a"))) shouldBe true
    makeHdfsObject("/a/plik.txt").isContainedWithin(List(Path("/a/plik"))) shouldBe false
    makeHdfsObject("/a/plik.txt").isContainedWithin(List(Path("/a/plik.txt"))) shouldBe true

    makeHdfsObject("/a/b/c/").isContainedWithin(List(Path("/a/b/c/d"))) shouldBe false
    makeHdfsObject("/a/b/c").isContainedWithin(List(Path("/a/b/cccc"))) shouldBe false
    makeHdfsObject("/a/b/c").isContainedWithin(List(Path("/abc"))) shouldBe false
    makeHdfsObject("/a/b/c").isContainedWithin(List(Path("/a/b/c/"))) shouldBe true

    makeHdfsObject("/a/b/c.txt").isContainedWithin(List(Path("/a/d/"), Path("/a/b/"))) shouldBe true
  }

  private def makeHdfsObject(path: String) = {
    HdfsObject(Path(path), 2, "2015-11-18 03:01", "2015-11-18 03:01", 42, 1, 42, 0, 0, "rwxr-xr-x", "hdfs", "supergroup")
  }
}
