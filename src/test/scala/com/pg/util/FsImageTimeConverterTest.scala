package com.pg.util

import org.joda.time.DateTimeZone
import org.scalatest.{Matchers, FunSuite}

class FsImageTimeConverterTest extends FunSuite with Matchers {

  test("should convert local time from fsImage to UTC timestamp") {
    FsImageTimeConverter.toUnixTimestamp("2015-11-30 22:24", DateTimeZone.UTC) shouldBe 1448922240
    FsImageTimeConverter.toUnixTimestamp("2015-11-18 02:59", DateTimeZone.forID("+01:00")) shouldBe 1447811940
  }

}
