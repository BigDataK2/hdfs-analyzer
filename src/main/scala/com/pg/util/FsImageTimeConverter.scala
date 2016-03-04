package com.pg.util

import org.joda.time.{DateTimeZone, LocalDateTime}
import org.joda.time.format.DateTimeFormat

object FsImageTimeConverter {

  /**
    * Converts fsImage string date representation to UNIX timestamp (UTC)
    *
    * @param dateTime date in fsImage text file format, yyyy-MM-dd HH:mm
    * @param tz       timezone of date from fsImage, default is the same as on runing OS
    * @return UNIX timestamp (seconds from epoch, in UTC)
    */
  def toUnixTimestamp(dateTime: String, tz: DateTimeZone = DateTimeZone.getDefault) = {
    val fsImageLocalTimePattern: String = "yyyy-MM-dd HH:mm"
    LocalDateTime.parse(dateTime, DateTimeFormat.forPattern(fsImageLocalTimePattern)).toDateTime(tz).getMillis / 1000
  }

}
