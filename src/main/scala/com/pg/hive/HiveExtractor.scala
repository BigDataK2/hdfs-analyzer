package com.pg.hive

import org.apache.spark.sql.hive.HiveContext

class HiveExtractor(sqlContext: HiveContext) {

  private[hive] def getAllTables(hiveDb: String) = {
    sqlContext.sql(s"use $hiveDb")
    sqlContext.sql("SHOW TABLES").collect().map(_.getString(0))
  }

  def getHdfsLocationsForTablesInDb(hiveDb: String) = {
    val tables = getAllTables(hiveDb)
    tables.map { table =>
      val rows = sqlContext.sql(s"DESCRIBE FORMATTED $table").collect()
        .map(r => if (r.length > 0) r.getString(0) else "")

      val locationRow = rows.filter(_.startsWith("Location"))
      assert(locationRow.length > 0)

      // format is as follows:
      // "Location:\tfile:/a/b/c"
      locationRow(0).split("\t")(1).trim.stripPrefix("file:")
    }
  }
}
