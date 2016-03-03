package com.pg.hive

import com.pg.model.Application
import org.apache.spark.sql.hive.HiveContext

class HiveExtractor(sqlContext: HiveContext) {

  private[hive] def getAllTablesFromDb(hiveDb: String) = {
    sqlContext.sql(s"use $hiveDb")
    sqlContext.sql("SHOW TABLES").collect().map(_.getString(0))
  }

  private[hive] def getHDFSPathsFromHive(hiveDb: String) = {
    val tables = getAllTablesFromDb(hiveDb)
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

  def getAllHdfsLocationsFromHive(apps: Iterable[Application]) = {
    apps.flatMap(app => getHDFSPathsFromHive(app.hiveDb)).toList
  }
}
