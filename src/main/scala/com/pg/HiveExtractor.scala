package com.pg

import org.apache.hadoop.hive.metastore.api.Table
import org.apache.spark.sql.hive.HiveContext

class HiveExtractor(sqlContext: HiveContext) {

  def getAllTablesFromDb(hiveDb: String) = {
    sqlContext.sql(s"use $hiveDb")
    sqlContext.sql("SHOW TABLES").collect().map(_.getString(0))
  }

  def getHDFSPathsFromHive(hiveDb: String) = {
    val tables = getAllTablesFromDb(hiveDb)
    List("path")
  }

  def getAllHdfsLocationsFromHive(apps: Iterable[Application]) = {
    apps.flatMap( app => getHDFSPathsFromHive(app.hiveDb)).toList
  }
}
