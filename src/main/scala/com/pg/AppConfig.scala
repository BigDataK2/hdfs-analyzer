package com.pg

import com.pg.hive.HiveExtractor
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._

class AppConfig(sqlContext: HiveContext) {

  val hiveExtractor = new HiveExtractor(sqlContext)

  def readAppsFromConf() = {
    val conf = ConfigFactory.load("projects")
    conf.getConfigList("projects").toList.map {
      c =>
        val entries = c.entrySet().map(entry => (entry.getKey, entry.getValue)).toMap
        val name = if (entries.contains("name")) c.getString("name") else ""
        val hdfsDirs = if (entries.contains("hdfs_dirs")) c.getStringList("hdfs_dirs").toList else List()
        val hiveDb = if (entries.contains("hive_db")) c.getString("hive_db") else ""
        new model.Application(name, hdfsDirs, hiveDb)
    }
  }

  def getAllHdfsDirs(apps: Iterable[model.Application]) = {
    apps.flatMap(_.hdfsDirs).toList
  }

  def getAllHdfsPathsToMonitor(apps: Iterable[model.Application]) = {
    val hdfsDirPaths = getAllHdfsDirs(apps)
    val hdfsHivePaths = hiveExtractor.getAllHdfsLocationsFromHive(apps)
    hdfsDirPaths ++ hdfsHivePaths
  }

}
