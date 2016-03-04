package com.pg

import com.pg.hive.HiveExtractor
import com.pg.model.Path
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._

class AppConfig(sqlContext: HiveContext) {

  val hiveExtractor = new HiveExtractor(sqlContext)

  def resolveProjectsFromConf() = {
    val conf = ConfigFactory.load("projects")
    conf.getConfigList("projects").toList.map {
      c =>
        val entries = c.entrySet().map(entry => (entry.getKey, entry.getValue)).toMap
        val name = if (entries.contains("name")) c.getString("name") else ""
        var hdfsDirs = if (entries.contains("hdfs_dirs")) c.getStringList("hdfs_dirs").toList else List()
        if (entries.contains("hive_db")) {
          hdfsDirs ++= hiveExtractor.getHdfsLocationsForTablesInDb(c.getString("hive_db"))
        }
        new model.Project(name, hdfsDirs.map(Path(_)))
    }
  }

}
