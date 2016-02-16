package com.pg

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._

object AppConfig {

  def readAppsFromConf() = {
    val conf = ConfigFactory.load("projects")
    conf.getConfigList("projects").toList.map{
      c =>
        val entries = c.entrySet().map( entry => (entry.getKey, entry.getValue)).toMap
        val name = if (entries.contains("name")) c.getString("name") else ""
        val hdfsDirs = if (entries.contains("hdfs_dirs")) c.getStringList("hdfs_dirs").toList else List()
        val hiveDb = if (entries.contains("hive_db")) c.getString("hive_db") else ""
        new Application(name, hdfsDirs, hiveDb)
    }
  }

  def getAllHdfsDirs(apps: Iterable[Application]) = {
    apps.flatMap(_.hdfsDirs).toList
  }

  def getAllHdfsPathsToMonitor(apps: Iterable[Application]) = {
    val hdfsDirPaths = getAllHdfsDirs(apps)
    val allPaths = hdfsDirPaths
    println(allPaths)
    allPaths
  }

}
