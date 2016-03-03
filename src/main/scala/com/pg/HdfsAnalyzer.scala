package com.pg

import com.pg.model.{HdfsObject, Application}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * todo:
 * refactor
 * include replication factor in size calculations
 * store results in hive table
 * extract paths from hive db / table
 */

object HdfsAnalyzer {

  val MB = 1024 * 1024
  val GB = MB * 1024
  val TB = GB * 1024
  val UNIT = GB

  def main(args: Array[String]) {

    val sqlContext = initSqlContext()
    val appConfig = new AppConfig(sqlContext)

    val options = new CliOptions(args.toList)

    makeHdfsUsageReport(sqlContext, appConfig, options)

  }

  def makeHdfsUsageReport(sqlContext: HiveContext, appConfig: AppConfig, options: CliOptions) = {
    val applications = appConfig.readAppsFromConf()
    val hdfsUsageReport = calculateTotalHdfsUsage(sqlContext, applications, appConfig, options)
    storeInHive(sqlContext, hdfsUsageReport, options)
  }

  private def storeInHive(sqlContext: HiveContext, usageReport: Iterable[AppUsage], options: CliOptions) = {
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(usageReport.toSeq).toDF().registerTempTable("usageReport")

    sqlContext.sql(
      s"""
         INSERT OVERWRITE TABLE ${options.usagereportdb}.${options.usagereporttable}
         PARTITION(dt=${options.dt})
         SELECT
            app.name,
            CAST(size AS DOUBLE) / $UNIT,
            fileCnt
         FROM usageReport
       """.stripMargin)
  }

  private def initSqlContext() = {
    val conf = new SparkConf().setAppName(s"HDFS Analyzer")
    val sc = new SparkContext(conf)
    new HiveContext(sc)
  }

  private def getLatestFsImageData(sqlContext: HiveContext, dt: String) = {
    val stmt =
      s"""
         SELECT
          path,
          replication,
          mod_time,
          access_time,
          block_size,
          num_blocks,
          file_size,
          namespace_quota,
          diskspace_quota,
          perms,
          username,
          groupname
         FROM stats.fsimage
         WHERE dt = $dt
       """.stripMargin

    sqlContext.sql(stmt).map( r => HdfsObject(
      r.getString(0), r.getInt(1), r.getString(2), r.getString(3), r.getLong(4),
      r.getInt(5), r.getLong(6), r.getInt(7), r.getInt(8), r.getString(9), r.getString(10), r.getString(11)
    ))
  }

  /**
   * @return (Application, (total size, number of files))
   */
  private def calculateTotalHdfsUsage(sqlContext: HiveContext, apps: Iterable[Application], appConfig: AppConfig, options: CliOptions) = {
    val fsimage = getLatestFsImageData(sqlContext, options.dt)

    val paths = appConfig.getAllHdfsPathsToMonitor(apps)

    val groupedPaths =
      fsimage
      .filter(_.isContainedWithin(paths))
      .flatMap(_.filterPaths(paths))
      .groupByKey()
      .map{ case (path, itrHdfsObjects) =>
         (path, itrHdfsObjects.aggregate((0L, 0L))(
           { case ((size, fileCnt), ho2) =>
             (size + ho2.fileSize, fileCnt + 1) },
           { case ((s1, fc1), (s2, fc2)) => (s1 + s2, fc1 + fc2) }
         ))
       }.collect().toMap

    apps
      .map(app => {
      (app, app.hdfsDirs.aggregate((0L, 0L))((acc, path) => {
        val toAdd = groupedPaths.getOrElse(path, (0L, 0L))
        (acc._1 + toAdd._1, acc._2 + toAdd._2)
      }, { case ((s1, fc1), (s2, fc2)) => (s1 + s2, fc1 + fc2) }
      ))})
      .map{ case (app, (size, fileCnt)) => AppUsage(app, size, fileCnt)}
  }

}