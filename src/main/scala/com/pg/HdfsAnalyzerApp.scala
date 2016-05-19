package com.pg

import com.pg.analyzer.HdfsAnalyzer
import com.pg.model.{HdfsObject, Path, ProjectUsage}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HdfsAnalyzerApp {

  def main(args: Array[String]) {
    val sqlContext = initSqlContext()
    val appConfig = new AppConfig(sqlContext)
    val options = new CliOptions(args.toList)

    makeHdfsUsageReport(sqlContext, appConfig, options)
  }

  def makeHdfsUsageReport(sqlContext: HiveContext, appConfig: AppConfig, options: CliOptions) = {
    val fsImageRDD = getLatestFsImageData(sqlContext, options.dt)
    val apps = appConfig.resolveProjectsFromConf()

    val totalHdfsUsage: Iterable[ProjectUsage] = HdfsAnalyzer.calculateTotalHdfsUsage(fsImageRDD, apps)

    storeInHive(sqlContext, totalHdfsUsage, options)
  }

  private def getLatestFsImageData(sqlContext: HiveContext, dt: String): RDD[HdfsObject] = {
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

    sqlContext.sql(stmt).map(r => HdfsObject(
      Path(r.getString(0)), r.getInt(1), r.getString(2), r.getString(3), r.getLong(4),
      r.getInt(5), r.getLong(6), r.getInt(7), r.getInt(8), r.getString(9), r.getString(10), r.getString(11)
    ))
  }

  private def storeInHive(sqlContext: HiveContext, usageReport: Iterable[ProjectUsage], options: CliOptions) = {
    import sqlContext.implicits._
    val tmpTable: String = "usageReportTmp"
    sqlContext.sparkContext.parallelize(usageReport.map(_.toReport).toSeq).toDF().registerTempTable(tmpTable)

    sqlContext.sql(
      s"""
         INSERT OVERWRITE TABLE ${options.usagereportdb}.${options.usagereporttable}
         PARTITION(dt=${options.dt})
         SELECT
            projectName,
            size,
            filesCount,
            modTimestamp,
            accessTime
         FROM $tmpTable
       """.stripMargin)
  }

  private def initSqlContext(): HiveContext = {
    val conf = new SparkConf().setAppName(s"HDFS Analyzer")
    val sc = new SparkContext(conf)
    new HiveContext(sc)
  }


}