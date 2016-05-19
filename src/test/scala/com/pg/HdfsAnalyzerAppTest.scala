package com.pg

import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest._

class HdfsAnalyzerAppTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  var sqlContext: TestHiveContext = _
 
  override def beforeAll() {
    val sc = SparkContextFactory.getSparkContext
    sqlContext = new TestHiveContext(sc)
    sqlContext.sql("DROP DATABASE IF EXISTS stats")
    sqlContext.sql("DROP DATABASE IF EXISTS dba")
    sqlContext.sql("DROP DATABASE IF EXISTS dbb")
    sqlContext.sql("CREATE DATABASE stats")
    sqlContext.sql("CREATE DATABASE dba")
    sqlContext.sql("CREATE DATABASE dbb")

    sqlContext.sql(
      """
        CREATE TABLE IF NOT EXISTS stats.fsimage (
              path STRING,
              replication INT,
              mod_time STRING,
              access_time STRING,
              block_size  BIGINT,
              num_blocks  INT,
              file_size BIGINT,
              namespace_quota INT,
              diskspace_quota  INT,
              perms STRING,
              username STRING,
              groupname STRING
        )
        PARTITIONED BY (dt STRING)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE
      """.stripMargin)

    sqlContext.sql(
      """
         CREATE TABLE IF NOT EXISTS stats.usage_report (
            project_name STRING,
            total_size_gb DOUBLE,
            total_file_count BIGINT,
            mod_timestamp INT,
            last_access_time INT
         )
         PARTITIONED BY (dt STRING)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY '\t'
         STORED AS TEXTFILE
      """.stripMargin)

    sqlContext.sql("CREATE TABLE dbA.tab1 (a STRING) LOCATION 'file:///tmp/db.A/tab1'")

    sqlContext.sql(
      """LOAD DATA LOCAL INPATH 'src/test/resources/fsimage.txt'
         OVERWRITE INTO TABLE stats.fsimage PARTITION(dt=20160101)""")
  }

  test("should make hdfs usage report") {
    val dt: String = "20160101"

    val options = new CliOptions(List("--dt", dt))
    val appConfig = new AppConfig(sqlContext)
    HdfsAnalyzerApp.makeHdfsUsageReport(sqlContext, appConfig, options)

    val results = sqlContext.sql(
      s"""
          SELECT project_name, total_size_gb, total_file_count, mod_timestamp, last_access_time
          FROM stats.usage_report WHERE dt=$dt
        """.stripMargin)
      .collect()

    val expectations = Map(
      "projectA" ->(0.11802313383668661, 3, 1447941540, 1447984740),  // 2015-11-19 14:59 GMT+1
      "projectB" ->(0.0207145931199193, 1, 1447811940, 1447898340)    // 2015-11-18 02:59 GMT+1
    )

    for (res <- results) {
      val projectName = res.getString(0)
      val totalSizeGb = res.getDouble(1)
      val filesCount = res.getLong(2)
      val modTime = res.getInt(3)
      val accessTime = res.getInt(4)
      assert(expectations(projectName) === (totalSizeGb, filesCount, modTime, accessTime))
    }
  }

}
