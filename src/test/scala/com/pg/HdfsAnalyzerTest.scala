package com.pg

import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest._

class HdfsAnalyzerTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  var sqlContext: TestHiveContext = _

  override def beforeAll() {
    sqlContext = SparkContextFactory.getSqlContext
    sqlContext.sql( """CREATE DATABASE IF NOT EXISTS stats""")
    sqlContext.sql( """DROP TABLE IF EXISTS stats.fsimage""")
    sqlContext.sql( """
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
                    """)

    sqlContext.sql(
      """
         CREATE TABLE IF NOT EXISTS stats.usage_report (
            application_name STRING,
            total_size BIGINT,
            total_file_count BIGINT
         )
         PARTITIONED BY (dt STRING)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY '\t'
         STORED AS TEXTFILE
      """.stripMargin)
  }

  before {
    sqlContext.sql( """LOAD DATA LOCAL INPATH 'src/test/resources/fsimage.txt'
         OVERWRITE INTO TABLE stats.fsimage PARTITION(dt=20160101)""")
  }

  test("calculate total HDFS usage") {
    val options = new CliOptions(List("--dt", "20160101"))
    HdfsAnalyzer.makeHdfsUsageReport(sqlContext, AppConfig.readAppsFromConf(), options)

    val results = sqlContext
      .sql("SELECT application_name, total_size, total_file_count FROM stats.usage_report WHERE dt=20160101")
      .collect()

    val expectations = Map(
      "projectA" -> (44484250, 3),
      "projectB" -> (22242125, 2)
    )

    for(res <- results) {
      val appName = res.getString(0)
      val totalSize = res.getLong(1)
      val fileCnt = res.getLong(2)
      assert(expectations(appName) === (totalSize, fileCnt))
    }
  }

}
