package com.pg

import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest._

class HiveExtractorTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  var sqlContext: TestHiveContext = _
  var hiveExtractor: HiveExtractor = _

  override def beforeAll() {
    sqlContext = SparkContextFactory.getSqlContext
    hiveExtractor = new HiveExtractor(sqlContext)

    sqlContext.sql( """CREATE DATABASE IF NOT EXISTS dbA""")
    sqlContext.sql( """CREATE DATABASE IF NOT EXISTS dbB""")

    sqlContext.sql( """ CREATE TABLE IF NOT EXISTS dbA.tab1 (a STRING) LOCATION 'file:///tmp/db.A/tab1' """.stripMargin)
    sqlContext.sql( """ CREATE TABLE IF NOT EXISTS dbA.tab2 (a STRING) LOCATION 'file:///tmp/db.A/tab2' """.stripMargin)
    sqlContext.sql( """ CREATE TABLE IF NOT EXISTS dbA.tab3 (a STRING) LOCATION 'file:///tmp/db.A/tab3' """.stripMargin)
  }

  before {}

  test("list tables from db") {
    val tables = hiveExtractor.getAllTablesFromDb("dbA")

    tables should have size 3
    tables should contain allOf ("tab1", "tab2", "tab3")
  }

  test("get locations of tables") {
    val locations = hiveExtractor.getHDFSPathsFromHive("dbA")

    locations should have size 3
    locations should contain allOf ("/tmp/db.A/tab1", "/tmp/db.A/tab2", "/tmp/db.A/tab3")
  }

}