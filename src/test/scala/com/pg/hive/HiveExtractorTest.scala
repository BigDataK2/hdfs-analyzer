package com.pg.hive

import com.pg.SparkContextFactory
import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest._

class HiveExtractorTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers {

  var sqlContext: TestHiveContext = _
  var hiveExtractor: HiveExtractor = _

  override def beforeAll() {
    sqlContext = SparkContextFactory.getSqlContext
    hiveExtractor = new HiveExtractor(sqlContext)

    sqlContext.sql("DROP DATABASE IF EXISTS dbA")
    sqlContext.sql("DROP DATABASE IF EXISTS dbB")

    sqlContext.sql("CREATE DATABASE dbA")
    sqlContext.sql("CREATE DATABASE dbB")

    sqlContext.sql("CREATE TABLE dbA.tab1 (a STRING) LOCATION 'file:///tmp/db.A/tab1'")
    sqlContext.sql("CREATE TABLE dbA.tab2 (a STRING) LOCATION 'file:///tmp/db.A/tab2'")
    sqlContext.sql("CREATE TABLE dbA.tab3 (a STRING) LOCATION 'file:///tmp/db.A/tab3'")
  }

  test("should list tables from non-empty db") {
    val tables = hiveExtractor.getAllTables("dbA")

    tables should have size 3
    tables should contain allOf ("tab1", "tab2", "tab3")
  }

  test("should list tables from empty db") {
    val tables = hiveExtractor.getAllTables("dbB")

    tables should have size 0
  }

  test("should get locations of tables from non-empty db") {
    val locations = hiveExtractor.getHdfsLocationsForTablesInDb("dbA")

    locations should have size 3
    locations should contain allOf ("/tmp/db.A/tab1", "/tmp/db.A/tab2", "/tmp/db.A/tab3")
  }

  test("should get locations of tables from empty db") {
    val locations = hiveExtractor.getHdfsLocationsForTablesInDb("dbB")

    locations should have size 0
  }

}