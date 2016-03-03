package com.pg

import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkContextFactory {

  private val conf = new SparkConf()
    .setAppName("TestEnv")
    .setMaster("local[4]")
    .set("spark.executor.memory", "1g")
    .set("spark.driver.allowMultipleContexts", "true")

  private val sc: SparkContext = new SparkContext(conf)
  private val sqlContext: TestHiveContext = new TestHiveContext(sc)

  def getSqlContext = sqlContext

  def getSparkContext = sc

}