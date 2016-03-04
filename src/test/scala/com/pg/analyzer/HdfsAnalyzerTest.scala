package com.pg.analyzer

import com.pg.SparkContextFactory
import com.pg.model._
import com.pg.util.FsImageTimeConverter.toUnixTimestamp
import org.apache.spark.rdd.RDD
import org.scalatest._

class HdfsAnalyzerTest extends FunSuite with Matchers {

  val sc = SparkContextFactory.getSparkContext

  val PROJECT_A = Project("Project A", List(Path("/a"), Path("/a2")))
  val PROJECT_B = Project("Project B", List(Path("/b")))
  val PROJECT_C = Project("Project C", List(Path("/c")))

  val OLD_DATE = "2015-10-23 14:42"
  val FRESH_DATE = "2016-03-13 13:22"

  test("should calculate total HDFS usage") {
    // given
    val fsImageRDD: RDD[HdfsObject] = sc.parallelize(List(
      makeDir("/a"),
      makeFile("/a/plik.txt", 7, 100, 42),
      makeDir("/b"),
      makeFile("/b/dummy.exe", 3, 666, 66),
      makeDir("/c"),
      makeDir("/c/inner"),
      makeFile("/c/elo.txt", 2, 200, 11),
      makeFile("/c/ziom.txt", 1, 100, 22),
      makeFile("/c/inner/thing.bin", 2, 50, 33, FRESH_DATE)
    ))
    val projects = List(PROJECT_A, PROJECT_B, PROJECT_C)

    // when
    val totalHdfsUsage: Iterable[ProjectUsage] = HdfsAnalyzer.calculateTotalHdfsUsage(fsImageRDD, projects)

    // then
    totalHdfsUsage should have size 3
    totalHdfsUsage should contain allOf(
      ProjectUsage(PROJECT_A, 7 * 42, 1, toUnixTimestamp(OLD_DATE)),
      ProjectUsage(PROJECT_B, 3 * 66, 1, toUnixTimestamp(OLD_DATE)),
      ProjectUsage(PROJECT_C, 2 * 11 + 22 + 2 * 33, 3, toUnixTimestamp(FRESH_DATE)))
  }

  private def makeDir(path: String, date: String = OLD_DATE): HdfsObject = {
    HdfsObject(Path(path), 0, date, date, 0, 0, 0, -1, -1, "rwxr-xr-x", "hdfs", "supergroup")
  }

  private def makeFile(path: String, replication: Int, blockSize: Long, fileSize: Long,
                       date: String = OLD_DATE): HdfsObject = {
    HdfsObject(Path(path), replication, date, date, blockSize, 1, fileSize, 0, 0, "rwxrwxrwx", "hdfs", "supergroup")
  }
}