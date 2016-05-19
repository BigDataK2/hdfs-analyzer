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
  val A_ACCESS_TIME = "2015-11-20 02:59"
  val B_ACCESS_TIME = "2015-11-19 02:59"
  val FRESH_DATE = "2016-03-13 13:22"

  test("should calculate total HDFS usage") {
    // given
    val fsImageRDD: RDD[HdfsObject] = sc.parallelize(List(
      makeDir("/a"),
      makeFile("/a/plik.txt", 7, 100, 42, OLD_DATE, A_ACCESS_TIME),
      makeDir("/b"),
      makeFile("/b/dummy.exe", 3, 666, 66, OLD_DATE, B_ACCESS_TIME),
      makeDir("/c"),
      makeDir("/c/inner"),
      makeFile("/c/elo.txt", 2, 200, 11, OLD_DATE),
      makeFile("/c/ziom.txt", 1, 100, 22, OLD_DATE),
      makeFile("/c/inner/thing.bin", 2, 50, 33, FRESH_DATE)
    ))
    val projects = List(PROJECT_A, PROJECT_B, PROJECT_C)

    // when
    val totalHdfsUsage: Iterable[ProjectUsage] = HdfsAnalyzer.calculateTotalHdfsUsage(fsImageRDD, projects)

    // then
    totalHdfsUsage should have size 3
    totalHdfsUsage should contain allOf(
      ProjectUsage(PROJECT_A, 7 * 42, 1, toUnixTimestamp(OLD_DATE), toUnixTimestamp(A_ACCESS_TIME)),
      ProjectUsage(PROJECT_B, 3 * 66, 1, toUnixTimestamp(OLD_DATE), toUnixTimestamp(B_ACCESS_TIME)),
      ProjectUsage(PROJECT_C, 2 * 11 + 22 + 2 * 33, 3, toUnixTimestamp(FRESH_DATE), toUnixTimestamp(OLD_DATE)))
  }

  private def makeDir(path: String, date: String = OLD_DATE): HdfsObject = {
    HdfsObject(Path(path), 0, date, date, 0, 0, 0, -1, -1, "rwxr-xr-x", "hdfs", "supergroup")
  }

  private def makeFile(path: String, replication: Int, blockSize: Long, fileSize: Long,
                       date: String = OLD_DATE, accessTime: String = OLD_DATE): HdfsObject = {
    HdfsObject(Path(path), replication, date, accessTime, blockSize, 1, fileSize, 0, 0, "rwxrwxrwx", "hdfs", "supergroup")
  }
}