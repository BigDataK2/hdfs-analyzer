package com.pg.analyzer

import com.pg.model._
import com.pg.util.FsImageTimeConverter
import org.apache.spark.rdd.RDD

object HdfsAnalyzer {

  def calculateTotalHdfsUsage(fsImageRDD: RDD[HdfsObject], projects: Iterable[Project]) = {
    val pathsToAnalyze = projects.flatMap(_.hdfsDirs)

    // clean data (skip dirs, not interesting paths) and correlate hdfs object with project
    val projectAndHdfsObjectRDD: RDD[(Project, HdfsObject)] =
      fsImageRDD
        .filter(_.isContainedWithin(pathsToAnalyze))
        .filter(_.isFile)
        .flatMap(hdfsObject => findProject(hdfsObject, projects).map(project => (project, hdfsObject)))

    aggregateUsage(projectAndHdfsObjectRDD)
  }

  private def findProject(hdfsObject: HdfsObject, projects: Iterable[Project]): Option[Project] = {
    // there is assumption that every file (hdfs object) is in AT MOST ONE project
    projects.find(p => hdfsObject.isContainedWithin(p.hdfsDirs))
  }

  private def aggregateUsage(projectAndHdfsObjectRDD: RDD[(Project, HdfsObject)]): Array[ProjectUsage] = {
    val zeroValue = (0L, 0L, 0L, 0L)
    val accumulateNewHdfsObject = (acc: (Long, Long, Long, Long), elem: HdfsObject) => acc match {
      case (size, count, tstamp, accessTime) => (size + elem.fileSize * elem.replication, count + 1,
        takeNewer(tstamp, elem.modTime), takeNewer(accessTime, elem.accessTime))
    }
    val mergePartitions = (a: (Long, Long, Long, Long), b: (Long, Long, Long, Long)) => (a, b) match {
      case ((s1, c1, t1, at1), (s2, c2, t2, at2)) => (s1 + s2, c1 + c2, takeNewerTimestamp(t1, t2), takeNewerTimestamp(at1, at2))
    }

    projectAndHdfsObjectRDD
      .aggregateByKey(zeroValue)(accumulateNewHdfsObject, mergePartitions)
      .map { case (project, (size, filesCount, modTimestamp, accessTime)) => ProjectUsage(project, size, filesCount, modTimestamp, accessTime) }
      .collect()
  }

  private def takeNewer(timestamp: Long, modTime: String) = {
    takeNewerTimestamp(timestamp, FsImageTimeConverter.toUnixTimestamp(modTime))
  }

  private def takeNewerTimestamp(t1: Long, t2: Long) = if (t1 > t2) t1 else t2

}