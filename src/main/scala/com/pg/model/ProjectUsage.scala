package com.pg.model

import com.pg.util.Units
import Units.UNIT

case class ProjectUsage(project: Project, size: Long, filesCount: Long, modTimestamp: Long, accessTime: Long) {
  def toReport = ProjectUsageReport(project.name, size.toDouble / UNIT, filesCount, modTimestamp, accessTime)
}

case class ProjectUsageReport(projectName: String, size: Double, filesCount: Long, modTimestamp: Long, accessTime: Long)