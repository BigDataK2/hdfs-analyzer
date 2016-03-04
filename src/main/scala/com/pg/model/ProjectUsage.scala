package com.pg.model

import com.pg.util.Units
import Units.UNIT

case class ProjectUsage(project: Project, size: Long, filesCount: Long, modTimestamp: Long) {
  def toReport = ProjectUsageReport(project.name, size.toDouble / UNIT, filesCount, modTimestamp)
}

case class ProjectUsageReport(projectName: String, size: Double, filesCount: Long, modTimestamp: Long)