package com.pg

class CliOptions(arglist: List[String]) extends Serializable {

  type OptionMap = Map[Symbol, Any]

  val DEFAULT_USAGE_REPORT_DB = "stats"
  val DEFAULT_USAGE_REPORT_TABLE = "usage_report"

  val optionMap = parseOptions(arglist)

  val dt = getValueString('dt)
  val usagereportdb = getValueString('usagereportdb)
  val usagereporttable = getValueString('usagereporttable)


  def getValueString(s: Symbol): String = {
    optionMap.getOrElse(s, "none").toString
  }

  def getValueInt(s: Symbol): Int = {
    def value = optionMap.get(s)
    if (value.isEmpty) {
      return -1
    }
    value.get.toString.toInt
  }

  def getValueBoolean(s: Symbol): Boolean = optionMap.getOrElse(s, false).asInstanceOf[Boolean]

  def parseOptions(arglist: List[String]): OptionMap = {
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      (list: @unchecked) match {
        case Nil => map
        case "--dt" :: value :: tail =>
          nextOption(map ++ Map('dt -> value), tail)
        case "--usagereportdb" :: value :: tail =>
          nextOption(map ++ Map('usagereportdb -> value), tail)
        case "--usagereporttable" :: value :: tail =>
          nextOption(map ++ Map('usagereporttable -> value), tail)
      }
    }
    var options = nextOption(Map(), arglist)

    if (!options.contains('usagereportdb)) options = options ++ Map('usagereportdb -> DEFAULT_USAGE_REPORT_DB)
    if (!options.contains('usagereporttable)) options = options ++ Map('usagereporttable -> DEFAULT_USAGE_REPORT_TABLE)

    options
  }

}
