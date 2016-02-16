package com.pg

case class Application(val name: String, val hdfsDirs: List[String], val hiveDb: String) {
  override def toString(): String = {
    s"App: $name"
  }
}