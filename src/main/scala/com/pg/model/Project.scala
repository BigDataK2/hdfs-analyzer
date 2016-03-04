package com.pg.model

case class Project(val name: String, val hdfsDirs: List[Path]) {
  override def toString: String = {
    s"Project: $name"
  }
}