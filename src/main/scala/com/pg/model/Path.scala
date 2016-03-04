package com.pg.model

import java.nio.file.Paths

class Path private(p: String) extends Serializable {
  def path = p

  def startsWith(other: Path) = Paths.get(p).startsWith(Paths.get(other.path))

  override def toString = p

  override def hashCode = p.hashCode

  override def equals(that: Any): Boolean =
    that match {
      case that: Path => this.isInstanceOf[Path] && this.p == that.path
      case _ => false
    }
}

object Path {
  def apply(path: String): Path = {
    new Path(Paths.get(path).toString)
  }
}
