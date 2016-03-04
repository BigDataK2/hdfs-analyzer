package com.pg.model

case class HdfsObject(path: Path, replication: Int, modTime: String, accessTime: String, blockSize: Long,
                      numBlocks: Int, fileSize: Long, namespaceQuota: Int, diskspaceQuota: Int,
                      perms: String, user: String, group: String) {

  def isFile = replication > 0

  // check if this HdfsObject is under any of the paths from pathsList
  def isContainedWithin(pathsList: Iterable[Path]): Boolean = {
    pathsList.exists(p => path.startsWith(p))
  }
}