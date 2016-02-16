package com.pg

case class HdfsObject(path: String, replication: Int, modTime: String, accessTime: String, blockSize: Long,
                      numBlocks: Int, fileSize: Long, namespaceQuota: Int, diskspaceQuota: Int,
                      perms: String, username: String, groupname: String) {

  // check if this HdfsObject is under any of the paths from 'paths' list
  def isContainedWithin(paths: List[String]): Boolean = {
    val res = paths.exists(p => path.startsWith(p))
    println((path, res))
    res
  }

  // for every path in the list returns only these that contain this HdfsObject
  def filterPaths(paths: List[String]): List[(String, HdfsObject)] = {
    for (p <- paths; if (path.startsWith(p)))
      yield (p, this)
  }
}