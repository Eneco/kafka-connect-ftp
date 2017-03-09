package com.eneco.trading.kafka.connect.ftp.source

import java.time.Instant

// what the ftp can tell us without actually fetching the file
case class FileAttributes(path: String, size: Long, timestamp: Instant) {
  override def toString() = s"(path: ${path}, size: ${size}, timestamp: ${timestamp})"
}

// used to administer the files
// this is persistent data, stored into the connect offsets
case class FileMetaData(attribs:FileAttributes, hash:String, firstFetched:Instant, lastModified:Instant, lastInspected:Instant, offset: Long = -1L) {
  def modifiedNow() = FileMetaData(attribs, hash, firstFetched, Instant.now, lastInspected, offset)
  def inspectedNow() = FileMetaData(attribs, hash, firstFetched, lastModified, Instant.now, offset)
  override def toString() = s"(remoteInfo: ${attribs}, hash: ${hash}, firstFetched: ${firstFetched}, lastModified: ${lastModified}, lastInspected: ${lastInspected}"
}

// the store where com.eneco.trading.kafka.connect.ftp.source.FileMetaData is kept and can be retrieved from
trait FileMetaDataStore {
  def get(path:String) : Option[FileMetaData]
  def set(path:String, fileMetaData: FileMetaData)
}
