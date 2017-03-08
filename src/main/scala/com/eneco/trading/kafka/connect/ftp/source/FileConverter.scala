package com.eneco.trading.kafka.connect.ftp.source

import java.util

import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.util.{Failure, Success, Try}

/**
  * Generic converter for files to source records. Needs to track
  * file offsets for the FtpMonitor.
  */
abstract class FileConverter(props: util.Map[String, String], offsetStorageReader : OffsetStorageReader) {
  def convert(topic: String, meta: FileMetaData, body: FileBody) : Seq[SourceRecord]
  def getFileOffset(path: String) : Option[FileMetaData]
}

object FileConverter {
  def apply(klass: Class[_], props: util.Map[String, String], offsetStorageReader: OffsetStorageReader) : FileConverter = {
    Try(klass.getDeclaredConstructor(classOf[util.Map[String, String]], classOf[OffsetStorageReader])
      .newInstance(props, offsetStorageReader).asInstanceOf[FileConverter]) match {
      case Success(fc) => fc
      case Failure(err) => throw new Exception(s"Failed to create ${klass} as instance of ${classOf[FileConverter]}", err)
    }
  }
}
