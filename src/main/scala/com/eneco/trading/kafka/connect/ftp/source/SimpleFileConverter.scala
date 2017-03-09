package com.eneco.trading.kafka.connect.ftp.source

import java.util

import com.eneco.trading.kafka.connect.ftp.source.SourceRecordProducers.SourceRecordProducer
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._

/**
  * Simple file converter. Writes the complete file into a single record
  * including the file attributes.
  */
class SimpleFileConverter(props: util.Map[String, String], offsetStorageReader : OffsetStorageReader)
  extends FileConverter(props, offsetStorageReader) {

  val cfg = new FtpSourceConfig(props)
  val metaStore = new ConnectFileMetaDataStore(offsetStorageReader)
  val recordConverter: SourceRecordConverter = cfg.sourceRecordConverter
  val recordMaker:SourceRecordProducer = cfg.keyStyle match {
    case KeyStyle.String => SourceRecordProducers.stringKeyRecord
    case KeyStyle.Struct => SourceRecordProducers.structKeyRecord
  }

  override def convert(topic: String, meta: FileMetaData, body: FileBody): Seq[SourceRecord] = {
    metaStore.set(meta.attribs.path, meta)
    recordConverter.convert(recordMaker(metaStore, topic, meta, body)).asScala
  }

  override def getFileOffset(path: String): Option[FileMetaData] = metaStore.get(path)
}

object SourceRecordProducers {
  type SourceRecordProducer = (ConnectFileMetaDataStore, String, FileMetaData, FileBody) => SourceRecord

  val fileInfoSchema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.ftp.FileInfo")
    .field("name", Schema.STRING_SCHEMA)
    .field("offset", Schema.INT64_SCHEMA)
    .build()

  def stringKeyRecord(store: ConnectFileMetaDataStore, topic: String, meta: FileMetaData, body: FileBody): SourceRecord =
    new SourceRecord(
      store.fileMetasToConnectPartition(meta), // source part
      store.fileMetasToConnectOffset(meta), // source off
      topic, //topic
      Schema.STRING_SCHEMA, // key sch
      meta.attribs.path, // key
      Schema.BYTES_SCHEMA, // val sch
      body.bytes // val
    )

  def structKeyRecord(store: ConnectFileMetaDataStore, topic: String, meta: FileMetaData, body: FileBody): SourceRecord = {
    new SourceRecord(
      store.fileMetasToConnectPartition(meta), // source part
      store.fileMetasToConnectOffset(meta), // source off
      topic, //topic
      fileInfoSchema, // key sch
      new Struct(fileInfoSchema)
        .put("name",meta.attribs.path)
        .put("offset",body.offset),
      Schema.BYTES_SCHEMA, // val sch
      body.bytes // val
    )
  }
}
