package com.eneco.trading.kafka.connect.ftp.source

import java.time.{Duration, Instant}
import java.util

import com.eneco.trading.kafka.connect.ftp.source.SourceRecordProducers.SourceRecordProducer
import org.apache.kafka.connect.data.{Struct, SchemaBuilder, Schema}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask, SourceTaskContext}
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

// holds functions that translate a file meta+body into a source record
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

// bridges between the Connect reality and the (agnostic) FtpMonitor reality.
// logic could have been in FtpSourceTask directly, but FtpSourceTask's imperative nature made things a bit ugly,
// from a Scala perspective.
class FtpSourcePoller(cfg: FtpSourceConfig, offsetStorage: OffsetStorageReader) extends Logging {
  var lastPoll = Instant.EPOCH

  val metaStore = new ConnectFileMetaDataStore(offsetStorage)

  val monitor2topic = cfg.ftpMonitorConfigs()
    .map(monitorCfg => (MonitoredDirectory(monitorCfg.path, ".*", monitorCfg.tail), monitorCfg.topic)).toMap

  val pollDuration = Duration.parse(cfg.getString(FtpSourceConfig.RefreshRate))

  val ftpMonitor = new FtpMonitor(
    FtpMonitorSettings(
      cfg.getString(FtpSourceConfig.Address),
      cfg.getString(FtpSourceConfig.User),
      cfg.getPassword(FtpSourceConfig.Password).value,
      Some(Duration.parse(cfg.getString(FtpSourceConfig.FileMaxAge))),
      monitor2topic.keys.toSeq),
    metaStore)

  val recordMaker:SourceRecordProducer = cfg.keyStyle match {
    case KeyStyle.String => SourceRecordProducers.stringKeyRecord
    case KeyStyle.Struct => SourceRecordProducers.structKeyRecord
  }

  def poll(): Seq[SourceRecord] = {
    if (Instant.now.isAfter(lastPoll.plus(pollDuration))) {
      log.info("poll")
      ftpMonitor.poll() match {
        case Success(fileChanges) =>
          lastPoll = Instant.now
          fileChanges.map { case (meta, body, w) =>
            log.info(s"got some fileChanges: ${meta.attribs.path}")
            metaStore.set(meta.attribs.path, meta)
            val topic = monitor2topic.get(w).get
            recordMaker(metaStore, topic, meta, body)
          }
        case Failure(err) =>
          log.warn(s"ftp monitor says no: ${err}")
          Seq[SourceRecord]()
      }
    } else {
      Thread.sleep(1000)
      Seq[SourceRecord]()
    }
  }
}

class FtpSourceTask extends SourceTask with Logging {
  var poller: Option[FtpSourcePoller] = None

  override def stop(): Unit = {
    log.info("stop")
    poller = None
  }

  override def start(props: util.Map[String, String]): Unit = {
    log.info("start")
    val sourceConfig = new FtpSourceConfig(props)
    sourceConfig.ftpMonitorConfigs.foreach(cfg => {
      val style = if (cfg.tail) "tail" else "updates"
      log.info(s"config tells us to track the ${style} of files in `${cfg.path}` to topic `${cfg.topic}")
    })
    poller = Some(new FtpSourcePoller(sourceConfig, context.offsetStorageReader))
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def poll(): util.List[SourceRecord] = poller match {
    case Some(poller) => poller.poll().asJava
    case None => throw new ConnectException("FtpSourceTask is not initialized but it is polled")
  }
}
