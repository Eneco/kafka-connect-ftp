package com.eneco.trading.kafka.connect.ftp.source

import java.time.{Duration, Instant}
import java.util

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask, SourceTaskContext}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class FtpSourceTask extends SourceTask with Logging {
  var ftpMonitor: Option[FtpMonitor] = None
  var lastPoll = Instant.EPOCH
  var pollDuration = Duration.ofSeconds(Long.MaxValue)
  var metaStore:Option[ConnectFileMetaDataStore] = None
  var monitor2topic = Map[MonitoredDirectory, String]()

  override def initialize(context: SourceTaskContext ) {
    metaStore = Some(new ConnectFileMetaDataStore(context))
    super.initialize(context)
  }

  override def stop (): Unit = {
    log.info("stop")
  }

  override def start(props: util.Map[String, String]): Unit = {
    require(metaStore.isDefined) // TODO
    val Some(store) = metaStore
    val sourceConfig = new FtpSourceConfig(props)
    sourceConfig.ftpMonitorConfigs.foreach(cfg=> {
      val style = if (cfg.tail) "tail" else "updates"
      log.info(s"config tells us to track the ${style} of files in `${cfg.path}` to topic `${cfg.topic}")})

    monitor2topic = sourceConfig.ftpMonitorConfigs().map(cfg=>(MonitoredDirectory(cfg.path,".*",cfg.tail),cfg.topic)).toMap

    pollDuration = Duration.ofSeconds(sourceConfig.getLong(FtpSourceConfig.RefreshRate))

    ftpMonitor = Some(new FtpMonitor(
      sourceConfig.getString(FtpSourceConfig.Address),
      sourceConfig.getString(FtpSourceConfig.User),
      sourceConfig.getPassword(FtpSourceConfig.Password).value,
      monitor2topic.keys.toSeq,
      store))
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def poll(): util.List[SourceRecord] = {
    log.info("poll")
    require(metaStore.isDefined) // TODO
    val Some(store) = metaStore
    ftpMonitor match {
      case Some(ftp) if Instant.now.isAfter(lastPoll.plus(pollDuration)) =>
        ftp.poll() match {
          case Success(fileChanges) =>
            lastPoll = Instant.now
            fileChanges.map { case (meta, body, w) =>
              log.info(s"got some fileChanges: ${meta.attribs.path}")
              store.set(meta.attribs.path, meta)
              val topic = monitor2topic.get(w).get
              new SourceRecord(
                store.fileMetasToConnectPartition(meta),  // source part
                store.fileMetasToConnectOffset(meta), // source off
                topic, //topic
                Schema.STRING_SCHEMA, // key sch
                meta.attribs.path, // key
                Schema.BYTES_SCHEMA, // val sch
                body.bytes // val
              )
            }.asJava
          case Failure(err) =>
            Seq[SourceRecord]().asJava
        }
      case Some(ftp) =>
        Thread.sleep(1000)
        Seq[SourceRecord]().asJava
      case None => throw new ConnectException("not initialised")
    }
  }
}

