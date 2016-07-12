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
  val PollDuration = Duration.ofSeconds(10)
  var metaStore:Option[ConnectFileMetaDataStore] = None

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
    ftpMonitor = Some(new FtpMonitor(
      sourceConfig.getString(FtpSourceConfig.Address),
      sourceConfig.getString(FtpSourceConfig.User),
      sourceConfig.getPassword(FtpSourceConfig.Password).value,
      new MonitoredDirectory("/", ".*", true) :: Nil,
      store))
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def poll(): util.List[SourceRecord] = {
    require(metaStore.isDefined) // TODO
    val Some(store) = metaStore
    log.info("poll")
    ftpMonitor match {
      case Some(ftp) if Instant.now.isAfter(lastPoll.plus(PollDuration)) =>
        ftp.poll() match {
          case Success(fileChanges) =>
            lastPoll = Instant.now
            fileChanges.map { case (meta, body) =>
              log.info(s"got some fileChanges: ${meta.attribs.path}")
              store.set(meta.attribs.path, meta)
              new SourceRecord(
                store.fileMetasToConnectPartition(meta),  // source part
                store.fileMetasToConnectOffset(meta), // source off
                "ftp-hardcoded", //top
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

