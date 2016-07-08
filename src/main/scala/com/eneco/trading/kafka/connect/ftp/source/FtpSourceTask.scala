package com.eneco.trading.kafka.connect.ftp.source

import java.time.{Duration, Instant}
import java.util

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class FtpSourceTask extends SourceTask with Logging {
  var ftpMonitor: Option[FtpMonitor] = None
  var lastPoll = Instant.EPOCH
  val PollDuration = Duration.ofSeconds(10)

  override def stop (): Unit = {
  }

  override def start(props: util.Map[String, String]): Unit = {
    val sourceConfig = new FtpSourceConfig(props)
    ftpMonitor = Some(new FtpMonitor(sourceConfig.getString(FtpSourceConfig.Address),
      sourceConfig.getString(FtpSourceConfig.User),
      sourceConfig.getPassword(FtpSourceConfig.Password).value, Map[String, FileMetaData]()))
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def poll(): util.List[SourceRecord] = {
    log.info("poll")
    ftpMonitor match {
      case Some(ftp) if Instant.now.isAfter(lastPoll.plus(PollDuration)) =>
        ftp.poll(
          new MonitoredDirectory("/", ".*", true) :: Nil
        ) match {
          case Success((fileChanges, knownFiles)) =>
            lastPoll = Instant.now
            fileChanges.map { case (meta, body) =>
              new SourceRecord(
                Map("path" -> meta.attribs.path).asJava, // source part
                Map("offset" -> body.offset).asJava, // source off
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

