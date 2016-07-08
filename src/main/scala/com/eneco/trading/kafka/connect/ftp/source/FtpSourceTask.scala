package com.eneco.trading.kafka.connect.ftp.source

import java.util

import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._

class FtpSourceTask extends SourceTask with Logging {

  override def stop(): Unit = {
  }

  override def start(map: util.Map[String, String]): Unit = {
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def poll(): util.List[SourceRecord] = {
    log.info("poll")
    Seq[SourceRecord]().asJava
  }
}

