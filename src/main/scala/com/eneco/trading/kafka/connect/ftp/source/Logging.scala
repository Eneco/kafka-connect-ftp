package com.eneco.trading.kafka.connect.ftp.source

import org.slf4j.LoggerFactory

trait Logging {
  val loggerName = this.getClass.getName
  @transient lazy val log = LoggerFactory.getLogger(loggerName)
}