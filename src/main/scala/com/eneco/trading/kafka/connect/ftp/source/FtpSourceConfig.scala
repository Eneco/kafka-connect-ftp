package com.eneco.trading.kafka.connect.ftp.source

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object FtpSourceConfig {
  val Address = "ftp.address"
  val User = "ftp.user"
  val Password = "ftp.password"

  val definition: ConfigDef = new ConfigDef()
    .define(Address, Type.STRING, Importance.HIGH, "ftp address")
    .define(User, Type.STRING, Importance.HIGH, "ftp user name to login")
    .define(Password, Type.PASSWORD, Importance.HIGH, "ftp password to login")
}

class FtpSourceConfig(props: util.Map[String, String])
  extends AbstractConfig(FtpSourceConfig.definition, props)