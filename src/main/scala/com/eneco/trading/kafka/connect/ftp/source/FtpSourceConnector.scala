package com.eneco.trading.kafka.connect.ftp.source

import java.util

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

object FtpSourceConfig {
  val Address = "ftp.address"
  val User = "ftp.user"
  val Password = "ftp.password"

  val definition: ConfigDef = new ConfigDef()
    .define(Address, Type.STRING,Importance.HIGH,"ftp address")
    .define(User, Type.STRING,Importance.HIGH,"ftp user name to login")
    .define(Password, Type.PASSWORD,Importance.HIGH,"ftp password to login")
}

class FtpSourceConfig(props: util.Map[String, String])
  extends AbstractConfig(FtpSourceConfig.definition, props)


class FtpSourceConnector extends SourceConnector with Logging {
  private var configProps : util.Map[String, String] = null

  override def taskClass(): Class[_ <: Task] = classOf[FtpSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    log.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(_ => configProps).toList.asJava
  }

  override def stop(): Unit = {
  }

  override def start(props: util.Map[String, String]): Unit = {
    configProps = props
    Try(new FtpSourceConfig(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start due to configuration error: " + f.getMessage, f)
      case _ =>
    }
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def config() = FtpSourceConfig.definition
}
