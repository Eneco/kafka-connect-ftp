package com.eneco.trading.kafka.connect.ftp.source

import java.time.{Duration, Instant}
import java.util

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceTaskContext, SourceRecord, SourceTask}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success}

// allows storage and retrieval of
class ConnectFileMetaDataStore(c: SourceTaskContext) extends FileMetaDataStore with Logging {
  // connect offsets aren't directly committed, hence we'll cache them
  val cache = mutable.Map[String, FileMetaData]()

  override def get(path: String): Option[FileMetaData] =
    cache.get(path).orElse(getFromStorage(path))

  override def set(path: String, fileMetaData: FileMetaData): Unit = {
    log.info(s"ConnectFileMetaDataStore set ${path}")
    cache.put(path, fileMetaData)
  }

  def getFromStorage(path: String): Option[FileMetaData] =
  c.offsetStorageReader().offset(Map("path" -> path).asJava) match {
    case null =>
      log.info(s"meta store storage HASN'T ${path}")
      None
    case o =>
      log.info(s"meta store storage has ${path}")
      Some(connectOffsetToFileMetas(path, o))
  }

  def fileMetasToConnectPartition(meta:FileMetaData): util.Map[String, String] = {
    Map("path" -> meta.attribs.path).asJava
  }

  def connectOffsetToFileMetas(path:String, o:AnyRef): FileMetaData = {
    val jm = o.asInstanceOf[java.util.Map[String, AnyRef]]
    FileMetaData(FileAttributes(path, jm.get("size").asInstanceOf[Long],
      Instant.ofEpochMilli(jm.get("timestamp").asInstanceOf[Long])
    ), jm.get("hash").asInstanceOf[String],
      Instant.ofEpochMilli(jm.get("firstfetched").asInstanceOf[Long]),
      Instant.ofEpochMilli(jm.get("lastmodified").asInstanceOf[Long]),
      Instant.ofEpochMilli(jm.get("lastinspected").asInstanceOf[Long])
    )
  }

  def fileMetasToConnectOffset(meta: FileMetaData) = {
    Map("size" -> meta.attribs.size,
      "timestamp" -> meta.attribs.timestamp.toEpochMilli,
      "hash" -> meta.hash,
      "firstfetched" -> meta.firstFetched.toEpochMilli,
      "lastmodified" -> meta.lastModified.toEpochMilli,
      "lastinspected" -> meta.lastInspected.toEpochMilli
    ).asJava
  }
}

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
    ftpMonitor = Some(new FtpMonitor(sourceConfig.getString(FtpSourceConfig.Address),
      sourceConfig.getString(FtpSourceConfig.User),
      sourceConfig.getPassword(FtpSourceConfig.Password).value, store))
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def poll(): util.List[SourceRecord] = {
    require(metaStore.isDefined) // TODO
    val Some(store) = metaStore
    log.info("poll")
    ftpMonitor match {
      case Some(ftp) if Instant.now.isAfter(lastPoll.plus(PollDuration)) =>
        ftp.poll(
          new MonitoredDirectory("/", ".*", true) :: Nil
        ) match {
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

