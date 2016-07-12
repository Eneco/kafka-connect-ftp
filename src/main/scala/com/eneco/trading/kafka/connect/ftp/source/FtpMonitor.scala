package com.eneco.trading.kafka.connect.ftp.source

import java.io.ByteArrayOutputStream
import java.nio.file.Paths
import java.time.{Duration, Instant}
import java.util

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTPFile

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

// what the ftp can tell us without actually fetching the file
case class FileAttributes(path: String, size: Long, timestamp: Instant) {
  override def toString() = s"(path: ${path}, size: ${size}, timestamp: ${timestamp})"
}

//this is persistent data
case class FileMetaData(attribs:FileAttributes, hash:String, firstFetched:Instant, lastModified:Instant, lastInspected:Instant) {
  def modifiedNow() = FileMetaData(attribs, hash, firstFetched, Instant.now, lastInspected)
  def inspectedNow() = FileMetaData(attribs, hash, firstFetched, lastModified, Instant.now)
  override def toString() = s"(remoteInfo: ${attribs}, hash: ${hash}, firstFetched: ${firstFetched}, lastModified: ${lastModified}, lastInspected: ${lastInspected}"
}

case class FetchedFile(meta:FileMetaData, body: Array[Byte])

case class AbsoluteFtpFile(ftpFile:FTPFile, parentDir:String) {
  def path() = Paths.get(parentDir, ftpFile.getName).toString
  def age(): Duration = Duration.between(ftpFile.getTimestamp.toInstant,Instant.now)
}

case class MonitoredDirectory(directory:String, filenameRegex:String, tail:Boolean) {
  def isFileRelevant(f:AbsoluteFtpFile):Boolean = true
}

case class FileBody(bytes:Array[Byte], offset:Long)

trait FileMetaDataStore {
  def get(path:String) : Option[FileMetaData]
  def set(path:String, fileMetaData: FileMetaData)
}

class FtpMonitor(host:String, user:String, pass:String, watchers: Seq[MonitoredDirectory], knownFiles: FileMetaDataStore) extends Logging {
  val MaxAge = Duration.ofDays(1)

  val ftp = new FTPClient()

  def requiresFetch(file: AbsoluteFtpFile, knownFile: Option[FileMetaData]):Boolean = knownFile match {
    // file previously unknown? fetch
    case None => !MaxAge.minus(file.age).isNegative
    case Some(known) if known.attribs.size != file.ftpFile.getSize => true
    case Some(known) if known.attribs.timestamp != file.ftpFile.getTimestamp.toInstant => true
    case _ => false
  }

  // Retrieves the FtpAbsoluteFile and returns a new or updated KnownFile
  def fetch(file: AbsoluteFtpFile, knownFile: Option[FileMetaData]): Try[FetchedFile] = {
    log.info(s"fetch ${file.path}")
    val baos = new ByteArrayOutputStream()
    if (ftp.retrieveFile(file.path, baos)) {
      val bytes = baos.toByteArray
      val hash = DigestUtils.sha256Hex(bytes)
      Success(FetchedFile(knownFile match {
        case None => FileMetaData(new FileAttributes(file.path, file.ftpFile.getSize, file.ftpFile.getTimestamp.toInstant), hash, Instant.now, Instant.now, Instant.now)
        case Some(old) => FileMetaData(new FileAttributes(file.path, file.ftpFile.getSize, file.ftpFile.getTimestamp.toInstant), hash, old.firstFetched, old.lastModified, Instant.now)
      }, bytes))
    } else {
      new Failure(new Exception("ftp says no: " + ftp.getReplyString))
    }
  }

  def handleFetchedFile(w:MonitoredDirectory, optPreviously: Option[FileMetaData], current:FetchedFile): (FileMetaData, Option[FileBody]) =
    optPreviously match {
      case Some(previously) if previously.attribs.size != current.meta.attribs.size || previously.hash != current.meta.hash =>
        // file changed in size and/or hash
        log.info(s"fetched ${current.meta.attribs.path}, it was known before and it changed")
        if (w.tail) {
          if (current.meta.attribs.size > previously.attribs.size) {
            val hashPrevBlock = DigestUtils.sha256Hex(util.Arrays.copyOfRange(current.body, 0, previously.attribs.size.toInt))
            if (previously.hash == hashPrevBlock) {
              log.info(s"tail ${current.meta.attribs.path} [${previously.attribs.size.toInt}, ${current.meta.attribs.size.toInt})")
              val tail = util.Arrays.copyOfRange(current.body, previously.attribs.size.toInt, current.meta.attribs.size.toInt)
              (current.meta.inspectedNow().modifiedNow(), Some(FileBody(tail,previously.attribs.size)))
            } else {
              log.warn(s"the tail of ${current.meta.attribs.path} is to be followed, but previously seen content changed. we'll provide the entire file.")
              (current.meta.inspectedNow().modifiedNow(), Some(FileBody(current.body,0)))
            }
          } else {
            // the file shrunk or didn't grow
            log.warn(s"the tail of ${current.meta.attribs.path} is to be followed, but it shrunk")
            (current.meta.inspectedNow().modifiedNow(), None)
          }
        } else { // !w.tail: we're not tailing but dumping the entire file on change
          log.info(s"dump entire ${current.meta.attribs.path}")
          (current.meta.inspectedNow().modifiedNow(), Some(FileBody(current.body,0)))
        }
      case Some(previouslyKnownFile) =>
        // file didn't change
        log.info(s"fetched ${current.meta.attribs.path}, it was known before and it didn't change")
        (current.meta.inspectedNow(), None)
      case None =>
        // file is new
        log.info(s"fetched ${current.meta.attribs.path}, wasn't known before")
        log.info(s"dump entire ${current.meta.attribs.path}")
        (current.meta.inspectedNow().modifiedNow(), Some(FileBody(current.body,0)))
    }

  def fetchFromMonitoredPlaces(w:MonitoredDirectory): Seq[(FileMetaData, Option[FileBody])] = {
    val toBeFetched = ftp.listFiles(w.directory).toSeq
      .filter(_.isFile)
      .map(AbsoluteFtpFile(_, w.directory))
      .filter(w.isFileRelevant)
      .filter { f => requiresFetch(f, knownFiles.get(f.path)) }

    log.info(s"we'll be fetching ${toBeFetched.length} items")

    val previouslyKnown = toBeFetched.map(f => knownFiles.get(f.path))

    val fetchResults = toBeFetched zip previouslyKnown map { case (f, k) => fetch(f, k) }

    toBeFetched zip previouslyKnown zip fetchResults map { case((a,b),c) => (a,b,c)} flatMap {
      case (ftpFile, optPrevKnown, Success(currentFile)) => Some(handleFetchedFile(w, optPrevKnown, currentFile))
      case (ftpFile, previouslyKnownFile, Failure(err)) =>
        log.warn(s"failed to fetch ${ftpFile.path}: ${err.toString}")
        None
    }
  }

  // TODO
  def connectFtp(): Try[FTPClient] = {
    if (!ftp.isConnected) {
      ftp.connect(host)
      println(ftp.getReplyString)
      ftp.login(user, pass)
      println(ftp.getReplyString)
      if (!ftp.isConnected) {
        Failure(new Exception("cannot connect ftp TODO"))
      }
    }
    Success(ftp)
  }

  def poll(): Try[Seq[(FileMetaData, FileBody)]] = {
    log.info("poll")
    //knownFiles.values.foreach(kf => log.info(s"we know upfront: ${kf}"))

    val result: Try[Seq[(FileMetaData, FileBody)]] = connectFtp() match {
      case Success(_) =>
        val v = watchers.flatMap(w => {
          val results: Seq[(FileMetaData, Option[FileBody])] = fetchFromMonitoredPlaces(w)
          results.flatMap {
            case (meta, Some(body)) =>
              log.info(s"${meta.attribs.path} got @ offset ${body.offset} `" + new String(body.bytes) + "`")
              Some((meta,body))
            case (meta, None) =>
              log.info(s"${meta.attribs.path} got no bytes")
              None
          }
        })
        Success(v)
      case Failure(err) => log.warn(s"cannot connect to ftp: ${err.toString}")
        Failure(err)
    }

    //knownFiles.values.foreach(kf => log.info(s"we know afterwards: ${kf}"))
    log.info("~poll")
    //result.map(files=>files)
    result
  }
}
