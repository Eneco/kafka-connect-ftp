package com.eneco.trading.kafka.connect.ftp.source

import java.nio.file.{FileSystems, Paths}
import java.time.{Duration, Instant}

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.net.ftp.{FTPClient, FTPFile}

// org.apache.commons.net.ftp.FTPFile only contains the relative path
case class AbsoluteFtpFile(ftpFile:FTPFile, parentDir:String) {
  def path() = Paths.get(parentDir, ftpFile.getName).toString
  def age(): Duration = Duration.between(ftpFile.getTimestamp.toInstant, Instant.now)
}

case class FtpFileLister(ftp: FTPClient) extends StrictLogging {

  def pathMatch(pattern: String, path: String):Boolean = {
    val g = s"glob:$pattern"
    FileSystems.getDefault.getPathMatcher(g).matches(Paths.get(path))
  }

  def isGlobPattern(pattern: String): Boolean = List("*", "?", "[", "{").exists(pattern.contains(_))

  def listFiles(path: String) : Seq[AbsoluteFtpFile] = {
    val pathParts : Seq[String] = path.split("/")

    val (basePath, patterns) = pathParts.zipWithIndex.view.find{case (part, _) => isGlobPattern(part)} match {
      case Some((_, index)) => pathParts.splitAt(index)
      case _ => (pathParts.init, Seq[String](pathParts.last))
    }

    def iter(basePath: String, patterns: List[String]) : Seq[AbsoluteFtpFile] = {
      Option(ftp.listFiles(basePath + "/")) match {
        case Some(files) => patterns match {
          case pattern :: Nil => {
            files.filter(f => f.isFile && pathMatch(pattern, f.getName))
              .map(AbsoluteFtpFile(_, basePath + "/"))
          }
          case pattern :: rest => {
            files.filter(f => f.getName() != "." && f.getName() != ".." && pathMatch(pattern, f.getName))
              .flatMap(f => iter(Paths.get(basePath, f.getName).toString, rest))
          }
          case _ => Seq()
        }
        case _ => Seq()
      }
    }

    iter(Paths.get("/", basePath:_*).toString, patterns.toList)
  }
}
