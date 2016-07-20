Kafka Connect FTP
=================

[![Build Status](https://travis-ci.org/Eneco/kafka-connect-ftp.svg?branch=master)](https://travis-ci.org/Eneco/kafka-connect-ftp)

Monitors files on an FTP server and feeds changes into Kafka.

Status: WIP. Immature, naive, convoluted, happy path only.

Usage
-----

Files are monitored in the provided paths. Files can be `tail` or `update`:

- *Tailed* files are only allowed to grow. Bytes that have been appended to it since a last inspection are yielded. Preceding bytes are not allowed to change;
- *Updated* files can grow, shrink and change anywhere. The entire contents are yielded.

Example properties:

```
name=ftp-source
connector.class=com.eneco.trading.kafka.connect.ftp.source.FtpSourceConnector
tasks.max=1

#server settings
ftp.address=localhost
ftp.user=ftp
ftp.password=ftp

#refresh rate. Java's crippled ISO8601 duration
ftp.refresh=PT5S

#ignore files older than this. Java's crippled ISO8601 duration
ftp.file.maxage=P14D

#comma separated lists of path:destinationtopic
#only yield the tail of these files
ftp.monitor.tail=/appending/:ftp-appends,/logs/:ftp-logs
#yield the entire content of these files
ftp.monitor.update=/replacing/:ftp-updates

#keystyle controls the format of the key and can be string or struct.
#string only provides the file name
#struct provides a structure with the filename and offset
ftp.keystyle=struct

```


