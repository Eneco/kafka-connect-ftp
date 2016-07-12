Kafka Connect FTP
=================

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

#refresh rate in seconds
ftp.refresh=15

#comma separated lists of path:destinationtopic
#only yield the tail of these files
ftp.monitor.tail=/appending/:ftp-appends,/logs/:ftp-logs
#yield the entire content of these files
ftp.monitor.update=/replacing/:ftp-updates
```


