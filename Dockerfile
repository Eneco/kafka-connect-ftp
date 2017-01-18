FROM eneco/connector-base:0.2.0

COPY target/kafka-connect-ftp-0.1-jar-with-dependencies.jar /etc/kafka-connect/jars
