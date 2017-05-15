FROM eneco/connector-base:0.3.0

ARG version
COPY target/kafka-connect-ftp-${version}-jar-with-dependencies.jar /etc/kafka-connect/jars
