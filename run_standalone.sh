#!/bin/sh

CONNECT_PROCESS_FILE=connect-standalone.properties
CONNECT_FILE_SOURCE=connect-file-source.properties
CONNECT_ES_SINK=connect-elasticsearch-sink-connector.properties

sed -i '' -e "s/bootstrap.servers=.*/bootstrap.servers=$DOCKER_IP:9092/g" $CONNECT_PROCESS_FILE
sed -i '' -e "s/es.host=.*/es.host=$DOCKER_IP/g" $CONNECT_ES_SINK

CLASSPATH=target/*-jar-with-dependencies.jar
exec java -cp $CLASSPATH org.apache.kafka.connect.cli.ConnectStandalone $CONNECT_PROCESS_FILE $CONNECT_FILE_SOURCE $CONNECT_ES_SINK