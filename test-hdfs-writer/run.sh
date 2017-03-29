#!/bin/bash
TESTPATH=/user/chinmay/test-location
INTERVAL=10000

hadoop fs -rm $TESTPATH

hadoop jar target/test-hdfs-writer-1.0-SNAPSHOT.jar com.datatorrent.RunTest src/main/resources/config.properties

