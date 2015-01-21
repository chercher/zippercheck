#!/bin/bash

export LIBJARS=/usr/local/hadoop/hadoop-release/share/hadoop/common/hadoop-common-2.4.1-dp_1.jar,/usr/local/hadoop/hadoop-release/share/hadoop/mapreduce/hadoop-mapreduce-client-app-2.4.1-dp_1.jar,/usr/local/hadoop/hadoop-release/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.4.1-dp_1.jar,/usr/local/hadoop/hadoop-release/share/hadoop/common/lib/commons-lang-2.6.jar
export HADOOP_CLASSPATH=/usr/local/hadoop/hadoop-release/share/hadoop/common/hadoop-common-2.4.1-dp_1.jar:/usr/local/hadoop/hadoop-release/share/hadoop/mapreduce/hadoop-mapreduce-client-app-2.4.1-dp_1.jar:/usr/local/hadoop/hadoop-release/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.4.1-dp_1.jar:/usr/local/hadoop/hadoop-release/share/hadoop/common/lib/commons-lang-2.6.jar

hadoop jar zippercheck-1.0-SNAPSHOT.jar com.dianping.data.ZipperCheck -libjars ${LIBJARS} $@