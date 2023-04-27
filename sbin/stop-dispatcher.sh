#!/usr/bin/env bash

if [ -z "${EXTERNAL_SPARK_HOME}" ]; then
  export EXTERNAL_SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

exec "${EXTERNAL_SPARK_HOME}/sbin/external-spark-daemon.sh" stop org.apache.spark.java.dispatcher.Dispatcher