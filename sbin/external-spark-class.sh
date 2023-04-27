#!/usr/bin/env bash

EXTERNAL_SPARK_CONF_DIR="${EXTERNAL_SPARK_CONF_DIR:-"${EXTERNAL_SPARK_HOME}/conf"}"

EXTERNAL_SPARK_JAR_DIR="${EXTERNAL_SPARK_JAR_DIR:-"${EXTERNAL_SPARK_HOME}/jars"}"

EXTERNAL_SPARK_CLASS_PATH=${EXTERNAL_SPARK_CONF_DIR}:${EXTERNAL_SPARK_JAR_DIR}/*

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  echo "JAVA_HOME is not set!"
  exit 1
fi

# Turn off posix mode since it does not allow process substitution
set +o posix
CMD=()
CMD+=("$RUNNER")
CMD+=("-cp")
CMD+=("$EXTERNAL_SPARK_CLASS_PATH")
CMD=(${CMD[@]} "$@")
COUNT=${#CMD[@]}

echo "Start to launch ${CMD[@]}"

exec "${CMD[@]}"
