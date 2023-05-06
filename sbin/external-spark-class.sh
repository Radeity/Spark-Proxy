#!/usr/bin/env bash


JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  echo "JAVA_HOME is not set!"
  exit 1
fi

command=$1
shift

case ${command} in
  (org.apache.spark.java.dispatcher.Dispatcher)
    EXTERNAL_SPARK_JAR_DIR="${EXTERNAL_SPARK_DISPATCHER_JAR_DIR:-"${EXTERNAL_SPARK_HOME}/dispatcher-jars"}"
    JAVA_AGENT_CONF="-javaagent:${EXTERNAL_SPARK_JAR_DIR}/aspectjweaver-1.9.6.jar"
    ;;
  (org.apache.spark.worker.Worker)
    EXTERNAL_SPARK_JAR_DIR="${EXTERNAL_SPARK_WORKER_JAR_DIR:-"${EXTERNAL_SPARK_HOME}/worker-jars"}"
    ;;
esac

EXTERNAL_SPARK_CONF_DIR="${EXTERNAL_SPARK_CONF_DIR:-"${EXTERNAL_SPARK_HOME}/conf"}"

EXTERNAL_SPARK_CLASS_PATH=${EXTERNAL_SPARK_CONF_DIR}:${EXTERNAL_SPARK_JAR_DIR}/*


# Turn off posix mode since it does not allow process substitution
set +o posix
CMD=()
CMD+=("$RUNNER")
CMD+=("${JAVA_AGENT_CONF}")
CMD+=("-cp")
CMD+=("$EXTERNAL_SPARK_CLASS_PATH")
CMD+=("${command}")
CMD=(${CMD[@]} "$@")
COUNT=${#CMD[@]}

echo "Start to launch ${CMD[@]}"

exec "${CMD[@]}"
