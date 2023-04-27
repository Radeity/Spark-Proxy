#!/usr/bin/env bash

# Runs a external Spark command as a daemon.
#
# Environment Variables
#
#   EXTERNAL_SPARK_HOME      The HOME of external Spark.
#   EXTERNAL_SPARK_CONF_DIR  Alternate conf dir. Default is ${EXTERNAL_SPARK_HOME}/conf.
#   EXTERNAL_SPARK_PID_DIR   The pid files are stored. /tmp by default.
##


usage="Usage: external-spark-daemon.sh [--config <conf-dir>] (start|stop) <external-spark-command> <args...>"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

option=$1
shift
command=$1
shift

EXTERNAL_SPARK_PID_DIR="${EXTERNAL_SPARK_PID_DIR:-"${EXTERNAL_SPARK_HOME}/tmp"}"

pid="${EXTERNAL_SPARK_PID_DIR}/external-spark-$USER-$command.pid"
log="${EXTERNAL_SPARK_PID_DIR}/external-spark-$USER-$command.log"

exec_command() {
  echo "execute command: $@"
  # last `&` makes sure this process runs in background
  exec nohup -- "$@" >> $log 2>&1 &
  # `$!` get the last running processId
  newpid="$!"
  echo $newpid
  echo "$newpid" > "$pid"
  sleep 2
}

run_command() {
  mode="$1"
  shift

  # create if absent
  mkdir -p "${EXTERNAL_SPARK_PID_DIR}"

  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
      echo "$command running as process $TARGET_ID. Stop it first."
      exit 1
    fi
  fi

  case "$mode" in
    (class)
      exec_command "${EXTERNAL_SPARK_HOME}/sbin/external-spark-class.sh" "$command" "$@"
      ;;
    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

}

case $option in
  (start)
    # `$@` represents other args, not in use now.
    run_command class "$@"
    ;;
  (stop)
    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]] || [[ $(ps -p "$TARGET_ID" -o comm=) =~ "jboot" ]]; then
        echo "stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $command to stop"
      fi
    else
      echo "no $command to stop"
    fi
    ;;
  (*)
    echo $usage
    exit 1
    ;;
esac
