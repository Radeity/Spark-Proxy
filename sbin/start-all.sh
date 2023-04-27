#!/usr/bin/env bash

EXTERNAL_SPARK_CONF_DIR="${EXTERNAL_SPARK_CONF_DIR:-"${EXTERNAL_SPARK_HOME}/conf"}"

if [ -f "${EXTERNAL_SPARK_CONF_DIR}/hosts" ]; then
  HOST_LIST=$(awk '/\[/{prefix=$0; ne:qxt} $1{print prefix,$0}' "${EXTERNAL_SPARK_CONF_DIR}/hosts")
else
  HOST_LIST="[dispatcher] localhost\n[worker] localhost"
fi

# start Dispatcher on dispatcher hosts
for host in `echo "$HOST_LIST" |  grep '\[dispatcher\]' | awk '{print $NF}'`
do
  ssh -o StrictHostKeyChecking=no "$host" "${EXTERNAL_SPARK_HOME}/sbin/start-dispatcher.sh"
done

# sleep 3 seconds to make sure that dispatcher is ready, which can reduce `Worker` waiting time
sleep 3s

for host in `echo "$HOST_LIST" |  grep '\[worker\]' | awk '{print $NF}'`
do
  ssh -o StrictHostKeyChecking=no "$host" "${EXTERNAL_SPARK_HOME}/sbin/start-worker.sh"
done

wait