#!/usr/bin/env bash

EXTERNAL_SPARK_CONF_DIR="${EXTERNAL_SPARK_CONF_DIR:-"${EXTERNAL_SPARK_HOME}/conf"}"

if [ -f "${EXTERNAL_SPARK_CONF_DIR}/hosts" ]; then
  HOST_LIST=$(awk '/\[/{prefix=$0; next} $1{print prefix,$0}' "${EXTERNAL_SPARK_CONF_DIR}/hosts")
else
  HOST_LIST="[dispatcher] localhost\n[worker] localhost"
fi

# start Dispatcher on dispatcher hosts
for host in `echo "$HOST_LIST" |  grep '\[dispatcher\]' | awk '{print $NF}'`
do
  ssh -o StrictHostKeyChecking=no "$host" "${EXTERNAL_SPARK_HOME}/sbin/start-dispatcher.sh"
  dispatcher=$host
done

if [ $? == 16 ]; then
  echo "Dispatcher launch error, port is not available, Worker will not launch"
  exit 16
fi

# sleep 3 seconds to make sure that dispatcher is ready, which can reduce `Worker` waiting time
sleep 3s

executorId=17
for host in `echo "$HOST_LIST" |  grep '\[worker\]' | awk '{print $NF}'`
do
  ssh -o StrictHostKeyChecking=no "$host" "${EXTERNAL_SPARK_HOME}/sbin/start-worker.sh $dispatcher $executorId"
  executorId=`expr $executorId + 1`;
done

wait