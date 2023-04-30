package org.apache.spark.worker;

import org.apache.spark.SparkConf;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.SecurityManager;

import static fdu.daslab.constants.Constants.executorSystemName;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2023/4/24 11:02 AM
 */
public class Worker {
    public static void main(String[] args) {
        run(args[0]);
    }

    public static void run(String dispatcherHost) {
        SparkConf executorConf = new SparkConf();

        RpcEnv executorRpcEnv = RpcEnv.create(executorSystemName,
                WorkerConstants.bindAddress,
                WorkerConstants.bindAddress,
                16162,
                executorConf,
                new SecurityManager(executorConf, null, null),
                0,
                false);

        System.out.println(executorRpcEnv.address());

        WorkerEndPoint workerEndPoint = new WorkerEndPoint(executorRpcEnv, executorConf, dispatcherHost);

        executorRpcEnv.setupEndpoint("Worker", workerEndPoint);

        executorRpcEnv.awaitTermination();

    }
}
