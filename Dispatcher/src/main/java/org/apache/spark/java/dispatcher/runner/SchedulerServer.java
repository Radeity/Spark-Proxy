package org.apache.spark.java.dispatcher.runner;

import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.java.dispatcher.DispatcherConstants;
import org.apache.spark.java.dispatcher.DispatcherEndpoint;
import org.apache.spark.rpc.RpcEnv;

import java.io.Closeable;

import static fdu.daslab.constants.Constants.executorSystemName;

/**
 * @author Aaron Wang
 * @date 2023/4/20 4:40 PM
 * @version 1.0
 */
public class SchedulerServer implements Closeable {

    public void run() {
        SparkConf executorConf = new SparkConf();

        RpcEnv executorRpcEnv = RpcEnv.create(executorSystemName,
                DispatcherConstants.bindAddress,
                DispatcherConstants.bindAddress,
                16161,
                executorConf,
                new SecurityManager(executorConf, null, null),
                0,
                false);

        System.out.println(executorRpcEnv.address());

        DispatcherEndpoint dispatcherEndpoint = new DispatcherEndpoint(executorRpcEnv, executorConf);

        executorRpcEnv.setupEndpoint("Dispatcher", dispatcherEndpoint);

        // TODO: Send onStop message to stop executor
        executorRpcEnv.awaitTermination();
    }

    @Override
    public void close() {

    }
}
