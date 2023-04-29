package org.apache.spark.java.dispatcher.runner;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.java.dispatcher.DispatcherEndpoint;
import org.apache.spark.rpc.RpcEnv;

import java.io.Closeable;

import static org.apache.spark.java.dispatcher.DispatcherConstants.DEFAULT_EXECUTOR_ID;
import static org.apache.spark.java.dispatcher.DispatcherConstants.bindAddress;

/**
 * @author Aaron Wang
 * @date 2023/4/20 4:40 PM
 * @version 1.0
 */
public class SchedulerServer implements Closeable {

    public void run() {
        SparkConf dispatcherConf = new SparkConf().setMaster("local").setAppName("Dispatcher");
        dispatcherConf.set("spark.driver.port", "16161");

        // 不用在此创建rpcEnv, 将SparkEnv的创建放到Receiver
//        SparkEnv sparkEnv = SparkEnv.createExecutorEnv(
//                dispatcherConf,
//                DEFAULT_EXECUTOR_ID,
//                bindAddress,
//                bindAddress,
//                1,
//                null,
//                false);

//        RpcEnv executorRpcEnv = sparkEnv.rpcEnv();
//        RpcEnv executorRpcEnv = RpcEnv.create(executorSystemName,
//                bindAddress,
//                bindAddress,
//                16161,
//                dispatcherConf,
//                new SecurityManager(dispatcherConf, null, null),
//                0,
//                false);


        DispatcherEndpoint dispatcherEndpoint = new DispatcherEndpoint(dispatcherConf);

    }

    @Override
    public void close() {

    }
}
