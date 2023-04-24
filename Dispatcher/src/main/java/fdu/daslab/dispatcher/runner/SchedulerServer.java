package fdu.daslab.dispatcher.runner;

import fdu.daslab.dispatcher.DispatcherConstants;
import fdu.daslab.dispatcher.DispatcherEndpoint;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.rpc.RpcEnv;
import org.springframework.stereotype.Service;

import java.io.Closeable;

import static fdu.daslab.constants.Constants.executorSystemName;

/**
 * @author Aaron Wang
 * @date 2023/4/20 4:40 PM
 * @version 1.0
 */
@Service
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

        executorRpcEnv.setupEndpoint("Executor", dispatcherEndpoint);

        // TODO: Send onStop message to stop executor
        executorRpcEnv.awaitTermination();
    }

    @Override
    public void close() {

    }
}
