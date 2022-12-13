package fdu.daslab;

import fdu.daslab.utils.IpUtils;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.rpc.RpcEnv;

import static fdu.daslab.constants.Constants.driverAddress;
import static fdu.daslab.constants.Constants.executorSystemName;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/8 4:29 PM
 */
public class Worker {

    public static void main(String[] args) throws InterruptedException {
        run();
    }

    public static void run() throws InterruptedException {
        SparkConf executorConf = new SparkConf();

        String bindAddress = IpUtils.fetchLANIp();

        RpcEnv executorRpcEnv = RpcEnv.create(executorSystemName,
                bindAddress,
                bindAddress,
                16161,
                executorConf,
                new SecurityManager(executorConf, null, null),
                0,
                false);

        System.out.println(executorRpcEnv.address());

        MocExecutorEndpoint mocWorkerEndpoint = new MocExecutorEndpoint(executorRpcEnv);

        executorRpcEnv.setupEndpoint("Executor", mocWorkerEndpoint);

        // TODO: Send onStop message to stop executor
        executorRpcEnv.awaitTermination();
    }
}
