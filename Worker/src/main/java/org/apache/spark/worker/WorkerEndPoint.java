package org.apache.spark.worker;

import fdu.daslab.utils.PropertyUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.Receiver;
import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.rpc.IsolatedRpcEndpoint;
import org.apache.spark.rpc.RpcAddress;
import org.apache.spark.rpc.RpcCallContext;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.PartialFunction;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;
import scala.reflect.ClassTag$;
import scala.runtime.BoxedUnit;

import static fdu.daslab.constants.Constants.dispatcherURLPrefix;
import static fdu.daslab.constants.Constants.DISPATCHER_PORT;
import static fdu.daslab.constants.Constants.COMMON_PROPERTIES_PATH;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2023/4/24 11:12 AM
 */
public class WorkerEndPoint implements IsolatedRpcEndpoint {

    protected static final Logger logger = LoggerFactory.getLogger(WorkerEndPoint.class);

    public RpcEnv rpcEnv;

    public SparkConf conf;

    public String dispatcherURL;

    public RpcEndpointRef dispatcher;

    public RpcEndpointRef driver;

    public Receiver receiver;

    public java.util.HashSet<String> runningApplication;

    public String executorId;

    public WorkerEndPoint(RpcEnv rpcEnv, SparkConf conf, String dispatcherHost, String executorId) {
        this.rpcEnv = rpcEnv;
        this.conf = conf;
        this.runningApplication = new java.util.HashSet<>();
        this.executorId = executorId;
        this.dispatcherURL = String.format("%s%s:%s",
                dispatcherURLPrefix, dispatcherHost, PropertyUtils.getValue(DISPATCHER_PORT, COMMON_PROPERTIES_PATH));
    }

    @Override
    public RpcEnv rpcEnv() {
        return this.rpcEnv;
    }

    @Override
    public void onStart() {
        logger.info("Starting Worker server ...");

        int nTries = 0;
        while (dispatcher == null && nTries < 3) {
            try {
                dispatcher = rpcEnv().setupEndpointRefByURI(dispatcherURL);
            } catch (Throwable e) {
                if (nTries == 2) {
                    logger.error("Connect to Dispatcher failed, have tried three times!");
                    throw e;
                }
            }
            nTries++;
        }

        Map<String, String> emptyMap = new HashMap<>();
        Map<String, ResourceInformation> emptyResourceInformationMap = new HashMap<>();
        dispatcher.ask(new RegisterExecutor(executorId, self(), WorkerConstants.bindAddress, 1, emptyMap, emptyMap, emptyResourceInformationMap, 0), ClassTag$.MODULE$.apply(Boolean.class));

        logger.info("Successfully connect to Dispatcher {}", dispatcher.address());

        receiver = new Receiver(this);

    }

    @Override
    public int threadCount() {
        return IsolatedRpcEndpoint.super.threadCount();
    }

    @Override
    public RpcEndpointRef self() {
        return IsolatedRpcEndpoint.super.self();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receive() {
        return receiver.receive();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receiveAndReply(RpcCallContext context) {
        return IsolatedRpcEndpoint.super.receiveAndReply(context);
    }

    @Override
    public void onError(Throwable cause) {
        IsolatedRpcEndpoint.super.onError(cause);
    }

    @Override
    public void onConnected(RpcAddress remoteAddress) {
        IsolatedRpcEndpoint.super.onConnected(remoteAddress);
    }

    @Override
    public void onDisconnected(RpcAddress remoteAddress) {
        IsolatedRpcEndpoint.super.onDisconnected(remoteAddress);
    }

    @Override
    public void onNetworkError(Throwable cause, RpcAddress remoteAddress) {
        IsolatedRpcEndpoint.super.onNetworkError(cause, remoteAddress);
    }

    @Override
    public void onStop() {
        IsolatedRpcEndpoint.super.onStop();
    }

    @Override
    public void stop() {
        IsolatedRpcEndpoint.super.stop();
    }
}
