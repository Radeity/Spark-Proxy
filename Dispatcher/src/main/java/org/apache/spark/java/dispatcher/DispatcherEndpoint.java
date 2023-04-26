package org.apache.spark.java.dispatcher;

import org.apache.spark.java.dispatcher.scheduler.SchedulingStrategy;
import fdu.daslab.registry.RedisRegistry;
import org.apache.spark.dispatcher.ReplyReceiver;
import org.apache.spark.dispatcher.TaskPool;
import org.apache.spark.dispatcher.Receiver;
import org.apache.spark.SparkConf;
import org.apache.spark.dispatcher.TaskDispatcher;
import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.rpc.IsolatedRpcEndpoint;
import org.apache.spark.rpc.RpcAddress;
import org.apache.spark.rpc.RpcCallContext;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterExecutor;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkAppConfig;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.SparkAppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import scala.PartialFunction;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.reflect.ClassTag$;
import scala.runtime.BoxedUnit;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.HashMap;

import static fdu.daslab.constants.Constants.driverURLKey;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/11 4:05 PM
 */
public class DispatcherEndpoint implements IsolatedRpcEndpoint {

    protected static final Logger logger = LoggerFactory.getLogger(DispatcherEndpoint.class);

    public RpcEnv rpcEnv;

    public SparkConf conf;

    public boolean syncConfDone = false;

    public Receiver receiver;

    public ReplyReceiver replyReceiver;

    public TaskDispatcher taskDispatcher;

    public RpcEndpointRef driver;

    public String driverURL = null;

    public SparkAppConfig cfg;

    public HashMap<String, RpcEndpointRef> executorMap;

    public DispatcherEndpoint(RpcEnv rpcEnv, SparkConf conf) {
        this.rpcEnv = rpcEnv;
        this.conf = conf;
        this.executorMap = new HashMap<>();
    }

    @Override
    public RpcEnv rpcEnv() {
        return this.rpcEnv;
    }

    @Override
    public void onStart() {
        logger.info("Starting Dispatcher server ...");
        Jedis redisClient = RedisRegistry.getRedisClientInstance();
        redisClient.del(driverURLKey);
        while (driverURL == null) {
            driverURL = redisClient.get(driverURLKey);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        int nTries = 0;
        while (driver == null && nTries < 3) {
            try {
                logger.info("Dispatcher is trying to connect to driver ...");
                driver = rpcEnv().setupEndpointRefByURI(driverURL);
            } catch (Throwable e) {
                if (nTries == 2) {
                    logger.error("Connect to Driver failed, have tried three times!");
                    throw e;
                }
            }
            nTries++;
        }
        logger.info("Successfully connect to Driver {}", driver.address());

        logger.info("Dispatcher retrieve Spark app Config ...");
        // TODO: maintain different spark config for different Spark application, set different config and create different SparkEnv in Worker
        cfg = (SparkAppConfig) driver.askSync(new RetrieveSparkAppConfig(0), ClassTag$.MODULE$.apply(SparkAppConfig.class));

        // can receive sync conf request from external Worker now.
        syncConfDone = true;

        Seq<Tuple2<String, String>> props = cfg.sparkProperties();
        props.foreach(prop -> {
            logger.info("Set executor conf : {} = {}", prop._1, prop._2);
            if (SparkConf.isExecutorStartupConf(prop._1)) {
                conf.setIfMissing(prop._1, prop._2);
            } else {
                conf.set(prop._1, prop._2);
            }
            return prop;
        });
        conf.set(DispatcherConstants.EXECUTOR, DispatcherConstants.DEFAULT_EXECUTOR_ID);

        Map<String, String> emptyMap = new scala.collection.immutable.HashMap<>();
        Map<String, ResourceInformation> emptyResourceInformationMap = new scala.collection.immutable.HashMap<>();

        // TODO: Executor directly register to Dispatcher is better
        driver.ask(new RegisterExecutor(DispatcherConstants.DEFAULT_EXECUTOR_ID, self(), DispatcherConstants.bindAddress, 1, emptyMap, emptyMap, emptyResourceInformationMap, 0), ClassTag$.MODULE$.apply(Boolean.class));

        TaskPool taskPool = new TaskPool(SchedulingStrategy.FIFO);

        taskDispatcher = new TaskDispatcher(this, taskPool);

        receiver = new Receiver(this);
        replyReceiver = new ReplyReceiver(this);

        MocDispatchTaskCaller mocDispatchTaskCaller = new MocDispatchTaskCaller(taskDispatcher);
        mocDispatchTaskCaller.startDispatchTask();
    }

    @Override
    public int threadCount() {
        return IsolatedRpcEndpoint.super.threadCount();
    }

    @Override
    public RpcEndpointRef self() {
        return IsolatedRpcEndpoint.super.self();
    }

    public static <T, U, R> Function<U, R> partial(BiFunction<T, U, R> f, T x) {
        return (y) -> f.apply(x, y);
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receive() {
        return receiver.receive();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receiveAndReply(RpcCallContext context) {
        return replyReceiver.receiveAndReply(context);
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

    // TODO: handle TransportResponseHandler ERROR: Still have 1 request outstanding when connection from analysis-5/10.176.24.55:33885 is closed
    @Override
    public void onStop() {
        IsolatedRpcEndpoint.super.onStop();
    }

    @Override
    public void stop() {
        IsolatedRpcEndpoint.super.stop();
    }
}
