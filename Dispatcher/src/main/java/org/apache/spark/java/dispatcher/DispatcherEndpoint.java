package org.apache.spark.java.dispatcher;

import fdu.daslab.ExecutorEndpointRefInfo;
import fdu.daslab.registry.RedisRegistry;
import fdu.daslab.utils.PropertyUtils;
import fdu.daslab.utils.SerializeUtils;
import org.apache.spark.ExternalApplicationContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SecurityManager;
import org.apache.spark.dispatcher.Receiver;
import org.apache.spark.dispatcher.ReplyReceiver;
import org.apache.spark.dispatcher.TaskDispatcher;
import org.apache.spark.dispatcher.TaskPool;
import org.apache.spark.java.dispatcher.scheduler.SchedulingStrategy;
import org.apache.spark.rpc.IsolatedRpcEndpoint;
import org.apache.spark.rpc.RpcAddress;
import org.apache.spark.rpc.RpcCallContext;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.rpc.netty.NettyRpcEndpointRef;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkAppConfig;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.SparkAppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import scala.PartialFunction;
import scala.Tuple2;
import scala.collection.immutable.List;
import scala.reflect.ClassTag$;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static fdu.daslab.constants.Constants.executorEndpointRefKey;
import static fdu.daslab.constants.Constants.executorSystemName;
import static fdu.daslab.constants.Constants.DISPATCHER_PORT;
import static fdu.daslab.constants.Constants.COMMON_PROPERTIES_PATH;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/11 4:05 PM
 */
public class DispatcherEndpoint implements IsolatedRpcEndpoint {

    protected static final Logger logger = LoggerFactory.getLogger(DispatcherEndpoint.class);

    public RpcEnv rpcEnv;

    public SparkConf conf;

    public Receiver receiver;

    public ReplyReceiver replyReceiver;

    public TaskDispatcher taskDispatcher;

    public HashMap<String, RpcEndpointRef> executorMap;

    public HashMap<String, String> workerApplicationMap;

    public int dispatcherPort;

    // TODO: Memory leak risk, should be fixed in the future, add remove operation
    // key = `driverURL`
    public HashMap<String, ExternalApplicationContext> applicationContextMap;

    public DispatcherEndpoint(SparkConf conf) {
        this.conf = conf;
        this.executorMap = new HashMap<>();
        this.workerApplicationMap = new HashMap<>();
        this.applicationContextMap = new HashMap<>();
        this.dispatcherPort = Integer.parseInt(PropertyUtils.getValue(DISPATCHER_PORT, COMMON_PROPERTIES_PATH));
        checkPortAvailable();
        run();
    }

    /**
     * If port is in used(unavailable), process directly exit with error code 16.
     */
    public void checkPortAvailable() {
        try (Socket socket = new Socket(InetAddress.getLocalHost(), dispatcherPort)) {
            logger.error("Port {} is not available", dispatcherPort);
            // use code 16 to represent port is not available error
            System.exit(16);
        } catch (Exception e) {
            logger.info("Port {} for Dispatcher is available", dispatcherPort);
        }
    }

    public void run() {
        TaskPool taskPool = new TaskPool(SchedulingStrategy.FIFO);

        taskDispatcher = new TaskDispatcher(this, taskPool, new scala.collection.mutable.HashMap<>());

        rpcEnv = RpcEnv.create(executorSystemName,
                DispatcherConstants.bindAddress,
                DispatcherConstants.bindAddress,
                this.dispatcherPort,
                this.conf,
                new SecurityManager(conf, null, null),
                0,
                false);

        receiver = new Receiver(this);
        replyReceiver = new ReplyReceiver(this);

        System.out.println(rpcEnv.address());

        rpcEnv.setupEndpoint("Dispatcher", this);

        // TODO: Send onStop message to stop executor
        rpcEnv.awaitTermination();
    }

    /**
     * Request and save ExternalApplicationContext for each Application(Driver)
     *
     * @param driverURL
     */
    public void receiveNewApplication(String driverURL) {
        RpcEndpointRef driver = null;
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

        SparkAppConfig cfg = (SparkAppConfig) driver.askSync(new RetrieveSparkAppConfig(0), ClassTag$.MODULE$.apply(SparkAppConfig.class));

        SparkConf conf = new SparkConf();

        List<Tuple2<String, String>> props = cfg.sparkProperties().toList();

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

        ExternalApplicationContext externalApplicationContext = new ExternalApplicationContext(conf, driver, cfg);
        applicationContextMap.put(driverURL, externalApplicationContext);
    }

    @Override
    public RpcEnv rpcEnv() {
        return this.rpcEnv;
    }

    @Override
    public void onStart() {
        logger.info("Starting Dispatcher server ...");

        Jedis redisClient = RedisRegistry.getRedisClientInstance();

        // Register Dispatcher
        try {
            String key = String.format(executorEndpointRefKey, DispatcherConstants.DEFAULT_EXECUTOR_ID);
            RpcEndpointRef self = self();
            ExecutorEndpointRefInfo executorEndpointRefInfo = new ExecutorEndpointRefInfo((NettyRpcEndpointRef) self, DispatcherConstants.DEFAULT_EXECUTOR_ID);
            byte[] value = SerializeUtils.serialize(executorEndpointRefInfo);
            redisClient.set(key.getBytes(), value);
        } catch (IOException e) {
            logger.error("Register external Dispatcher error", e);
        }

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
