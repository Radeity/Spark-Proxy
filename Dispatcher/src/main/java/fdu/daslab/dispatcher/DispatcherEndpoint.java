package fdu.daslab.dispatcher;

import fdu.daslab.dispatcher.scheduler.SchedulingStrategy;
import fdu.daslab.registry.RedisRegistry;
import org.apache.spark.TaskPool;
import org.apache.spark.Receiver;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskDispatcher;
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
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;
import scala.reflect.ClassTag$;
import scala.runtime.BoxedUnit;

import java.util.function.BiFunction;
import java.util.function.Function;

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

    public Receiver receiver;

    public TaskDispatcher taskDispatcher;

    public RpcEndpointRef driver;

    public DispatcherEndpoint(RpcEnv rpcEnv, SparkConf conf) {
        this.rpcEnv = rpcEnv;
        this.conf = conf;
    }

    @Override
    public RpcEnv rpcEnv() {
        return this.rpcEnv;
    }

    @Override
    public void onStart() {
        Jedis redisClient = RedisRegistry.getRedisClientInstance();
        redisClient.del(driverURLKey);
        String driverURL = null;
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
                driver = rpcEnv().setupEndpointRefByURI(driverURL);
                logger.info("Dispatcher connect to driver {} ...", driver);
            } catch (Throwable e) {
                if (nTries == 2) throw e;
            }
            nTries++;
        }

        logger.info("Dispatcher retrieve Spark app Config ...");
        SparkAppConfig cfg = driver.askSync(new RetrieveSparkAppConfig(0), ClassTag$.MODULE$.apply(SparkAppConfig.class));
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

        logger.info("Driver address: {}", driver.address());

        Map<String, String> emptyMap = new HashMap<>();
        Map<String, ResourceInformation> emptyResourceInformationMap = new HashMap<>();

        driver.ask(new RegisterExecutor(DispatcherConstants.DEFAULT_EXECUTOR_ID, self(), DispatcherConstants.bindAddress, 1, emptyMap, emptyMap, emptyResourceInformationMap, 0), ClassTag$.MODULE$.apply(Boolean.class));

        TaskPool taskPool = new TaskPool(SchedulingStrategy.FIFO);

        taskDispatcher = new TaskDispatcher(driver, conf, cfg, taskPool);

        receiver = new Receiver(taskDispatcher);

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