package fdu.daslab;

import fdu.daslab.registry.RedisRegistry;
import org.apache.spark.Receiver;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.rpc.*;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import scala.PartialFunction;
import scala.collection.immutable.Map;
import scala.reflect.ClassTag;
import scala.runtime.BoxedUnit;

import java.util.function.BiFunction;
import java.util.function.Function;

import static fdu.daslab.constants.Constants.driverAddress;
import static fdu.daslab.constants.Constants.driverURLKey;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/11 4:05 PM
 */
public class MocExecutorEndpoint implements IsolatedRpcEndpoint {

    protected static final Logger logger = LoggerFactory.getLogger(MocExecutorEndpoint.class);

    public static final String executorId = "16";

    public RpcEnv rpcEnv;

    public Receiver receiver;

    public RpcEndpointRef driver;

    public MocExecutorEndpoint(RpcEnv rpcEnv) {
        this.rpcEnv = rpcEnv;
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
                logger.info("MockWorker connecting to driver {} ...", driver);
            } catch (Throwable e) {
                if (nTries == 2) throw e;
            }
            nTries ++;
        }
        logger.info("Driver address: {}", driver.address());

        ClassTag tag = scala.reflect.ClassTag$.MODULE$.apply(Boolean.class);

        Map<String, String> emptyMap = new scala.collection.immutable.HashMap<>();
        Map<String, ResourceInformation> emptyResourceInformationMap = new scala.collection.immutable.HashMap<>();

        driver.ask(new CoarseGrainedClusterMessages.RegisterExecutor(executorId, self(), "null", 1, emptyMap, emptyMap, emptyResourceInformationMap, 0), tag);

        receiver = new Receiver(driver);
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

    @Override
    public void onStop() {
        IsolatedRpcEndpoint.super.onStop();
    }

    @Override
    public void stop() {
        IsolatedRpcEndpoint.super.stop();
    }
}
