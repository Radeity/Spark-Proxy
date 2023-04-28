package fdu.daslab.utils;

import fdu.daslab.ExecutorEndpointRefInfo;
import fdu.daslab.registry.RedisRegistry;
import org.apache.spark.rpc.RpcAddress;
import org.apache.spark.rpc.netty.NettyRpcEndpointRef;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static fdu.daslab.constants.Constants.executorEndpointRefKey;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/2 10:11 PM
 */
public class RegisterUtils {

    protected static final Logger logger = LoggerFactory.getLogger(RegisterUtils.class);

    public static Map<String, ExecutorEndpointRefInfo> executorDataMap = new HashMap<>();

    public static List<String> externalExecutorIndex = new ArrayList<>();

    public static List<String> executorIndex = new ArrayList<>();

    public static ExecutorEndpointRefInfo getExecutorEndpointRef(String key) {
        return executorDataMap.get(key);
    }

    public static void recordExecutor(CoarseGrainedClusterMessages.RegisterExecutor content, boolean inClusterFlag) {
        String execId = content.executorId();
        logger.info("Record Executor {}, InClusterFlag: {}", execId, inClusterFlag);
        NettyRpcEndpointRef executorRef = (NettyRpcEndpointRef) content.executorRef();
        ExecutorEndpointRefInfo executorEndpointRefInfo = new ExecutorEndpointRefInfo(executorRef, execId);

        if (inClusterFlag) {
            // maintain executorEndpointRef map
            String key = getExecutorKey(executorRef);
            executorIndex.add(key);
            executorDataMap.put(key, executorEndpointRefInfo);
            logger.info("Record Executor Finished!!! Size of executorDataMap: {}", executorDataMap.size());
        } else {
            String key = String.format(executorEndpointRefKey, content.executorId());
            externalExecutorIndex.add(key);
            Jedis redisClient = RedisRegistry.getRedisClientInstance();
            try {
                byte[] value = SerializeUtils.serialize(executorEndpointRefInfo);
                redisClient.set(key.getBytes(), value);
            } catch (IOException e) {
                logger.error("Register external executor error", e);
            }
        }
    }

    public static String getExecutorKey(NettyRpcEndpointRef executorEndpointRef) {
        RpcAddress address = executorEndpointRef.address();
        return executorEndpointRef.client() != null ? String.valueOf(executorEndpointRef.client().getSocketAddress())
                : String.format("spark://%s:%s", address.host(), address.port());
    }

    public static ExecutorEndpointRefInfo getNewExecutorEndpointRef(String oriKey, boolean inClusterFlag) {
        // TODO: Add some other scheduling strategies, random selection should be just one of them
        ExecutorEndpointRefInfo executorEndpointRefInfo;

        Random r = new Random();

        r.setSeed(new Date().getTime());
        try {
            if (inClusterFlag) {
                int i = r.nextInt(executorDataMap.size());
                while (executorIndex.get(i).equals(oriKey)) {
                    i = r.nextInt(executorDataMap.size());
                }
                // randomly choose new executor
                executorEndpointRefInfo = executorDataMap.get(executorIndex.get(i));
            } else {
                Jedis redisClient = RedisRegistry.getRedisClientInstance();
                int i = r.nextInt(externalExecutorIndex.size());
                byte[] bytes = redisClient.get(externalExecutorIndex.get(i).getBytes());
                try {
                    executorEndpointRefInfo = (ExecutorEndpointRefInfo) SerializeUtils.deserialize(bytes);
                } catch (Exception e) {
                    logger.error("Can not deserialize external executor's EndPointRef, dispatch to in-cluster executor");
                    executorEndpointRefInfo = getNewExecutorEndpointRef(oriKey, true);
                }
            }
        } catch (Exception e) {
            logger.error("Get NewExecutorEndpointRef error, inClusterFlag: {}, internal nodes: {}, external nodes: {}", inClusterFlag, executorDataMap.size(), externalExecutorIndex.size());
            executorEndpointRefInfo = null;
        }

        return executorEndpointRefInfo;
    }
}
