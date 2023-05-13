package org.apache.spark.java.dispatcher;

import fdu.daslab.registry.RedisRegistry;
import org.apache.spark.java.dispatcher.runner.SchedulerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import static fdu.daslab.constants.Constants.executorEndpointRefKey;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/8 4:29 PM
 */
public class Dispatcher {

    protected static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            String key = String.format(executorEndpointRefKey, DispatcherConstants.DEFAULT_EXECUTOR_ID);
            Jedis redisClient = RedisRegistry.getRedisClientInstance();
            redisClient.del(key);
            logger.info("Dispatcher shutdown.");
        }));

        SchedulerServer schedulerServer = new SchedulerServer();
        schedulerServer.run();
    }
}
