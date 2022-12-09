package fdu.daslab;

import fdu.daslab.registry.RedisRegistry;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.executor.CoarseGrainedExecutorBackend;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

import static fdu.daslab.constants.Constants.driverURLKey;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/8 4:29 PM
 */
public class Worker {
    // TODO: Read from Redis
//    private static final String driverURL = "spark://CoarseGrainedScheduler@analysis-5:39601";

    private static final String driverAddress = "10.176.24.55";

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        SparkConf executorConf = new SparkConf();
        RpcEnv fetcher = RpcEnv.create("driverPropsFetcher",
                driverAddress,
                driverAddress,
                -1,
                executorConf,
                new SecurityManager(executorConf, null, null),
                0,
                true);
        RpcEndpointRef driver = null;
        Jedis redisClient = RedisRegistry.getRedisClientInstance();
        String driverURL = redisClient.get(driverURLKey);
        int nTries = 0;
        while (driver == null && nTries < 3) {
            try {
                driver = fetcher.setupEndpointRefByURI(driverURL);
                System.out.println(driver);
            } catch (Throwable e) {
                if (nTries == 2) throw e;
            }
            nTries ++;
        }

        SparkConf driverConf = new SparkConf();
        // TODO: read executorId from Redis
        String executorId = "16";
        driverConf.set("spark.executor.id", executorId);
        System.out.println(driver.address());
        driver.send(new CoarseGrainedClusterMessages.LaunchedExecutor("16"));
        System.out.println("Message Send Successfully!");
        fetcher.shutdown();
    }
}
