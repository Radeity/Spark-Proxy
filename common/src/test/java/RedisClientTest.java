import fdu.daslab.registry.RedisRegistry;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Random;

import static fdu.daslab.constants.Constants.driverURLKey;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/9 3:21 PM
 */
public class RedisClientTest {
    private static final String driverURL = "analysis-5:0000";

    @Test
    public void redisClientTest() {
        Jedis redisClient = RedisRegistry.getRedisClientInstance();
        redisClient.set(driverURLKey, driverURL);
        String val = redisClient.get(driverURLKey);

        Assert.assertNotNull("Redis connect failure!", val);
    }
}
