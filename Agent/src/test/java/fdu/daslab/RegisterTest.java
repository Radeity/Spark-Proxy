package fdu.daslab;

import fdu.daslab.registry.RedisRegistry;
import fdu.daslab.utils.SerializeUtils;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/13 12:34 AM
 */
public class RegisterTest {

    @Test
    public void testRandom() {
        Random r = new Random();
        HashMap<String, String> map = new HashMap<>();
        map.put("111","222");
        int n = 5;
        while (n > 0) {
            System.out.println(r.nextInt(map.size()));
            n--;
        }
    }

    @Ignore
    @Test
    public void testReadRedis() throws IOException, ClassNotFoundException {
        Jedis redisClientInstance = RedisRegistry.getRedisClientInstance();
        ExecutorEndpointRefInfo executorEndpointRefInfo = new ExecutorEndpointRefInfo(null, "10");
        redisClientInstance.set("test".getBytes(), SerializeUtils.serialize(executorEndpointRefInfo));
        byte[] bytes = redisClientInstance.get("test".getBytes());
//        byte[] bytes = redisClientInstance.get("executor-16".getBytes());
        ExecutorEndpointRefInfo deserialize = (ExecutorEndpointRefInfo) SerializeUtils.deserialize(bytes);
        System.out.println(deserialize.execId);
    }

}
