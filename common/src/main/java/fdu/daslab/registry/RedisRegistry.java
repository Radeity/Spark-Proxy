package fdu.daslab.registry;

import redis.clients.jedis.Jedis;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/9 2:08 PM
 */
public class RedisRegistry {

    public static Jedis jedis;

    public static synchronized Jedis getRedisClientInstance() {
        if (jedis == null) {
            jedis = new Jedis("139.196.231.71", 6379);
            jedis.auth("Ramsey.16");
            jedis.ping();
        }
        return jedis;
    }

    public static boolean write(String key, String value) {
        return true;
    }

    public static boolean read(String key, String value) {
        return true;
    }

}
