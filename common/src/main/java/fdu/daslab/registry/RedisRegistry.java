package fdu.daslab.registry;

import fdu.daslab.utils.PropertyUtils;
import redis.clients.jedis.Jedis;

import static fdu.daslab.constants.Constants.REDIS_HOST;
import static fdu.daslab.constants.Constants.REDIS_PASSWORD;
import static fdu.daslab.constants.Constants.COMMON_PROPERTIES_PATH;


/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/9 2:08 PM
 */
public class RedisRegistry {

    public static Jedis jedis;

    public static synchronized Jedis getRedisClientInstance() {
        if (jedis == null) {
            jedis = new Jedis(PropertyUtils.getValue(REDIS_HOST, COMMON_PROPERTIES_PATH), 6379);
            jedis.auth(PropertyUtils.getValue(REDIS_PASSWORD, COMMON_PROPERTIES_PATH));
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
