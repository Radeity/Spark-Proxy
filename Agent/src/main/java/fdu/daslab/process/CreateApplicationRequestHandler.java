package fdu.daslab.process;

import com.google.auto.service.AutoService;
import fdu.daslab.registry.RedisRegistry;
import org.apache.spark.rpc.netty.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashSet;

import static fdu.daslab.constants.Constants.driverURLKey;
import static fdu.daslab.constants.Constants.driverURLPrefix;

/**
 * @author Aaron Wang
 * @date 2023/4/19 3:05 PM
 * @version 1.0
 */
@AutoService(MessageRequestHandler.class)
public class CreateApplicationRequestHandler implements MessageRequestHandler {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public RequestMessage handle(RequestMessage message) {
        // acquire driver url
        String driverURL = driverURLPrefix + message.senderAddress().toString();
        Jedis redisClient = RedisRegistry.getRedisClientInstance();
        redisClient.set(driverURLKey, driverURL);
        logger.info("Register Driver: {}", driverURL);
        return message;
    }

    @Override
    public HashSet<Class> getMessageClass() {
        return MessageType.CREATE_APPLICATION_MESSAGE_SET;
    }

}
