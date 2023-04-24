package fdu.daslab.process;

import com.google.auto.service.AutoService;
import fdu.daslab.registry.RedisRegistry;
import org.apache.spark.deploy.DeployMessages;
import org.apache.spark.rpc.netty.RequestMessage;
import redis.clients.jedis.Jedis;

import java.util.HashSet;

import static fdu.daslab.constants.Constants.workerURLKey;
import static fdu.daslab.constants.Constants.workerURLPrefix;

/**
 * @author Aaron Wang
 * @date 2023/4/19 8:23 PM
 * @version 1.0
 */
@Deprecated
@AutoService(MessageRequestHandler.class)
public class ExecutorAddedRequestHandler implements MessageRequestHandler {

    @Override
    public RequestMessage handle(RequestMessage message) {
        // acquire worker url
        DeployMessages.ExecutorAdded content = (DeployMessages.ExecutorAdded) (message.content());
        String workerURL = workerURLPrefix + content.hostPort();
        Jedis redisClient = RedisRegistry.getRedisClientInstance();
        redisClient.set(workerURLKey + content.id(), workerURL);
        return message;
    }

    @Override
    public HashSet<Class> getMessageClass() {
        return MessageType.EXECUTOR_ADD_MESSAGE_SET;
    }
}
