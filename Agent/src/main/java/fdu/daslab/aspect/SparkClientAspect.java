package fdu.daslab.aspect;

import fdu.daslab.SparkMessageHandler;
import fdu.daslab.process.MessageRequestHandler;
import fdu.daslab.registry.RedisRegistry;
import fdu.daslab.utils.RegisterUtils;
import org.apache.spark.deploy.DeployMessages;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.rpc.netty.OneWayMessage;
import org.apache.spark.rpc.netty.RequestMessage;
import org.apache.spark.rpc.netty.RpcMessage;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.net.InetSocketAddress;

import static fdu.daslab.constants.Constants.workerURLKey;
import static fdu.daslab.constants.Constants.workerURLPrefix;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/11/29 12:30 PM
 */
@Aspect
public class SparkClientAspect {

    private boolean fetchDriverURLFlag = false;

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // work
    @Around("execution(* org.apache.spark.rpc.netty.NettyRpcEnv.send(..)) && " +
            "within(org.apache.spark.rpc.netty..*) && !within(SparkClientAspect)")
    public Object sendMessage(ProceedingJoinPoint point) throws Throwable {
        Object[] args = point.getArgs();
        logger.info("########## Send Message: {}", args[0]);
        // TODO: Use design pattern to tidy up the following code
        if (args != null && args.length > 0 && args[0].getClass() == RequestMessage.class) {
            RequestMessage message = (RequestMessage) args[0];

            logger.info("Find {} in handlerMap", message.content().getClass().toString());
            MessageRequestHandler messageHandler = SparkMessageHandler.handlerMap.getOrDefault(message.content().getClass(), null);
            if (messageHandler != null) {
                args[0] = messageHandler.handle(message);
            }
        }
        return point.proceed(args);
    }

    // TODO: Replace with two methods above
    // deprecated
    @Around("execution(* org.apache.spark.rpc.netty.Inbox.post(..)) && within(org.apache.spark.rpc.netty..*) && !within(SparkClientAspect)")
    public Object receiveMessage(ProceedingJoinPoint point) throws Throwable {
        Object[] args = point.getArgs();
        if (args != null && args.length > 0) {
//            logger.info("Find {} in handlerMap", message.content().getClass().toString());
//            if (args[0].getClass() == OneWayMessage.class) {
//                OneWayMessage message = (OneWayMessage) (args[0]);
//                MessageRequestHandler messageHandler = SparkMessageHandler.handlerMap.getOrDefault(message.content().getClass(), null);
//            } else {
//                RpcMessage message = (RpcMessage) (args[0]);
//                MessageRequestHandler messageHandler = SparkMessageHandler.handlerMap.getOrDefault(message.content().getClass(), null);
//            }
//            if (messageHandler != null) {
//                args[0] = messageHandler.handle(message);
//            }
//            if (args[0] == null) return null;

            if (args[0].getClass() == OneWayMessage.class) {
                logger.info("^^^^^^^ Receive One Way Message: {}", args[0]);
                OneWayMessage message = (OneWayMessage) (args[0]);
                // acquire worker url
                if (message.content().getClass() == DeployMessages.ExecutorAdded.class) {
                    DeployMessages.ExecutorAdded content = (DeployMessages.ExecutorAdded) (message.content());
                    String workerURL = workerURLPrefix + content.hostPort();
                    Jedis redisClient = RedisRegistry.getRedisClientInstance();
                    redisClient.set(workerURLKey + content.id(), workerURL);
                }
            } else if (args[0].getClass() == RpcMessage.class) {
                logger.info("^^^^^^^ Receive RPC Message: {}", args[0]);
                RpcMessage message = (RpcMessage) (args[0]);
                boolean inClusterFlag = false;
                if (message.content().getClass() == CoarseGrainedClusterMessages.RegisterExecutor.class) {
                    CoarseGrainedClusterMessages.RegisterExecutor content = (CoarseGrainedClusterMessages.RegisterExecutor) (message.content());
                    if (Integer.parseInt(content.executorId()) < 3) {
                        inClusterFlag = true;
                    }
                    RegisterUtils.recordExecutor(content, inClusterFlag);
                    // Do not register to Driver
                    if (!inClusterFlag) return null;
                }
            }
        }

        return point.proceed(args);
    }

    // Work
    @AfterReturning(pointcut = "execution(* org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend.executorAdded(..)) &&" +
            "within(org.apache.spark.scheduler.cluster..*)")
    public void standaloneRegisterExecutor(JoinPoint point) {
        Object[] args = point.getArgs();
        if (args[1].getClass() != String.class || args[2].getClass() != String.class) {
            return;
        }
        String workerId = (String) args[1];
        String hostPort = (String) args[2];
        logger.info("?????????? Register Executor {}, hostport: {} !!!!!!!!!", workerId, hostPort);
    }

    // Work
    @AfterReturning(pointcut = "execution(org.apache.spark.network.client.TransportClient org.apache.spark.network.client.TransportClientFactory.createClient(java.net.InetSocketAddress)) &&" +
            "within(org.apache.spark.network.client..*) && args(address)",
            returning = "client", argNames = "address,client")
    public void createClient(InetSocketAddress address, TransportClient client) {
        logger.info("!!!!!!!!!!! Successfully created connection to {}", address);
        logger.info("!!!!!!!!!!! Aop in createClient !!!!!!!!!");
    }

}
