package fdu.daslab;

import fdu.daslab.registry.RedisRegistry;
import fdu.daslab.utils.RegisterUtils;
import org.apache.spark.deploy.ApplicationDescription;
import org.apache.spark.deploy.Command;
import org.apache.spark.deploy.DeployMessages;
import org.apache.spark.deploy.worker.ExecutorRunner;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.rpc.RpcAddress;
import org.apache.spark.rpc.netty.*;
import org.apache.spark.scheduler.TaskDescription;
import org.apache.spark.scheduler.cluster.*;
import org.apache.spark.util.SerializableBuffer;
import org.apache.spark.util.Utils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static fdu.daslab.constants.Constants.*;
import static fdu.daslab.utils.RegisterUtils.executorDataMap;
import static fdu.daslab.utils.RegisterUtils.executorIndex;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/11/29 12:30 PM
 */
@Aspect
public class SparkClientAspect {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private boolean fetchDriverURLFlag = false;

    Random r = new Random();

//    @AfterReturning(pointcut = "execution(* org.apache.spark.util.Utils.startServiceOnPort(..)) &&" +
//            "within(org.apache.spark.util..*)", returning = "startedService")
//    public <T> void startService(JoinPoint point, Object startedService) {
//        Object[] args = point.getArgs();
//        if (args[1].getClass() != String.class) {
//            return;
//        }
//        String serviceName = (String) args[3];
//        System.out.println("%%%%%%%%%% Type: " + startedService.getClass().toString());
//        Tuple2<T, Integer> service = (Tuple2<T, Integer>) startedService;
//        logger.info("!!!!!!!!!!! Start Service {}, port: {} !!!!!!!!!", serviceName, service._2);
//    }

//    @AfterReturning(pointcut = "cflow(execution(* org.apache.spark.rpc.netty.NettyRpcEnvFactory.create(..))) " +
//            "&& (within(org.apache.spark.rpc.netty..*) || within(org.apache.spark.util..*)) && !within(SparkClientAspect) && execution(* org.apache.spark.util.Utils.startServiceOnPort(..)) " +
//            "&& args(*,*,*,serviceName)",
//            returning = "retValue", argNames = "serviceName,retValue")
//    public <T> void startService(String serviceName, Object retValue) {
//        System.out.println("%%%%%%%%%% Type: " + serviceName);
//        Tuple2<T, Integer> service = (Tuple2<T, Integer>) retValue;
//        logger.info("!!!!!!!!!!! Start Service {}, port: {} !!!!!!!!!", serviceName, service._2);
//    }
//
//    @AfterReturning(pointcut = "cflow(execution(* org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend.executorAdded(..))) " +
//            "&& within(org.apache.spark..*) && !within(SparkClientAspect) && execution(* org.apache.spark.internal.Logging$.logInfo(..)) " +
//            "&& args(msg)", argNames = "msg")
//    public <T> void log(String msg) {
//        logger.info("%%%%%%%%%%%%%%%% {}", msg);
//    }

    // work
//    @Around("cflow(execution(* org.apache.spark.scheduler.cluster.launchTasks(..) && !within(SparkClientAspect) " +
//            "&& within(within(org.apache.spark..*)) && execution(* org.apache.spark.rpc.netty.NettyRpcEndpointRef.send(..)))")
    @Around("execution(* org.apache.spark.rpc.netty.NettyRpcEnv.send(..)) && " +
            "within(org.apache.spark.rpc.netty..*) && !within(SparkClientAspect)")
    public Object sendMessage(ProceedingJoinPoint point) throws Throwable {
        Object[] args = point.getArgs();
//        logger.info("############ Sending Request");
        logger.info("########## Send Message: {}", args[0]);
        // TODO: Use design pattern to tidy up the following code
        if (args != null && args.length > 0 && args[0].getClass() == RequestMessage.class) {
            RequestMessage message = (RequestMessage) args[0];
            // acquire driver url
            if (!fetchDriverURLFlag && message.content().getClass() == DeployMessages.RegisterApplication.class) {
                String driverURL = driverURLPrefix + message.senderAddress().toString();
                Jedis redisClient = RedisRegistry.getRedisClientInstance();
                redisClient.set(driverURLKey, driverURL);
                // TODO: useless
                fetchDriverURLFlag = true;
            }
            else if (message.content().getClass() == CoarseGrainedClusterMessages.LaunchTask.class) {
                NettyRpcEndpointRef executorEndpointRef = message.receiver();
                RpcAddress address = executorEndpointRef.address();
                String key = executorEndpointRef.client() != null ? String.valueOf(executorEndpointRef.client().getSocketAddress())
                        : String.format("spark://%s:%s", address.host(), address.port());
                logger.info("key = {}, executorDataMap.size = {}", key, executorDataMap.size());

                // maintain executorEndpointRef map
                ExecutorEndpointRefInfo newExecutorEndpointRef = RegisterUtils.getExecutorEndpointRef(key, executorEndpointRef);

                String originExecutorId = newExecutorEndpointRef.toString();
                if (executorDataMap.size() > 1) {
                    // add random seed
                    r.setSeed(new Date().getTime());
                    int i = r.nextInt(executorDataMap.size() - 1);
                    while (executorIndex.get(i).equals(key)) {
                        i = r.nextInt(executorDataMap.size() - 1);
                    }
                    // randomly choose new executor
                    newExecutorEndpointRef = executorDataMap.get(executorIndex.get(i));
                }

                SerializableBuffer content = ((CoarseGrainedClusterMessages.LaunchTask) message.content()).data();
                ByteBuffer newByteBuffer = content.value().duplicate();
//                TaskDescription decode = DecodeUtils.decode(newByteBuffer);
                TaskDescription decode = TaskDescription.decode(newByteBuffer);
                logger.info(". . . . . . . . . Dispatching Task{}, Executor:{}, Partition:{}, JAR size:{}, Archive size:{}",
                        decode.taskId(), originExecutorId, decode.partitionId(), decode.addedJars().size(), decode.addedFiles().size());

                // TODO: Synchronize re-dispatch info with Driver
                if (newExecutorEndpointRef != null) {
                    RequestMessage newMessage = new RequestMessage(message.senderAddress(), newExecutorEndpointRef.executorEndpointRef, message.content());
                    args[0] = newMessage;
                    logger.info(". . . . . . . . . Redispatching Task{}, Executor:{}, Partition:{}, JAR size:{}, Archive size:{}",
                            decode.taskId(), newExecutorEndpointRef, decode.partitionId(), decode.addedJars().size(), decode.addedFiles().size());
                }
            }
        }
        return point.proceed(args);
    }

    @Around("execution(* org.apache.spark.rpc.netty.Dispatcher.postOneWayMessage(..)) && " +
            "within(org.apache.spark.rpc.netty..*) && !within(SparkClientAspect)")
    public Object nettyReceiveMessage(ProceedingJoinPoint point) throws Throwable {
        Object[] args = point.getArgs();
        if (args != null && args.length > 0) {
            logger.info("^^^^^^^ Receive Message: {}", args[0]);
        }
        return point.proceed(args);
    }

    // deprecated
    @Around("execution(* org.apache.spark.rpc.netty.Inbox.post(..)) && within(org.apache.spark.rpc.netty..*) && !within(SparkClientAspect)")
    public Object receiveMessage(ProceedingJoinPoint point) throws Throwable {
        Object[] args = point.getArgs();
        if (args != null && args.length > 0) {
             if(args[0].getClass() == OneWayMessage.class) {
                 logger.info("^^^^^^^ Receive One Way Message: {}", args[0]);
                 OneWayMessage message = (OneWayMessage)(args[0]);
                 // acquire worker url
                 if (message.content().getClass() == DeployMessages.ExecutorAdded.class) {
                     DeployMessages.ExecutorAdded content = (DeployMessages.ExecutorAdded) (message.content());
                     String workerURL = workerURLPrefix + content.hostPort();
                     Jedis redisClient = RedisRegistry.getRedisClientInstance();
                     redisClient.set(workerURLKey + content.id(), workerURL);
                 }
//                 else if (message.content().getClass() == DeployMessages.LaunchExecutor.class) {
//                     DeployMessages.LaunchExecutor launchExecutorMsg = (DeployMessages.LaunchExecutor) message.content();
//                     Inbox target = (Inbox)point.getTarget();
//                     Worker worker = (Worker) (target.endpoint());
//                 }
             } else if (args[0].getClass() == RpcMessage.class) {
                 logger.info("^^^^^^^ Receive RPC Message: {}", args[0]);
             }
        }
        return point.proceed(args);
    }

    @Around("execution(* org.apache.spark.deploy.worker.ExecutorRunner.start(..)) && within(org.apache.spark.deploy..*) && !within(SparkClientAspect)")
    public Object runExecutor (ProceedingJoinPoint point) throws Throwable {
        ExecutorRunner target = (ExecutorRunner) (point.getTarget());
        logger.info("(((((((((((((((((((((((((((((((((((((   Run Executor   ))))))))))))))))))))))))))))))))");
        ApplicationDescription appDesc = target.appDesc();
        Seq<String> arguments = appDesc.command().arguments();
        List<String> optList = (List<String>) appDesc.command().javaOpts().seq();
        optList = optList.stream().map(opt -> Utils.substituteAppNExecIds(opt, target.appId(), Integer.toString(target.execId())))
                .collect(Collectors.toList());
        Seq<String> subsOpts = JavaConverters.asScalaIteratorConverter(optList.iterator()).asScala().toSeq();
        Command command = appDesc.command();
        Command subsCommand = command.copy(command.mainClass(), arguments, command.environment(), command.classPathEntries(), command.libraryPathEntries(), subsOpts);
//        CommandUtils.buildProcessBuilder(subsCommand, new SparkConf(), target.memory(), target.sparkHome().getAbsolutePath());
        // It's toooooooooooooooo hard ...
        return point.proceed(point.getArgs());
    }


//    @Before("execution(* org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend.DriverEndpoint.receiveAndReply(..)) &&" +
//            "within(org.apache.spark.scheduler.cluster..*)")
//    public void registerExecutor(JoinPoint point) {
//        RpcCallContext context = (RpcCallContext) point.getArgs()[0];
//        switch ()
//        List<TaskDescription> tasks = arg.stream().flatMap(Collection::stream).collect(Collectors.toList());
//        Object target = point.getTarget();
//        System.out.println(target.getClass().toString());
//        if (target.getClass() == CoarseGrainedSchedulerBackend.class) {
//            (CoarseGrainedSchedulerBackend)
//        }
//    }

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

//    @AfterReturning(pointcut = "execution(* org.apache.spark.executor.CoarseGrainedExecutorBackend.onStart(..))")
//    public void onStart() {
//        logger.info("!!!!!!!!!!! Aop in onStart: {} !!!!!!!!!", 1111111111);
//    }
//


}
