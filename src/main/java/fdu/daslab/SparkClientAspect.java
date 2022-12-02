package fdu.daslab;

import fdu.daslab.utils.DecodeUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.rpc.RpcAddress;
import org.apache.spark.rpc.RpcCallContext;
import org.apache.spark.rpc.RpcEndpointAddress;
import org.apache.spark.rpc.netty.NettyRpcEndpointRef;
import org.apache.spark.rpc.netty.NettyRpcEnv;
import org.apache.spark.rpc.netty.RequestMessage;
import org.apache.spark.scheduler.TaskDescription;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessage;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend;
import org.apache.spark.scheduler.cluster.ExecutorData;
import org.apache.spark.util.ByteBufferInputStream;
import org.apache.spark.util.SerializableBuffer;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.ArrayBuffer;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/11/29 12:30 PM
 */
@Aspect
public class SparkClientAspect {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    static HashMap<String, ExecutorEndpointRefInfo> executorDataMap = new HashMap<>();

    List<String> executorIndex = new ArrayList<>();

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
    public Object send(ProceedingJoinPoint point) throws Throwable {
        Object[] args = point.getArgs();
        if (args != null && args.length > 0 && args[0].getClass() == RequestMessage.class) {
            logger.info("Send Message: {}", args[0]);
            RequestMessage message = (RequestMessage) args[0];
            if (message.content().getClass() == CoarseGrainedClusterMessages.LaunchTask.class) {
                ExecutorEndpointRefInfo newExecutorEndpointRef = null;
                int originExecutorId = -1;

                // TODO: Re-dispatch task to external executor, current way only re-dispatches task to the other executor in cluster.
                // maintain executorEndpointRef map
                NettyRpcEndpointRef executorEndpointRef = message.receiver();
                RpcAddress address = executorEndpointRef.address();
                String key = executorEndpointRef.client() != null ? String.valueOf(executorEndpointRef.client().getSocketAddress())
                        : String.format("spark://%s:%s", address.host(), address.port());
                logger.info("key = {}, executorDataMap.size = {}", key, executorDataMap.size());
                if (!executorDataMap.containsKey(key)) {
                    executorIndex.add(key);
                    executorDataMap.put(key, new ExecutorEndpointRefInfo(executorEndpointRef, executorIndex.size()));
                }
                newExecutorEndpointRef = executorDataMap.get(key);
                originExecutorId = newExecutorEndpointRef.execId;
                if (executorDataMap.size() > 1) {
                    // add random seed
                    r.setSeed(new Date().getTime());
                    int i = r.nextInt(executorDataMap.size() - 1);;
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
                            decode.taskId(), newExecutorEndpointRef.execId, decode.partitionId(), decode.addedJars().size(), decode.addedFiles().size());
                }
            }
        }
        return point.proceed(args);
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
