package fdu.daslab;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.rpc.netty.RequestMessage;
import org.apache.spark.scheduler.TaskDescription;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import org.apache.spark.util.SerializableBuffer;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/11/29 12:30 PM
 */
@Aspect
public class SparkClientAspect {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

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
    @AfterReturning(pointcut = "execution(* org.apache.spark.rpc.netty.NettyRpcEnv.send(..)) &&" +
            "within(org.apache.spark.rpc.netty..*) && args(message)", argNames = "message")
    public void send(RequestMessage message) {
        logger.info("!!!!!!!!!!! Send message: {}", message.toString());
        if (message.content().getClass() == CoarseGrainedClusterMessages.LaunchTask.class) {
            SerializableBuffer content = ((CoarseGrainedClusterMessages.LaunchTask) message.content()).data();
            TaskDescription decode = TaskDescription.decode(content.value());
            logger.info(". . . . . . . . . Dispatching Task{}, Executor:{}, Partition:{}, JAR size:{}, Archive size:{}",
                    decode.taskId(), decode.executorId(), decode.partitionId(), decode.addedJars().size(), decode.addedFiles().size());
        }
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

//    @AfterReturning(pointcut = "execution(* org.apache.spark.executor.CoarseGrainedExecutorBackend.onStart(..))")
//    public void onStart() {
//        logger.info("!!!!!!!!!!! Aop in onStart: {} !!!!!!!!!", 1111111111);
//    }
//

}
