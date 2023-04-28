package fdu.daslab.process;

import fdu.daslab.ExecutorEndpointRefInfo;
import fdu.daslab.utils.PropertyUtils;
import fdu.daslab.utils.RegisterUtils;
import org.apache.spark.rpc.netty.NettyRpcEndpointRef;
import org.apache.spark.rpc.netty.RequestMessage;
import org.apache.spark.scheduler.TaskDescription;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import org.apache.spark.util.SerializableBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import java.nio.ByteBuffer;
import java.util.HashSet;

import static fdu.daslab.constants.Constants.COMMON_PROPERTIES_PATH;
import static fdu.daslab.constants.Constants.RESCHEDULE_STRATEGY;
import static fdu.daslab.utils.RegisterUtils.getExecutorKey;

/**
 * @author Aaron Wang
 * @date 2023/4/19 3:07 PM
 * @version 1.0
 */
@AutoService(MessageRequestHandler.class)
public class LaunchTaskRequestHandler implements MessageRequestHandler {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public RequestMessage handle(RequestMessage message) {
        logger.info("Handle launch task request!");
        NettyRpcEndpointRef executorEndpointRef = message.receiver();
        RequestMessage newMessage = message;

        String oriKey = getExecutorKey(executorEndpointRef);
        ExecutorEndpointRefInfo newExecutorEndpointRef = RegisterUtils.getExecutorEndpointRef(oriKey);
        String oriExecutorEndpointRef = newExecutorEndpointRef.toString();

        boolean inClusterFlag = PropertyUtils.getValue(RESCHEDULE_STRATEGY, COMMON_PROPERTIES_PATH).equals("internal");

        newExecutorEndpointRef = RegisterUtils.getNewExecutorEndpointRef(oriKey, inClusterFlag);

        SerializableBuffer content = ((CoarseGrainedClusterMessages.LaunchTask) message.content()).data();
        ByteBuffer newByteBuffer = content.value().duplicate();
        TaskDescription decode = TaskDescription.decode(newByteBuffer);
        logger.info(". . . . . . . . . Dispatching Task{}, Executor:{}, Partition:{}, JAR size:{}, Archive size:{}",
                decode.taskId(), oriExecutorEndpointRef, decode.partitionId(), decode.addedJars().size(), decode.addedFiles().size());

        // If newExecutorEndpointRef = null, degrade dispatch tasks to nodes within the cluster.
        // It happens because there's no registered external Dispatcher when task launching. When new Dispatcher register later, task will be re-dispatched normally.
        // TODO: Synchronize re-dispatch info with Driver
        if (newExecutorEndpointRef != null) {
            newMessage = new RequestMessage(message.senderAddress(), newExecutorEndpointRef.executorEndpointRef, message.content());
            logger.info(". . . . . . . . . Redispatching Task{}, Schedule Strategy: {}, Executor:{}, Partition:{}, JAR size:{}, Archive size:{}",
                    decode.taskId(), inClusterFlag, newExecutorEndpointRef, decode.partitionId(), decode.addedJars().size(), decode.addedFiles().size());
        }
        return newMessage;
    }

    @Override
    public HashSet<Class> getMessageClass() {
        return MessageType.LAUNCH_TASK_MESSAGE_SET;
    }

}
