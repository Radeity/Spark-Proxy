package fdu.daslab.process;

import com.google.auto.service.AutoService;
import fdu.daslab.utils.RegisterUtils;
import org.apache.spark.rpc.netty.RequestMessage;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

/**
 * @author Aaron Wang
 * @date 2023/4/19 8:15 PM
 * @version 1.0
 */
@Deprecated
@AutoService(MessageRequestHandler.class)
public class ExecutorRegisterRequestHandler implements MessageRequestHandler {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public RequestMessage handle(RequestMessage message) {
        boolean inClusterFlag = false;
        CoarseGrainedClusterMessages.RegisterExecutor content = (CoarseGrainedClusterMessages.RegisterExecutor) (message.content());
        if (Integer.parseInt(content.executorId()) < 3) {
            logger.info("Register in-cluster Executor!!!!!");
            inClusterFlag = true;
        }
        RegisterUtils.recordExecutor(content, inClusterFlag);
        // Do not register to Driver
        if (!inClusterFlag) {
            logger.info("Register out-of-cluster Executor!!!!!");
            return null;
        }
        return message;
    }

    @Override
    public HashSet<Class> getMessageClass() {
        return MessageType.EXECUTOR_REGISTER_MESSAGE_SET;
    }
}
