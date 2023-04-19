package fdu.daslab.process;

import org.apache.spark.deploy.DeployMessages;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2023/4/19 3:50 PM
 */
public class MessageType {

    /**
     * Create Application
     */
    public static final HashSet<Class> CREATE_APPLICATION_MESSAGE_SET = new HashSet<>(Arrays.asList(
            CoarseGrainedClusterMessages.RegisterClusterManager.class,
            DeployMessages.RegisterApplication.class)
    );

    /**
     * Launch Task
     */
    public static final HashSet<Class> LAUNCH_TASK_MESSAGE_SET = new HashSet<>(Arrays.asList(CoarseGrainedClusterMessages.LaunchTask.class));

    /**
     * Executor Add
     */
    public static final HashSet<Class> EXECUTOR_ADD_MESSAGE_SET = new HashSet<>(Arrays.asList(DeployMessages.ExecutorAdded.class));

    /**
     * Executor Register
     */
    public static final HashSet<Class> EXECUTOR_REGISTER_MESSAGE_SET = new HashSet<>(Arrays.asList(CoarseGrainedClusterMessages.RegisterExecutor.class));


}
