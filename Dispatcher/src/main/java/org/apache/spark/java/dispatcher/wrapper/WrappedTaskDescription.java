package org.apache.spark.java.dispatcher.wrapper;

import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.scheduler.TaskDescription;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 23/4/28 7:55 PM
 */
public class WrappedTaskDescription {

    private TaskDescription taskDescription;

    private String driverURL;

    public WrappedTaskDescription(TaskDescription taskDescription, String driverURL) {
        this.taskDescription = taskDescription;
        this.driverURL = driverURL;
    }

    public TaskDescription getTaskDescription() {
        return taskDescription;
    }

    public String getDriverURL() {
        return driverURL;
    }
}
