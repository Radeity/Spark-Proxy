package org.apache.spark.java.dispatcher.wrapper;

import org.apache.spark.util.SerializableBuffer;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 23/4/28 7:55 PM
 */
public class WrappedTaskDescription {

    private SerializableBuffer taskDescription;

    private String driverURL;

    public WrappedTaskDescription(SerializableBuffer taskDescription, String driverURL) {
        this.taskDescription = taskDescription;
        this.driverURL = driverURL;
    }

    public SerializableBuffer getTaskDescription() {
        return taskDescription;
    }

    public String getDriverURL() {
        return driverURL;
    }
}
