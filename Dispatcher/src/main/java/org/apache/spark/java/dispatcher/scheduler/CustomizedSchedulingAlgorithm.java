package org.apache.spark.java.dispatcher.scheduler;

import org.apache.spark.dispatcher.WrappedTask;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 4/23/23 1:21 PM
 */
// TODO: novel strategy
public class CustomizedSchedulingAlgorithm implements SchedulingAlgorithm {

    @Override
    public int compare(WrappedTask t1, WrappedTask t2) {
        return -1;
    }

}
