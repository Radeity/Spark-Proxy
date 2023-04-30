package org.apache.spark.java.dispatcher.runner;

import org.apache.spark.SparkConf;
import org.apache.spark.java.dispatcher.DispatcherEndpoint;

import java.io.Closeable;

/**
 * @author Aaron Wang
 * @date 2023/4/20 4:40 PM
 * @version 1.0
 */
public class SchedulerServer implements Closeable {

    public void run() {
        SparkConf dispatcherConf = new SparkConf().setMaster("local").setAppName("Dispatcher");

        DispatcherEndpoint dispatcherEndpoint = new DispatcherEndpoint(dispatcherConf);
    }

    @Override
    public void close() {

    }
}
