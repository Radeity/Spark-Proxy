package org.apache.spark.java.dispatcher;

import org.apache.spark.java.dispatcher.runner.SchedulerServer;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/8 4:29 PM
 */
// TODO: maybe don't need spring framework...
public class Dispatcher {

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        SchedulerServer schedulerServer = new SchedulerServer();
        schedulerServer.run();
    }
}
