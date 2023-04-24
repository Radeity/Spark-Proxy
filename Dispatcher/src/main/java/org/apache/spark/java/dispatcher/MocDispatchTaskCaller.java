package org.apache.spark.java.dispatcher;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.dispatcher.TaskDispatcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;


/**
 * @author Aaron Wang
 * @version 1.0
 * @date 4/23/23 3:10 PM
 */
public class MocDispatchTaskCaller {

    TaskDispatcher taskDispatcher;

    public void startDispatchTask() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("Dispatch Task")
                .setDaemon(true)
                .build();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        scheduledExecutorService.submit(() -> {
            while(true) {
                taskDispatcher.dispatchTask(10);
                Thread.sleep(666);
            }
        });
    }

    public MocDispatchTaskCaller(TaskDispatcher taskDispatcher) {
        this.taskDispatcher = taskDispatcher;
    }

}
