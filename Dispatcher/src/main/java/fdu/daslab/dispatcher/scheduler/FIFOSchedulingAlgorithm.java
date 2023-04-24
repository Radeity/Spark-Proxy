package fdu.daslab.dispatcher.scheduler;

import org.apache.spark.WrappedTask;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 4/23/23 1:45 PM
 */
public class FIFOSchedulingAlgorithm implements SchedulingAlgorithm {

    @Override
    public int compare(WrappedTask t1, WrappedTask t2) {
        int res = t1.getPriority() - t2.getPriority();
        if (res == 0) {
            res = t1.getId() - t2.getId();
        }
        return res;
    }
}
