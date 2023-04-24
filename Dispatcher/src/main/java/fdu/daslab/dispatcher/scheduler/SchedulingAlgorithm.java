package fdu.daslab.dispatcher.scheduler;

import org.apache.spark.WrappedTask;

import java.util.Comparator;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 4/23/23 5:45 AM
 */
public interface SchedulingAlgorithm extends Comparator<WrappedTask> {

}

