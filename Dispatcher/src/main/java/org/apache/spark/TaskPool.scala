package org.apache.spark

import fdu.daslab.dispatcher.scheduler.{CustomizedSchedulingAlgorithm, FIFOSchedulingAlgorithm, SchedulingStrategy}
import org.apache.spark.scheduler.TaskDescription

import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Aaron Wang
 * @date 2023/4/23 15:32 PM
 * @version 1.0
 */
class TaskPool(schedulingStrategy: SchedulingStrategy) {

  val schedulingAlgorithm = {
    schedulingStrategy match {
      case SchedulingStrategy.FIFO =>
        new FIFOSchedulingAlgorithm
      case _ => new CustomizedSchedulingAlgorithm
    }
  }

  // TODO: ConcurrentLinkedQueue or PriorityBlockingQueue ?
  private val taskQueues: PriorityBlockingQueue[WrappedTask] = new PriorityBlockingQueue[WrappedTask](100, schedulingAlgorithm)
  //        taskQueues = new ConcurrentLinkedQueue<>();

  private val id: AtomicInteger = new AtomicInteger(0)

  def addTask(task: TaskDescription): Unit = {
    val wrappedTask = new WrappedTask(id.incrementAndGet, 0, task)
    taskQueues.add(wrappedTask)
  }

  def pollTask: WrappedTask = { //        List<WrappedTask> sortedTasks = taskQueues.stream().sorted(schedulingAlgorithm);
    taskQueues.poll
  }

  def getWaitingTaskSize: Int = taskQueues.size

}
