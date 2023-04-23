package org.apache.spark

import org.apache.spark.scheduler.TaskDescription

/**
 * @author Aaron Wang
 * @date 4/23/23 3:32 PM
 * @version 1.0
 */
class WrappedTask(
                   id: Int,
                   priority: Int,
                   taskDescription: TaskDescription) {

  // TODO: replace getter method
  def getId(): Int = {
    id
  }

  def getPriority: Int = {
    priority
  }

  def getTaskDescription: TaskDescription = {
    taskDescription
  }

}
