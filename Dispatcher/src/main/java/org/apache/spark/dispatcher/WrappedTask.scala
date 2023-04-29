package org.apache.spark.dispatcher

import org.apache.spark.java.dispatcher.wrapper.WrappedTaskDescription

/**
 * @author Aaron Wang
 * @date 2023/4/23 3:34 PM
 * @version 1.0
 */
class WrappedTask(
                   id: Int,
                   priority: Int,
                   wrappedTaskDescription: WrappedTaskDescription) {

  // TODO: replace getter method
  def getId(): Int = {
    id
  }

  def getPriority: Int = {
    priority
  }

  def getWrappedTaskDescription: WrappedTaskDescription = {
    wrappedTaskDescription
  }

}
