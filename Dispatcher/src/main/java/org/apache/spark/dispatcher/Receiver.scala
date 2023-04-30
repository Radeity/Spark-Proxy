package org.apache.spark.dispatcher

import org.apache.spark.java.dispatcher.DispatcherEndpoint
import org.apache.spark.java.dispatcher.wrapper.WrappedTaskDescription
import org.apache.spark.message.ExtraMessages.WrappedMessage
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{LaunchTask, StatusUpdate}

import java.nio.ByteBuffer

/**
 * @author Aaron Wang
 * @date 2022/12/12 7:40 PM
 * @version 1.0
 */
class Receiver(dispatcherEndpoint: DispatcherEndpoint) {

  def receive: PartialFunction[Any, Unit] = {
    case WrappedMessage(driverURL: String, message: Any) =>
      if (!dispatcherEndpoint.applicationContextMap.containsKey(driverURL)) {
        dispatcherEndpoint.receiveNewApplication(driverURL)
      }

      val context = dispatcherEndpoint.applicationContextMap.get(driverURL)
      val driver: RpcEndpointRef = context.getDriver

      message match {
        case LaunchTask(data) =>
          println("Receive new task: " + data)
          // TODO: No need to serialize and deserialize taskDescription in Dispatcher
          val newByteBuffer: ByteBuffer = data.value.duplicate
          val taskDescription: TaskDescription = TaskDescription.decode(newByteBuffer)
          dispatcherEndpoint.taskDispatcher.receiveTask(new WrappedTaskDescription(taskDescription, driverURL))

        case StatusUpdate(executorId, taskId, state, data, resources) =>
          driver.send(StatusUpdate(executorId, taskId, state, data, resources))

        case _ => println("No matching receiver!")
      }

    case _ => println("No matching receiver!")
  }

}