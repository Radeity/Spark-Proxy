package org.apache.spark.dispatcher

import org.apache.spark.java.dispatcher.DispatcherEndpoint
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{LaunchTask, LaunchedExecutor, StatusUpdate}

import java.nio.ByteBuffer

/**
 * @author Aaron Wang
 * @date 2022/12/12 7:40 PM
 * @version 1.0
 */
class Receiver(dispatcherEndpoint: DispatcherEndpoint) {

  //  val securityManager = new SecurityManager(conf, null, null)
  //  val ms: MetricsSystem = MetricsSystem.createMetricsSystem(MetricsSystemInstances.DRIVER, conf, securityManager)
  //  ms.start(true)

  def main(args: Array[String]): Unit = {
    val isOdd: PartialFunction[Int, String] = {
      case x if x % 2 == 1 => x + " is odd"
    }
    println(isOdd.applyOrElse(100, { _: Int => "null" }))

    val executor: LaunchedExecutor = LaunchedExecutor("0")
    println(executor)
  }
  

  def receive: PartialFunction[Any, Unit] = {
    case LaunchTask(data) =>
      println("Receive task: " + data)
      val newByteBuffer: ByteBuffer = data.value.duplicate
      val taskDescription: TaskDescription = TaskDescription.decode(newByteBuffer)

      dispatcherEndpoint.taskDispatcher.receiveTask(taskDescription)

    case StatusUpdate(executorId, taskId, state, data, resources) =>
      dispatcherEndpoint.driver.send(StatusUpdate(executorId, taskId, state, data, resources))

    case _ => println("No matching receiver!")
  }

}