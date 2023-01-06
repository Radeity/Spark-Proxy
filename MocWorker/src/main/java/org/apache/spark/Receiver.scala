package org.apache.spark

import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.{DirectTaskResult, Task, TaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{LaunchTask, LaunchedExecutor, StatusUpdate}
import org.apache.spark.serializer.{JavaSerializationStream, JavaSerializer, SerializerInstance}
import org.apache.spark.util.{ByteBufferOutputStream, Utils}

import java.nio.ByteBuffer
import scala.collection.immutable.{HashMap, Map}

/**
 * @author Aaron Wang
 * @date 2022/12/12 7:40 PM
 * @version 1.0
 */
class Receiver(driver: RpcEndpointRef, conf: SparkConf) {
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

      // if executeFlag = true, task logic will be actually executed
      val executeFlag: Boolean = true

      val emptyResourceInformationMap = new HashMap[String, ResourceInformation]
      val valueBytes: ByteBuffer = {
        if (executeFlag) {
          val ser: SerializerInstance = new JavaSerializer(conf).newInstance()
          val task: Task[Any] = ser.deserialize[Task[Any]](
            taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
          val ms: MetricsSystem = MetricsSystem.createMetricsSystem(MetricsSystemInstances.DRIVER, conf)
          ms.start(true)
          val value: Any = task.run(
            taskAttemptId = taskDescription.taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = ms,
            resources = taskDescription.resources,
            plugins = None)
          serialize2ByteBuffer(value)
        } else {
          // Skip task running and directly return `Integer.MAX_VALUE/100`
          serialize2ByteBuffer(Option(Integer.MAX_VALUE / 100))
        }
      }

      val directResult = new DirectTaskResult(valueBytes, Seq(), Array())
      val resultBuffer: ByteBuffer = serialize2ByteBuffer(directResult)
      // Assume that result size is smaller than maxDirectResultSize, directly send back without block manager.
      val msg: StatusUpdate = StatusUpdate(taskDescription.executorId, taskDescription.taskId, TaskState.FINISHED, resultBuffer, emptyResourceInformationMap)
      println("Sending message: " + msg)
      driver.send(msg)


    case _ => println("No matching receiver!")
  }

  def serialize2ByteBuffer(source: Any): ByteBuffer = {
    val bos = new ByteBufferOutputStream
    val out = new JavaSerializationStream(bos, 100, true)
    out.writeObject(source)
    out.close()
    bos.toByteBuffer
  }

}
