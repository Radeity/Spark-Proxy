package org.apache.spark

import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.{DirectTaskResult, TaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{LaunchTask, LaunchedExecutor, StatusUpdate}
import org.apache.spark.serializer.JavaSerializationStream
import org.apache.spark.util.ByteBufferOutputStream

import java.nio.ByteBuffer
import scala.collection.immutable.{HashMap, Map}

/**
 * @author Aaron Wang
 * @date 2022/12/12 7:40 PM
 * @version 1.0
 */
class Receiver(driver: RpcEndpointRef) {
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
      val task: TaskDescription = TaskDescription.decode(newByteBuffer)
      val emptyResourceInformationMap = new HashMap[String, ResourceInformation]
      // Skip task running and directly return `Integer.MAX_VALUE/100`
      val valueBytes: ByteBuffer = serialize2ByteBuffer(Option(Integer.MAX_VALUE / 100))
      val directResult = new DirectTaskResult(valueBytes, Seq(), Array())
      val resultBuffer: ByteBuffer = serialize2ByteBuffer(directResult)
      // Assume that result size is smaller than maxDirectResultSize, directly send back without block manager.
      val msg: StatusUpdate = StatusUpdate(task.executorId, task.taskId, TaskState.FINISHED, resultBuffer, emptyResourceInformationMap)
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
