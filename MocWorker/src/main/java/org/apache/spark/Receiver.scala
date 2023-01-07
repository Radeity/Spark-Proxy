package org.apache.spark

import fdu.daslab.MocWorkerConstants.DEFAULT_EXECUTOR_ID
import fdu.daslab.Worker
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{LaunchTask, LaunchedExecutor, SparkAppConfig}

import java.nio.ByteBuffer

/**
 * @author Aaron Wang
 * @date 2022/12/12 7:40 PM
 * @version 1.0
 */
class Receiver(driver: RpcEndpointRef, conf: SparkConf, cfg: SparkAppConfig) {

  val executorEnv = SparkEnv.createExecutorEnv(conf, DEFAULT_EXECUTOR_ID, Worker.bindAddress, Worker.bindAddress, 1, cfg.ioEncryptionKey, false)

  executorEnv.blockManager.initialize(conf.getAppId)


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

      val tr = new TaskRunner(driver, conf, cfg, taskDescription)
      tr.run()

    case _ => println("No matching receiver!")
  }

}