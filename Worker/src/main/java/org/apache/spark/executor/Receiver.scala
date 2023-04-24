package org.apache.spark.executor

import org.apache.spark.SparkEnv
import org.apache.spark.message.ExtraMessages.LaunchRemoteTask
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.SparkAppConfig
import org.apache.spark.worker.WorkerEndPoint
import org.apache.spark.worker.WorkerConstants.DEFAULT_EXECUTOR_ID
import org.apache.spark.worker.WorkerConstants.bindAddress

import java.nio.ByteBuffer

/**
 * @author Aaron Wang
 * @date 2023/4/24 9:43 PM
 * @version 1.0
 */
class Receiver(workerEndPoint: WorkerEndPoint, cfg: SparkAppConfig) {

  val executorEnv = SparkEnv.createExecutorEnv(workerEndPoint.conf, DEFAULT_EXECUTOR_ID, bindAddress, bindAddress, 1, cfg.ioEncryptionKey, false)

  executorEnv.blockManager.initialize(workerEndPoint.conf.getAppId)

  def receive: PartialFunction[Any, Unit] = {
    case LaunchRemoteTask(data) =>
      val newByteBuffer: ByteBuffer = data.value.duplicate
      val taskDescription: TaskDescription = TaskDescription.decode(newByteBuffer)
      println("External Worker receive task: " + taskDescription.taskId)
      val tr = new TaskRunner(workerEndPoint.dispatcher, workerEndPoint.conf, taskDescription)
      tr.run()
  }

}
