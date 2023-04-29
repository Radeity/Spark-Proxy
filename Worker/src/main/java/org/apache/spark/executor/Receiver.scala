package org.apache.spark.executor

import org.apache.spark.message.ExtraMessages.{LaunchRemoteTask, RetrieveApplicationContext}
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.util.SerializableBuffer
import org.apache.spark.worker.WorkerConstants.{DEFAULT_EXECUTOR_ID, bindAddress}
import org.apache.spark.worker.WorkerEndPoint
import org.apache.spark.{ExternalApplicationContext, SparkEnv}

import java.nio.ByteBuffer

/**
 * @author Aaron Wang
 * @date 2023/4/24 9:43 PM
 * @version 1.0
 */
class Receiver(workerEndPoint: WorkerEndPoint) {

  def receive: PartialFunction[Any, Unit] = {
    case LaunchRemoteTask(driverURL: String, data: SerializableBuffer) =>
      // Initialize Spark environment for different application (update driver|conf|RpcEnv)
      // TODO: Now, can not sense the application is done
      if (!workerEndPoint.runningApplication.contains(driverURL)) {
        val externalApplicationContext: ExternalApplicationContext = workerEndPoint.dispatcher.askSync[ExternalApplicationContext](RetrieveApplicationContext(driverURL))

        var nTries: Int = 0
        while ( {
          workerEndPoint.driver == null && nTries < 3
        }) {
          try {
            workerEndPoint.driver = workerEndPoint.rpcEnv().setupEndpointRefByURI(driverURL)
          } catch {
            case e: Throwable =>
              if (nTries == 2) {
                println("Connect to Driver failed, have tried three times!")
                throw e
              }
          }
          nTries += 1
        }
        println("Successfully connect to Driver:" + workerEndPoint.driver.address)

        workerEndPoint.conf = externalApplicationContext.getConf
        val executorEnv = SparkEnv.createExecutorEnv(
          workerEndPoint.conf,
          DEFAULT_EXECUTOR_ID,
          bindAddress,
          bindAddress,
          1,
          externalApplicationContext.getCfg.ioEncryptionKey,
          false)
        executorEnv.blockManager.initialize(workerEndPoint.conf.getAppId)

        workerEndPoint.runningApplication.add(driverURL)
      }

      val newByteBuffer: ByteBuffer = data.value.duplicate
      val taskDescription: TaskDescription = TaskDescription.decode(newByteBuffer)
      println("External Worker receive task: " + taskDescription.taskId)
      val tr = new TaskRunner(workerEndPoint.driver, workerEndPoint.conf, taskDescription)
      tr.run()

  }

}
