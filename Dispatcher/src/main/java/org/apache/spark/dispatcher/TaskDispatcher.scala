package org.apache.spark.dispatcher

import fdu.daslab.constants.Constants.{COMMON_PROPERTIES_PATH, HOST_SELECTOR}
import org.apache.spark.java.dispatcher.host.RandomSelector
import org.apache.spark.SparkEnv
import org.apache.spark.java.dispatcher.{DispatcherConstants, DispatcherEndpoint}
import org.apache.spark.message.ExtraMessages.LaunchRemoteTask
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.util.SerializableBuffer
import org.slf4j.{Logger, LoggerFactory}
import fdu.daslab.utils.PropertyUtils

/**
 * @author Aaron Wang
 * @date 4/23/23 2:37 PM
 * @version 1.0
 */
class TaskDispatcher(
                      dispatcherEndpoint: DispatcherEndpoint,
                      taskPool: TaskPool) {

  protected val logger: Logger = LoggerFactory.getLogger(classOf[TaskDispatcher])

  val executorEnv = SparkEnv.createExecutorEnv(dispatcherEndpoint.conf, DispatcherConstants.DEFAULT_EXECUTOR_ID, DispatcherConstants.bindAddress, DispatcherConstants.bindAddress, 1, dispatcherEndpoint.cfg.ioEncryptionKey, false)

  executorEnv.blockManager.initialize(dispatcherEndpoint.conf.getAppId)


  val hostSelector = PropertyUtils.getValue(HOST_SELECTOR, COMMON_PROPERTIES_PATH) match {
    case "RANDOM" =>
      new RandomSelector()

    // TODO: add other host selectors
    case _ =>
      new RandomSelector()
  }

  def receiveTask(taskDescription: TaskDescription): Unit = {
    taskPool.addTask(taskDescription)
  }

  // TODO: Make sure it's thread-safe
  // dispatch task entry point
  def dispatchTask(batchNum: Int): Unit = {
    val actualBatchNum: Int = math.min(taskPool.getWaitingTaskSize, batchNum)
    for (_ <- 0 until actualBatchNum) {
      logger.info("Dispatch single task")
      // poll and run one task
      val taskDescription = taskPool.pollTask.getTaskDescription
      val targetWorker = hostSelector.select(dispatcherEndpoint.executorMap.values())
      targetWorker.send(LaunchRemoteTask(new SerializableBuffer(TaskDescription.encode(taskDescription))))
    }
  }

}
