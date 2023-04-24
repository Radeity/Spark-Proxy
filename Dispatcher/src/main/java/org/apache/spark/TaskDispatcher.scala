package org.apache.spark

import fdu.daslab.dispatcher.DispatcherConstants
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.SparkAppConfig
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Aaron Wang
 * @date 4/23/23 2:37 PM
 * @version 1.0
 */
class TaskDispatcher(
                      driver: RpcEndpointRef,
                      conf: SparkConf,
                      cfg: SparkAppConfig,
                      taskPool: TaskPool) {

  protected val logger: Logger = LoggerFactory.getLogger(classOf[TaskDispatcher])

  val executorEnv = SparkEnv.createExecutorEnv(conf, DispatcherConstants.DEFAULT_EXECUTOR_ID, DispatcherConstants.bindAddress, DispatcherConstants.bindAddress, 1, cfg.ioEncryptionKey, false)

  executorEnv.blockManager.initialize(conf.getAppId)

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
      val tr = new TaskRunner(driver, conf, cfg, taskDescription)
      tr.run()
    }
  }

}
