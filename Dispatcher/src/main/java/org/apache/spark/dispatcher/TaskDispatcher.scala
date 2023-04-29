package org.apache.spark.dispatcher

import fdu.daslab.constants.Constants.{COMMON_PROPERTIES_PATH, HOST_SELECTOR}
import fdu.daslab.utils.PropertyUtils
import org.apache.spark.SparkEnv
import org.apache.spark.java.dispatcher.DispatcherConstants.{DEFAULT_EXECUTOR_ID, bindAddress}
import org.apache.spark.java.dispatcher.DispatcherEndpoint
import org.apache.spark.java.dispatcher.host.RandomSelector
import org.apache.spark.java.dispatcher.wrapper.WrappedTaskDescription
import org.apache.spark.message.ExtraMessages.LaunchRemoteTask
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.util.SerializableBuffer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{HashMap, LinkedHashSet}

/**
 * @author Aaron Wang
 * @date 4/23/23 2:37 PM
 * @version 1.0
 */
class TaskDispatcher(
                      dispatcherEndpoint: DispatcherEndpoint,
                      taskPool: TaskPool,
                      bufferedHosts: HashMap[String, LinkedHashSet[RpcEndpointRef]] ) {

  protected val logger: Logger = LoggerFactory.getLogger(classOf[TaskDispatcher])

  val hostSelector = PropertyUtils.getValue(HOST_SELECTOR, COMMON_PROPERTIES_PATH) match {
    case "RANDOM" =>
      new RandomSelector()

    // TODO: add other host selectors
    case _ =>
      new RandomSelector()
  }

  def receiveTask(wrappedTaskDescription: WrappedTaskDescription): Unit = {
    taskPool.addTask(wrappedTaskDescription)
  }

  // TODO: Make sure it's thread-safe
  // dispatch task only when there are register Workers
  def dispatchTask(batchNum: Int): Unit = {
    val actualBatchNum: Int = math.min(taskPool.getWaitingTaskSize, batchNum)
    for (_ <- 0 until actualBatchNum) {
      logger.info("Dispatch single task")
//      // ================================================================================================
//      val driverAddress = taskPool.peekTask.getWrappedTaskDescription.getDriver.address.toString()
//      if (!bufferedHosts.contains(driverAddress)) {
//        val hosts = new util.ArrayList[RpcEndpointRef]()
//        val values = dispatcherEndpoint.executorMap.values()
//        values.forEach(worker => {
//          val workerDriver = dispatcherEndpoint.workerApplicationMap.getOrDefault(worker.address.toString(), driverAddress)
//          if (workerDriver == driverAddress) {
//            hosts.add(worker)
//          }
//        })
//      }
//
//      val targetWorker = hostSelector.select(hosts)
//      dispatcherEndpoint.workerApplicationMap.put(targetWorker.address.toString(), driverAddress)
//      bufferedHosts.get(driverAddress).get.add(targetWorker)
//      // ================================================================================================
      val targetWorker = hostSelector.select(dispatcherEndpoint.executorMap.values())
      // poll and run one task
      if (targetWorker != null) {
        val wrappedTaskDescription = taskPool.pollTask.getWrappedTaskDescription
        targetWorker.send(LaunchRemoteTask(wrappedTaskDescription.getDriverURL, new SerializableBuffer(TaskDescription.encode(wrappedTaskDescription.getTaskDescription))))
        logger.info("Send task to target worker {}", targetWorker.address)
        logger.info("Driver of this task: {}", wrappedTaskDescription.getDriverURL)
      } else {}
    }
  }

}
