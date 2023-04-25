package org.apache.spark.dispatcher

import org.apache.spark.java.dispatcher.DispatcherEndpoint
import org.apache.spark.message.ExtraMessages.GetDriver
import org.apache.spark.rpc.RpcCallContext
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, RetrieveSparkAppConfig}

/**
 * @author Aaron Wang
 * @date 2023/4/24 9:56 PM
 * @version 1.0
 */
class ReplyReceiver(dispatcherEndpoint: DispatcherEndpoint) {

   def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls, attributes, resources, resourceProfileId) =>
      // record in executorMap (external executor only)
      dispatcherEndpoint.executorMap.put(executorId, executorRef)
      context.reply(true)

    case RetrieveSparkAppConfig(_) =>
      while (dispatcherEndpoint.syncConfDone == false) {
        Thread.sleep(1000)
      }
      // sync Spark driver conf
      context.reply(dispatcherEndpoint.cfg)

    case GetDriver() =>
       context.reply(dispatcherEndpoint.driverURL)

    case _ => println("No matching reply receiver!")

   }
}
