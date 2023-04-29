package org.apache.spark.message

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.SerializableBuffer

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2023/4/24 22:42 PM
 */
object ExtraMessages {

  case class GetDriver()

  case class LaunchRemoteTask(driverURL: String, data: SerializableBuffer)

  case class RetrieveApplicationContext(driverURL: String)

  case class WrappedMessage(driverHost: String, content: Any)

}

