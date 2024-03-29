package org.apache.spark.executor

import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.StatusUpdate
import org.apache.spark.scheduler.{DirectTaskResult, Task, TaskDescription}
import org.apache.spark.serializer.{JavaSerializationStream, SerializerInstance}
import org.apache.spark.util.{ByteBufferOutputStream, MutableURLClassLoader}
import org.apache.spark.{SparkConf, SparkEnv, TaskState}
import org.apache.spark.worker.WorkerConstants.EXTERNAL_APPLICATION_JAR_DIR

import java.io.File
import java.net.URL
import java.nio.ByteBuffer
import java.util.Properties
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer


/**
 * @author Aaron Wang
 * @date 2022/12/12 8:16 PM
 * @version 1.0
 */
class TaskRunner(driver: RpcEndpointRef, conf: SparkConf, taskDescription: TaskDescription, executorId: String) extends Runnable {

  override def run(): Unit = {

    // if executeFlag = true, task logic will be actually executed
    val executeFlag: Boolean = true

    val emptyResourceInformationMap = new HashMap[String, ResourceInformation]
    val valueBytes: ByteBuffer = {
      if (executeFlag) {
        val currentLoader = Thread.currentThread.getContextClassLoader
        val urls = getSparkClassLoader()
        Thread.currentThread.setContextClassLoader(new MutableURLClassLoader(urls, currentLoader))

        //
        val ser: SerializerInstance = SparkEnv.get.closureSerializer.setDefaultClassLoader(Thread.currentThread.getContextClassLoader).newInstance()
        //        val ser: SerializerInstance = new JavaSerializer(conf).setDefaultClassLoader(Thread.currentThread.getContextClassLoader).newInstance()
        //      val ser: SerializerInstance = new KryoSerializer(conf).setDefaultClassLoader(Thread.currentThread.getContextClassLoader).newInstance()

        val task: Task[Any] = ser.deserialize[Task[Any]](taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
        val taskMemoryManager = new TaskMemoryManager(SparkEnv.get.memoryManager, taskDescription.taskId)
        task.setTaskMemoryManager(taskMemoryManager)
        task.localProperties = new Properties()
        task.localProperties.setProperty("spark.sql.execution.id", executorId)
        val value: Any = task.run(
          taskAttemptId = taskDescription.taskId,
          attemptNumber = taskDescription.attemptNumber,
          metricsSystem = SparkEnv.get.metricsSystem,
          resources = taskDescription.resources,
          plugins = None)

        val resSer = SparkEnv.get.serializer.newInstance()
        resSer.serialize(value)
      } else {
        // Skip task running and directly return `Integer.MAX_VALUE/100`
        serialize2ByteBuffer(Option(Integer.MAX_VALUE / 100))
      }
    }

    val directResult = new DirectTaskResult(valueBytes, Seq(), Array())
    val resultBuffer: ByteBuffer = serialize2ByteBuffer(directResult)
    // Assume that result size is smaller than maxDirectResultSize, directly send back without block manager.
    val msg: StatusUpdate = StatusUpdate(taskDescription.executorId, taskDescription.taskId, TaskState.FINISHED, resultBuffer, emptyResourceInformationMap)
    println("Sending message: " + msg + "to driver: " + driver.address)
    driver.send(msg)
//    dispatcher.send(msg)
  }

  def serialize2ByteBuffer(source: Any): ByteBuffer = {
    val bos = new ByteBufferOutputStream
    val out = new JavaSerializationStream(bos, 100, true)
    out.writeObject(source)
    out.close()
    bos.toByteBuffer
  }

  def getSparkClassLoader(): Array[URL] = {
    // add example jars to external classpath for deserializing Task
    val externalJarPath = System.getenv(EXTERNAL_APPLICATION_JAR_DIR)
    val jarDir = new File(externalJarPath)
    val jars = jarDir.listFiles()
    val userClassPath: ArrayBuffer[URL] = ArrayBuffer()
    jars.foreach(jar => userClassPath.append(new URL("file://" + jar.getAbsolutePath)))
    userClassPath.toArray
  }

}