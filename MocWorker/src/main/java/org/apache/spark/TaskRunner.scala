package org.apache.spark

import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{SparkAppConfig, StatusUpdate}
import org.apache.spark.scheduler.{DirectTaskResult, Task, TaskDescription}
import org.apache.spark.serializer.{JavaSerializationStream, JavaSerializer, SerializerInstance}
import org.apache.spark.util.{ByteBufferOutputStream, ChildFirstURLClassLoader}

import java.io.File
import java.net.URL
import java.nio.ByteBuffer
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

class TaskRunner(driver: RpcEndpointRef, conf: SparkConf, cfg: SparkAppConfig, taskDescription: TaskDescription) extends Runnable {
  override def run(): Unit = {
    val currentLoader = Thread.currentThread.getContextClassLoader
    val urls = getSparkClassLoader()
    Thread.currentThread.setContextClassLoader(new ChildFirstURLClassLoader(urls, currentLoader))

    // if executeFlag = true, task logic will be actually executed
    val executeFlag: Boolean = true

    val emptyResourceInformationMap = new HashMap[String, ResourceInformation]
    val valueBytes: ByteBuffer = {
      if (executeFlag) {
//        val ser: SerializerInstance = serializer.setDefaultClassLoader(Thread.currentThread.getContextClassLoader).newInstance()
        val ser: SerializerInstance = new JavaSerializer(conf).setDefaultClassLoader(Thread.currentThread.getContextClassLoader).newInstance()

        val task: Task[Any] = ser.deserialize[Task[Any]](taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
        val securityManager = new SecurityManager(conf, null, null)
        val ms: MetricsSystem = MetricsSystem.createMetricsSystem(MetricsSystemInstances.DRIVER, conf, securityManager)
        ms.start(true)

        val value: Any = task.run(
          taskAttemptId = taskDescription.taskId,
          attemptNumber = taskDescription.attemptNumber,
          metricsSystem = ms,
          resources = taskDescription.resources,
          plugins = None)
        serialize2ByteBuffer(value)
      } else {
        // Skip task running and directly return `Integer.MAX_VALUE/100`
        serialize2ByteBuffer(Option(Integer.MAX_VALUE / 100))
      }
    }

    val directResult = new DirectTaskResult(valueBytes, Seq(), Array())
    val resultBuffer: ByteBuffer = serialize2ByteBuffer(directResult)
    // Assume that result size is smaller than maxDirectResultSize, directly send back without block manager.
    val msg: StatusUpdate = StatusUpdate(taskDescription.executorId, taskDescription.taskId, TaskState.FINISHED, resultBuffer, emptyResourceInformationMap)
    println("Sending message: " + msg)
    driver.send(msg)
  }

  def serialize2ByteBuffer(source: Any): ByteBuffer = {
    val bos = new ByteBufferOutputStream
    val out = new JavaSerializationStream(bos, 100, true)
    out.writeObject(source)
    out.close()
    bos.toByteBuffer
  }

  def getSparkClassLoader(): Array[URL] = {
//    val jarDir = new File("/home/workflow/software/spark/spark-3.1.2-bin-hadoop3.2/jars")

    // add example jars to external classpath for deserializing Task
    val jarDir = new File("/home/workflow/software/spark/spark-3.1.2-bin-hadoop3.2/examples/jars")
    val jars = jarDir.listFiles()
    val userClassPath: ArrayBuffer[URL] = ArrayBuffer()
    jars.foreach(jar => userClassPath.append(new URL("file://" + jar.getAbsolutePath)))
    userClassPath.toArray
  }

}
