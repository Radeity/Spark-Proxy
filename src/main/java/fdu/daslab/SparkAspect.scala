package fdu.daslab

import org.aspectj.lang.JoinPoint
import org.aspectj.lang.annotation.{AfterReturning, Aspect}


/**
 * @author Aaron Wang
 * @date 2022/11/28 10:18 PM
 * @version 1.0
 */
@Aspect
class SparkAspect {

//  @AfterReturning(pointcut = "execution(* org.apache.spark.util.Utils.startServiceOnPort(..))",
//    returning = "startedService", argNames = "startedService")
//  def startService[T](startedService: (T, Int)) {
//    println("111111111111111111111111111111111111111111111111111111111111111111111111111111")
//    println(startedService._2)
//  }

  // Work
  @AfterReturning(pointcut = "execution(* org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend.executorAdded(..)) &&" +
    "within(org.apache.spark.scheduler.cluster..*) && args(*,workerId,hostPort,*,*)",
    argNames = "workerId,hostPort")
  def standaloneRegisterExecutor(workerId: String , hostPort: String): Unit = {
    println("!!!!!!!!!!! Register Executor {}, host-port: {} !!!!!!!!!", workerId, hostPort);
  }

}
