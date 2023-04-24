package fdu.daslab.aspect

import org.apache.spark.deploy.ApplicationDescription
import org.aspectj.lang.{JoinPoint, ProceedingJoinPoint}
import org.aspectj.lang.annotation.{AfterReturning, Around, Aspect}
import org.apache.spark.deploy.worker.ExecutorRunner
import org.apache.spark.internal.config.SPARK_EXECUTOR_PREFIX
import org.apache.spark.util.Utils

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


//  @Around("execution(* org.apache.spark.deploy.worker.ExecutorRunner.start(..)) && within(org.apache.spark.deploy..*) && !within(SparkClientAspect)")
//  def runExecutor (point: ProceedingJoinPoint): Object = {
//    val args: Array[AnyRef] = point.getArgs
//    val target: AnyRef = point.getTarget
//    println("(((((((((((((((((((((((((((((((((((((   Run Executor   ))))))))))))))))))))))))))))))))")
//    val appDesc: ApplicationDescription = target.asInstanceOf[ExecutorRunner].appDesc
//    // Launch the process
//    val arguments = appDesc.command.arguments ++ resourceFileOpt.map(f =>
//      Seq("--resourcesFile", f.getAbsolutePath)).getOrElse(Seq.empty)
//    val subsOpts = appDesc.command.javaOpts.map {
//      Utils.substituteAppNExecIds(_, appId, execId.toString)
//    }
//    point.proceed(point.getArgs)
//  }

}
