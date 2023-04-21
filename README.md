# Spark-Proxy
Spark-Proxy supports push-based calculation for `Spark` job via aop. It can intercept launch task messages in Spark's driver, re-dispatch them to external executors which have been registered in register center (simply use Redis now).
<br><br>

## Code Structure

**Aop**

`demo/src/main/java/fdu/daslab/SparkClientAspect.java`

**Mock Remote Worker**

`MocWorker/src/main/java/fdu/daslab/Worker.java`
<br><br>

## Quick Start

> Please make sure that version of Spark dependency (demo/pom.xml) and your Spark cluster must be consistent.

**Package:**

```shell
mvn clean scala:compile compile package
```
<br>

**Add Spark configuration to support aop:**

1. Download `aspectjweaver-1.9.7.jar` from mvn repository

2. Edit `$SPARK_HOME/conf/spark_env.sh` (Standalone Mode)

  ```shell
  export SPARK_SUBMIT_OPTS="-javaagent:{DOWNLOAD_JAR_PATH}/aspectjweaver-1.9.7.jar"
  ```

3. Edit `$SPARK_HOME/conf/spark_default.conf` (Yarn Mode)

   ```shell
   spark.executor.extraJavaOptions  "-javaagent:{DOWNLOAD_JAR_PATH}/aspectjweaver-1.9.7.jar"
   spark.driver.extraJavaOptions    "-javaagent:{DOWNLOAD_JAR_PATH}/aspectjweaver-1.9.7.jar"
   ```

4. Move aop jar to spark resource path:

  ```shell
  mv common/target/common-1.0-SNAPSHOT.jar $SPARK_HOME/jars
  mv demo/target/demo-1.0-SNAPSHOT.jar $SPARK_HOME/jars
  mv jedis-4.3.1.jar (Download from https://repo1.maven.org/maven2/redis/clients/jedis/4.3.1/jedis-4.3.1.jar) $SPARK_HOME/jars
  ```

5. Move `demo/src/main/resources/common.properties` to Spark conf directory. 

<br>

**Submit application:**

1. Standalone Mode

  - Configure `reschedule.dst.executor` in `common.properties` which decides re-scheduler to internal or external executors. Move to `$SPARK_HOME/conf`

  - Replace `jarDir` in `TaskRunner` with example JAR path in external executor (will support auto-fetching later).

  - Launch worker first

  - Submit Spark application

  ```shell
  spark-submit --class org.apache.spark.examples.SparkPi --master spark://analysis-5:7077 $SPARK_HOME/examples/jars/spark-examples_2.12-3.1.2.jar 10
  ```

2. Yarn Mode
   
  - Move `common.properties` to `HADOOP_CONF_DIR`, due to lack of some configuration, will make sure how to avoid this step later. 

  - Support Remote Shuffle Service [(apache/incubator-celeborn)](https://github.com/apache/incubator-celeborn).
    - Follow celeborn doc to deploy celeborn cluster.
    - Move celeborn client jar to `$SPARK_HOME/jars`, also to external worker node.
    - Update `MocWorker/pom.xml` and set your own celeborn client jar path.
    - Add celeborn-related spark configuration refer to celeborn doc. 

  - Other steps remain the same with standalone mode.
    
<br>

## Futurn Plan

1. Support auto-fetching JAR files. 

2. Validate correctness in shuffle task.

3. Synchronize re-dispatch info with Driver.

4. External worker can be aware of and register to new driver automatically. 

5. Support task graph generation.

6. Support whole life cycle management of external executor (start, stop, listening).

7. Support `IndirectTaskResult`.

8. Support metrics report.

9. Package external worker, and support dynamically edit properties such as application and rss jar path. 

    
