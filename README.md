# Spark-Proxy
[![Field Badge](https://img.shields.io/badge/Distributed%20Computing-pink.svg)](mailto:wangweirao16@gmail.com)
[![Field Badge](https://img.shields.io/badge/Volunteer%20Computing-purple.svg)](mailto:wangweirao16@gmail.com)

Spark-Proxy supports push-based calculation for `Spark` job via aop. It can intercept launch task messages in Spark's driver, send them to external `Dispatcher` which have been registered in register center (simply use Redis now), `Dispatcher` maintains connection with external `Worker`, tasks will be finally executed on external `Worker`. It's just a demo now, and has much room for improvement.
### ALWAYS WORK IN PROGRESS : )
<br>

## Code Structure

**Agent**

`Agent/src/main/java/fdu/daslab/SparkClientAspect.java`

**Remote Dispatcher**

`Dispatcher/src/main/java/org/apache/spark/java/dispatcher/Dispatcher.java`

**Remote Worker**

`Worker/src/main/java/org/apache/spark/worker/Worker.java`
<br><br>

## Quick Start

> Please make sure that version of Spark dependency (Agent/pom.xml) and your Spark cluster must be consistent.

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
  mv Agent/target/Agent-1.0-SNAPSHOT.jar $SPARK_HOME/jars
  mv jedis-4.3.1.jar (Download from https://repo1.maven.org/maven2/redis/clients/jedis/4.3.1/jedis-4.3.1.jar) $SPARK_HOME/jars
  ```

5. Move `Agent/src/main/resources/common.properties` to Spark conf directory. 

<br>

**common.properties**
| Property Name | Default Value | Meaning |
|--|--|--|
|reschedule.dst.executor|external|The default value means that re-scheduler each tasks to external workers. If you don't want to re-schedule, set this value to internal|
|redis.host|(none)|Redis instance host, used to connect to redis(registry center).|
|redis.password |(none)|Redis instance password, used to connect to redis(registry center).|
<br>

**Submit application:**

1. Standalone Mode

  - Configure `reschedule.dst.executor` in `common.properties` which decides re-scheduler to internal or external executors. Move to `$SPARK_HOME/conf`

  - Replace `jarDir` in `TaskRunner` with example JAR path in external executor (will support auto-fetching later).

  - Launch Dispatcher

  - Launch Worker

  - Submit Spark application

  ```shell
  spark-submit --class org.apache.spark.examples.SparkPi --master spark://analysis-5:7077 $SPARK_HOME/examples/jars/spark-examples_2.12-3.1.2.jar 10
  ```

2. Yarn Mode
   
  - Move `common.properties` to `HADOOP_CONF_DIR`, due to lack of some configuration, will make sure how to avoid this step later. 

  - Support Remote Shuffle Service [(apache/incubator-celeborn)](https://github.com/apache/incubator-celeborn).
    - Follow celeborn doc to deploy celeborn cluster.
    - Move celeborn client jar to `$SPARK_HOME/jars`, also to external worker node.
    - Update `Dispatcher/pom.xml` and set your own celeborn client jar path.
    - Add celeborn-related spark configuration refer to celeborn doc. 

  - Other steps remain the same with standalone mode.
    
<br>

## Futurn Plan (unordered)

- [ ] Add launch script or Launcher, make it automatically.

- [ ] Try to figure out better scheduler strategy, which have to support task graph generation (research work).

- [ ] Worker selector.

- [ ] Support auto-fetching JAR files. 

- [ ] Validate correctness in shuffle task (security issue). 

- [ ] Synchronize re-dispatch info with Driver.

- [ ] External worker can be aware of and register to new driver automatically. 

- [ ] Support whole life cycle management of external executor (start, stop, listening).

- [ ] Support `IndirectTaskResult`.

- [ ] Support metrics report.

- [ ] Package external worker, and support dynamically edit properties such as application and rss jar path. 

    
