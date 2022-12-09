# Spark-Proxy
Push-based calculation for `Spark` job via aop.
<br><br>

## Code Structure

**Aop**

`demo/src/main/java/fdu/daslab/SparkClientAspect.java`

**Mock Remote Worker**

`MocWorker/src/main/java/fdu/daslab/Worker.java`
<br><br>

## Quick Start

**Package:**

```shell
mvn clean scala:compile compile
```
<br>

**Add Spark configuration to support aop:**

1. Download `aspectjweaver-1.9.7.jar` from mvn repository

2. Edit `$SPARK_HOME/conf/spark_env.sh`

  ```shell
  export SPARK_SUBMIT_OPTS="-javaagent:{DOWNLOAD_JAR_PATH}/aspectjweaver-1.9.7.jar"
  ```

3. Move aop jar to spark resource path:

  ```shell
  mv common/target/common-1.0-SNAPSHOT.jar $SPARK_HOME/jars
  mv demo/target/demo-1.0-SNAPSHOT.jar $SPARK_HOME/jars
  mv jedis-4.3.1.jar (Download from https://repo1.maven.org/maven2/redis/clients/jedis/4.3.1/jedis-4.3.1.jar) $SPARK_HOME/jars
  ```

<br>

**Submit application:**

1. Standalone Mode

  ```shell
  spark-submit --class org.apache.spark.examples.SparkPi --master spark://analysis-5:7077 $SPARK_HOME/examples/jars/spark-examples_2.12-3.1.2.jar 10
  ```

2. Yarn Mode

    Will support in few days 👨🏻‍💻...
    
<br>

## Futurn Plan

1. Support `yarn` mode.

2. Support re-dispatch to external executor. 
    
