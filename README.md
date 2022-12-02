# Spark-Proxy
push-based calculation for spark job by proxy

## Core Code
`src/main/java/fdu/daslab/SparkClientAspect.java`

## Quick Start

**Package:**

```shell
mvn clean scala:compile compile
```

**Add Spark configuration to support aop:**

1. Download `aspectjweaver-1.9.7.jar` from mvn repository

2. Edit $SPARK_HOME/conf/spark_env.sh

  ```shell
  export SPARK_SUBMIT_OPTS="-javaagent:{DOWNLOAD_JAR_PATH}/aspectjweaver-1.9.7.jar"
  ```

3. Move aop jar to spark resource path:

  ```shell
  mv target/Aop2YarnClient-1.0-SNAPSHOT.jar $SPARK_HOME/jars
  ```


**Submit application:**

1. Standalone Mode

  ```shell
  spark-submit --class org.apache.spark.examples.SparkPi --master spark://analysis-5:7077 $SPARK_HOME/examples/jars/spark-examples_2.12-3.1.2.jar 10
  ```

2. Yarn Mode

  Support few days later...
