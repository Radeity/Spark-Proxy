package org.apache.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.SparkAppConfig;

import java.io.Serializable;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 23/4/28 6:31 PM
 */
@Data
@AllArgsConstructor
public class ExternalApplicationContext implements Serializable {

    private SparkConf conf;

    private RpcEndpointRef driver;

    private SparkAppConfig cfg;

}
