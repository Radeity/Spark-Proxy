package org.apache.spark.worker;

import fdu.daslab.utils.IpUtils;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2023/4/24 11:08 AM
 */
public class WorkerConstants {

    public static final String EXECUTOR = "spark.executor.id";

    // TODO: replace hard-code
    public static final String DEFAULT_EXECUTOR_ID = "26";

    public static String bindAddress = IpUtils.fetchLANIp();

}
