package org.apache.spark.java.dispatcher;

import fdu.daslab.utils.IpUtils;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2023/1/6 8:25 PM
 */
public final class DispatcherConstants {

    public static final String EXECUTOR = "spark.executor.id";

    // TODO: replace hard-code
    public static final String DEFAULT_EXECUTOR_ID = "16";

    public static String bindAddress = IpUtils.fetchLANIp();

}
