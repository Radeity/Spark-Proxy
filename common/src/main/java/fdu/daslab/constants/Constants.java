package fdu.daslab.constants;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/9 2:14 PM
 */
public class Constants {

    public static final String driverURLPrefix = "spark://CoarseGrainedScheduler@";

    public static final String driverURLKey = "driver-url";

    public static final String workerURLPrefix = "spark://Worker@";

    public static final String dispatcherURLPrefix = "spark://Dispatcher@";

    public static final String workerURLKey = "worker-";

    public static final String executorSystemName = "sparkExecutor";

    public static final String driverAddress = "10.176.24.55";

    public static final String executorEndpointRefKey = "executor-%s";

    public static final String COMMON_PROPERTIES_PATH = "/common.properties";

    /**
     * Properties Key
     */
    public static final String RESCHEDULE_STRATEGY = "reschedule.dst.executor";

    public static final String REDIS_HOST = "redis.host";

    public static final String REDIS_PASSWORD = "redis.password";

    public static final String HOST_SELECTOR = "host.selector";

    public static final String DISPATCHER_PORT = "dispatcher.port";

}
