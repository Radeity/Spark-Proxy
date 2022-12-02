package fdu.daslab.utils;

import fdu.daslab.ExecutorEndpointRefInfo;
import org.apache.spark.rpc.netty.NettyRpcEndpointRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/2 10:11 PM
 */
public class RegisterUtils {

    public static HashMap<String, ExecutorEndpointRefInfo> executorDataMap = new HashMap<>();

    public static List<String> executorIndex = new ArrayList<>();

    public static ExecutorEndpointRefInfo getExecutorEndpointRef(String key, NettyRpcEndpointRef executorEndpointRef) {
        if (!executorDataMap.containsKey(key)) {
            executorIndex.add(key);
            executorDataMap.put(key, new ExecutorEndpointRefInfo(executorEndpointRef, executorIndex.size()));
        }
        return executorDataMap.get(key);
    }
}
