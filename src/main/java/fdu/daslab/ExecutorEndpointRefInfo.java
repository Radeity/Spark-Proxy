package fdu.daslab;

import org.apache.spark.rpc.netty.NettyRpcEndpointRef;

import java.io.Serializable;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/1 6:13 PM
 */
class ExecutorEndpointRefInfo implements Serializable {
    NettyRpcEndpointRef executorEndpointRef;
    int execId;

    ExecutorEndpointRefInfo(NettyRpcEndpointRef executorEndpointRef, int execId) {
        this.executorEndpointRef = executorEndpointRef;
        this.execId = execId;
    }
}
