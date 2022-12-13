package fdu.daslab;

import org.apache.spark.rpc.netty.NettyRpcEndpointRef;

import java.io.Serializable;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/1 6:13 PM
 */
public class ExecutorEndpointRefInfo implements Serializable {
    NettyRpcEndpointRef executorEndpointRef;
    String execId;

    public ExecutorEndpointRefInfo(NettyRpcEndpointRef executorEndpointRef, String execId) {
        this.executorEndpointRef = executorEndpointRef;
        this.execId = execId;
    }

    @Override
    public String toString() {
        if (executorEndpointRef.client() == null) {
            return String.format("%s-%s", execId, executorEndpointRef.toString());
        }
        return String.format("%s-%s", execId, executorEndpointRef.client().getSocketAddress());
    }
}
