package org.apache.spark.java.dispatcher.host;

import org.apache.spark.rpc.RpcEndpointRef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 23/4/26 11:49 PM
 */
public class RandomSelector extends AbstractSelector<RpcEndpointRef> {

    @Override
    public RpcEndpointRef select(Collection<RpcEndpointRef> workerSet) {
        try {
            List<RpcEndpointRef> hosts = new ArrayList<>(workerSet);
            int idx = ThreadLocalRandom.current().nextInt(hosts.size());
            return hosts.get(idx);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }
}
