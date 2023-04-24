package fdu.daslab.process;

import org.apache.spark.rpc.netty.InboxMessage;
import org.apache.spark.rpc.netty.RequestMessage;

import java.util.HashSet;

/**
 * @author Aaron Wang
 * @date 2023/4/19 3:01 PM
 * @version 1.0
 */
public interface MessageRequestHandler {

    /**
     * handle logic of specific sending message
     *
     * @param message
     * @return
     */
    RequestMessage handle(final RequestMessage message);

    /**
     * handle logic of specific received message
     *
     * @param message
     * @return
     */
    // TODO: Find a complement way to handle `OnewayMessage` and `RpcMessage`
    // InboxMessage handle(final InboxMessage message);

    /**
     * get corresponding classes of given message
     *
     * @return  a set of classes
     */
    HashSet<Class> getMessageClass();

}
