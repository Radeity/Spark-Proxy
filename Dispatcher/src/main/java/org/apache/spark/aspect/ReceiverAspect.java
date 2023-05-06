package org.apache.spark.aspect;

import org.apache.spark.message.ExtraMessages.WrappedMessage;
import org.apache.spark.rpc.netty.OneWayMessage;
import org.apache.spark.rpc.netty.RpcMessage;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import static fdu.daslab.constants.Constants.driverURLPrefix;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 23/4/28 7:44 PM
 */
@Aspect
public class ReceiverAspect {

    @Around("execution(* org.apache.spark.rpc.netty.Inbox.post(..)) && within(org.apache.spark.rpc.netty..*) && !within(ReceiverAspect)")
    public Object receiveMessage(ProceedingJoinPoint point) throws Throwable {
        Object[] args = point.getArgs();
        if (args != null && args.length > 0 && args[0].getClass() == OneWayMessage.class) {
            OneWayMessage message = (OneWayMessage) (args[0]);
            if (message.content().getClass().equals(CoarseGrainedClusterMessages.LaunchTask.class)) {
                WrappedMessage wrappedMessage = new WrappedMessage(driverURLPrefix + message.senderAddress(), message.content());
                OneWayMessage newMessage = new OneWayMessage(message.senderAddress(), wrappedMessage);
                args[0] = newMessage;
            }
        }

        return point.proceed(args);
    }
}
