package fdu.daslab;

import fdu.daslab.process.MessageRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.ServiceLoader;

/**
 * @author Aaron Wang
 * @date 2023/4/19 3:57 PM
 * @version 1.0
 */
public class SparkMessageHandler {

    protected static final Logger logger = LoggerFactory.getLogger(SparkMessageHandler.class);

    public static HashMap<Class, MessageRequestHandler> handlerMap = new HashMap<>();

    static {
        ServiceLoader.load(MessageRequestHandler.class)
                .forEach(messageRequestHandler -> {
                    HashSet<Class> messageClassSet = messageRequestHandler.getMessageClass();
                    messageClassSet.forEach(messageClass -> {
                        handlerMap.put(messageClass, messageRequestHandler);
                        logger.info("Add {} into handlerMap", messageClass.toString());
                    });
                });
    }

//    @Lazy
//    @Autowired
//    List<MessageRequestHandler> messageHandlerList;
//
//    @PostConstruct
//    public void init() {
//        for (MessageRequestHandler requestHandler : messageHandlerList) {
//            HashSet<Class> messageClassSet = requestHandler.getMessageClass();
//            messageClassSet.forEach(messageClass -> {
//                handlerMap.put(messageClass, requestHandler);
//                logger.info("Add {} into handlerMap", messageClass.toString());
//            });
//        }
//    }

}
