package org.apache.spark.java.dispatcher.host;

import java.util.Collection;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 23/4/26 11:45 PM
 */
public abstract class AbstractSelector<T> {

    public abstract T select(Collection<T> workerSet);

}
