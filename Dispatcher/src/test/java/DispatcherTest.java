import fdu.daslab.dispatcher.DispatcherEndpoint;
import fdu.daslab.registry.RedisRegistry;
import fdu.daslab.dispatcher.utils.IpUtils;
import org.apache.spark.Receiver;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static fdu.daslab.constants.Constants.driverURLKey;
import static fdu.daslab.constants.Constants.executorSystemName;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/12 9:00 PM
 */
public class DispatcherTest {
    ByteArrayOutputStream stdoutStream = new ByteArrayOutputStream();

    @Before
    public void beforeEveryTest(){
        System.setOut(new PrintStream(stdoutStream));
    }

    @Test
    @Ignore
    public void workerReceiverTest() throws InterruptedException {
        SparkConf executorConf = new SparkConf();
        String bindAddress = IpUtils.fetchLANIp();
        RpcEnv executorRpcEnv = RpcEnv.create(executorSystemName,
                bindAddress,
                bindAddress,
                16161,
                executorConf,
                new SecurityManager(executorConf, null, null),
                0,
                false);

        DispatcherEndpoint dispatcherEndpoint = new DispatcherEndpoint(executorRpcEnv, executorConf);

        dispatcherEndpoint.receiver = new Receiver(null);
        executorRpcEnv.setupEndpoint("Executor", dispatcherEndpoint);
        Thread.sleep(1000);
        Jedis redisClient = RedisRegistry.getRedisClientInstance();
        redisClient.set(driverURLKey, "ramsey");
        RpcEndpointRef rpcEndpointRef = executorRpcEnv.endpointRef(dispatcherEndpoint);
        rpcEndpointRef.send(new CoarseGrainedClusterMessages.LaunchedExecutor("1"));
        // Waiting for message send asynchronously
        Thread.sleep(2000);
        String stdoutContent = stdoutStream.toString();
        Assert.assertTrue("Receiver is ineffective!", stdoutContent.contains("No matching receiver!"));
    }

    class A {
        public int id;
        public A(int id) {
            this.id = id;
        }
    }

    class B extends A {
        public String no;

        public B(int id, String no) {
            super(id);
            this.no = no;
        }
    }

    @Test
    public void testCast() {
        B b = new B(1, "sss");
        A a = (A) b;
        System.out.println(a.id);

    }

}