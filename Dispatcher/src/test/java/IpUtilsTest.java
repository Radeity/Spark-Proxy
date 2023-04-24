import fdu.daslab.dispatcher.utils.IpUtils;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/13 12:34 AM
 */
public class IpUtilsTest {

    @Test
    public void testFetchPublicIp() {
        String ip = IpUtils.fetchPublicIp();
        Assert.assertNotNull("Fetch public IP fail", ip);
    }

    @Test
    public void testFetchLANIp() {
        String ip = IpUtils.fetchLANIp();
        Assert.assertNotNull("Fetch LAN IP fail!", ip);
    }

    @Test
    public void loadJars() throws MalformedURLException, ClassNotFoundException {
        File jarDir = new File("/home/workflow/software/spark/spark-3.1.2-bin-hadoop3.2/jars");
        File[] jars = jarDir.listFiles();
        ArrayList<URL> urls = new ArrayList<>();
        for (File jar : jars) {
            urls.add(new URL("file://" + jar.getAbsolutePath()));
        }
        URLClassLoader urlClassLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]));
        Class<?> aClass = urlClassLoader.loadClass("org.apache.spark.scheduler.ResultTask");
    }

}
