import fdu.daslab.utils.IpUtils;
import org.junit.Assert;
import org.junit.Test;

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

}
