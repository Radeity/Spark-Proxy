package fdu.daslab.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.util.Enumeration;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/13 10:32 AM
 */
public class IpUtils {

    private static final String[] IPV4_SERVICES = {
            "https://ipv4.icanhazip.com/",
            "http://checkip.amazonaws.com/",
            "http://bot.whatismyipaddress.com/"
    };

    public static String fetchLANIp() {
        try {
            Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();

            while (nifs.hasMoreElements()) {
                NetworkInterface nif = nifs.nextElement();
                Enumeration<InetAddress> address = nif.getInetAddresses();
                while (address.hasMoreElements()) {
                    InetAddress addr = address.nextElement();
                    if (addr instanceof Inet4Address) {
                        if (nif.getName().equals("en0")) {
                            return addr.getHostAddress();
                        }
                    }
                }
            }
        } catch (
                Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String fetchPublicIp() {
        // Find public IP address
        String systemipaddress = "";
        int i = 0;
        while (i < 3) {
            try {
                URL url_name = new URL(IPV4_SERVICES[i]);
                BufferedReader sc = new BufferedReader(new InputStreamReader(url_name.openStream()));
                // reads system IPAddress
                systemipaddress = sc.readLine().trim();
                System.out.println(systemipaddress);
                if (!systemipaddress.equals("")) {
                    break;
                }
            } catch (Exception e) {
                i++;
            }
        }
        return systemipaddress;
    }


}
