package fdu.daslab.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static fdu.daslab.constants.Constants.rescheduleStrategy;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/12 2:57 PM
 */
public class PropertyUtils {

    protected static final Logger logger = LoggerFactory.getLogger(PropertyUtils.class);

    private static final Properties properties = new Properties();

    public static synchronized String getValue(String propertyName, String... propertyFiles) {
        String strategy = properties.getProperty(propertyName, "none");
        if (strategy.equals("none")) {
            for (String fileName : propertyFiles) {
                try (InputStream fis = PropertyUtils.class.getResourceAsStream(fileName);) {
                    Properties subProperties = new Properties();
                    subProperties.load(fis);
                    subProperties.forEach((k, v) -> {
                        logger.debug("Get property {} -> {}", k, v);
                    });
                    properties.putAll(subProperties);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    System.exit(1);
                }
            }

            // Override from system properties
            System.getProperties().forEach((k, v) -> {
                final String key = String.valueOf(k);
                logger.debug("Overriding property from system property: {}", key);
                PropertyUtils.setValue(key, String.valueOf(v));
            });
        }
        return properties.getProperty(rescheduleStrategy);
    }

    public static void setValue(String key, String value) {
        properties.setProperty(key, value);
    }
}
