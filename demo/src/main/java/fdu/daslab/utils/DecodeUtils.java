package fdu.daslab.utils;

import org.apache.spark.resource.ResourceInformation;
import org.apache.spark.scheduler.TaskDescription;
import org.apache.spark.util.ByteBufferInputStream;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/2 1:21 PM
 */
public class DecodeUtils {

    private DecodeUtils() {
        throw new IllegalStateException("DecodeUtils class");
    }

    public static TaskDescription decode(ByteBuffer byteBuffer) throws IOException {
        DataInputStream dataIn = null;
        try {
            dataIn = new DataInputStream(new ByteBufferInputStream(byteBuffer));
            long taskId = dataIn.readLong();
            int attemptNumber = dataIn.readInt();
            String executorId = dataIn.readUTF();
            String name = dataIn.readUTF();
            int index = dataIn.readInt();
            int partitionId = dataIn.readInt();

            // Read files.
            HashMap<String, Long> taskFiles = deserializeStringLongMap(dataIn);

            // Read jars.
            HashMap<String, Long> taskJars = deserializeStringLongMap(dataIn);

            // Read archives.
            HashMap<String, Long> taskArchives = deserializeStringLongMap(dataIn);

            Properties properties = new Properties();
            int numProperties = dataIn.readInt();
            for (int i = 0; i < numProperties; i++) {
                String key = dataIn.readUTF();
                int valueLength = dataIn.readInt();
                byte[] valueBytes = new byte[valueLength];
                properties.setProperty(key, new String(valueBytes, StandardCharsets.UTF_8));
            }

            // Read resources.
            Map<String, ResourceInformation> resources = deserializeResources(dataIn);

            // Create a sub-buffer for the serialized task into its own buffer (to be deserialized later).
            ByteBuffer serializedTask = byteBuffer.slice();
            return new TaskDescription(taskId, attemptNumber, executorId, name, index, partitionId, (scala.collection.mutable.Map<String, Object>) taskFiles,
                    (scala.collection.mutable.Map<String, Object>)taskJars, (scala.collection.mutable.Map<String, Object>)taskArchives, properties,
                    (scala.collection.immutable.Map<String, ResourceInformation>) resources, serializedTask);
        } finally {
            dataIn.close();
        }

    }

    private static HashMap<String, Long> deserializeStringLongMap(DataInputStream dataIn) throws IOException {
        HashMap<String, Long> map = new HashMap<>();
        int mapSize = dataIn.readInt();
        int i = 0;
        while (i < mapSize) {
            map.put(dataIn.readUTF(), dataIn.readLong());
            i += 1;
        }
        return map;
    }

    private static Map<String, ResourceInformation> deserializeResources(DataInputStream dataIn) throws IOException {
        HashMap<String, ResourceInformation> map = new HashMap<>();
        int mapSize = dataIn.readInt();
        int i = 0;
        while (i < mapSize) {
            String resType = dataIn.readUTF();
            String name = dataIn.readUTF();
            int numIdentifier = dataIn.readInt();
            List<String> identifiers = new ArrayList<>(numIdentifier);
            int j = 0;
            while (j < numIdentifier) {
                identifiers.add(dataIn.readUTF());
                j += 1;
            }
            map.put(resType, new ResourceInformation(name, identifiers.toArray(new String[identifiers.size()])));
            i += 1;
        }
        return map;
    }
}
