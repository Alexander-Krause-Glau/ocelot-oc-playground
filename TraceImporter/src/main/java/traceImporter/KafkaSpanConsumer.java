package traceImporter;

import java.util.Properties;

/**
 * Reads the from the kafka topic "cluster-spans"
 */
public class KafkaSpanConsumer {

    public KafkaSpanConsumer() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9091");
        properties.put("group.id", "trace-importer-1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer",
            "org.apache.kafka.common.serialization.ByteArrayDeserializer");// NOCS
        properties.put("value.deserializer",
            "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }
}
