package traceImporter;

import org.apache.kafka.streams.processor.TimestampExtractor;

public final class KafkaConfig {

    public static final String BROKER = "localhost:9091";

    public static final String APP_ID = "trace-reconstruction";

    public static final String REGISTRY_URL = "http://localhost:8081";

    public static final int COMMIT_INTERVAL_MS = 2 * 1000;

    public static final Class TIMESTAMP_EXTRACTOR = EVSpanTimestampKafkaExtractor.class;

    public static final String IN_TOPIC = "explorviz-spans";

    public static final String OUT_TOPIC = "explorviz-traces";

}
