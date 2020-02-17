package traceImporter;

public class KafkaConfig {

    public static final String BROKER = "localhost:9091";
    public static final String APPLICATION_ID = "span-translating";


    public static final String IN_TOPIC = "cluster-dump-spans";
    public static final String OUT_TOPIC = "explorviz-spans";


}
