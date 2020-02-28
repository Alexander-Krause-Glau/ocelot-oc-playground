package traceImporter;

public class KafkaConfig {

  // Broker host
  public static final String BROKER = "localhost:9091";

  // Application ID
  public static final String APPLICATION_ID = "span-translating";

  // Topic to read from
  public static final String IN_TOPIC = "cluster-dump-spans";

  // Target topic
  public static final String OUT_TOPIC = "explorviz-spans";

}
