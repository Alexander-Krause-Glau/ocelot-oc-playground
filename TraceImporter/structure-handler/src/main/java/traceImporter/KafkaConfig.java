package traceImporter;

public class KafkaConfig {

  // Broker host
  public static final String BROKER = "localhost:9091";

  // Application ID
  public static final String APPLICATION_ID = "span-translating";

  // Topic to read from
  public static final String IN_TOPIC = "explorviz-traces";

  public static final String REGISTRY_URL = "http://localhost:8081";

}