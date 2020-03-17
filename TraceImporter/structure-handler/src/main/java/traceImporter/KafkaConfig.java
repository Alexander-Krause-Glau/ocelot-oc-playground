package traceImporter;

public class KafkaConfig {

  // Broker host
  public static final String BROKER = "localhost:9091";

  // Application ID
  public static final String APPLICATION_ID = "structure-reconstruction";

  // Topic to read from
  public static final String TOPIC_TRACES = "explorviz-traces";

  public static final String TOPIC_COMPONENTS = "explorviz-landscape-components";

  public static final String REGISTRY_URL = "http://localhost:8081";

}