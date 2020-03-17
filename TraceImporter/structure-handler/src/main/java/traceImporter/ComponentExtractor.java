package traceImporter;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class ComponentExtractor extends Thread {


  private final Properties streamsConfig = new Properties();

  private Topology topology;

  private final SchemaRegistryClient registry;

  public ComponentExtractor(SchemaRegistryClient registry) {

    this.registry = registry;
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKER);
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfig.APPLICATION_ID);

    this.topology = buildTopology();
  }

  public Topology getTopology() {
    return topology;
  }

  public Topology buildTopology() {

    StreamsBuilder builder = new StreamsBuilder();

    // Stream 1

    KStream<String, Trace> traceStream =
            builder.stream(KafkaConfig.IN_TOPIC, Consumed.with(Serdes.String(), getAvroValueSerde()));

    KStream<String, LandscapeComponent> componentStream = traceStream.flatMapValues(Trace::getSpanList)
            .mapValues(s -> {
              LandscapeComponent.Builder b = LandscapeComponent.newBuilder();

              Node node = new Node("todo", s.getHostname());
              b.setNode(node);

              Application app = Application.newBuilder().setName(s.getAppName()).build();
              b.setApplication(app);

              /*
                By definition: getOperationName().split(".")
                  Last entry is method name,
                  next to last is class name,
                  remaining elements form the package name
               */
              String[] operationFqnSplit = s.getOperationName().split("\\.");

              if (operationFqnSplit.length < 2) {
                System.out.println(Arrays.toString(operationFqnSplit));
                System.out.println("Invalid operation name: " + s.getOperationName());

              } else {

                String pkgName =
                        String.join(".", Arrays.copyOf(operationFqnSplit, operationFqnSplit.length - 2));
                String className = operationFqnSplit[operationFqnSplit.length - 2];
                String methodName = operationFqnSplit[operationFqnSplit.length - 1];


                b.setPackage$(pkgName);
                b.setClass$(className);
                b.setMethod(methodName);

              }
              return b.build();
            });

    componentStream.peek((k, c) -> System.out.println(c));

    // TODO: Another Key?
    componentStream.to(KafkaConfig.OUT_TOPIC, Produced.with(Serdes.String(), getAvroValueSerde()));
    return builder.build();
  }


  public void run() {

    final KafkaStreams streams = new KafkaStreams(this.topology, streamsConfig);
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private <T extends SpecificRecord> SpecificAvroSerde<T> getAvroValueSerde() {
    final SpecificAvroSerde<T> valueSerde = new SpecificAvroSerde<>(registry);
    valueSerde.configure(
            Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"),
            false);
    return valueSerde;
  }

}
