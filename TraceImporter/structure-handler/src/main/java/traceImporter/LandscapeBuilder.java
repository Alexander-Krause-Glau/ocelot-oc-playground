package traceImporter;


import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import landscape.Application;
import landscape.Component;
import landscape.Landscape;
import landscape.Node;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.*;

public class LandscapeBuilder {


  private final Properties streamsConfig = new Properties();

  private Topology topology;

  private final SchemaRegistryClient registry;

  private Landscape landscape;

  public LandscapeBuilder(SchemaRegistryClient registry) {
    this.registry = registry;
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKER);
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "landscape-generator");

    this.topology = buildTopology();
    this.landscape = new Landscape();
    landscape.setNodes(new ArrayList<>());
  }

  public Topology getTopology() {
    return topology;
  }

  public Topology buildTopology() {

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, LandscapeComponent> components =
            builder.stream(KafkaConfig.TOPIC_COMPONENTS, Consumed.with(Serdes.String(), getAvroValueSerde()));

    components.foreach((k, c) -> {

      Optional<Node> existingNode = landscape.getNodes().stream()
              .filter(n -> n.getIpAddress().contentEquals(c.getNode().getIpAddress()))
              .filter(n -> n.getHostName().contentEquals(c.getNode().getHostName()))
              .findAny();

      Node currentNode;
      if (existingNode.isPresent()) {
        currentNode = existingNode.get();

      } else {
        currentNode = new Node(c.getNode().getIpAddress(), c.getNode().getHostName(), new ArrayList<>());
        landscape.getNodes().add(currentNode);
      }

      Optional<Application> existingApp = currentNode.getApplications().stream()
              .filter(a -> c.getApplication().getName().contentEquals(a.getName()))
              .filter(a -> c.getApplication().getLanguage().contentEquals(a.getLanguage()))
              .findAny();

      Application currentApplication;
      if (existingApp.isPresent()) {
        currentApplication = existingApp.get();
      } else {
        currentApplication =
                new Application(c.getApplication().getName(), c.getApplication().getLanguage(), new ArrayList<>());
        currentNode.getApplications().add(currentApplication);
      }

      Component lowest = mergePackage(currentApplication, c.getPackage$());
      if (lowest.getClasses().stream().noneMatch(cls -> cls.contentEquals(c.getClass$()))) {
        lowest.getClasses().add(c.getClass$());
      }

      System.out.println(landscape);

    });

    return builder.build();
  }


  private Component mergePackage(Application application, String packageFQN) {
    String[] packages = packageFQN.split("\\.");
    Optional<Component> possibleRoot = application.getComponents().stream()
            .filter(c -> c.getName().contentEquals(packages[0]))
            .findAny();

    Component root;
    int i = 1;
    if (possibleRoot.isPresent()) {
      // Traverse until nex package in hierarchy is not in landscape
      root = possibleRoot.get();
      while (i < packages.length) {
        final String nexPkgName = packages[i];
        possibleRoot = root.getChildren().stream().filter(r -> r.getName().contentEquals(nexPkgName)).findAny();
        if (possibleRoot.isPresent()) {
          root = possibleRoot.get();
          i++;
        } else {
          break;
        }
      }

    } else {
      root = new Component(new ArrayList<>(), new ArrayList<>(), packages[0]);
      application.getComponents().add(root);
    }

    for (;i < packages.length; i++) {
      Component newComp = new Component(new ArrayList<>(), new ArrayList<>(), packages[i]);
      root.getChildren().add(newComp);
      root = newComp;
    }
    return root;
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

