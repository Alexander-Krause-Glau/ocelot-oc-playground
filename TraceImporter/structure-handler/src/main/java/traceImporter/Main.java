package traceImporter;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import java.io.IOException;

public class Main {

  public static void main(String[] args) throws IOException, InterruptedException {

    // TODO set reasonable value
    SchemaRegistryClient src = new CachedSchemaRegistryClient(KafkaConfig.REGISTRY_URL, 20);

    new StructureExtractor(src).run();
    // new SpanSingleProducerStream().run();
  }

}
