package traceImporter;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.IOException;

public class Main {

  public static void main(String[] args) throws IOException, InterruptedException {
    SchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient("http://localhost:8081", 10);
    new SpanTranslator(schemaRegistryClient).run();
  }
}
